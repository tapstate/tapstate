package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.ConnectorBindingStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelinePreparationInputs;
import io.tapstate.spi.store.PipelineStateMapping;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Latest configuration intent, state mapping contention and authoring version share one transaction. */
final class MongoPipelineIntentStore implements PipelineIntentStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final Set<String> HEADER = Set.of("_id", "formatVersion", "clusterId", "bindingVersion", "cacheIncarnation",
            "stateGeneration", "intentRevision", "deleted");
    private static final Set<String> BODY = Set.of("targetState", "revision", "purgeState", "assemblyRevision", "reassemble", "rebuiltAtStateEpoch");
    private static final String PREPARATION = "startPreparation";
    private static final String FAILURE = "startFailure";
    private static final String FAILURE_PARAMS = "startFailureParams";
    private static final String DOCUMENT_ID_PARAM = "id";
    private static final String DOCUMENT_FIELD_PARAM = "field";
    private static final Pattern DOCUMENT_FIELD = Pattern.compile("[\\p{L}\\p{Nd}._\\[\\]-]++");
    private final MongoClient client;
    private final MongoDatabase database;
    private final MongoCollection<Document> intents;
    private final MongoCollection<Document> mappings;
    private final MongoCollection<Document> pipelines;
    private final MongoCollection<Document> identity;
    private final MongoCollection<Document> versions;
    private final String clusterId;
    private final MongoStorageBindingStore bindings;
    private final Runnable requireActive;
    private final MongoRequiredNoteStore notes;

    MongoPipelineIntentStore(MongoClient client, MongoDatabase database, String clusterId, MongoStorageBindingStore bindings) {
        this(client, database, clusterId, bindings, null, () -> {});
    }
    MongoPipelineIntentStore(MongoClient client, MongoDatabase database, String clusterId,
            MongoStorageBindingStore bindings, MongoRequiredNoteStore notes) {
        this(client, database, clusterId, bindings, notes, () -> {});
    }
    private MongoPipelineIntentStore(MongoClient client, MongoDatabase database, String clusterId,
            MongoStorageBindingStore bindings, MongoRequiredNoteStore notes, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.database=Objects.requireNonNull(database,"database");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        if (!bindings.belongsTo(client, database.getName())) {
            throw new IllegalArgumentException("configuration intent and binding must share one original client and database");
        }
        this.intents = SystemCollections.PIPELINE_INTENTS.on(database);
        this.mappings = SystemCollections.OPERATOR_GENERATIONS.on(database);
        this.pipelines = SystemCollections.PIPELINES.on(database);
        this.identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database);
        this.versions = SystemCollections.CONFIGURATION_VERSION.on(database);
        this.requireActive = requireActive;
        this.notes = notes;
    }
    @Override public PipelineIntentStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoPipelineIntentStore(client, client.getDatabase(intents.getNamespace().getDatabaseName()), clusterId, bindings, notes,
                () -> { requireActive.run(); additional.run(); });
    }
    @Override public Optional<PipelineIntentSnapshot> load(String pipelineId) {
        requireId(pipelineId);
        return transaction(session -> loadInTransaction(session, pipelineId));
    }
    Optional<PipelineIntentSnapshot> loadInTransaction(ClientSession session, String pipelineId) {
        return loadInTransaction(session, pipelineId, null);
    }

    /** The optional cache identity must come from the host audit in this transaction callback. */
    Optional<PipelineIntentSnapshot> loadInTransaction(ClientSession session, String pipelineId,
            io.tapstate.spi.store.CachePreparation checkedCache) {
        if (checkedCache != null && !session.hasActiveTransaction())
            throw new IllegalStateException("checked cache identity requires the original transaction");
        return Optional.ofNullable(intents.find(session, new Document("_id", pipelineId)).first())
                .map(row -> decodeCurrent(session, row, checkedCache));
    }

    Optional<PipelineIntentSnapshot> loadRetainedInTransaction(ClientSession session, String pipelineId) {
        return Optional.ofNullable(intents.find(session, new Document("_id", pipelineId)).first()).map(this::decode);
    }
    void rebindStopped(ClientSession session, io.tapstate.spi.store.PipelineCacheRecovery recovery, String pipelineHash) {
        var target = recovery.targetScope();
        var previous = loadRetainedInTransaction(session, target.pipelineId());
        if (previous.filter(PipelineIntentSnapshot::deleted).isPresent() || pipelineHash == null || !pipelineHash.matches("[0-9a-f]{64}")) throw fenced();
        long revision = previous.map(PipelineIntentSnapshot::intentRevision).orElse(0L);
        if (revision >= Long.MAX_VALUE - 1) throw fenced();
        var stopped = header(target, revision + 1, false).append("targetState", PipelineState.STOPPED.name())
                .append("revision", pipelineHash).append("purgeState", false).append("assemblyRevision", null)
                .append("reassemble", false).append("rebuiltAtStateEpoch", null);
        if (previous.isEmpty()) intents.insertOne(session, stopped);
        else if (intents.replaceOne(session, new Document("_id", target.pipelineId()).append("intentRevision", revision), stopped).getMatchedCount() != 1) throw fenced();
    }
    void stopForSharedRecovery(ClientSession session,PipelineIntentScope scope,String pipelineHash) {
        var current=loadInTransaction(session,scope.pipelineId()).orElseThrow(MongoPipelineIntentStore::fenced);
        if(current.deleted() || !current.scope().equals(scope) || current.intentRevision()>=Long.MAX_VALUE-1
                || pipelineHash==null || !pipelineHash.matches("[0-9a-f]{64}"))throw fenced();
        var stopped=header(scope,current.intentRevision()+1,false).append("targetState",PipelineState.STOPPED.name())
                .append("revision",pipelineHash).append("purgeState",false).append("assemblyRevision",null)
                .append("reassemble",false).append("rebuiltAtStateEpoch",null);
        if(intents.replaceOne(session,new Document("_id",scope.pipelineId()).append("intentRevision",current.intentRevision()),stopped).getMatchedCount()!=1)throw fenced();
    }
    void requireSharedStopped(ClientSession session,PipelineIntentScope scope) {
        var current=loadInTransaction(session,scope.pipelineId()).orElseThrow(MongoPipelineIntentStore::fenced);
        if(current.deleted() || !current.scope().equals(scope) || current.desired().targetState()!=PipelineState.STOPPED || current.desired().purgeState())throw fenced();
    }
    @Override public Optional<PipelineIntentSnapshot> loadForActuation(String pipelineId) {
        return actuation(pipelineId, false).map(ActuationReading::intent);
    }

    @Override public Optional<ActuationReading> readForActuation(String pipelineId) {
        return actuation(pipelineId, true);
    }

    private record ActuationAttempt(ActuationReading reading, boolean needsTransaction) {
        private static final ActuationAttempt ABSENT = new ActuationAttempt(null, false);
        private static final ActuationAttempt NEEDS_TRANSACTION = new ActuationAttempt(null, true);
    }

    private Optional<ActuationReading> actuation(String pipelineId, boolean readVersion) {
        requireId(pipelineId);
        var result = readSnapshot(session -> bindings.supportsActuationBatch()
                ? batchActuation(session, pipelineId, readVersion)
                : unbatchedActuation(session, pipelineId, readVersion));
        if (result.needsTransaction()) {
            // Shared recovery reads the complete current actuation state again in its CONFIG transaction.
            result = transaction(session -> unbatchedActuation(session, pipelineId, readVersion));
        }
        return Optional.ofNullable(result.reading());
    }

    private ActuationAttempt unbatchedActuation(ClientSession session, String pipelineId, boolean readVersion) {
        var row = intents.find(session, new Document("_id", pipelineId)).first();
        if (row == null) return ActuationAttempt.ABSENT;
        var snapshot = decode(row);
        var facts = bindings.admittedFacts(session, readVersion);
        return validateActuation(session, row, snapshot, facts,
                () -> mappings.find(session, new Document("_id", pipelineId)).first(),
                () -> pipelines.find(session, new Document("_id", pipelineId)).first() != null);
    }

    private ActuationAttempt batchActuation(ClientSession session, String pipelineId, boolean readVersion) {
        var query = new java.util.ArrayList<Document>();
        query.add(MongoPipelineObservationRead.match(new Document("_id", pipelineId)));
        query.add(MongoPipelineObservationRead.boundedRow());
        query.add(MongoPipelineObservationRead.originalRow());
        query.add(MongoPipelineObservationRead.one(SystemCollections.OPERATOR_GENERATIONS, "mapping", pipelineId));
        query.add(MongoPipelineObservationRead.lookup(SystemCollections.PIPELINES, "pipeline", List.of(
                MongoPipelineObservationRead.match(new Document("_id", pipelineId)),
                new Document("$project", new Document("_id", 1)), new Document("$limit", 2))));
        query.add(MongoPipelineObservationRead.one(SystemCollections.CONFIG_CLUSTER_IDENTITY, "cluster", "cluster"));
        query.add(MongoPipelineObservationRead.one(SystemCollections.STORAGE_SETUP, "setup", "cluster"));
        query.add(MongoPipelineObservationRead.lookup(SystemCollections.STORAGE_BINDING, "binding", List.of(
                MongoPipelineObservationRead.match(new Document("_id", "cluster")),
                new Document("$project", new Document("cache", 0)), MongoPipelineObservationRead.boundedRow())));
        query.add(MongoPipelineObservationRead.lookup(SystemCollections.STORAGE_RECOVERY, "recovery",
                List.of(new Document("$limit", 2), MongoPipelineObservationRead.boundedRow())));
        if (readVersion) query.add(MongoPipelineObservationRead.one(SystemCollections.CONFIGURATION_VERSION, "configuration", ConfigurationVersion.ID));

        var row = intents.aggregate(session, query).first();
        if (row == null) return ActuationAttempt.ABSENT;
        var original = row.get("body", Document.class);
        if (!pipelineId.equals(original.get("_id"))) return unbatchedActuation(session, pipelineId, readVersion);
        var cluster = MongoPipelineObservationRead.single(row,"cluster");
        var setup = MongoPipelineObservationRead.single(row,"setup");
        var binding = MongoPipelineObservationRead.single(row,"binding");
        var recovery = MongoPipelineObservationRead.single(row,"recovery");
        var configuration = MongoPipelineObservationRead.single(row,"configuration");
        var mapping = MongoPipelineObservationRead.single(row, "mapping");
        var pipeline = MongoPipelineObservationRead.single(row, "pipeline");
        if (!exact(cluster, "cluster") || !exact(setup, "cluster") || !exact(binding, "cluster")
                || recovery == null || !exact(mapping, pipelineId) || !exact(pipeline, pipelineId)
                || readVersion && !exact(configuration, ConfigurationVersion.ID))
            return unbatchedActuation(session, pipelineId, readVersion);
        var snapshot = decode(original);
        var facts = bindings.admittedObservationFacts(cluster, setup, binding, configuration, recovery);
        return validateActuation(session, original, snapshot, facts, () -> mapping, () -> true);
    }

    private static boolean exact(Document row, String id) {
        return row != null && id.equals(row.get("_id"));
    }

    private ActuationAttempt validateActuation(ClientSession session, Document row, PipelineIntentSnapshot snapshot,
            MongoStorageBindingStore.AdmittedFacts facts, java.util.function.Supplier<Document> currentMapping,
            java.util.function.BooleanSupplier pipelineExists) {
        var admitted = facts.preparation();
        var scope = snapshot.scope();
        if (admitted.isEmpty() || !admitted.orElseThrow().businessScope().equals(new io.tapstate.spi.store.CacheBusinessScope(
                scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()))) throw unreadable();
        if (snapshot.deleted() || row.containsKey(FAILURE)) return ActuationAttempt.ABSENT;
        Document mapping = currentMapping.get();
        var identity = MongoPipelineStateMappings.decode(scope.pipelineId(), mapping);
        if (identity.phase() != PipelineStateMapping.Phase.ACTIVE) return ActuationAttempt.ABSENT;
        if (identity.stateGeneration() != scope.stateGeneration() || !pipelineExists.getAsBoolean()) throw fenced();
        var prepared = MongoPipelinePreparationFacts.decode(scope.pipelineId(), mapping, clusterId);
        if (prepared.phase() != io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE) return ActuationAttempt.ABSENT;
        if (!scope.equals(prepared.scope())) throw fenced();
        var shared = MongoPipelineSharedRecoveryFacts.read(mapping);
        var revision = facts.version() == null ? java.util.OptionalLong.empty()
                : java.util.OptionalLong.of(ConfigurationVersion.revision(facts.version()));
        var attempt = new ActuationAttempt(new ActuationReading(snapshot, revision), false);
        if (shared.isPresent()) {
            if (snapshot.desired().targetState() == PipelineState.STOPPED && !snapshot.desired().purgeState()) return attempt;
            if (!session.hasActiveTransaction()) return ActuationAttempt.NEEDS_TRANSACTION;
            if (!MongoPipelineSharedRecoveryFacts.ready(session, database, shared.orElseThrow())) return ActuationAttempt.ABSENT;
            if (snapshot.desired().targetState() != PipelineState.STOPPED && !snapshot.desired().purgeState()) return ActuationAttempt.ABSENT;
        }
        return attempt;
    }

    private <T> T readSnapshot(Function<ClientSession,T> read){
        return StoreIo.call(()->{
            try{
                requireActive.run();
                if(!MongoReadBudget.supportsSnapshotReads(client))return transaction(read);
                try(var session=client.startSession(com.mongodb.ClientSessionOptions.builder().snapshot(true).build())){
                    T result=read.apply(session);requireActive.run();return result;
                }
            }catch(RuntimeException failure){requireActive.run();throw failure;}
        });
    }

    @Override public Optional<DispatchSnapshot> dispatchSnapshot() {
        return Optional.of(transaction(session -> {
            var ids = new ArrayList<String>();
            var stopping = new java.util.HashSet<String>();
            for (var row : intents.find(session, new Document("deleted", new Document("$ne", true)))
                    .projection(new Document("_id", 1).append("targetState", 1))) {
                if (!(row.get("_id") instanceof String id) || id.isBlank()) throw unreadable();
                ids.add(id);
                if (PipelineState.STOPPED.name().equals(row.get("targetState"))) stopping.add(id);
            }
            return new DispatchSnapshot(ids, stopping);
        }));
    }

    @Override public List<String> pipelineIds() {
        return transaction(session -> intents.find(session, new Document("deleted", new Document("$ne", true)))
                .projection(new Document("_id", 1)).map(row -> {
                    if (!(row.get("_id") instanceof String id) || id.isBlank()) throw unreadable();
                    return id;
                }).into(new ArrayList<>()));
    }
    @Override public List<ScanCandidate> scanCandidates() {
        return transaction(session -> intents.find(session, new Document("deleted", new Document("$ne", true)))
                .projection(new Document("_id", 1).append("intentRevision", 1).append("targetState", 1).append(FAILURE, 1)
                        .append("clusterId", 1).append("bindingVersion", 1).append("cacheIncarnation", 1)
                        .append("stateGeneration", 1))
                .map(row -> {
                    if (!(row.get("_id") instanceof String id) || id.isBlank()) throw unreadable();
                    long start = PipelineState.RUNNING.name().equals(row.get("targetState")) && !row.containsKey(FAILURE)
                            && row.get("intentRevision") instanceof Long revision && revision > 0 ? revision : 0;
                    try {
                        if (!(row.get("intentRevision") instanceof Long revision) || revision < 1 || revision == Long.MAX_VALUE
                                || !(row.get("bindingVersion") instanceof Long binding)
                                || !(row.get("stateGeneration") instanceof Long generation)) return new ScanCandidate(id, start);
                        var scope = new PipelineIntentScope(row.getString("clusterId"), binding,
                                row.getString("cacheIncarnation"), id, generation);
                        return new ScanCandidate(id, start, scope, revision, PipelineState.valueOf(row.getString("targetState")));
                    } catch (IllegalArgumentException | NullPointerException | ClassCastException invalidHint) {
                        // The full actuation read retains responsibility for diagnosing malformed rows.
                        return new ScanCandidate(id, start);
                    }
                }).into(new ArrayList<>()));
    }

    @Override public boolean compareAndSet(PipelineIntentScope scope, long expected, DesiredState desired) {
        Objects.requireNonNull(desired, "desired");
        if (!scope.pipelineId().equals(desired.pipelineId()) || desired.revision().isBlank()
                || (desired.rebuiltAtStateEpoch() != null && desired.rebuiltAtStateEpoch() < 0)) {
            throw new IllegalArgumentException("invalid Pipeline intent");
        }
        return change(scope, expected, desired);
    }
    @Override public boolean compareAndSetStart(PipelineIntentScope scope, long expected, DesiredState desired,
            PipelinePreparationInputs inputs) {
        Objects.requireNonNull(inputs);
        if (!scope.pipelineId().equals(desired.pipelineId()) || desired.targetState() != PipelineState.RUNNING
                || !desired.revision().equals(inputs.pipelineHash()) || desired.purgeState()
                || inputs.directChannels().stream().anyMatch(channel -> !scope.equals(channel.context().scope()))) throw fenced();
        return change(scope, expected, desired, null, inputs, true);
    }

    @Override public boolean compareAndSetExplicitStart(PipelineIntentScope scope, long expected, DesiredState desired) {
        if (desired.targetState() != PipelineState.RUNNING || desired.purgeState()) throw fenced();
        return change(scope, expected, desired, null, null, true);
    }

    static void addRetainedArtifactHashes(ClientSession session, MongoDatabase database, java.util.Set<String> retained) {
        var mappings = SystemCollections.OPERATOR_GENERATIONS.on(database);
        mappings.find(session, new Document("connectorBindings", new Document("$exists", true))).forEach(row -> {
            var binding = MongoPipelineStateMappings.connectorBindings(row).orElseThrow(MongoPipelineIntentStore::fenced);
            binding.effective().forEach(value -> retained.add(value.sha256()));
            if (binding.handoff() != null) {
                binding.handoff().previous().forEach(value -> retained.add(value.sha256()));
                binding.handoff().candidates().forEach(value -> retained.add(value.sha256()));
            }
        });
    }

    private record BindingRows(Document mapping, PipelineIntentSnapshot intent) {}

    /** The mapping, intent and CACHE incarnation belong to the same CONFIG snapshot. */
    private BindingRows bindingRows(ClientSession session, PipelineIntentScope scope, boolean readIntent) {
        if (MongoReadBudget.supportsSnapshotReads(client)) {
            var row = identity.aggregate(session, bindingReadStages(scope.pipelineId(), readIntent)).first();
            var mapping = MongoStartConfigRead.boundedRow(row, "mapping", scope.pipelineId());
            var intent = MongoStartConfigRead.boundedRow(row, "intent", scope.pipelineId());
            var cluster = row == null ? null : row.get("root", Document.class);
            var setup = MongoStartConfigRead.boundedRow(row, "setup", "cluster");
            var binding = MongoStartConfigRead.boundedRow(row, "binding", "cluster");
            var recovery = MongoStartConfigRead.boundedRow(row, "recovery", "cluster");
            if (cluster != null && "cluster".equals(cluster.get("_id")) && setup != null && binding != null
                    && recovery != null && mapping != null && (!readIntent || intent != null)) {
                var current = bindings.admittedObservationFacts(cluster, setup, binding, null, recovery).preparation();
                if (current.filter(value -> sameCache(value, scope)).isEmpty()) throw fenced();
                requireGeneration(scope, mapping);
                return new BindingRows(mapping, readIntent ? decode(intent) : null);
            }
        }
        var mapping = requireAdmittedScope(session, scope);
        return new BindingRows(mapping, readIntent ? loadRetainedInTransaction(session, scope.pipelineId()).orElse(null) : null);
    }

    private static List<Document> bindingReadStages(String pipelineId, boolean readIntent) {
        var stages = MongoStartConfigRead.root();
        stages.add(new Document("$project", new Document("root", MongoStartConfigRead.boundedValue())));
        MongoStorageBindingStore.appendAdmittedPreparation(stages);
        stages.add(MongoStartConfigRead.boundedLookup(SystemCollections.OPERATOR_GENERATIONS, "mapping", pipelineId));
        if (readIntent) stages.add(MongoStartConfigRead.boundedLookup(SystemCollections.PIPELINE_INTENTS, "intent", pipelineId));
        return stages;
    }

    @Override public Optional<ExecutionBindings> executionBindings(PipelineIntentScope scope) {
        return readSnapshot(session -> {
            var rows = bindingRows(session, scope, true);
            return currentConnectorBindings(rows.mapping(), scope).map(value -> {
                if (rows.intent() == null || !scope.equals(rows.intent().scope())) throw fenced();
                return new ExecutionBindings(value, rows.intent().intentRevision());
            });
        });
    }

    @Override public ConnectorBindingStore connectorBindings() {
        return new PipelineConnectorBindings();
    }

    private final class PipelineConnectorBindings implements ConnectorBindingStore {
        @Override public Optional<ConnectorRunBindings> load(PipelineIntentScope scope) {
            return readSnapshot(session -> currentConnectorBindings(bindingRows(session, scope, false).mapping(), scope));
        }
        @Override public Optional<ConnectorRunBindings> loadCurrent(String pipelineId) {
            requireId(pipelineId);
            return transaction(session -> {
                var intent = loadInTransaction(session, pipelineId);
                if (intent.isEmpty() || intent.orElseThrow().deleted()) return Optional.empty();
                var scope = intent.orElseThrow().scope();
                var mapping = readScopedMapping(session, scope, false);
                var state = MongoPipelineStateMappings.decode(pipelineId, mapping);
                if (state.stateGeneration() != scope.stateGeneration()) throw fenced();
                var current = currentConnectorBindings(mapping, scope);
                return switch (state.phase()) {
                    case ACTIVE -> current;
                    case REBUILDING -> {
                        MongoPipelineRebuildFacts.requireObservablePending(mapping, state, intent.orElseThrow());
                        yield Optional.empty();
                    }
                    case RETIRING -> {
                        var retirement = MongoPipelineRetirementFacts.read(mapping);
                        if (!retirement.phase().equals("RETIRING") || !retirement.request().scope().equals(scope)
                                || !retirement.request().operationId().equals(state.operationId())) throw fenced();
                        yield Optional.empty();
                    }
                    case RETIRED -> throw fenced();
                };
            });
        }
        @Override public Optional<ConnectorRunBindings> previousForRecovery(PipelineIntentScope target) {
            return transaction(session -> {
                var mapping = requireScope(session, target, false);
                var recovery = MongoCacheRecoveryFacts.read(mapping).orElseThrow(MongoPipelineIntentStore::fenced);
                if (!target.equals(recovery.targetScope())) throw fenced();
                return MongoPipelineStateMappings.connectorBindings(mapping)
                        .filter(value -> value.scope().equals(recovery.previousScope()));
            });
        }
        @Override public Map<String, ConnectorRunBindings> loadAll() {
            return transaction(session -> {
                var business = bindings.cachePreparationForTransaction(session).orElseThrow(MongoPipelineIntentStore::fenced).businessScope();
                if (!clusterId.equals(business.clusterId())) throw fenced();
                var result = new java.util.HashMap<String, ConnectorRunBindings>();
                for (var row : mappings.find(session, new Document("phase", "ACTIVE")
                        .append("connectorBindings", new Document("$exists", true)))) {
                    var value = MongoPipelineStateMappings.connectorBindings(row).orElseThrow(MongoPipelineIntentStore::fenced);
                    var scope = value.scope();
                    if (!row.getLong("stateGeneration").equals(scope.stateGeneration())) continue;
                    if (!business.clusterId().equals(scope.clusterId()) || business.bindingVersion() != scope.bindingVersion()
                            || !business.cacheIncarnation().equals(scope.cacheIncarnation())) throw fenced();
                    result.put(scope.pipelineId(), value);
                }
                return Map.copyOf(result);
            });
        }
        @Override public ConnectorRunBindings seed(PipelineIntentSnapshot original, List<ConnectorRunBindings.Binding> effective) {
            var fixed = List.copyOf(effective);
            return bindingChange(original, (session, held) -> {
                if (held.effectiveIntentRevision() != 0) {
                    if (!held.effective().equals(fixed)) throw fenced();
                    return held;
                }
                long revision = held.handoff() == null ? original.intentRevision() : held.handoff().intentRevision() - 1;
                if (revision < 1) throw fenced();
                return new ConnectorRunBindings(original.scope(), revision, fixed, held.handoff());
            });
        }
        @Override public ConnectorRunBindings prepare(PipelineIntentSnapshot original, List<ConnectorRunBindings.Binding> candidates) {
            var fixed = List.copyOf(candidates);
            return bindingChange(original, (session, held) -> {
                var pending = requirePending(held, original.intentRevision());
                if (pending.phase() != ConnectorRunBindings.Phase.REQUESTED) {
                    if (!pending.candidates().equals(fixed)) throw fenced();
                    return held;
                }
                return new ConnectorRunBindings(held.scope(), held.effectiveIntentRevision(), held.effective(),
                        new ConnectorRunBindings.Handoff(pending.intentRevision(), ConnectorRunBindings.Phase.PREPARED, fixed, List.of()));
            });
        }
        @Override public ConnectorRunBindings publish(PipelineIntentSnapshot original) {
            return bindingChange(original, (session, held) -> {
                var pending = requirePending(held, original.intentRevision());
                if (pending.phase() == ConnectorRunBindings.Phase.PUBLISHED) return held;
                if (pending.phase() != ConnectorRunBindings.Phase.PREPARED) throw fenced();
                for (var binding : pending.candidates()) {
                    if (notes == null) throw fenced();
                    notes.requireBinding(session, binding);
                }
                return new ConnectorRunBindings(held.scope(), pending.intentRevision(), pending.candidates(),
                        new ConnectorRunBindings.Handoff(pending.intentRevision(), ConnectorRunBindings.Phase.PUBLISHED,
                                pending.candidates(), held.effective()));
            });
        }
        @Override public void complete(PipelineIntentScope scope, long startIntentRevision) {
            finishBindings(scope, startIntentRevision, true);
        }
        @Override public void abort(PipelineIntentScope scope, long startIntentRevision) {
            finishBindings(scope, startIntentRevision, false);
        }
    }

    private static Optional<ConnectorRunBindings> currentConnectorBindings(Document mapping, PipelineIntentScope scope) {
        var retained = MongoPipelineStateMappings.connectorBindings(mapping);
        if (retained.isEmpty() || retained.orElseThrow().scope().equals(scope)) return retained;
        var recovery = MongoCacheRecoveryFacts.read(mapping).orElseThrow(MongoPipelineIntentStore::fenced);
        if (!scope.equals(recovery.targetScope()) || !retained.orElseThrow().scope().equals(recovery.previousScope())) throw fenced();
        return Optional.empty();
    }

    private ConnectorRunBindings bindingChange(PipelineIntentSnapshot original,
            java.util.function.BiFunction<ClientSession, ConnectorRunBindings, ConnectorRunBindings> change) {
        Objects.requireNonNull(original, "original");
        return transaction(session -> {
            if (original.deleted()) throw fenced();
            var rows = bindingRows(session, original.scope(), true);
            if (!original.equals(rows.intent())) throw fenced();
            var mapping = rows.mapping();
            var held = MongoPipelineStateMappings.connectorBindings(mapping)
                    .orElse(new ConnectorRunBindings(original.scope(), 0, List.of(), null));
            if (!held.scope().equals(original.scope())) throw fenced();
            var next = change.apply(session, held);
            if (!next.equals(held)) {
                ConfigurationVersion.advance(session, versions, client);
                MongoPipelineStateMappings.writeConnectorBindings(session, database, mapping, next);
            }
            return next;
        });
    }

    private void finishBindings(PipelineIntentScope scope, long startIntentRevision, boolean published) {
        transaction(session -> {
            var mapping = requireAdmittedScope(session, scope);
            var held = MongoPipelineStateMappings.connectorBindings(mapping).orElseThrow(MongoPipelineIntentStore::fenced);
            if (!held.scope().equals(scope)) throw fenced();
            if (held.handoff() == null) {
                if (published && held.effectiveIntentRevision() != startIntentRevision) throw fenced();
                return null;
            }
            var pending = requirePending(held, startIntentRevision);
            if ((pending.phase() == ConnectorRunBindings.Phase.PUBLISHED) != published) throw fenced();
            ConfigurationVersion.advance(session, versions, client);
            MongoPipelineStateMappings.writeConnectorBindings(session, database, mapping,
                    new ConnectorRunBindings(scope, held.effectiveIntentRevision(), held.effective(), null));
            return null;
        });
    }

    private static ConnectorRunBindings.Handoff requirePending(ConnectorRunBindings held, long revision) {
        var pending = held.handoff();
        if (pending == null || pending.intentRevision() != revision) throw fenced();
        return pending;
    }

    MongoConfigurationDomain.RetainedStart commitRetainedStart(PipelineIntentScope scope, long expected,
            DesiredState desired, PipelinePreparationInputs inputs, MongoConfigurationArtifactStore artifacts,
            Runnable verifyRetainedSources) {
        var fallback = MongoConfigurationDomain.RetainedStart.FALLBACK;
        if (!artifacts.belongsTo(client, database.getName()) || expected < 1 || expected >= Long.MAX_VALUE - 1
                || !scope.pipelineId().equals(desired.pipelineId()) || desired.targetState() != PipelineState.RUNNING
                || desired.purgeState() || !desired.revision().equals(inputs.pipelineHash())) return fallback;

        var ids = new java.util.LinkedHashSet<String>();
        ids.add(scope.pipelineId());
        ids.addAll(inputs.sourceHashes().keySet());
        if (ids.size() > 16) return fallback;
        var keys = List.copyOf(ids);
        String pipeline = scope.pipelineId();

        return transaction(session -> {
            var stages = MongoStartConfigRead.root();
            MongoStartConfigRead.addBinding(stages);
            MongoStartConfigRead.addArtifacts(stages, keys);
            stages.add(MongoValidationClaimObservation.lookup(SystemCollections.OPERATOR_GENERATIONS, "generation", pipeline, null));
            stages.add(MongoValidationClaimObservation.lookup(SystemCollections.PIPELINE_INTENTS, "intent", pipeline, null));
            stages.add(MongoValidationClaimObservation.lookup(SystemCollections.CONFIGURATION_VERSION, "version", ConfigurationVersion.ID, null));
            stages.add(MongoValidationClaimObservation.lookup(SystemCollections.SCHEMA_MIGRATIONS, "maintenance", "config", null));
            var snapshot = MongoStartConfigRead.snapshot(database, session, stages);

            var raw = MongoStartConfigRead.artifacts(snapshot, keys);
            var mapping = MongoStartConfigRead.row(snapshot, "generation", pipeline);
            var schema = MongoStartConfigRead.row(snapshot, "maintenance", "config");
            if (raw.isEmpty() || mapping.isEmpty()
                    || (schema.isEmpty() && !snapshot.getList("maintenance", Document.class).isEmpty())) return fallback;
            var generation = mapping.orElseThrow();
            if (generation.containsKey("cacheRecovery") || generation.containsKey("sharedRecovery")
                    || !(generation.get("cachePreparation") instanceof Document preparation)
                    || !io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE.name().equals(preparation.get("phase"))) return fallback;

            var artifact = raw.orElseThrow().stream().filter(row -> pipeline.equals(row.get("_id"))).findFirst();
            if (artifact.isEmpty()) return MongoConfigurationDomain.RetainedStart.VERSION_CONFLICT;
            var definition = artifacts.decode(artifact.orElseThrow());
            if (!(definition instanceof io.tapstate.core.model.PipelineResource)
                    || !desired.revision().equals(io.tapstate.core.model.canonical.CanonicalHash.of(definition)))
                return MongoConfigurationDomain.RetainedStart.VERSION_CONFLICT;

            var facts = MongoPipelinePreparationFacts.decode(pipeline, generation, clusterId);
            if (!scope.equals(facts.scope())) throw fenced();
            var business = new io.tapstate.spi.store.CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
            if (MongoStartConfigRead.binding(snapshot, bindings, business).isEmpty()) throw fenced();
            var state = MongoPipelineStateMappings.decode(pipeline, generation);
            if (state.phase() != PipelineStateMapping.Phase.ACTIVE || state.stateGeneration() != scope.stateGeneration()) throw fenced();
            var captured = new java.util.HashMap<String, io.tapstate.core.model.Resource>();
            captured.put(pipeline, definition);
            for (var row : raw.orElseThrow()) {
                if (!pipeline.equals(row.get("_id"))) {
                    var source = artifacts.decode(row);
                    captured.put(source.id(), source);
                }
            }
            MongoPipelineStatePreparations.verifiedInputs(scope, inputs, captured, captured::get);

            var current = MongoStartConfigRead.row(snapshot, "intent", pipeline);
            if (current.isEmpty()) {
                if (!snapshot.getList("intent", Document.class).isEmpty()) return fallback;
                throw fenced();
            }
            var held = decode(current.orElseThrow());
            if (!new io.tapstate.spi.store.CacheBusinessScope(held.scope().clusterId(), held.scope().bindingVersion(),
                    held.scope().cacheIncarnation()).equals(business)) throw unreadable();
            if (held.intentRevision() != expected || !held.scope().equals(scope)) throw fenced();

            var version = MongoStartConfigRead.row(snapshot, "version", ConfigurationVersion.ID);
            if (version.isEmpty()) return fallback;
            // Read the original CACHE generation before committing the guarded CONFIG intent.
            verifyRetainedSources.run();
            ConfigurationVersion.advanceStart(session, versions, database, version.orElseThrow(), schema.orElse(null), snapshot);
            writeIntent(session, scope, expected, desired, null, current.orElseThrow(), generation, true);
            return MongoConfigurationDomain.RetainedStart.SAVED;
        });
    }

    @Override public Optional<PendingStart> pendingStart(String pipelineId) {
        requireId(pipelineId);
        return readSnapshot(session -> {
            var row = intents.find(session, new Document("_id", pipelineId)
                    .append(PREPARATION, new Document("$exists", true))
                    .append(FAILURE, new Document("$exists", false))).first();
            if (row == null) return Optional.empty();
            var snapshot = decode(row);
            return Optional.of(new PendingStart(snapshot, MongoPipelineSourcePreparations.decode(row.get(PREPARATION), snapshot.scope())));
        });
    }

    static void requirePending(ClientSession session, MongoDatabase database, PendingStart pending) {
        var row = SystemCollections.PIPELINE_INTENTS.on(database).find(session, pendingFilter(pending)).first();
        if (row == null) throw fenced();
    }

    static Document pendingFilter(PendingStart pending) {
        var original = pending.intent();
        var scope = original.scope();
        return new Document("_id", scope.pipelineId()).append("intentRevision", original.intentRevision())
                .append("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                .append("cacheIncarnation", scope.cacheIncarnation()).append("stateGeneration", scope.stateGeneration())
                .append("targetState", PipelineState.RUNNING.name()).append("deleted", false)
                .append("revision", pending.inputs().pipelineHash()).append(PREPARATION, MongoPipelineSourcePreparations.encode(pending.inputs()))
                .append(FAILURE, new Document("$exists", false));
    }

    @Override public void failStart(PendingStart pending, IoError error) {
        failStart(pending, error, Map.of());
    }

    @Override public void failStart(PendingStart pending, IoError error, Map<String, Object> params) {
        var rejection = new Document(FAILURE, error.name());
        if (error == IoError.DOCUMENT_UNREADABLE && validDocumentParams(params)) {
            rejection.append(FAILURE_PARAMS, new Document(DOCUMENT_ID_PARAM, params.get(DOCUMENT_ID_PARAM))
                    .append(DOCUMENT_FIELD_PARAM, params.get(DOCUMENT_FIELD_PARAM)));
        }
        transaction(session -> {
            var generation = mappings.find(session, new Document("_id", pending.intent().scope().pipelineId())).first();
            var scope = pending.intent().scope();
            if (generation == null || !Long.valueOf(scope.stateGeneration()).equals(generation.get("stateGeneration"))
                    || !PipelineStateMapping.Phase.ACTIVE.name().equals(generation.get("phase"))
                    || MongoPipelinePreparationFacts.decode(scope.pipelineId(), generation, clusterId)
                            .phase() == io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE) return null;
            if (intents.updateOne(session, pendingFilter(pending), new Document("$set", rejection)).getMatchedCount() != 1)
                return null;
            // Serialize reporting this failure with issuance/completion on the same state generation.
            if (mappings.updateOne(session, new Document("_id", scope.pipelineId())
                    .append("noteWriteFence", generation.getLong("noteWriteFence")),
                    new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1) throw fenced();
            return null;
        });
    }

    static Optional<IoError> startFailure(Document row) {
        if (!row.containsKey(FAILURE)) return Optional.empty();
        try { return Optional.of(IoError.valueOf(row.getString(FAILURE))); }
        catch (IllegalArgumentException | ClassCastException | NullPointerException invalid) { throw unreadable(); }
    }

    static Map<String, String> startFailureParams(Document row, IoError code) {
        if (!row.containsKey(FAILURE_PARAMS)) return Map.of();
        if (code != IoError.DOCUMENT_UNREADABLE || !(row.get(FAILURE_PARAMS) instanceof Document params)
                || !validDocumentParams(params)) throw unreadable();
        return Map.of(DOCUMENT_ID_PARAM, params.getString(DOCUMENT_ID_PARAM),
                DOCUMENT_FIELD_PARAM, params.getString(DOCUMENT_FIELD_PARAM));
    }

    private static boolean validDocumentParams(Map<String, Object> params) {
        return params.keySet().equals(IoError.DOCUMENT_UNREADABLE.placeholders())
                && params.get(DOCUMENT_ID_PARAM) instanceof String id && boundedText(id) && !id.contains("://")
                && params.get(DOCUMENT_FIELD_PARAM) instanceof String field && boundedText(field) && DOCUMENT_FIELD.matcher(field).matches();
    }

    private static boolean boundedText(String text) {
        return !text.isBlank() && text.length() <= 256
                && text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 256
                && text.codePoints().noneMatch(cp -> Character.isISOControl(cp)
                        || Character.getType(cp) == Character.LINE_SEPARATOR
                        || Character.getType(cp) == Character.PARAGRAPH_SEPARATOR);
    }

    @Override public boolean remove(PipelineIntentScope scope, long expected) { return change(scope, expected, null); }

    @Override public boolean startRecovered(io.tapstate.spi.store.CacheRecoveryStore.FullPreparation confirmation,long expected,String pipelineHash) {
        if(!(confirmation instanceof MongoCacheRecoveryStore.Permission permission))throw fenced();
        var scope=permission.recovery().targetScope();
        var desired=new DesiredState(scope.pipelineId(),PipelineState.RUNNING,pipelineHash,false,null,false,null);
        return change(scope,expected,desired,permission);
    }

    private boolean change(PipelineIntentScope scope, long expected, DesiredState desired) {
        return change(scope,expected,desired,null);
    }
    private boolean change(PipelineIntentScope scope,long expected,DesiredState desired,MongoCacheRecoveryStore.Permission permission) {
        return change(scope, expected, desired, permission, null);
    }

    private boolean change(PipelineIntentScope scope, long expected, DesiredState desired,
            MongoCacheRecoveryStore.Permission permission, PipelinePreparationInputs pending) {
        return change(scope, expected, desired, permission, pending, false);
    }

    private boolean change(PipelineIntentScope scope, long expected, DesiredState desired,
            MongoCacheRecoveryStore.Permission permission, PipelinePreparationInputs pending, boolean explicitStart) {
        Objects.requireNonNull(scope, "scope");
        if (expected < 0 || expected >= Long.MAX_VALUE - 1) throw new IllegalArgumentException("invalid expected intent revision");
        return transaction(session -> {
            Document current = intents.find(session, new Document("_id", scope.pipelineId())).first();
            PipelineIntentSnapshot held = current == null ? null : pending == null ? decodeCurrent(session, current) : decode(current);
            long revision = held == null ? 0 : held.intentRevision();
            if (revision != expected) return false;
            // decodeCurrent already proved the exact cache coordinates in this callback.
            Document generation = pending == null ? requireScope(session, scope, held != null && held.scope().equals(scope))
                    : requireAdmittedScope(session, scope);
            MongoPipelineSharedRecoveryFacts.requireIntent(session,database,generation,scope,held,desired);
            var recovery=MongoCacheRecoveryFacts.read(generation);
            if(permission!=null) {
                permission.fence(session,client,database);
                if(!recovery.filter(permission.recovery()::equals).isPresent()
                        || MongoPipelinePreparationFacts.decode(scope.pipelineId(),generation,clusterId).phase()!=io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE)
                    throw fenced();
            } else if(recovery.isPresent() && desired!=null && desired.targetState()==PipelineState.RUNNING) {
                var preparation=MongoPipelinePreparationFacts.decode(scope.pipelineId(),generation,clusterId);
                var root=SystemCollections.STORAGE_RECOVERY.on(database).find(session,new Document("_id","cluster")).first();
                if(root==null)throw fenced();
                var recoveryPlan=MongoStorageRecoveryCommands.pending(root);
                if(preparation.phase()!=io.tapstate.spi.store.PipelinePreparation.Phase.COMPLETE
                        || recoveryPlan.filter(plan -> plan.phase()==io.tapstate.spi.store.StorageRecoveryCommands.Phase.RUNNING
                        && plan.manifest().affectedPipelineIds().contains(scope.pipelineId())).isPresent())
                    throw new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED,Map.of("pipeline",scope.pipelineId()),null);
            }
            if (held != null && !held.scope().equals(scope)) throw fenced();
            if (pending == null) {
                Document artifact = pipelines.find(session, new Document("_id", scope.pipelineId())).first();
                if (artifact == null) throw fenced();
                if (desired != null && !desired.revision().equals(artifact.get("contentHash"))) return false;
            }
            if (desired == null && current != null && Boolean.TRUE.equals(current.get("deleted"))) return true;
            ConfigurationVersion.advance(session, versions, client);
            writeIntent(session, scope, revision, desired, pending, current, generation, explicitStart);
            return true;
        });
    }

    private void writeIntent(ClientSession session, PipelineIntentScope scope, long revision, DesiredState desired,
            PipelinePreparationInputs pending, Document current, Document generation, boolean explicitStart) {
        Document replacement = header(scope, revision + 1, desired == null);
        if (desired != null) replacement.append("targetState", desired.targetState().name()).append("revision", desired.revision())
                .append("purgeState", desired.purgeState()).append("assemblyRevision", desired.assemblyRevision())
                .append("reassemble", desired.reassemble()).append("rebuiltAtStateEpoch", desired.rebuiltAtStateEpoch());
        if (pending != null) replacement.append(PREPARATION, MongoPipelineSourcePreparations.encode(pending));
        if (explicitStart) {
            var heldBindings = currentConnectorBindings(generation, scope)
                    .orElse(new ConnectorRunBindings(scope, 0, List.of(), null));
            var unfinished = heldBindings.handoff();
            if (!heldBindings.scope().equals(scope)) throw fenced();
            if (unfinished != null) {
                if (unfinished.phase() != ConnectorRunBindings.Phase.REQUESTED) throw fenced();
                // Deferred preparation can record a rejection before any connector handoff is prepared.
                if (unfinished.intentRevision() == revision && (current == null || startFailure(current).isEmpty()))
                    throw fenced();
            }
            var nextBindings = new ConnectorRunBindings(scope, heldBindings.effectiveIntentRevision(), heldBindings.effective(),
                    new ConnectorRunBindings.Handoff(revision + 1, ConnectorRunBindings.Phase.REQUESTED, List.of(), List.of()));
            MongoPipelineStateMappings.writeConnectorBindings(session, database, generation, nextBindings);
        } else if (mappings.updateOne(session, new Document("_id", scope.pipelineId()).append("stateGeneration", scope.stateGeneration())
                        .append("phase", PipelineStateMapping.Phase.ACTIVE.name()).append("noteWriteFence", generation.getLong("noteWriteFence")),
                new Document("$inc", new Document("noteWriteFence", 1L))).getMatchedCount() != 1) {
            throw new IllegalStateException("Pipeline state mapping changed within one transaction snapshot");
        }
        if (current == null) intents.insertOne(session, replacement);
        else if (intents.replaceOne(session, new Document("_id", scope.pipelineId()).append("intentRevision", revision), replacement,
                new ReplaceOptions().upsert(false)).getMatchedCount() != 1) {
            throw new IllegalStateException("Pipeline intent changed within one transaction snapshot");
        }
    }
    static void recreate(ClientSession session,MongoDatabase database,PipelineIntentScope old,PipelineIntentScope next) {
        var table=SystemCollections.PIPELINE_INTENTS.on(database);var row=table.find(session,new Document("_id",old.pipelineId())).first();if(row==null)return;
        if(!(row.get("intentRevision") instanceof Long revision) || revision<1 || revision>=Long.MAX_VALUE-1
                || !row.equals(header(old,revision,true)))throw unreadable();
        if(table.replaceOne(session,new Document("_id",old.pipelineId()).append("intentRevision",revision),header(next,revision+1,true)).getMatchedCount()!=1)throw fenced();
    }

    void retire(ClientSession session,PipelineIntentScope original) {
        var held=loadInTransaction(session,original.pipelineId());if(held.isEmpty())return;
        var prior=held.orElseThrow();if(!prior.scope().equals(original) || prior.intentRevision()>=Long.MAX_VALUE-1)throw fenced();
        if(prior.deleted())return;
        if(intents.replaceOne(session,new Document("_id",original.pipelineId()).append("intentRevision",prior.intentRevision()),
                header(original,prior.intentRevision()+1,true)).getMatchedCount()!=1)throw fenced();
    }

    /** Only the original controlled rebuild transaction may re-scope and consume an already carried-out command. */
    long completeRebuild(ClientSession session, PipelineIntentSnapshot original, PipelineIntentScope target) {
        var current = loadInTransaction(session, original.scope().pipelineId()).orElseThrow(MongoPipelineIntentStore::fenced);
        if (!current.equals(original) || original.deleted() || original.intentRevision() >= Long.MAX_VALUE - 1
                || !target.pipelineId().equals(original.scope().pipelineId()) || target.stateGeneration() != original.scope().stateGeneration() + 1
                || !matchesCurrentCache(session, target)) throw fenced();
        var desired = original.desired(); long next = original.intentRevision() + 1;
        var replacement = header(target, next, false).append("targetState", desired.targetState().name()).append("revision", desired.revision())
                .append("purgeState", false).append("assemblyRevision", desired.assemblyRevision()).append("reassemble", false).append("rebuiltAtStateEpoch", null);
        if (intents.replaceOne(session, new Document("_id", target.pipelineId()).append("intentRevision", original.intentRevision())
                        .append("stateGeneration", original.scope().stateGeneration()), replacement).getMatchedCount() != 1)
            throw new IllegalStateException("original rebuild intent changed within one CONFIG snapshot");
        return next;
    }

    private Document requireScope(ClientSession session, PipelineIntentScope scope, boolean cacheAlreadyMatched) {
        return requireGeneration(scope, readScopedMapping(session, scope, cacheAlreadyMatched));
    }

    private Document readScopedMapping(ClientSession session, PipelineIntentScope scope, boolean cacheAlreadyMatched) {
        Document stored = identity.find(session, new Document("_id", "cluster")).first();
        if (stored == null || !MongoConfigurationDomain.LAYOUT.equals(stored.get("layoutId"))
                || !(stored.get("bindingVersion") instanceof Long binding)) throw unreadable();
        if (!clusterId.equals(scope.clusterId()) || !clusterId.equals(stored.get("clusterId")) || binding != scope.bindingVersion()) throw fenced();
        if (!cacheAlreadyMatched && !matchesCurrentCache(session, scope)) throw fenced();
        return mappings.find(session, new Document("_id", scope.pipelineId())).first();
    }
    private Document requireAdmittedScope(ClientSession session, PipelineIntentScope scope) {
        var cache = bindings.admittedCachePreparation(session).orElseThrow(MongoPipelineIntentStore::fenced).businessScope();
        if (!clusterId.equals(scope.clusterId()) || !cache.clusterId().equals(scope.clusterId())
                || cache.bindingVersion() != scope.bindingVersion() || !cache.cacheIncarnation().equals(scope.cacheIncarnation())) throw fenced();
        Document row = mappings.find(session, new Document("_id", scope.pipelineId())).first();
        return requireGeneration(scope, row);
    }

    private static Document requireGeneration(PipelineIntentScope scope, Document row) {
        var state = MongoPipelineStateMappings.decode(scope.pipelineId(), row);
        if (state.phase() != PipelineStateMapping.Phase.ACTIVE || state.stateGeneration() != scope.stateGeneration()) throw fenced();
        return row;
    }

    private boolean matchesCurrentCache(ClientSession session, PipelineIntentScope expected) {
        return bindings.cachePreparationForTransaction(session).filter(value -> sameCache(value, expected)).isPresent();
    }
    private static boolean sameCache(io.tapstate.spi.store.CachePreparation preparation, PipelineIntentScope expected) {
        var current = preparation.businessScope();
        return current.clusterId().equals(expected.clusterId()) && current.bindingVersion() == expected.bindingVersion()
                && current.cacheIncarnation().equals(expected.cacheIncarnation());
    }
    private PipelineIntentSnapshot decodeCurrent(ClientSession session, Document row) {
        return decodeCurrent(session, row, null);
    }
    private PipelineIntentSnapshot decodeCurrent(ClientSession session, Document row, io.tapstate.spi.store.CachePreparation checkedCache) {
        var snapshot = decode(row);
        boolean matches = checkedCache == null ? matchesCurrentCache(session, snapshot.scope()) : sameCache(checkedCache, snapshot.scope());
        if (!matches) throw unreadable();
        return snapshot;
    }
    private static Document header(PipelineIntentScope scope, long revision, boolean deleted) {
        return new Document("_id", scope.pipelineId()).append("formatVersion", 1).append("clusterId", scope.clusterId())
                .append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation())
                .append("stateGeneration", scope.stateGeneration()).append("intentRevision", revision).append("deleted", deleted);
    }
    PipelineIntentSnapshot decode(Document row) {
        try {
            if (!(row.get("deleted") instanceof Boolean deleted) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                    || !(row.get("intentRevision") instanceof Long revision) || !(row.get("bindingVersion") instanceof Long binding)
                    || !(row.get("stateGeneration") instanceof Long generation) || !clusterId.equals(row.get("clusterId"))) throw unreadable();
            validateFields(row, deleted);
            var scope = new PipelineIntentScope(row.getString("clusterId"), binding, row.getString("cacheIncarnation"), row.getString("_id"), generation);
            DesiredState desired = deleted ? null : decodeDesired(row, scope);
            return new PipelineIntentSnapshot(scope, revision, desired);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private static void validateFields(Document row, boolean deleted) {
        var fields = new HashSet<>(HEADER);
        if (!deleted) fields.addAll(BODY);
        if (!deleted && row.containsKey(PREPARATION)) {
            fields.add(PREPARATION);
            if (!PipelineState.RUNNING.name().equals(row.get("targetState"))) throw unreadable();
            var code = startFailure(row);
            if (code.isPresent()) {
                fields.add(FAILURE);
                if (row.containsKey(FAILURE_PARAMS)) fields.add(FAILURE_PARAMS);
                startFailureParams(row, code.orElseThrow());
            }
        }
        if (!fields.equals(row.keySet())) throw unreadable();
    }

    private static DesiredState decodeDesired(Document row, PipelineIntentScope scope) {
        if (!(row.get("purgeState") instanceof Boolean purge) || !(row.get("reassemble") instanceof Boolean reassemble)
                || !(row.get("revision") instanceof String hash) || hash.isBlank()
                || (row.get("assemblyRevision") != null && !(row.get("assemblyRevision") instanceof String))
                || (row.get("rebuiltAtStateEpoch") != null
                        && (!(row.get("rebuiltAtStateEpoch") instanceof Long epoch) || epoch < 0))) throw unreadable();
        return new DesiredState(scope.pipelineId(), PipelineState.valueOf(row.getString("targetState")), hash, purge,
                row.getString("assemblyRevision"), reassemble, row.getLong("rebuiltAtStateEpoch"));
    }

    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> { requireActive.run(); T result = action.apply(session); requireActive.run(); return result; }, TRANSACTION);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        });
    }
    private static void requireId(String id) { if (id == null || id.isBlank()) throw new IllegalArgumentException("Pipeline id must not be blank"); }
    private static TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
    static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of(DOCUMENT_ID_PARAM, "pipeline-intent", DOCUMENT_FIELD_PARAM, "intent"), null); }
}
