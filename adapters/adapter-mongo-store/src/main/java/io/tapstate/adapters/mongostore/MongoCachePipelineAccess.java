package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** A fixed Pipeline state identity and, for mutations, the original actuation claim. */
final class MongoCachePipelineAccess {
    private static final String PIPELINE_SCOPE = "pipelineScope";
    private static final String CLAIM_SCOPE_UNREADABLE = "pipeline-claim-scope";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    final MongoDatabase database;
    final PipelineIntentScope scope;
    private final MongoClient client;
    private final WorkloadClaimFence original;
    private final Runnable requireActive;
    private final boolean unpreparedModels;
    private final WorkloadClaimFence originalNode;
    private final java.util.Optional<io.tapstate.spi.store.PipelineRetirementResult> authoringPredecessor;
    private final MongoConfigurationDomain.AuthoringModels authoringModels;
    final java.time.Duration rateRetention;
    final io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases;

    MongoCachePipelineAccess(MongoClient client, MongoDatabase database, PipelineIntentScope scope,
            WorkloadClaimFence original, Runnable requireActive) {
        this(client, database, scope, original, requireActive, MongoRateHistoryStore.DEFAULT_RETENTION);
    }

    MongoCachePipelineAccess(MongoClient client, MongoDatabase database, PipelineIntentScope scope,
            WorkloadClaimFence original, Runnable requireActive, java.time.Duration rateRetention) {
        this(client, database, scope, original, requireActive, rateRetention, null);
    }

    MongoCachePipelineAccess(MongoClient client, MongoDatabase database, PipelineIntentScope scope,
            WorkloadClaimFence original, Runnable requireActive, java.time.Duration rateRetention,
            io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases) {
        this(client, database, scope, original, requireActive, rateRetention, operatorDatabases, false, null, java.util.Optional.empty(),null);
    }
    private MongoCachePipelineAccess(MongoClient client, MongoDatabase database, PipelineIntentScope scope,
            WorkloadClaimFence original, Runnable requireActive, java.time.Duration rateRetention,
            io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases, boolean unpreparedModels, WorkloadClaimFence originalNode,
            java.util.Optional<io.tapstate.spi.store.PipelineRetirementResult> authoringPredecessor,MongoConfigurationDomain.AuthoringModels authoringModels) {
        this.unpreparedModels = unpreparedModels; this.originalNode = originalNode;
        this.authoringModels=authoringModels;
        this.authoringPredecessor = Objects.requireNonNull(authoringPredecessor);
        this.operatorDatabases = operatorDatabases;
        this.rateRetention = MongoCacheMetadataStructure.requireRetention(rateRetention);
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.original = original;
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (original != null && (original.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !scope.clusterId().equals(original.key().clusterId()) || !scope.pipelineId().equals(original.key().resourceId()))) throw fenced();
    }
    static MongoCachePipelineAccess unpreparedModels(MongoClient client, MongoDatabase database, PipelineIntentScope scope,
            WorkloadClaimFence node, Runnable active, java.time.Duration retention) {
        return new MongoCachePipelineAccess(client, database, scope, null, active, retention, null, true, node, java.util.Optional.empty(),null);
    }
    static MongoCachePipelineAccess unpreparedModels(MongoClient client, MongoDatabase database, MongoConfigurationDomain.AuthoringModels original,
            WorkloadClaimFence node, Runnable active, java.time.Duration retention) {
        return new MongoCachePipelineAccess(client, database, original.scope(), null,
                active, retention, null, true, node, original.predecessor(),original);
    }
    void requireModelPin() { if (unpreparedModels) throw fenced(); }

    <T> T transaction(Function<ClientSession, T> action) { return transaction(false, action); }

    <T> T observationTransaction(Function<ClientSession, T> action) { return transaction(true, action); }

    private <T> T transaction(boolean observation, Function<ClientSession, T> action) {
        return transaction(observation, TRANSACTION, action);
    }

    private <T> T transaction(boolean observation, TransactionOptions options, Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> {
                        requireActive.run();
                        new MongoCacheBusinessIdentity(database, new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()))
                                .requireAdmissionState(session);
                        if (observation) verifyStructures(); else verifyStateStructure();
                        T result = action.apply(session);
                        requireActive.run();
                        return result;
                    }, options);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        });
    }

    /**
     * A write under an already proved lease: one transaction holding the claim compare-and-set, the caller's
     * own conditions and the action, with no admission, structure, state or inventory decoding. The
     * compare-and-set is a real write to the claim document, so a takeover committing concurrently either
     * conflicts with this transaction or leaves it matching nothing. The pipeline state's runtime write fence
     * is not advanced: every ownership change it would catch also rewrites the claim document. Any failure,
     * including a refused claim, propagates unchanged; the caller decides the lease is lost.
     */
    <T> T leasedWrite(LeasedClock committed, Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    T written = session.withTransaction(() -> {
                        requireActive.run();
                        compareAndSetClaim(session, "fencedPipelineWrites");
                        T result = action.apply(session);
                        requireActive.run();
                        return result;
                    }, TRANSACTION);
                    committed.observe(session);
                    return written;
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        });
    }

    /**
     * A read under an already proved lease: no transaction and no preamble, read concern {@code majority}
     * (set by the caller on the collection), in a causally consistent session that starts no earlier than
     * {@code after}, the last write this store committed.
     */
    <T> T leasedRead(LeasedClock after, Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            requireActive.run();
            try (var session = client.startSession(com.mongodb.ClientSessionOptions.builder().causallyConsistent(true).build())) {
                after.advance(session);
                T result = action.apply(session);
                requireActive.run();
                return result;
            }
        });
    }

    /**
     * The claim liveness half of a lease proof, inside the proof's transaction: the original claim is live
     * and prepared under this scope. Answers how long its lease still runs on the store's own clock, in
     * milliseconds, so the caller can count it down on its monotonic clock without reading a deadline.
     */
    long proveLease(ClientSession session) {
        if (original == null) throw fenced();
        MongoCacheWorkloadClaimStore.requireCollection(database);
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var filter = WorkloadClaimDocuments.livePipeline(session, table, original)
                .append(PIPELINE_SCOPE, MongoPipelinePreparationFacts.scope(scope)).append("pipelinePreparation.phase", "PREPARED");
        var row = table.find(session, filter).first();
        if (row == null) throw claimFenced();
        new MongoWorkloadClaimStore(table).inStrictSession(session).decodeStored(row);
        return MongoSrsLease.remainingMillis(table, session, original);
    }

    /** The latest operation a store committed, so its next out-of-transaction read observes it. */
    static final class LeasedClock {
        private org.bson.BsonDocument clusterTime;
        private org.bson.BsonTimestamp operationTime;

        synchronized void observe(ClientSession session) {
            org.bson.BsonDocument cluster = session.getClusterTime();
            if (later(timestamp(cluster), timestamp(clusterTime))) clusterTime = cluster;
            org.bson.BsonTimestamp operation = session.getOperationTime();
            if (later(operation, operationTime)) operationTime = operation;
        }

        private static boolean later(org.bson.BsonTimestamp candidate, org.bson.BsonTimestamp known) {
            return candidate != null && (known == null || candidate.compareTo(known) > 0);
        }

        synchronized void advance(ClientSession session) {
            if (clusterTime != null) session.advanceClusterTime(clusterTime);
            if (operationTime != null) session.advanceOperationTime(operationTime);
        }

        private static org.bson.BsonTimestamp timestamp(org.bson.BsonDocument cluster) {
            return cluster == null ? null : cluster.getTimestamp("clusterTime");
        }
    }

    <T> T metadataTransaction(SystemCollections table, boolean mutation, Function<ClientSession, T> action) {
        return metadataTransaction(table, database, mutation, action);
    }

    /** One bounded publication batch keeps the original proof at the transaction's write boundaries. */
    <T> T publicationModelsTransaction(Function<ClientSession, T> action) {
        if (!unpreparedModels || authoringModels == null)
            return metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, true, action);
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                T result = session.withTransaction(() -> {
                    requireActive.run();
                    authoringModels.requirePublicationActive();
                    verifyStateStructure();
                    requireUnpreparedModels(session);
                    new MongoCacheMetadataStructure(database, rateRetention).verifyFacet(SystemCollections.CACHE_DERIVED_SCHEMAS);
                    if (originalNode == null) throw fenced();
                    var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
                    new MongoCacheBusinessIdentity(database, business).admitMetadataWrite(session);
                    MongoCachePipelinePreparations.proveNode(session, database, business, originalNode);
                    T value = action.apply(session);
                    requireActive.run();
                    return value;
                }, TRANSACTION);
                requireActive.run();
                return result;
            }
        });
    }
    /** Read-only control header access; never materializes Pipeline positions or operator bodies. */
    <T> T validationControlTransaction(SystemCollections table,Function<ClientSession,T> action){
        return validationControlTransaction(table,(session,state)->action.apply(session));
    }
    <T> T validationControlTransaction(SystemCollections table,java.util.function.BiFunction<ClientSession,Document,T> action){
        return transaction(session->{
            var state=MongoValidationMetadata.read(session,SystemCollections.CACHE_PIPELINE_STATE.on(database),new Document("_id",scope.pipelineId()),
                    new Document("scope",1).append("epoch",1).append("runtimeWriteFence",1).append("stateJson",1).append("touchMillis",1),64*1024);
            if(state==null)throw unreadable("checkpoint");requireScope(state);
            if(!(state.get("epoch") instanceof Long epoch)||epoch<0||epoch==Long.MAX_VALUE
                    ||!(state.get("runtimeWriteFence") instanceof Long revision)||revision<1||revision==Long.MAX_VALUE
                    ||!(state.get("stateJson") instanceof String)||!(state.get("touchMillis") instanceof Long))throw unreadable("checkpoint");
            MongoCacheWorkloadClaimStore.requireCollection(database);
            var key=new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId());
            var claim=MongoValidationMetadata.read(session,SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database),new Document("_id",MongoWorkloadClaimStore.id(key)),
                    MongoValidationMetadata.claimProjection("pipelineScope","pipelinePreparation"),64*1024);
            requirePreparedClaim(session,claim);new MongoCacheMetadataStructure(database,rateRetention).verifyFacet(table);return action.apply(session,state);
        });
    }

    <T> T metadataTransaction(SystemCollections table, MongoDatabase target, boolean mutation, Function<ClientSession, T> action) {
        if(authoringModels!=null)authoringModels.requireActive();
        T result = transaction(session -> {
            if (unpreparedModels) {
                if (table != SystemCollections.CACHE_DERIVED_SCHEMAS || !database.getName().equals(target.getName())) throw fenced();
                requireUnpreparedModels(session);
                new MongoCacheMetadataStructure(database, rateRetention).verifyFacet(table);
                if (mutation) {
                    if (originalNode == null) throw fenced();
                    var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
                    new MongoCacheBusinessIdentity(database, business).admitMetadataWrite(session);
                    MongoCachePipelinePreparations.proveNode(session, database, business, originalNode);
                }
                return action.apply(session);
            }
            Document current = state(session);
            if (!database.getName().equals(target.getName())) {
                String preparation = MongoCacheOperatorDatabases.require(session, target, operatorDatabases);
                MongoOperatorInventory.requireDatabase(MongoOperatorInventory.require(session, database, scope), target.getName(), preparation, scope);
            }
            new MongoCacheMetadataStructure(target, rateRetention).verifyFacet(table);
            if (mutation) { proveClaim(session); updateState(session, current, new Document(), false); }
            return action.apply(session);
        });
        if(authoringModels!=null)authoringModels.requireActive();
        requireActive.run(); return result;
    }
    private void requireUnpreparedModels(ClientSession session) {
        var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
        var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,
                new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        var state = SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session, new Document("_id", scope.pipelineId())).first();
        if (claim == null && state == null) {
            if(authoringModels!=null && authoringModels.recovery().filter(value -> value.targetScope().equals(scope)).isPresent())return;
            if (scope.stateGeneration() == 1 && authoringPredecessor.isEmpty()) return;
            var previous = authoringPredecessor.orElseThrow(MongoCachePipelineAccess::fenced);
            if (previous.scope().stateGeneration() + 1 != scope.stateGeneration()) throw fenced();
            if (previous.cache().isEmpty() || !MongoPipelineRetirementFacts.sameBusiness(scope, previous.cache().orElseThrow().scope())) return;
            throw fenced();
        }
        if (claim == null || state == null || !(claim.get("pipelineRetirement") instanceof Document retiredRow)) throw fenced();
        var retired = MongoPipelineRetirementFacts.cache(retiredRow);
        var expected = authoringPredecessor.flatMap(io.tapstate.spi.store.PipelineRetirementResult::cache).orElseThrow(MongoCachePipelineAccess::fenced);
        if (!expected.equals(retired)) throw fenced();
        if (!MongoPipelineRetirementFacts.sameBusiness(scope, retired.scope()) || !scope.pipelineId().equals(retired.scope().pipelineId())
                || scope.stateGeneration() <= retired.scope().stateGeneration()) throw fenced();
        new MongoCachePipelineRetirements(client, database,
                new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()), requireActive).verifyRetired(session, retired);
    }

    <T> T operatorPreparation(Function<ClientSession, T> action) {
        var local = TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
        return transaction(false, local, session -> {
            Document current = state(session); proveClaim(session); updateState(session, current, new Document(), false);
            return action.apply(session);
        });
    }

    <T> T recoveredOperatorPreparation(WorkloadClaimFence node,Function<ClientSession,T> action) {
        var local=TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
        return transaction(false,local,session -> {
            var business=new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation());
            new MongoCacheBusinessIdentity(database,business).admitMetadataWrite(session);
            MongoCachePipelinePreparations.proveNode(session,database,business,node);
            Document current=state(session);updateState(session,current,new Document(),false);
            return action.apply(session);
        });
    }

    MongoDatabase operatorDatabase(String name, io.tapstate.spi.store.OperatorPlacement.Resource resource) {
        if (database.getName().equals(name)) return database;
        if (operatorDatabases == null || resource != io.tapstate.spi.store.OperatorPlacement.Resource.NEST_STATE
                || !operatorDatabases.scope().equals(new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()))
                || !database.getName().equals(operatorDatabases.cacheDatabase())) throw fenced();
        try {
            MongoStorePort.requireOperatorStateDatabase(name, "admin");
            MongoStorePort.requireOperatorStateDatabase(name, operatorDatabases.configurationDatabase());
            MongoStorePort.requireOperatorStateDatabase(name, operatorDatabases.masterDatabase());
        } catch (TapstateException rejected) {
            throw new TapstateException(StoreError.INVALID_OPERATOR_STATE_DATABASE, Map.of(), null);
        }
        return client.getDatabase(name);
    }

    WorkloadClaimFence originalClaim() { return original; }
    void requireObservationActive() { requireActive.run(); }

    void requirePipeline(String pipelineId) { if (!scope.pipelineId().equals(pipelineId)) throw fenced(); }

    Document state(ClientSession session) {
        return state(session, false);
    }

    Document state(ClientSession session, boolean structureChecked) {
        Document state = stateRow(session);
        requirePreparedClaim(session, structureChecked);
        return state;
    }

    Document stateRow(ClientSession session) {
        Document state = SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session, new Document("_id", scope.pipelineId())).first();
        validateState(state);
        return state;
    }

    Document stateForWriterProgress(ClientSession session, boolean structureChecked) {
        Document state = SystemCollections.CACHE_PIPELINE_STATE.on(database)
                .find(session, new Document("_id", scope.pipelineId())).first();
        validateState(state);
        if (original == null) throw fenced();
        if (!structureChecked) MongoCacheWorkloadClaimStore.requireCollection(database);
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var filter = WorkloadClaimDocuments.live(original)
                .append("pipelineScope", MongoPipelinePreparationFacts.scope(scope)).append("pipelinePreparation.phase", "PREPARED");
        var row = table.find(session, filter).first();
        if (row == null && MongoWorkloadClaimStore.canUseNodeLease(original)) {
            var nodeFilter = WorkloadClaimDocuments.livePipeline(session, table, original);
            if (nodeFilter.containsKey(MongoWorkloadClaimStore.NODE_LEASE_GENERATION))
                row = table.find(session, nodeFilter.append("pipelineScope", MongoPipelinePreparationFacts.scope(scope))
                        .append("pipelinePreparation.phase", "PREPARED")).first();
        }
        if (row == null) {
            requirePreparedClaim(session, structureChecked);
            throw claimFenced();
        }
        requirePreparedClaim(session, row);
        return state;
    }

    void validateState(Document state) {
        if (state == null) throw unreadable("checkpoint");
        requireScope(state);
        if (!(state.get("epoch") instanceof Long epoch) || epoch < 0 || epoch == Long.MAX_VALUE
                || !(state.get("runtimeWriteFence") instanceof Long revision) || revision < 1 || revision == Long.MAX_VALUE
                || !(state.get("stateJson") instanceof String) || !(state.get("touchMillis") instanceof Long)) throw unreadable("checkpoint");
    }

    private void requirePreparedClaim(ClientSession session, boolean structureChecked) {
        if (!structureChecked) MongoCacheWorkloadClaimStore.requireCollection(database);
        var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        Document row = table.find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        requirePreparedClaim(session,row);
    }
    void requirePreparedClaim(ClientSession session,Document row){
        if (row == null) throw MongoCachePipelinePreparations.rebuild(scope);
        new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).inStrictSession(session).decodeStored(row);
        try {
            if (!(row.get(PIPELINE_SCOPE) instanceof Document stored)) throw unreadable(CLAIM_SCOPE_UNREADABLE);
            var decoded = MongoPipelinePreparationFacts.scope(stored);
            if (!scope.pipelineId().equals(decoded.pipelineId()) || !new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation())
                    .equals(new CacheBusinessScope(decoded.clusterId(), decoded.bindingVersion(), decoded.cacheIncarnation()))) throw unreadable(CLAIM_SCOPE_UNREADABLE);
            if (!scope.equals(decoded)) throw claimFenced();
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(CLAIM_SCOPE_UNREADABLE); }
        if (MongoCachePipelinePreparations.phase(row, scope) != io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED) throw claimFenced();
    }

    void requireScope(Document row) {
        if (!scope.pipelineId().equals(row.get("_id"))) throw unreadable("scope");
        Object stored = row.get("scope");
        if (!(stored instanceof Document value)) throw unreadable("scope");
        try {
            if (!(value.get("bindingVersion") instanceof Long binding) || !(value.get("stateGeneration") instanceof Long generation)) throw unreadable("scope");
            var decoded = new PipelineIntentScope(value.getString("clusterId"), binding, value.getString("cacheIncarnation"), value.getString("pipelineId"), generation);
            if (!scope.equals(decoded)) throw fenced();
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadable("scope"); }
    }

    /** A recovery discovery read may use only its original live execution, without advancing state. */
    void verifyOriginalClaim(ClientSession session) {
        verifyOriginalClaim(session, false);
    }

    void verifyOriginalClaim(ClientSession session, boolean structureChecked) {
        if (original == null) throw fenced();
        if (!structureChecked) MongoCacheWorkloadClaimStore.requireCollection(database);
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var filter = WorkloadClaimDocuments.livePipeline(session, table, original)
                .append("pipelineScope", MongoPipelinePreparationFacts.scope(scope)).append("pipelinePreparation.phase", "PREPARED");
        Document row = table.find(session, filter).first();
        if (row == null) throw claimFenced();
        new MongoWorkloadClaimStore(table).inStrictSession(session).decodeStored(row);
    }

    void proveClaim(ClientSession session) { proveClaim(session,"fencedPipelineWrites",false); }

    void proveConsumerClaim(ClientSession session) { proveConsumerClaim(session,false); }

    void proveConsumerClaim(ClientSession session, boolean structureChecked) {
        proveClaim(session,"fencedConsumerWrites",structureChecked);
    }

    private void proveClaim(ClientSession session,String counter,boolean structureChecked) {
        if (original == null) throw fenced();
        if (!structureChecked) MongoCacheWorkloadClaimStore.requireCollection(database);
        compareAndSetClaim(session, counter);
    }

    private void compareAndSetClaim(ClientSession session, String counter) {
        if (original == null) throw fenced();
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var update = new Document("$inc", new Document(counter, 1L));
        var physical = MongoPipelinePreparationFacts.scope(scope);
        var live = WorkloadClaimDocuments.live(original).append("pipelineScope", physical).append("pipelinePreparation.phase", "PREPARED");
        if (table.updateOne(session, live, update).getMatchedCount() == 1) return;
        var node = new MongoWorkloadClaimStore(table).inStrictSession(session).nodeBackedFilter(original);
        if (node.isEmpty() || table.updateOne(session, node.orElseThrow().append("pipelineScope", physical)
                .append("pipelinePreparation.phase", "PREPARED"), update).getMatchedCount() != 1) throw claimFenced();
    }

    /** This real write also serializes an observation with replacement of the current state scope. */
    void updateState(ClientSession session, Document current, Document values, boolean advanceEpoch) {
        long revision = current.getLong("runtimeWriteFence");
        if (revision == Long.MAX_VALUE - 1 || (advanceEpoch && current.getLong("epoch") == Long.MAX_VALUE - 1)) throw fenced();
        Document increments = new Document("runtimeWriteFence", 1L);
        if (advanceEpoch) increments.append("epoch", 1L);
        Document update = new Document("$inc", increments);
        if (!values.isEmpty()) update.append("$set", values);
        if (SystemCollections.CACHE_PIPELINE_STATE.on(database).updateOne(session,
                new Document("_id", scope.pipelineId()).append("scope", current.get("scope"))
                        .append("epoch", current.getLong("epoch")).append("runtimeWriteFence", revision), update).getMatchedCount() != 1)
            throw new IllegalStateException("Pipeline state changed within its transaction snapshot");
    }

    void verifyStructures() {
        verifyStateStructure(); verifyStructure(SystemCollections.CACHE_PIPELINE_OBSERVATION);
    }
    void verifyObservationStructure() { verifyStructure(SystemCollections.CACHE_PIPELINE_OBSERVATION); }

    void verifyStateStructure() { verifyStructure(SystemCollections.CACHE_PIPELINE_STATE); }
    void verifyStateStructure(MongoReadBudget budget) { verifyStructure(SystemCollections.CACHE_PIPELINE_STATE,budget); }

    private void verifyStructure(SystemCollections table) {
        verifyStructure(table,null);
    }

    private void verifyStructure(SystemCollections table,MongoReadBudget budget) {
        var found = (budget==null?database:budget.database(database)).listCollections().filter(new Document("name", table.collectionName())).first();
        if (found == null || !"collection".equals(found.get("type"))) throw unreadable("structure");
        var options = found.get("options", Document.class);
        if (options != null && (Boolean.TRUE.equals(options.get("capped")) || options.containsKey("timeseries"))) throw unreadable("structure");
        var indexes = table.on(budget==null?database:budget.database(database)).listIndexes().into(new ArrayList<>());
        if (indexes.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) throw unreadable("index");
        var id = indexes.stream().filter(index -> "_id_".equals(index.get("name"))).findFirst().orElse(null);
        if (id == null || !new Document("_id", 1).equals(id.get("key")) || Boolean.TRUE.equals(id.get("hidden"))
                || Boolean.TRUE.equals(id.get("sparse")) || id.containsKey("partialFilterExpression")) throw unreadable("index");
    }

    static TapstateException fenced() { return MongoCacheBusinessIdentity.businessFenced(); }
    static TapstateException claimFenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
    TapstateException unreadable(String field) { return new TapstateException(IoError.DOCUMENT_UNREADABLE,
            Map.of("id", scope.pipelineId(), "field", field), null); }
}
