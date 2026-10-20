package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.FirstPipelineCreation;
import io.tapstate.spi.store.PipelinePreparationInputs;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelinePreparationReceipt;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.spi.store.PipelineStatePreparations;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** One-time CONFIG issuance; its callback never opens a CACHE client or publishes a grant before commit. */
final class MongoPipelineStatePreparations implements PipelineStatePreparations {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoCollection<Document> mappings;
    private final MongoCollection<Document> pipelines;
    private final String clusterId;
    private final MongoConfigurationArtifactStore artifacts;
    private final MongoStorageBindingStore bindings;
    private final Runnable requireActive;

    MongoPipelineStatePreparations(MongoClient client, MongoDatabase database, String clusterId,
            MongoStorageBindingStore bindings, MongoConfigurationArtifactStore artifacts, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.artifacts = Objects.requireNonNull(artifacts,"artifacts");
        if (!artifacts.belongsTo(client,database.getName())) throw new IllegalArgumentException("preparation resources must share the original CONFIG client");
        this.bindings = Objects.requireNonNull(bindings, "bindings"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (!bindings.belongsTo(client, database.getName())) throw new IllegalArgumentException("preparation and binding must share one CONFIG client");
        this.mappings = SystemCollections.OPERATOR_GENERATIONS.on(database);
        this.pipelines = SystemCollections.PIPELINES.on(database);
    }

    @Override public PipelineStatePreparations guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoPipelineStatePreparations(client, client.getDatabase(mappings.getNamespace().getDatabaseName()),
                clusterId, bindings, artifacts, () -> { requireActive.run(); additional.run(); });
    }

    @Override public Optional<PipelinePreparation> read(String pipelineId) {
        requireId(pipelineId);
        return transaction(session -> {
            Document mapping = mappings.find(session, new Document("_id", pipelineId)).first();
            Document pipeline = pipelines.find(session, new Document("_id", pipelineId)).first();
            if (mapping == null && pipeline == null) return Optional.empty();
            var identity = MongoPipelineStateMappings.decode(pipelineId, mapping);
            if ((identity.phase() == PipelineStateMapping.Phase.RETIRED) != (pipeline == null)) {
                throw MongoPipelinePreparationFacts.unreadable(pipelineId);
            }
            return Optional.of(MongoPipelinePreparationFacts.decode(pipelineId, mapping, clusterId));
        });
    }

    @Override public FirstPipelineCreationResult issueFirstCreate(PipelineIntentScope scope, WorkloadClaimFence originalNode) {
        return issue(scope,originalNode,null);
    }
    @Override public FirstPipelineCreationResult issueFirstCreate(PipelineIntentScope scope, WorkloadClaimFence originalNode,PipelinePreparationInputs inputs) {
        return issue(scope,originalNode,Objects.requireNonNull(inputs,"inputs"));
    }
    private FirstPipelineCreationResult issue(PipelineIntentScope scope,WorkloadClaimFence originalNode,PipelinePreparationInputs requested) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(originalNode, "originalNode");
        if (originalNode.key().type() != WorkloadClaimType.NODE_SESSION || !clusterId.equals(originalNode.key().clusterId())
                || !originalNode.key().resourceId().equals(originalNode.owner().nodeId())) throw fenced();
        String preparationId = UUID.randomUUID().toString();
        Issuance result = transaction(session -> {
            Document row = requireCurrent(session, scope);
            PipelinePreparation previous = MongoPipelinePreparationFacts.decode(scope.pipelineId(), row, clusterId);
            if (previous.phase() != PipelinePreparation.Phase.UNASSIGNED) {
                if (!scope.equals(previous.scope())) throw fenced();
                return new Issuance(previous, false, null,Optional.empty());
            }
            var fixed = verifyInputs(session,scope,requested);
            var issued = new PipelinePreparation(scope.pipelineId(), scope.stateGeneration(), PipelinePreparation.Phase.ISSUED,
                    scope, preparationId, originalNode);
            Optional<io.tapstate.spi.store.PipelineRetirementResult> retired=Optional.empty();
            if(row.containsKey("retirement")) {
                var completedRetirement=MongoPipelineRetirementFacts.read(row).completed();
                if(completedRetirement.scope().stateGeneration()+1!=scope.stateGeneration())throw fenced();retired=Optional.of(completedRetirement);
            } else if(scope.stateGeneration()!=1)throw fenced();
            replaceAnnex(session, scope, row, issued);
            return new Issuance(issued, true, fixed,retired);
        });
        // The grant is constructed only after withTransaction has confirmed its commit.
        requireActive.run();
        return result.won() ? new FirstPipelineCreationResult.Issued(new Creation(result.preparation(), result.inputs(), result.retirement(),requireActive))
                : new FirstPipelineCreationResult.AlreadyIssued(result.preparation());
    }

    @Override public void validateSources(PipelineIntentScope scope,PipelinePreparationInputs inputs) {
        Objects.requireNonNull(inputs,"inputs");
        transaction(session -> { requireCurrent(session,scope); verifyInputs(session,scope,inputs); return null; });
    }
    Optional<io.tapstate.spi.store.PipelineRetirementResult> authoringPredecessor(PipelineIntentScope scope) {
        return transaction(session -> {
            Document row = requireCurrent(session, scope);
            if (MongoPipelinePreparationFacts.decode(scope.pipelineId(), row, clusterId).phase() != PipelinePreparation.Phase.UNASSIGNED) throw fenced();
            if (!row.containsKey("retirement")) {
                if (scope.stateGeneration() != 1) throw fenced();
                return Optional.empty();
            }
            var original = MongoPipelineRetirementFacts.read(row).completed();
            if (original.scope().stateGeneration() + 1 != scope.stateGeneration()
                    || !original.scope().pipelineId().equals(scope.pipelineId()) || !original.scope().clusterId().equals(scope.clusterId())
                    || original.scope().bindingVersion() != scope.bindingVersion()) throw fenced();
            return Optional.of(original);
        });
    }
    void requireRetiredPredecessor(PipelineIntentScope scope, PipelineIntentScope earlier) {
        transaction(session -> {
            var business=bindings.cachePreparationForTransaction(session).orElseThrow(MongoPipelineStatePreparations::fenced).businessScope();
            if (!MongoPipelineRetirementFacts.sameBusiness(scope,earlier) || !scope.pipelineId().equals(earlier.pipelineId())
                    || earlier.stateGeneration()>=scope.stateGeneration() || !business.clusterId().equals(scope.clusterId())
                    || business.bindingVersion()!=scope.bindingVersion() || !business.cacheIncarnation().equals(scope.cacheIncarnation())) throw fenced();
            var row=mappings.find(session,new Document("_id",scope.pipelineId())).first();
            var current=MongoPipelineStateMappings.decode(scope.pipelineId(),row);
            if (current.stateGeneration()!=scope.stateGeneration()
                    || current.phase()!=PipelineStateMapping.Phase.ACTIVE && current.phase()!=PipelineStateMapping.Phase.RETIRING) throw fenced();
            if (current.phase()==PipelineStateMapping.Phase.RETIRING) {
                var deleting=MongoPipelineRetirementFacts.read(row);
                if (!deleting.request().scope().equals(scope) || !deleting.request().operationId().equals(current.operationId())) throw fenced();
            }
            if (row.containsKey("stateRebuild")) {
                var prepared=MongoPipelinePreparationFacts.decode(scope.pipelineId(),row,clusterId);
                var annex=row.get("stateRebuild",Document.class);
                // Deletion changes the current operation while the completed rebuild publication remains fixed.
                var published=new Document(row).append("operationId",annex==null?null:annex.get("operationId"));
                MongoPipelineStateRebuilds.requireCompletedPrior(published,prepared);
                var previous=MongoPipelinePreparationFacts.scope(annex.get("originalScope",Document.class));
                if (!prepared.scope().equals(scope) || !MongoPipelineRetirementFacts.sameBusiness(scope,previous)
                        || !scope.pipelineId().equals(previous.pipelineId()) || earlier.stateGeneration()>previous.stateGeneration()) throw fenced();
                return null;
            }
            if (!row.containsKey("retirement")) throw fenced();
            var fact=MongoPipelineRetirementFacts.read(row);
            if (current.phase()==PipelineStateMapping.Phase.ACTIVE) {
                var previous=fact.completed();
                if (!MongoPipelineRetirementFacts.sameBusiness(scope,previous.scope()) || !scope.pipelineId().equals(previous.scope().pipelineId())
                        || previous.scope().stateGeneration()+1!=scope.stateGeneration() || earlier.stateGeneration()>previous.scope().stateGeneration()) throw fenced();
            } else if (current.phase()==PipelineStateMapping.Phase.RETIRING) {
                if (!fact.request().scope().equals(scope) || !fact.request().operationId().equals(current.operationId())) throw fenced();
                var prepared=MongoPipelinePreparationFacts.decode(scope.pipelineId(),row,clusterId);
                // A pending deletion retains the native allocation of this later generation.
                // Unprepared reuse additionally retains the exact preceding CACHE retirement.
                if (prepared.phase()==PipelinePreparation.Phase.UNASSIGNED) {
                    var previous=fact.predecessor().orElseThrow(MongoPipelineStatePreparations::fenced);
                    if (!MongoPipelineRetirementFacts.sameBusiness(scope,previous.scope()) || !scope.pipelineId().equals(previous.scope().pipelineId())
                            || earlier.stateGeneration()>previous.scope().stateGeneration()) throw fenced();
                } else if (prepared.phase()!=PipelinePreparation.Phase.COMPLETE || !scope.equals(prepared.scope())) throw fenced();
            } else throw fenced();
            return null;
        });
    }
    PipelinePreparationInputs verifyInputs(ClientSession session,PipelineIntentScope scope,PipelinePreparationInputs requested) {
        var resource = artifacts.getInTransaction(session,scope.pipelineId()).orElseThrow(MongoPipelineStatePreparations::fenced);
        if (!(resource instanceof PipelineResource pipeline)) throw fenced();
        var sources = new java.util.HashMap<String,SourceResource>();
        for (var id : pipeline.sourceIds()) {
            var source = artifacts.getInTransaction(session,id).orElseThrow(MongoPipelineStatePreparations::fenced);
            if (!(source instanceof SourceResource actual) || sources.put(id,actual) != null) throw fenced();
        }
        var selected = new java.util.HashMap<String,java.util.Set<String>>();
        if (requested == null) {
            var settings = pipeline.settings(); ReadMode mode = settings == null || settings.readMode() == null ? ReadMode.SNAPSHOT_AND_CDC : settings.readMode();
            if (mode != ReadMode.SNAPSHOT_ONLY && pipeline.sources().stream().anyMatch(ref -> ref instanceof SourceRef.Spec spec && !spec.srs()))
                throw MongoPipelineSourcePreparations.identityUnproven(scope);
            sources.keySet().forEach(id -> selected.put(id,java.util.Set.of()));
        } else selected.putAll(requested.selectedTables());
        for (var entry : selected.entrySet()) {
            var source = sources.get(entry.getKey()); if (source == null) throw fenced();
            if (source.tables() != null) for (String table : entry.getValue()) {
                boolean accepted;
                try {
                    accepted = source.tables().stream().anyMatch(selector -> switch (selector) {
                        case TableRef.Literal literal -> literal.name().equals(table);
                        case TableRef.Spec spec -> spec.name().equals(table);
                        case TableRef.Regex regex -> java.util.regex.Pattern.matches(regex.pattern(),table);
                    });
                } catch (IllegalArgumentException invalid) { throw fenced(); }
                if (!accepted) throw fenced();
            }
        }
        PipelinePreparationInputs actual;
        try { actual = PipelinePreparationInputs.of(scope,pipeline,sources,selected); }
        catch (IllegalArgumentException invalid) { throw fenced(); }
        if (requested != null && !actual.equals(requested)) throw fenced();
        return actual;
    }

    @Override public void requireIssued(PipelineIntentScope scope, String preparationId) {
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(preparationId, "preparationId");
        transaction(session -> {
            var current = MongoPipelinePreparationFacts.decode(scope.pipelineId(), requireCurrent(session, scope), clusterId);
            if (current.phase() != PipelinePreparation.Phase.ISSUED || !scope.equals(current.scope())
                    || !preparationId.equals(current.preparationId())) throw fenced();
            return null;
        });
    }

    @Override public PipelinePreparation complete(PipelinePreparationReceipt receipt) {
        MongoCachePipelinePreparations.requireVerifiedReceipt(Objects.requireNonNull(receipt, "receipt"));
        PipelineIntentScope scope = Objects.requireNonNull(receipt.scope(), "scope");
        String preparationId = Objects.requireNonNull(receipt.preparationId(), "preparationId");
        return transaction(session -> {
            Document row = requireCurrent(session, scope);
            var current = MongoPipelinePreparationFacts.decode(scope.pipelineId(), row, clusterId);
            if (current.phase() == PipelinePreparation.Phase.UNASSIGNED || !scope.equals(current.scope())
                    || !preparationId.equals(current.preparationId())) throw fenced();
            if (current.phase() == PipelinePreparation.Phase.COMPLETE) return current;
            var completed = new PipelinePreparation(current.pipelineId(), current.stateGeneration(), PipelinePreparation.Phase.COMPLETE,
                    current.scope(), current.preparationId(), current.issuerNode());
            replaceAnnex(session, scope, row, completed);
            return completed;
        });
    }

    private Document requireCurrent(ClientSession session, PipelineIntentScope scope) {
        var cache = bindings.cachePreparationForTransaction(session).orElseThrow(MongoPipelineStatePreparations::fenced).businessScope();
        if (!clusterId.equals(scope.clusterId()) || !cache.clusterId().equals(scope.clusterId())
                || cache.bindingVersion() != scope.bindingVersion() || !cache.cacheIncarnation().equals(scope.cacheIncarnation())) throw fenced();
        Document row = mappings.find(session, new Document("_id", scope.pipelineId())).first();
        PipelineStateMapping mapping = MongoPipelineStateMappings.decode(scope.pipelineId(), row);
        if (mapping.phase() != PipelineStateMapping.Phase.ACTIVE || mapping.stateGeneration() != scope.stateGeneration()
                || pipelines.find(session, new Document("_id", scope.pipelineId())).first() == null) throw fenced();
        return row;
    }

    private void replaceAnnex(ClientSession session, PipelineIntentScope scope, Document original, PipelinePreparation next) {
        Document filter = new Document("_id", scope.pipelineId()).append("stateGeneration", scope.stateGeneration())
                .append("phase", PipelineStateMapping.Phase.ACTIVE.name()).append("cachePreparation", original.get("cachePreparation"));
        if (mappings.updateOne(session, filter, new Document("$set", new Document("cachePreparation", MongoPipelinePreparationFacts.encode(next))))
                .getMatchedCount() != 1) throw new IllegalStateException("preparation changed within one CONFIG transaction snapshot");
    }

    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                try (ClientSession session = client.startSession()) {
                    return session.withTransaction(() -> {
                        requireActive.run(); T result = action.apply(session); requireActive.run(); return result;
                    }, TRANSACTION);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        });
    }

    /** Only this adapter's committed winner is an insert capability; arbitrary port implementations are not. */
    static FirstPipelineCreation consumeFirst(FirstPipelineCreation invocation) {
        if (!(invocation instanceof Creation)) throw fenced();
        invocation.consume();
        return invocation;
    }

    static Optional<io.tapstate.spi.store.PipelineRetirementResult> retirement(FirstPipelineCreation original) {
        if(!(original instanceof Creation creation))throw fenced();return creation.retirement;
    }

    private record Issuance(PipelinePreparation preparation,boolean won,PipelinePreparationInputs inputs,Optional<io.tapstate.spi.store.PipelineRetirementResult> retirement) {}

    /** The implementation is neither a persistent DTO nor serializable. */
    private static final class Creation implements FirstPipelineCreation {
        private final PipelinePreparation preparation;
        private final Runnable originalActive;
        private final PipelinePreparationInputs inputs;
        private final Optional<io.tapstate.spi.store.PipelineRetirementResult> retirement;
        private final AtomicBoolean consumed = new AtomicBoolean();
        private Creation(PipelinePreparation preparation,PipelinePreparationInputs inputs,Optional<io.tapstate.spi.store.PipelineRetirementResult> retirement,Runnable originalActive) {
            this.retirement=retirement;
            this.preparation = preparation; this.inputs = Objects.requireNonNull(inputs,"inputs"); this.originalActive = originalActive;
        }
        @Override public PipelinePreparation preparation() { return preparation; }
        @Override public PipelinePreparationInputs inputs() { return inputs; }
        @Override public void consume() {
            if (!consumed.compareAndSet(false, true)) throw fenced();
            originalActive.run();
        }
        @Override public void requireActive() { originalActive.run(); }
    }

    private static void requireId(String pipelineId) {
        if (pipelineId == null || pipelineId.isBlank()) throw new IllegalArgumentException("Pipeline id must not be blank");
    }
    private static TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
}
