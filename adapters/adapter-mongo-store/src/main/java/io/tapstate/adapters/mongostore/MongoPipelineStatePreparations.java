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
    private final MongoStorageBindingStore bindings;
    private final Runnable requireActive;

    MongoPipelineStatePreparations(MongoClient client, MongoDatabase database, String clusterId,
            MongoStorageBindingStore bindings, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.bindings = Objects.requireNonNull(bindings, "bindings"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (!bindings.belongsTo(client, database.getName())) throw new IllegalArgumentException("preparation and binding must share one CONFIG client");
        this.mappings = SystemCollections.OPERATOR_GENERATIONS.on(database);
        this.pipelines = SystemCollections.PIPELINES.on(database);
    }

    @Override public PipelineStatePreparations guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoPipelineStatePreparations(client, client.getDatabase(mappings.getNamespace().getDatabaseName()),
                clusterId, bindings, () -> { requireActive.run(); additional.run(); });
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
        Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(originalNode, "originalNode");
        if (originalNode.key().type() != WorkloadClaimType.NODE_SESSION || !clusterId.equals(originalNode.key().clusterId())
                || !originalNode.key().resourceId().equals(originalNode.owner().nodeId())) throw fenced();
        String preparationId = UUID.randomUUID().toString();
        Issuance result = transaction(session -> {
            Document row = requireCurrent(session, scope);
            PipelinePreparation previous = MongoPipelinePreparationFacts.decode(scope.pipelineId(), row, clusterId);
            if (previous.phase() != PipelinePreparation.Phase.UNASSIGNED) {
                if (!scope.equals(previous.scope())) throw fenced();
                return new Issuance(previous, false);
            }
            var issued = new PipelinePreparation(scope.pipelineId(), scope.stateGeneration(), PipelinePreparation.Phase.ISSUED,
                    scope, preparationId, originalNode);
            replaceAnnex(session, scope, row, issued);
            return new Issuance(issued, true);
        });
        // The grant is constructed only after withTransaction has confirmed its commit.
        requireActive.run();
        return result.won() ? new FirstPipelineCreationResult.Issued(new Creation(result.preparation(), requireActive))
                : new FirstPipelineCreationResult.AlreadyIssued(result.preparation());
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

    private record Issuance(PipelinePreparation preparation, boolean won) {}

    /** The implementation is neither a persistent DTO nor serializable. */
    private static final class Creation implements FirstPipelineCreation {
        private final PipelinePreparation preparation;
        private final Runnable originalActive;
        private final AtomicBoolean consumed = new AtomicBoolean();
        private Creation(PipelinePreparation preparation, Runnable originalActive) {
            this.preparation = preparation; this.originalActive = originalActive;
        }
        @Override public PipelinePreparation preparation() { return preparation; }
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
