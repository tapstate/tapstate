package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
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
        this(client, database, scope, original, requireActive, rateRetention, operatorDatabases,
                false, null, java.util.Optional.empty(), null);
    }

    private MongoCachePipelineAccess(MongoClient client, MongoDatabase database, PipelineIntentScope scope,
            WorkloadClaimFence original, Runnable requireActive, java.time.Duration rateRetention,
            io.tapstate.spi.store.OperatorDatabaseBinding operatorDatabases, boolean unpreparedModels, WorkloadClaimFence originalNode,
            java.util.Optional<io.tapstate.spi.store.PipelineRetirementResult> authoringPredecessor,
            MongoConfigurationDomain.AuthoringModels authoringModels) {
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
        return new MongoCachePipelineAccess(client, database, scope, null, active, retention, null, true, node, java.util.Optional.empty(), null);
    }
    static MongoCachePipelineAccess unpreparedModels(MongoClient client, MongoDatabase database, MongoConfigurationDomain.AuthoringModels original,
            WorkloadClaimFence node, Runnable active, java.time.Duration retention) {
        return new MongoCachePipelineAccess(client, database, original.scope(), null,
                active, retention, null, true, node, original.predecessor(), original);
    }
    void requireModelPin() { if (unpreparedModels) throw fenced(); }

    <T> T transaction(Function<ClientSession, T> action) { return transaction(TRANSACTION, action); }

    <T> T observationTransaction(Function<ClientSession, T> action) { return transaction(action); }

    <T> T pointRead(java.util.function.Supplier<T> action) {
        return StoreIo.call(() -> {
            requireActive.run();
            T value = action.get();
            requireActive.run();
            return value;
        });
    }

    <T> T checkpointTransaction(Function<ClientSession, T> action) { return checkpointTransaction(TRANSACTION, action); }

    private <T> T checkpointTransaction(TransactionOptions options, Function<ClientSession, T> action) {
        return MongoCachePipelineStateStore.writeTransaction(client, scope.pipelineId(), () -> transaction(options, action));
    }

    CheckpointDoc readCheckpoint(MongoStructureProof structures) {
        try {
            if (!unpreparedModels && authoringModels == null && StoreIo.call(() -> MongoReadBudget.supportsSnapshotReads(client))) {
                var facts = MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
                    requireActive.run();
                    var snapshot = database.withReadPreference(com.mongodb.ReadPreference.primary()).withReadConcern(ReadConcern.SNAPSHOT);
                    var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
                    var captured = MongoPipelineProofFacts.readAuthority(snapshot, scope, key);
                    if (captured != null) validateAuthority(captured);
                    requireActive.run();
                    return captured;
                }));
                if (facts != null) return MongoStateStore.toCheckpoint(facts.checkpoint());
            }
            return transaction(session -> MongoStateStore.toCheckpoint(state(session, true)));
        }
        catch (RuntimeException failure) {
            if (structures != null) structures.clear();
            throw failure;
        }
    }

    Document startupSnapshot(List<Document> stages) {
        if (MongoValidationClaimObservation.lacksSnapshotRead(client)) return null;
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            requireActive.run();
            var selected = database.withReadPreference(com.mongodb.ReadPreference.primary()).withReadConcern(ReadConcern.SNAPSHOT);
            Document result = SystemCollections.CACHE_IDENTITY.on(selected).aggregate(stages).first();
            requireActive.run();
            return result;
        }));
    }

    MongoPipelineProofFacts.SinkReceipt sinkReceipt(Document receiptId) {
        if (original == null || unpreparedModels || authoringModels != null || !MongoBindingReadFacts.supported(client)
                || MongoValidationClaimObservation.lacksSnapshotRead(client)) return null;
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            requireActive.run();
            var snapshot = database.withReadPreference(com.mongodb.ReadPreference.primary()).withReadConcern(ReadConcern.SNAPSHOT);
            var receipt = MongoPipelineProofFacts.readSinkReceipt(snapshot, scope, original.key(), receiptId);
            requireActive.run();
            if (receipt != null) validateAuthority(receipt.authority());
            return receipt;
        }));
    }

    private void validateAuthority(MongoPipelineProofFacts.Authority facts) {
        var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
        MongoCacheBusinessIdentity.requireObservedAdmission(facts.identity(), facts.schema(), business);
        validateState(facts.checkpoint());
        requirePreparedClaim(facts.claim());
    }

    private Document preparedState(ClientSession session, MongoPipelineProofFacts.Authority facts) {
        if (facts != null) {
            validateAuthority(facts);
            return facts.checkpoint();
        }
        var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
        new MongoCacheBusinessIdentity(database, business).requireAdmissionState(session);
        return state(session, true);
    }

    <T> T changeSinkReceipt(Document id,
            java.util.function.BiFunction<ClientSession, MongoPipelineProofFacts.SinkReceipt, T> action) {
        if (original == null || unpreparedModels || authoringModels != null || !MongoBindingReadFacts.supported(client))
            return metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS, true, session -> action.apply(session, null));
        var result = MongoCachePipelineStateStore.writeTransaction(client, scope.pipelineId(), () -> transaction(TRANSACTION, false, session -> {
            var receipt = MongoPipelineProofFacts.readSinkReceipt(session, database, scope, original.key(), id);
            var current = preparedState(session, receipt == null ? null : receipt.authority());
            proveClaim(session, true);
            updateState(session, current, new Document(), false);
            return action.apply(session, receipt);
        }));
        requireActive.run();
        return result;
    }

    private record PreparedRows(Document state, Document claim) {}

    /** Read all three admission checks at one transaction snapshot. Its caller still performs the original write CAS. */
    private <T> T withPreparedRows(java.util.function.BiFunction<ClientSession, PreparedRows, T> action) {
        return transaction(TRANSACTION, false, session -> {
            var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
            var facts = MongoBindingReadFacts.supported(client) ? MongoPipelineProofFacts.readAuthority(session, database, scope, key) : null;
            if (facts != null) {
                validateAuthority(facts);
                return action.apply(session, new PreparedRows(facts.checkpoint(), facts.claim()));
            }
            new MongoCacheBusinessIdentity(database, new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(),
                    scope.cacheIncarnation())).requireAdmissionState(session);
            var state = stateRow(session);
            return action.apply(session, new PreparedRows(state, requirePreparedClaim(session, true)));
        });
    }

    <T> T checkpointWithPreparedState(java.util.function.BiFunction<ClientSession, Document, T> action) {
        return MongoCachePipelineStateStore.writeTransaction(client, scope.pipelineId(),
                () -> withPreparedRows((session, rows) -> action.apply(session, rows.state())));
    }

    <T> T stateTransaction(boolean mutation, Function<ClientSession, T> action) {
        java.util.function.Supplier<T> change = () -> withPreparedRows((session, rows) -> {
            if (mutation) {
                proveClaim(session, true);
                updateState(session, rows.state(), new Document(), false);
            }
            return action.apply(session);
        });
        return mutation ? MongoCachePipelineStateStore.writeTransaction(client, scope.pipelineId(), change) : change.get();
    }

    private <T> T transaction(TransactionOptions options, Function<ClientSession, T> action) {
        return transaction(options, true, action);
    }

    private <T> T transaction(TransactionOptions options, boolean checkAdmission, Function<ClientSession, T> action) {
        return transaction(options, checkAdmission, action, false);
    }

    private <T> T transaction(TransactionOptions options, boolean checkAdmission, Function<ClientSession, T> action,
            boolean recoverPublicationCheckout) {
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            var retryCheckout = new java.util.concurrent.atomic.AtomicBoolean(recoverPublicationCheckout);
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> {
                        try {
                            MongoStructureFailureScope.beginCallback();
                            requireActive.run();
                            if (checkAdmission) {
                                new MongoCacheBusinessIdentity(database, new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()))
                                        .requireAdmissionState(session);
                            }
                            T result = action.apply(session);
                            requireActive.run();
                            return result;
                        } catch (com.mongodb.MongoConnectionPoolClearedException checkout) {
                            // This checkout sent no command. Replay only this uncommitted publication callback.
                            if (!checkout.hasErrorLabel(com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
                                    && retryCheckout.compareAndSet(true, false)) {
                                checkout.addLabel(com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
                            }
                            throw checkout;
                        }
                    }, options);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        }));
    }

    /**
     * A write under an already proved lease: one transaction holding the claim compare-and-set, the caller's
     * own conditions and the action, with no admission, structure, state or inventory decoding. The
     * compare-and-set is a real write to the claim document, so a takeover committing concurrently either
     * conflicts with this transaction or leaves it matching nothing. The pipeline state's runtime write fence
     * is not advanced: every ownership change it would catch also rewrites the claim document. Any failure,
     * including a refused claim, propagates unchanged; the caller decides the lease is lost.
     */
    <T> T leasedWrite(LeasedClock committed, MongoDatabase target, Function<ClientSession, T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    T written = session.withTransaction(() -> {
                        MongoStructureFailureScope.beginCallback();
                        MongoStructureFailureScope.includeOperatorDatabase(client, target);
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
        }));
    }

    /**
     * A read under an already proved lease: no transaction and no preamble, read concern {@code majority}
     * (set by the caller on the collection), in a causally consistent session that starts no earlier than
     * {@code after}, the last write this store committed.
     */
    <T> T leasedRead(LeasedClock after, MongoDatabase target, Function<ClientSession, T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            MongoStructureFailureScope.includeOperatorDatabase(client, target);
            requireActive.run();
            try (var session = client.startSession(com.mongodb.ClientSessionOptions.builder().causallyConsistent(true).build())) {
                after.advance(session);
                T result = action.apply(session);
                requireActive.run();
                return result;
            }
        }));
    }

    /**
     * The claim liveness half of a lease proof, inside the proof's transaction: the original claim is live
     * and prepared under this scope. Answers how long its lease still runs on the store's own clock, in
     * milliseconds, so the caller can count it down on its monotonic clock without reading a deadline.
     */
    long proveLease(ClientSession session) {
        if (original == null) throw fenced();
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

    /** Returns the full prepared claim only to this retained startup snapshot. */
    <T> T retainedStartRead(java.util.function.BiFunction<ClientSession, Document, T> action) {
        T result = withPreparedRows((session, rows) -> action.apply(session, rows.claim()));
        requireActive.run();
        return result;
    }

    /** One bounded publication batch keeps the original proof at the transaction's write boundaries. */
    <T> T publicationModelsTransaction(Function<ClientSession, T> action) {
        if (!unpreparedModels || authoringModels == null)
            return metadataTransaction(SystemCollections.CACHE_DERIVED_SCHEMAS, database, true, action, !unpreparedModels);
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            try (var session = client.startSession()) {
                T result = session.withTransaction(() -> {
                    MongoStructureFailureScope.beginCallback();
                    requireActive.run();
                    authoringModels.requirePublicationActive();
                    requireUnpreparedModels(session);
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
        }));
    }
    /** Read-only control header access; never materializes Pipeline positions or operator bodies. */
    <T> T validationControlTransaction(Function<ClientSession,T> action){
        return validationControlTransaction((session,state)->action.apply(session));
    }
    <T> T validationControlTransaction(java.util.function.BiFunction<ClientSession,Document,T> action){
        return transaction(session->{
            var state=MongoValidationMetadata.read(session,SystemCollections.CACHE_PIPELINE_STATE.on(database),new Document("_id",scope.pipelineId()),
                    new Document("scope",1).append("epoch",1).append("runtimeWriteFence",1).append("stateJson",1).append("touchMillis",1),64*1024);
            if(state==null)throw unreadable("checkpoint");requireScope(state);
            if(!(state.get("epoch") instanceof Long epoch)||epoch<0||epoch==Long.MAX_VALUE
                    ||!(state.get("runtimeWriteFence") instanceof Long revision)||revision<1||revision==Long.MAX_VALUE
                    ||!(state.get("stateJson") instanceof String)||!(state.get("touchMillis") instanceof Long))throw unreadable("checkpoint");
            var key=new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId());
            var claim=MongoValidationMetadata.read(session,SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database),new Document("_id",MongoWorkloadClaimStore.id(key)),
                    MongoValidationMetadata.claimProjection("pipelineScope","pipelinePreparation"),64*1024);
            requirePreparedClaim(session,claim);return action.apply(session,state);
        });
    }

    <T> T metadataTransaction(SystemCollections table, MongoDatabase target, boolean mutation, Function<ClientSession, T> action) {
        return metadataTransaction(table, target, mutation, action, false);
    }

    private <T> T metadataTransaction(SystemCollections table, MongoDatabase target, boolean mutation,
            Function<ClientSession, T> action, boolean recoverPublicationCheckout) {
        if(authoringModels!=null)authoringModels.requireActive();
        boolean batch = !unpreparedModels && original != null && authoringModels == null
                && database.getName().equals(target.getName()) && MongoBindingReadFacts.supported(client);
        Function<ClientSession, T> write = session -> {
            if (unpreparedModels) {
                if (table != SystemCollections.CACHE_DERIVED_SCHEMAS || !database.getName().equals(target.getName())) throw fenced();
                requireUnpreparedModels(session);
                if (mutation) {
                    if (originalNode == null) throw fenced();
                    var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
                    new MongoCacheBusinessIdentity(database, business).admitMetadataWrite(session);
                    MongoCachePipelinePreparations.proveNode(session, database, business, originalNode);
                }
                return action.apply(session);
            }
            Document current = batch
                    ? preparedState(session, MongoPipelineProofFacts.readAuthority(session, database, scope, original.key()))
                    : state(session, true);
            if (!database.getName().equals(target.getName())) {
                var inventory = MongoOperatorInventory.require(session, database, scope);
                if (inventory.getList("databases", Document.class).stream()
                        .noneMatch(entry -> target.getName().equals(entry.get("database")))) throw unreadable("operator-database");
                MongoStructureFailureScope.includeOperatorDatabase(client, target);
                String preparation = MongoCacheOperatorDatabases.requireOwner(session, target, operatorDatabases);
                MongoOperatorInventory.requireDatabase(inventory, target.getName(), preparation, scope);
            }
            if (mutation) { proveClaim(session, true); updateState(session, current, new Document(), false); }
            return action.apply(session);
        };
        java.util.function.Supplier<T> change = () -> transaction(TRANSACTION, !batch, write, recoverPublicationCheckout);
        T result = mutation ? MongoCachePipelineStateStore.writeTransaction(client, scope.pipelineId(), change) : change.get();
        if(authoringModels!=null)authoringModels.requireActive();
        requireActive.run(); return result;
    }
    <T> T appendRateSample(java.time.Duration timeout, Function<ClientSession, T> action) {
        var budget = new MongoReadBudget(timeout);
        try {
            requireActive.run();
            return budget.transaction(client, session -> {
                requireActive.run();
                new MongoCacheBusinessIdentity(database, new CacheBusinessScope(scope.clusterId(),
                        scope.bindingVersion(), scope.cacheIncarnation())).requireAdmissionState(session);
                T result = rateHistoryOperation(session, true, budget, action);
                requireActive.run();
                return result;
            });
        } catch (RuntimeException failure) {
            requireActive.run();
            throw failure;
        }
    }

    <T> T rateHistoryTransaction(boolean mutation, Function<ClientSession, T> action) {
        return transaction(session -> rateHistoryOperation(session, mutation, null, action));
    }

    private <T> T rateHistoryOperation(ClientSession session, boolean mutation, MongoReadBudget budget,
            Function<ClientSession, T> action) {
        state(session, budget != null);
        new MongoCacheMetadataStructure(database, rateRetention)
                .verifyFacet(SystemCollections.CACHE_PIPELINE_RATE_HISTORY, budget == null ? null : budget::remainingMillis);
        // Keep the original claim write in the sample transaction. Ordinary checkpoint
        // progress neither invalidates this claim nor needs to serialize with sampling.
        if (mutation) proveClaim(session, "fencedPipelineWrites", budget != null);
        return action.apply(session);
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
        return checkpointTransaction(local, session -> {
            Document current = state(session); proveClaim(session); updateState(session, current, new Document(), false);
            return action.apply(session);
        });
    }

    <T> T recoveredOperatorPreparation(WorkloadClaimFence node,Function<ClientSession,T> action) {
        var local=TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
        return checkpointTransaction(local, session -> {
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

    <T> T inspectObservationFailure(java.util.function.Supplier<T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, action);
    }

    void writeObservation(Runnable action) {
        MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            requireActive.run();
            action.run();
            return null;
        }));
    }

    void requirePipeline(String pipelineId) { if (!scope.pipelineId().equals(pipelineId)) throw fenced(); }

    io.tapstate.core.lifecycle.CheckpointDoc startCheckpoint() {
        return transaction(TRANSACTION, false, session -> {
            var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
            var row = SystemCollections.CACHE_IDENTITY.on(database).aggregate(session, List.of(
                    new Document("$match", new Document("_id", "cluster")),
                    new Document("$project", new Document("identity", "$$ROOT")),
                    startLookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business"),
                    startLookup(SystemCollections.CACHE_PIPELINE_STATE, "state", scope.pipelineId()),
                    startLookup(SystemCollections.CACHE_WORKLOAD_CLAIMS, "claim", MongoWorkloadClaimStore.id(key)))).first();
            var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
            if (row != null && (!"cluster".equals(row.get("identity", Document.class).get("_id"))
                    || needsStartLookupFallback(row, "schema", "business")
                    || needsStartLookupFallback(row, "state", scope.pipelineId())
                    || needsStartLookupFallback(row, "claim", MongoWorkloadClaimStore.id(key)))) {
                new MongoCacheBusinessIdentity(database, business).requireAdmissionState(session);
                return MongoStateStore.toCheckpoint(state(session, true));
            }
            MongoCacheBusinessIdentity.requireObservedAdmission(row == null ? null : row.get("identity", Document.class),
                    startDocument(row, "schema"), business);
            var state = startDocument(row, "state");
            validateState(state);
            requirePreparedClaim(session, startDocument(row, "claim"));
            return MongoStateStore.toCheckpoint(state);
        });
    }

    private static Document startLookup(SystemCollections collection, String field, Object id) {
        var size = new Document("$bsonSize", "$$ROOT");
        var projection = new Document("_id", 0).append("large", new Document("$gt", List.of(size, 64 * 1024)))
                .append("value", new Document("$cond", List.of(new Document("$lte", List.of(size, 64 * 1024)), "$$ROOT", false)));
        return new Document("$lookup", new Document("from", collection.collectionName())
                .append("pipeline", List.of(new Document("$match", new Document("_id", id)), new Document("$limit", 2),
                        new Document("$project", projection)))
                .append("as", field));
    }

    private static boolean needsStartLookupFallback(Document row, String field, Object expected) {
        var records = row.getList(field, Document.class);
        if (records.isEmpty()) return false;
        if (records.size() > 1 || Boolean.TRUE.equals(records.getFirst().get("large"))) return true;
        Object found = records.getFirst().get("value", Document.class).get("_id");
        if (expected instanceof Document id) return !(found instanceof Document actual)
                || !new java.util.ArrayList<>(id.keySet()).equals(new java.util.ArrayList<>(actual.keySet())) || !id.equals(actual);
        return !expected.equals(found);
    }

    private static Document startDocument(Document row, String field) {
        if (row == null) return null;
        var rows = row.getList(field, Document.class);
        return rows.isEmpty() ? null : rows.getFirst().get("value", Document.class);
    }

    /** Startup already admitted this CACHE snapshot; reads the original prepared state and claim together. */
    Document stateForStartupAttach(ClientSession session) {
        var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION,
                scope.pipelineId());
        var id = MongoWorkloadClaimStore.id(key);
        var captured = SystemCollections.CACHE_PIPELINE_STATE.on(database).aggregate(session, List.of(
                new Document("$match", new Document("_id", scope.pipelineId())),
                new Document("$project", new Document("state", "$$ROOT")),
                startLookup(SystemCollections.CACHE_WORKLOAD_CLAIMS, "claim", id))).first();
        if (captured == null) return state(session, true);
        var state = captured.get("state", Document.class);
        if (!scope.pipelineId().equals(state.get("_id")) || needsStartLookupFallback(captured, "claim", id))
            return state(session, true);
        validateState(state);
        requirePreparedClaim(session, startDocument(captured, "claim"));
        return state;
    }

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

    Document stateForStartupWrite(ClientSession session, boolean batchReads,
            java.util.function.Consumer<MongoPipelineProofFacts.PinProof> schedulingProof) {
        var proof = batchReads && original != null ? MongoPipelineProofFacts.readPin(session, database, scope, original) : null;
        var business = new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation());
        if (proof == null) {
            new MongoCacheBusinessIdentity(database, business).requireReady(session);
            return stateForWriterProgress(session, true);
        }
        var state = proof.state();
        MongoCacheBusinessIdentity.requireObservedReady(state.identity(), state.schema(), business);
        validateState(state.checkpoint());
        requirePreparedClaim(proof.claim());
        schedulingProof.accept(proof);
        return state.checkpoint();
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

    private Document requirePreparedClaim(ClientSession session, boolean structureChecked) {
        if (!structureChecked) MongoCacheWorkloadClaimStore.requireCollection(database);
        var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        Document row = table.find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        requirePreparedClaim(session, row);
        return row;
    }
    void requirePreparedClaim(ClientSession session,Document row){ requirePreparedClaim(row); }

    void requirePreparedClaim(Document row) {
        if (row == null) throw MongoCachePipelinePreparations.rebuild(scope);
        MongoWorkloadClaimStore.decodeStrict(row);
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

    void proveClaim(ClientSession session) { proveClaim(session, false); }

    void proveClaim(ClientSession session, boolean structureChecked) {
        proveClaim(session, "fencedPipelineWrites", structureChecked);
    }

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
        var filter = MongoWorkloadClaimStore.canUseNodeLease(original)
                ? new MongoWorkloadClaimStore(table).inStrictSession(session).nodeBackedFilter(original)
                        .map(node -> node.append("pipelineScope", physical).append("pipelinePreparation.phase", "PREPARED")).orElse(live)
                : live;
        if (table.updateOne(session, filter, update).getMatchedCount() != 1) throw claimFenced();
    }

    /** This real write also serializes an observation with replacement of the current state scope. */
    void updateState(ClientSession session, Document current, Document values, boolean advanceEpoch) {
        MongoCachePipelineStateStore.advanceFence(database, session, current, values, advanceEpoch);
    }

    void verifyStructures() {
        verifyStateStructure(); verifyStructure(SystemCollections.CACHE_PIPELINE_OBSERVATION);
    }

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
