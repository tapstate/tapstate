package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimRenewalException;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/** One fixed business scope; no claim transition uses another client or an unbound transaction. */
final class MongoCacheWorkloadClaimStore implements io.tapstate.spi.store.DeadlineWorkloadClaimStore {
    private static final System.Logger LOG = System.getLogger(MongoCacheWorkloadClaimStore.class.getName());
    private static final String CLUSTER_ID = "clusterId";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final MongoWorkloadClaimStore raw;
    private final MongoValidationPreparations.Authority validation;
    private final MongoClaimOperations operations;
    private final java.util.function.Supplier<MongoClient> nodeSessionClient;

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this(client, database, scope, requireActive, null);
    }

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive,
            MongoValidationPreparations.Authority validation) {
        this(client,database,scope,requireActive,validation,new MongoClaimOperations(client));
    }

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive,
            MongoValidationPreparations.Authority validation, MongoClaimOperations operations) {
        this(client, database, scope, requireActive, validation, operations, null);
    }

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive,
            MongoValidationPreparations.Authority validation, MongoClaimOperations operations,
            java.util.function.Supplier<MongoClient> nodeSessionClient) {
        this.nodeSessionClient = nodeSessionClient;
        this.client = Objects.requireNonNull(client, "client");
        this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
        this.validation = validation;
        this.operations = operations;
    }

    @Override public <T> T withDeadline(long deadline, java.util.function.Supplier<T> operation) {
        return withDeadline(deadline,()->{},operation);
    }
    @Override public <T> T withDeadline(long deadline,Runnable active,java.util.function.Supplier<T> operation) {
        return StoreIo.call(() -> operations.call(deadline,active,operation));
    }

    @Override public <T> T withClaimAcquisition(WorkloadClaimKey key, Runnable active, java.util.function.Supplier<T> operation) {
        if (key.type() == WorkloadClaimType.NODE_SESSION || validation != null) { active.run(); return operation.get(); }
        return MongoCacheAdmissionQueue.other(client, database.getName(), scope, Long.MAX_VALUE,
                () -> { requireActive.run(); active.run(); }, operation);
    }

    @Override public <T> T withClaimAcquisitionDeadline(WorkloadClaimKey key, long deadline, Runnable active,
            java.util.function.Supplier<T> operation) {
        if (key.type() == WorkloadClaimType.NODE_SESSION || validation != null) return withDeadline(deadline, active, operation);
        return MongoCacheAdmissionQueue.other(client, database.getName(), scope, deadline,
                () -> { requireActive.run(); active.run(); }, () -> withDeadline(deadline, active, operation));
    }

    @Override public <T> T withInitialPipelineAcquisition(WorkloadClaimKey key, long deadline, Runnable active,
            java.util.function.Supplier<T> operation) {
        requireKey(key);
        if (key.type() != WorkloadClaimType.PIPELINE_ACTUATION || validation != null)
            return withClaimAcquisitionDeadline(key, deadline, active, operation);
        return MongoCacheAdmissionQueue.first(client, database.getName(), scope, deadline,
                () -> { requireActive.run(); active.run(); }, () -> withDeadline(deadline, active, operation));
    }

    @Override public <T> T withRenewalDeadline(long deadline,Runnable active,java.util.function.Supplier<T> operation) {
        return StoreIo.call(() -> operations.renew(deadline,active,operation));
    }

    @Override public <T> T withRenewalDeadline(WorkloadClaimKey key, long deadline, Runnable active,
            java.util.function.Supplier<T> operation) {
        requireKey(key);
        if (key.type() != WorkloadClaimType.PIPELINE_ACTUATION)
            throw new IllegalArgumentException("a Pipeline renewal lane requires a Pipeline claim key");
        return StoreIo.call(() -> operations.renew(key, deadline, active, operation));
    }

    @Override public <T> T withRenewalDeadline(WorkloadClaimKey key, java.util.Set<WorkloadClaimKey> recoveries,
            long deadline, Runnable active, java.util.function.Supplier<T> operation) {
        requireKey(key);
        if (key.type() != WorkloadClaimType.PIPELINE_ACTUATION) throw new IllegalArgumentException("a Pipeline renewal requires a Pipeline key");
        for (var recovery : recoveries) {
            requireKey(recovery);
            if (recovery.type() != WorkloadClaimType.PIPELINE_ACTUATION || recovery.equals(key))
                throw new IllegalArgumentException("recovery reservations require other original Pipeline keys");
        }
        return StoreIo.call(() -> operations.renew(key, recoveries, deadline, active, operation));
    }

    @Override public <T> T withNodeSessionDeadline(WorkloadClaimKey key, long deadline, Runnable active,
            java.util.function.Supplier<T> operation) {
        requireKey(key);
        if (key.type() != WorkloadClaimType.NODE_SESSION)
            throw new IllegalArgumentException("the node-session I/O lane requires a node-session key");
        return StoreIo.call(() -> operations.nodeSession(deadline, active, operation));
    }

    @Override public Optional<WorkloadClaimReading> confirmNodeSession(WorkloadClaimKey key, long deadline,
            Runnable active) {
        requireKey(key);
        if (key.type() != WorkloadClaimType.NODE_SESSION)
            throw new IllegalArgumentException("node-session confirmation requires a node-session key");
        return StoreIo.call(() -> operations.confirmNodeSession(deadline, active, () -> nodeMaintenance().readNodeConfirmation(key)));
    }

    private MongoCacheWorkloadClaimStore nodeMaintenance() {
        if (nodeSessionClient == null) return this;
        var isolated = nodeSessionClient.get();
        return new MongoCacheWorkloadClaimStore(isolated, isolated.getDatabase(database.getName()), scope,
                requireActive, validation, operations);
    }

    private Optional<WorkloadClaimReading> readNodeConfirmation(WorkloadClaimKey key) {
        // A read in the confirmation lane must not wait for the writer's transaction cleanup.
        if (MongoValidationClaimObservation.lacksSnapshotRead(client)) return read(key);
        requireActive.run();
        var budget = operations.budget();
        return MongoStructureFailureScope.inspectOnFailure(client, budget, () -> {
            var selected = budget.database(database).withReadPreference(ReadPreference.primary())
                    .withReadConcern(ReadConcern.SNAPSHOT);
            var id = MongoWorkloadClaimStore.id(key);
            var facts = SystemCollections.CACHE_IDENTITY.on(selected).aggregate(List.of(
                    new Document("$match", new Document("_id", "cluster")),
                    new Document("$project", new Document("identity", "$$ROOT")),
                    proofLookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business", List.of()),
                    proofLookup(SystemCollections.CACHE_WORKLOAD_CLAIMS, "claim", id,
                            List.of(MongoWorkloadClaimStore.leaseRemaining(true)))))
                    .collation(com.mongodb.client.model.Collation.builder().locale("simple").build()).first();
            var identity = facts == null ? null : facts.get("identity", Document.class);
            MongoCacheBusinessIdentity.requireObservedAdmission(identity, proofRow(facts, "schema"), scope);
            requireActive.run();
            return Optional.ofNullable(proofRow(facts, "claim")).map(MongoWorkloadClaimStore::nodeSessionReading);
        });
    }

    private static Document proofLookup(SystemCollections table, String name, Object id, List<Document> extra) {
        var stages = new java.util.ArrayList<>(List.of(
                new Document("$match", new Document("_id", id)), new Document("$limit", 2)));
        stages.addAll(extra);
        return new Document("$lookup", new Document("from", table.collectionName())
                .append("as", name).append("pipeline", stages));
    }

    private static Document proofRow(Document facts, String name) {
        if (facts == null) return null;
        var rows = facts.getList(name, Document.class);
        if (rows.size() > 1) throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                java.util.Map.of("id", "node-session", "field", name), null);
        return rows.isEmpty() ? null : rows.getFirst();
    }

    @Override public void awaitQuiescence(long deadline) { StoreIo.run(() -> operations.await(deadline)); }

    @Override public WorkloadClaimAttempt acquire(WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        requireKey(key);
        if (key.type() == WorkloadClaimType.PIPELINE_ACTUATION) throw MongoCacheBusinessIdentity.businessFenced();
        if (key.type() == WorkloadClaimType.CAPTURE) {
            return transaction((session, budget) -> {
                var row = new MongoCacheBusinessIdentity(database, scope).admitRetainedCapture(session, key);
                if (row == null) throw MongoCapturePreparationFacts.history(key.resourceId());
                MongoCaptureCachePreparations.requirePreparation(row, key, scope);
                return raw.inStrictSession(session).acquireRetainedCapture(key, owner, topologyRevision, ttl, row);
            }, null);
        }
        return transaction(true, (store, session) -> {
            if (key.type() == WorkloadClaimType.VALIDATION) {
                var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
                requireValidationClaim(session, row, key);
                return store.acquireExisting(key, owner, topologyRevision, ttl);
            }
            return store.acquire(key,owner,topologyRevision,ttl);
        });
    }
    @Override public Optional<WorkloadClaim> acquireValidationExecution(WorkloadClaimKey key, WorkloadOwner owner,
            long topologyRevision, Duration ttl) {
        return acquireValidationExecution(key, owner, topologyRevision, ttl, null);
    }

    Optional<WorkloadClaim> acquireValidationExecution(WorkloadClaimKey key, WorkloadOwner owner,
            long topologyRevision, Duration ttl, java.util.function.Consumer<WorkloadClaim> registerCountOwner) {
        requireKey(key);
        if (key.type() != WorkloadClaimType.VALIDATION || validation == null || validation.closing()) throw claimFenced();
        java.util.function.Consumer<Optional<WorkloadClaim>> verifyCommitted = registerCountOwner == null ? null : granted -> {
            if (granted.isEmpty()) requireValidationAuthority();
            else {
                requireActive.run();
                MongoValidationSchema.requireReady(database, false);
                registerCountOwner.accept(granted.orElseThrow());
                requireActive.run();
            }
        };
        return transaction(true, null, (store, session) -> {
            var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)
                    .find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
            MongoValidationControlStore.requirePreparedClaim(session, database, row, key, scope, validation.subject(), true, true);
            return store.acquireNewValidationExecution(store.decodeStored(row), owner, topologyRevision, ttl);
        }, false, verifyCommitted);
    }

    @Override public WorkloadClaimAttempt acquirePipeline(PipelineIntentScope original, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        requireScope(original);
        var key = pipelineKey(original);
        return MongoCachePipelineStateStore.writeTransaction(client, original.pipelineId(), () -> transaction(true, original, (store, session) -> store.acquireExisting(key, owner, topologyRevision, ttl)));
    }

    @Override public WorkloadClaimAttempt acquireNodeBackedPipeline(PipelineIntentScope original, WorkloadClaimFence node,
            long topologyRevision) {
        requireScope(original);
        if (topologyRevision != 1 || !node.owner().nodeId().equals(scope.clusterId())) throw claimFenced();
        return MongoCachePipelineStateStore.writeTransaction(client, original.pipelineId(), () -> transaction(true, original, (store, session) -> {
            MongoCachePipelinePreparations.proveNodeForClaim(session, database, scope, node);
            return store.acquireNodeBackedPipeline(pipelineKey(original), node, topologyRevision);
        }));
    }
    @Override public Optional<WorkloadClaimReading> readPipeline(PipelineIntentScope original) {
        requireScope(original);
        var key = pipelineKey(original);
        try {
            return StoreIo.call(() -> operations.pipelineRenewal(key, () -> {
                var observed = readPipelineAuthority(original, key);
                return observed == null ? readPipelineTransaction(original, key) : Optional.of(observed);
            }));
        } catch (RuntimeException failure) {
            operations.structures.clear();
            throw failure;
        }
    }

    private WorkloadClaimReading readPipelineAuthority(PipelineIntentScope original, WorkloadClaimKey key) {
        if (!MongoBindingReadFacts.supported(client) || MongoValidationClaimObservation.lacksSnapshotRead(client)) return null;
        var budget = operations.budget();
        return MongoStructureFailureScope.inspectOnFailure(client, budget, () -> StoreIo.call(() -> {
            if (budget == null) operations.requireAdmission();
            operations.check();
            requireActive.run();
            var selected = (budget == null ? database : budget.database(database))
                    .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.SNAPSHOT);
            var captured = MongoPipelineProofFacts.readPipelineAuthority(selected, original, key);
            requireActive.run();
            operations.check();
            if (captured == null) return null;
            var facts = captured.facts();
            MongoCacheBusinessIdentity.requireObservedAdmission(facts.identity(), facts.schema(), scope);
            var access = new MongoCachePipelineAccess(client, database, original, null, requireActive);
            access.validateState(facts.checkpoint());
            access.requirePreparedClaim(facts.claim());
            return MongoWorkloadClaimStore.observedPipelineReading(facts.claim(), captured.node());
        }));
    }

    private Optional<WorkloadClaimReading> readPipelineTransaction(PipelineIntentScope original, WorkloadClaimKey key) {
        return transaction((session, budget) -> {
            var identity = new MongoCacheBusinessIdentity(database, scope);
            var access = new MongoCachePipelineAccess(client, database, original, null, requireActive);
            var store = raw.inStrictSession(session);
            if (MongoBindingReadFacts.supported(client)) {
                // Refuse a retired or mismatched identity before accessing dependent namespaces.
                var originalIdentity = identity.readReadyIdentity(session);
                var facts = MongoPipelineProofFacts.read(session, database, original, key);
                if (facts != null) {
                    MongoCacheBusinessIdentity.requireObservedAdmission(originalIdentity, facts.schema(), scope);
                    access.validateState(facts.checkpoint());
                    access.requirePreparedClaim(session, facts.claim());
                    return Optional.of(store.reading(facts.claim()));
                }
                // A miss or oversized row can still be resolved by the original collection-local reads.
                identity.requireAdmissionState(session, originalIdentity);
            } else {
                identity.requireAdmissionState(session);
            }
            access.stateRow(session);
            var row = store.readWithRemainingLease(key);
            access.requirePreparedClaim(session, row);
            return Optional.of(store.reading(row));
        }, null);
    }
    @Override public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        if (expected.key().type() == WorkloadClaimType.NODE_SESSION)
            return operations.nodeSessionRenewal(expected.key(), () -> nodeMaintenance().renewClaim(expected, ttl));
        if (expected.key().type() != WorkloadClaimType.PIPELINE_ACTUATION) return renewClaim(expected, ttl);
        var previous = MongoCommandObservation.COLLECTIONS.get();
        var stage = new java.util.concurrent.atomic.AtomicReference<String>();
        MongoCommandObservation.COLLECTIONS.set((command, collection) -> {
            if (previous != null) previous.accept(command, collection);
            if (stage.getAndSet(command) == null) LOG.log(System.Logger.Level.DEBUG,
                    "Pipeline renewal first Mongo command {0}, claim={1}, execution={2}, command={3}",
                    expected.key().resourceId(), expected.claimGeneration(), expected.executionGeneration(), command);
        });
        try {
            return operations.pipelineRenewal(expected.key(), () -> {
                operations.requireRenewalTurn(WorkloadClaimFence.from(expected));
                return renewClaim(expected, ttl);
            });
        } catch (TapstateException failure) {
            LOG.log(System.Logger.Level.DEBUG,
                    "Pipeline renewal last Mongo command {0}, claim={1}, execution={2}, command={3}, result={4}",
                    expected.key().resourceId(), expected.claimGeneration(), expected.executionGeneration(), stage.get(), failure.code().code());
            throw failure;
        } finally {
            if (previous == null) MongoCommandObservation.COLLECTIONS.remove();
            else MongoCommandObservation.COLLECTIONS.set(previous);
        }
    }

    private Optional<WorkloadClaim> renewClaim(WorkloadClaim expected, Duration ttl) {
        var firstMongoError = new java.util.concurrent.atomic.AtomicBoolean();
        return transaction((session, budget) -> {
            try {
                var store = raw.inStrictSession(session);
                var renewed = store.renewBeforeReads(expected, ttl);
                if (renewed.isPresent() && expected.key().type() == WorkloadClaimType.PIPELINE_ACTUATION) {
                    var row = renewed.orElseThrow().row();
                    verifyPipelineRenewal(session, decodeScope(row, expected.key()), row);
                } else {
                    if (renewed.isPresent()) {
                        var retained = storedScope(session, expected.key());
                        if (retained != null) validateState(session, retained);
                    } else validateMutationScope(session, store, expected);
                    new MongoCacheBusinessIdentity(database, scope).requireAdmissionState(session);
                }
                return renewed.map(MongoWorkloadClaimStore.Renewal::claim);
            } catch (MongoException failure) {
                if (expected.key().type() == WorkloadClaimType.PIPELINE_ACTUATION
                        && firstMongoError.compareAndSet(false, true)) {
                    LOG.log(System.Logger.Level.WARNING,
                            "Pipeline renewal first Mongo error {0}, claim={1}, execution={2}, type={3}, code={4}, labels={5}",
                            expected.key().resourceId(), expected.claimGeneration(), expected.executionGeneration(),
                            failure.getClass().getSimpleName(), failure.getCode(), failure.getErrorLabels());
                }
                if (operations.pipelineRenewal() && failure.getCode() == 112
                        && failure.hasErrorLabel("TransientTransactionError")
                        && !failure.hasErrorLabel("UnknownTransactionCommitResult")) throw new RenewalWriteConflict();
                throw failure;
            }
        }, null);
    }

    private void verifyPipelineRenewal(ClientSession session, PipelineIntentScope retained, Document renewedRow) {
        var access = new MongoCachePipelineAccess(client, database, retained, null, requireActive);
        var proof = MongoBindingReadFacts.supported(client) ? MongoPipelineProofFacts.readRenewal(session, database, retained) : null;
        if (proof == null) {
            access.stateRow(session);
            access.requirePreparedClaim(renewedRow);
            new MongoCacheBusinessIdentity(database, scope).requireAdmissionState(session);
            return;
        }
        access.validateState(proof.checkpoint());
        access.requirePreparedClaim(renewedRow);
        MongoCacheBusinessIdentity.requireObservedAdmission(proof.identity(), proof.schema(), scope);
    }
    @Override public boolean release(WorkloadClaim expected) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        java.util.function.Supplier<Boolean> change = () -> transaction(false, (store, session) -> {
            if (knownRetiredPipeline(session, expected.key())) return false;
            var retained = storedScope(session, expected.key());
            boolean released = store.release(expected);
            if (released && retained != null) fenceProgress(session, retained);
            return released;
        });
        return expected.key().type() == WorkloadClaimType.PIPELINE_ACTUATION
                ? MongoCachePipelineStateStore.writeTransaction(client, expected.key().resourceId(), change) : change.get();
    }
    private boolean knownRetiredPipeline(ClientSession session, WorkloadClaimKey key) {
        if (key.type() != WorkloadClaimType.PIPELINE_ACTUATION) return false;
        var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,
                new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        if (row == null || !(row.get("pipelinePreparation") instanceof Document preparation)
                || !"RETIRED".equals(preparation.get("phase"))) return false;
        if (!(row.get("pipelineRetirement") instanceof Document saved)) throw claimFenced();
        if (!(row.get("pipelineScope") instanceof Document physical)) throw unreadableScope();
        var retired = MongoPipelineRetirementFacts.cache(saved);
        if (!key.equals(retired.claim().key())
                || !scope.equals(new CacheBusinessScope(retired.scope().clusterId(), retired.scope().bindingVersion(), retired.scope().cacheIncarnation()))
                || !retired.scope().equals(MongoPipelinePreparationFacts.scope(physical))) throw claimFenced();
        new MongoCachePipelineRetirements(client, database, scope, requireActive).verifyRetired(session, retired);
        // Retirement has already expired and replaced the live fence. A late release cannot rewrite that result.
        return true;
    }
    @Override public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision, Set<String> nodeIds) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        if (expected.key().type() == WorkloadClaimType.PIPELINE_ACTUATION) {
            return MongoCachePipelineStateStore.writeTransaction(client, expected.key().resourceId(), () -> transaction(false, null, (store, session) -> {
                var first = store.advanceExecutionBeforeReads(expected, topologyRevision, nodeIds);
                if (first.isEmpty()) {
                    // Preserve existing refusal diagnostics and the original node-backed lease path.
                    validateMutationScope(session, store, expected);
                    var advanced = store.advanceExecution(expected, topologyRevision, nodeIds);
                    if (advanced.isPresent()) fenceProgress(session, storedScope(session, expected.key()));
                    return advanced;
                }
                var advanced = first.orElseThrow();
                var retained = decodeScope(advanced.before(), expected.key());
                if (!retained.equals(decodeScope(advanced.row(), expected.key()))) throw claimFenced();
                validateState(session, retained);
                fenceProgress(session, retained);
                return Optional.of(advanced.claim());
            }, true));
        }
        return transaction(false, (store, session) -> {
            validateMutationScope(session, store, expected);
            var advanced = store.advanceExecution(expected, topologyRevision, nodeIds);
            if (advanced.isPresent() && expected.key().type() == WorkloadClaimType.PIPELINE_ACTUATION)
                fenceProgress(session, storedScope(session, expected.key()));
            return advanced;
        });
    }
    @Override public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean afterMemberLoss) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        return transaction(false, (store, session) -> {
            validateMutationScope(session, store, expected);
            return store.recordExecutionFailure(expected, afterMemberLoss);
        });
    }
    @Override public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
        requireKey(key);
        if (key.type() == WorkloadClaimType.CAPTURE) {
            var observed = readCaptureAuthority(key);
            if (observed != null) return Optional.of(observed);
        }
        return transaction(false, (store, session) -> {
            storedScope(session, key);
            return store.read(key);
        });
    }
    private WorkloadClaimReading readCaptureAuthority(WorkloadClaimKey key) {
        if (!MongoBindingReadFacts.supported(client) || MongoValidationClaimObservation.lacksSnapshotRead(client)) return null;
        var budget = operations.budget();
        return MongoStructureFailureScope.inspectOnFailure(client, budget, () -> StoreIo.call(() -> {
            if (budget == null) operations.requireAdmission();
            operations.check();
            requireActive.run();
            var selected = (budget == null ? database : budget.database(database))
                    .withReadPreference(ReadPreference.primary()).withReadConcern(ReadConcern.SNAPSHOT);
            var id = MongoWorkloadClaimStore.id(key);
            var bounded = new Document("$lte", List.of(new Document("$bsonSize", "$$ROOT"), 64 * 1024));
            var value = new Document("value", new Document("$cond", List.of(bounded, "$$ROOT", false)));
            var captured = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(selected).aggregate(List.of(
                    new Document("$match", new Document("_id", id)),
                    new Document("$limit", 1),
                    MongoWorkloadClaimStore.leaseRemaining(true),
                    new Document("$project", new Document("claim", new Document("$cond", List.of(bounded, List.of("$$ROOT"), List.of())))),
                    MongoValidationClaimObservation.lookup(SystemCollections.CACHE_IDENTITY, "identity", "cluster", value),
                    MongoValidationClaimObservation.lookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business", value))).first();
            var claim = MongoStartConfigRead.row(captured, "claim", id);
            var identity = capturedCaptureRow(captured, "identity", "cluster");
            var schema = capturedCaptureRow(captured, "schema", "business");
            requireActive.run();
            operations.check();
            if (claim.isEmpty() || identity == null || schema == null || MongoWorkloadClaimStore.nodeBacked(claim.orElseThrow())) return null;
            MongoCacheBusinessIdentity.requireObservedAdmission(identity, schema, scope);
            MongoCaptureCachePreparations.requirePreparation(claim.orElseThrow(), key, scope);
            return MongoWorkloadClaimStore.observedReading(claim.orElseThrow());
        }));
    }

    private static Document capturedCaptureRow(Document captured, String name, Object id) {
        return MongoStartConfigRead.row(captured, name, id).filter(row -> row.get("value") instanceof Document)
                .map(row -> row.get("value", Document.class)).orElse(null);
    }

    @Override public Map<WorkloadClaimKey, WorkloadClaimReading> readAll(Collection<WorkloadClaimKey> keys) {
        Objects.requireNonNull(keys, "keys");
        var captured = List.copyOf(keys);
        if (captured.isEmpty()) return Map.of();
        captured.forEach(this::requireKey);
        return transaction(false, (store, session) -> {
            for(var key:captured)if(key.type()==WorkloadClaimType.CAPTURE || key.type()==WorkloadClaimType.VALIDATION)storedScope(session,key);
            Map<Document, WorkloadClaimKey> pipelines = new LinkedHashMap<>();
            for (var key : captured) if (key.type() == WorkloadClaimType.PIPELINE_ACTUATION) pipelines.put(MongoWorkloadClaimStore.id(key), key);
            if (!pipelines.isEmpty()) {
                try (var rows = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,
                        new Document("_id", new Document("$in", List.copyOf(pipelines.keySet())))).iterator()) {
                    while (rows.hasNext()) {
                        var row = rows.next(); decodeScope(row, pipelines.get(row.get("_id")));
                    }
                }
            }
            return store.readAll(captured);
        });
    }

    /** A live tenure's stored scope is immutable; validation never substitutes the current claim for the caller's. */
    private void validateMutationScope(ClientSession session, MongoWorkloadClaimStore store, WorkloadClaim expected) {
        var retained = storedScope(session, expected.key());
        if (retained == null) return;
        var current = store.read(expected.key());
        if (current.isPresent() && current.get().leased()) {
            var claim = current.get().claim();
            if (claim.owner().equals(expected.owner()) && claim.claimGeneration() == expected.claimGeneration()
                    && claim.executionGeneration() == expected.executionGeneration()) {
                validateState(session, retained);
            }
        }
    }

    private void fenceProgress(ClientSession session, PipelineIntentScope retained) {
        var access = new MongoCachePipelineAccess(client, database, retained, null, requireActive);
        access.updateState(session, access.state(session, true), new Document(), false);
    }

    private void validateState(ClientSession session, PipelineIntentScope retained) {
        var access = new MongoCachePipelineAccess(client, database, retained, null, requireActive);
        access.state(session, true);
    }

    private PipelineIntentScope storedScope(ClientSession session, WorkloadClaimKey key) {
        if (key.type() == WorkloadClaimType.VALIDATION) {
            var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
            requireValidationClaim(session, row, key); return null;
        }
        if(key.type()==WorkloadClaimType.CAPTURE) {
            var capture=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
            if(capture!=null)MongoCaptureCachePreparations.requirePreparation(capture,key,scope);return null;
        }
        if (key.type() != WorkloadClaimType.PIPELINE_ACTUATION) return null;
        var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        return row == null ? null : decodeScope(row, key);
    }

    private PipelineIntentScope decodeScope(Document row, WorkloadClaimKey key) {
        return preparedScope(row,key,scope);
    }

    private void requireValidationClaim(ClientSession session, Document row, WorkloadClaimKey key) {
        MongoValidationControlStore.requirePreparedClaim(session, database, row, key, scope,
                validation == null ? null : validation.subject(), validation == null || !validation.closing());
    }
    static PipelineIntentScope preparedScope(Document row,WorkloadClaimKey key,CacheBusinessScope scope) {
        try {
            if (!(row.get("pipelineScope") instanceof Document value) || !(value.get("bindingVersion") instanceof Long binding)
                    || !(value.get("stateGeneration") instanceof Long generation)) throw unreadableScope();
            var decoded = new PipelineIntentScope(value.getString(CLUSTER_ID), binding, value.getString("cacheIncarnation"), value.getString("pipelineId"), generation);
            if (!value.equals(scopeDocument(decoded)) || !key.resourceId().equals(decoded.pipelineId()) || !key.clusterId().equals(decoded.clusterId())
                    || !scope.equals(new CacheBusinessScope(decoded.clusterId(), decoded.bindingVersion(), decoded.cacheIncarnation()))) throw unreadableScope();
            if (MongoCachePipelinePreparations.phase(row, decoded) != io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED) throw claimFenced();
            return decoded;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadableScope(); }
    }

    private static Document scopeDocument(PipelineIntentScope value) {
        return new Document(CLUSTER_ID, value.clusterId()).append("bindingVersion", value.bindingVersion())
                .append("cacheIncarnation", value.cacheIncarnation()).append("pipelineId", value.pipelineId()).append("stateGeneration", value.stateGeneration());
    }
    private static WorkloadClaimKey pipelineKey(PipelineIntentScope value) { return new WorkloadClaimKey(value.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, value.pipelineId()); }
    private static TapstateException unreadableScope() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "workload", "field", "pipeline-claim-scope"), null); }
    static TapstateException claimFenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }

    private <T> T transaction(boolean admission, BiFunction<MongoWorkloadClaimStore, ClientSession, T> operation) {
        return transaction(admission, null, operation);
    }

    private <T> T transaction(boolean admission, PipelineIntentScope pipeline, BiFunction<MongoWorkloadClaimStore, ClientSession, T> operation) {
        return transaction(admission, pipeline, operation, false);
    }

    private <T> T transaction(boolean admission, PipelineIntentScope pipeline,
            BiFunction<MongoWorkloadClaimStore, ClientSession, T> operation, boolean renewalFirst) {
        return transaction(admission, pipeline, operation, renewalFirst, null);
    }

    private <T> T transaction(boolean admission, PipelineIntentScope pipeline,
            BiFunction<MongoWorkloadClaimStore, ClientSession, T> operation, boolean renewalFirst,
            java.util.function.Consumer<T> verifyCommitted) {
        return transaction((session, budget) -> attempt(session, admission, pipeline, operation, renewalFirst), verifyCommitted);
    }

    private <T> T transaction(BiFunction<ClientSession, MongoReadBudget, T> callback,
            java.util.function.Consumer<T> verifyCommitted) {
        return MongoStructureFailureScope.inspectOnFailure(client, operations.budget(), () -> StoreIo.call(() -> {
            try {
                if(operations.budget()==null)operations.requireAdmission();
                requireActive.run();
                requireValidationAuthority();
                var budget = operations.budget();
                if (budget != null) {
                    var result = boundedTransaction(callback, budget);
                    if (verifyCommitted != null) verifyCommitted.accept(result);
                    return result;
                }
                for (int attempt = 0; ; attempt++) {
                    T committed;
                    try {
                        committed = MongoCacheAdmissionQueue.driverTransaction(client, operations, TRANSACTION,
                                session -> checkedAttempt(session, null, callback));
                    } catch (MongoException conflict) {
                        // A duplicate-key error aborts its transaction; retry the original callback,
                        // never keep using that session or adopt another scope.
                        if (attempt >= 2 || ErrorCategory.fromErrorCode(conflict.getCode()) != ErrorCategory.DUPLICATE_KEY) throw conflict;
                        continue;
                    }
                    // CONFIG registration verifies the exact committed owner without replaying a CACHE write on failure.
                    if (verifyCommitted == null) requireValidationAuthority();
                    else verifyCommitted.accept(committed);
                    return committed;
                }
            } catch (RuntimeException failed) {
                operations.structures.clear();
                // A known closed/revoked owner is diagnosed before a driver's closed-client failure.
                requireActive.run();
                throw failed;
            }
        }));
    }

    private <T> T attempt(ClientSession session, boolean admission, PipelineIntentScope pipeline,
            BiFunction<MongoWorkloadClaimStore,ClientSession,T> operation, boolean renewalFirst) {
        var identity = new MongoCacheBusinessIdentity(database, scope);
        if (!renewalFirst) {
            if (admission) identity.admitClaim(session); else identity.requireAdmissionState(session);
        }
        var store = raw.inStrictSession(session);
        if (pipeline != null) {
            var access = new MongoCachePipelineAccess(client, database, pipeline, null, requireActive);
            var current = access.state(session, true);
            // A real per-Pipeline write serializes admission with controlled state replacement.
            if (admission) access.updateState(session, current, new Document(), false);
        }
        T result = operation.apply(store, session);
        // Business validation follows the exact fenced mutation in the original transaction snapshot.
        if (renewalFirst) identity.requireAdmissionState(session);
        return result;
    }

    private <T> T checkedAttempt(ClientSession session, MongoReadBudget budget,
            BiFunction<ClientSession, MongoReadBudget, T> callback) {
        MongoStructureFailureScope.beginCallback();
        if (budget != null) operations.check();
        requireActive.run();
        T result = callback.apply(session, budget);
        requireActive.run();
        if (budget != null) operations.check();
        return result;
    }

    private <T> T boundedTransaction(BiFunction<ClientSession, MongoReadBudget, T> callback, MongoReadBudget budget) {
        for(int attempt=0;;attempt++) {
            operations.check();
            var session=client.startSession();
            var owned=operations.own(session, client);
            boolean committedAck=false;
            try {
                var result=session.withTransaction(()->{
                    budget.remainingMillis();
                    owned.beginAttempt();
                    try {
                        var value=checkedAttempt(session, budget, callback);
                        owned.returned();return value;
                    } catch(RuntimeException|Error failed) {
                        owned.capture();
                        try {operations.abortCurrent(owned,budget);}
                        catch(RuntimeException|Error unresolved) {
                            // Never let the driver's suppressed abort failure replay an uncertain callback.
                            if(unresolved instanceof Error error) {
                                if(error!=failed)error.addSuppressed(failed);
                                throw new UnresolvedTransaction(error);
                            }
                            throw new UnresolvedTransaction(failed);
                        }
                        throw failed;
                    }
                },TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                        .timeout(budget.remainingMillis(),java.util.concurrent.TimeUnit.MILLISECONDS).build());
                committedAck=true;
                operations.acknowledged(owned);
                requireValidationAuthority();
                budget.remainingMillis();
                return result;
            } catch(RuntimeException|Error caught) {
                Throwable failed=caught;
                boolean retryCommit=failed instanceof MongoClaimOperations.RetryAfterCommit;
                if(failed instanceof UnresolvedTransaction unresolved) {
                    if(unresolved.original instanceof Error error)failed=error;
                    else failed=(RuntimeException)unresolved.original;
                }
                if(!owned.ended()) {
                    try { operations.abort(owned,budget); }
                    catch(RuntimeException|Error unresolved) {
                        var safe=unresolved instanceof MongoException mongo?StoreIo.coded(mongo):unresolved;
                        if(safe!=failed)failed.addSuppressed(safe);
                    }
                }
                else if(!owned.released()&&!owned.closeAttempted()) {
                    try {operations.committed(owned);}
                    catch(RuntimeException|Error unresolved){
                        var safe=unresolved instanceof MongoException mongo?StoreIo.coded(mongo):unresolved;
                        if(safe!=failed)failed.addSuppressed(safe);
                    }
                }
                if (failed instanceof RenewalWriteConflict) {
                    throw new WorkloadClaimRenewalException(owned.released()
                            ? WorkloadClaimRenewalException.Reason.WRITE_CONFLICT
                            : WorkloadClaimRenewalException.Reason.UNCONFIRMED,
                            "Mongo Pipeline renewal WriteConflict code=112");
                }
                if (operations.nodeSessionRenewal() && (retryCommit || failed instanceof MongoException mongo
                        && (committedAck || !owned.released() || mongo.hasErrorLabel("UnknownTransactionCommitResult")))) {
                    throw new WorkloadClaimRenewalException(WorkloadClaimRenewalException.Reason.UNCONFIRMED,
                            "the node-session renewal result could not be confirmed");
                }
                if (operations.pipelineRenewal() && failed instanceof MongoException mongo
                        && mongo.hasErrorLabel("UnknownTransactionCommitResult")) {
                    throw new WorkloadClaimRenewalException(WorkloadClaimRenewalException.Reason.UNCONFIRMED,
                            "Mongo Pipeline renewal UnknownTransactionCommitResult");
                }
                if(!committedAck && owned.released() && retryCommit) {budget.remainingMillis();continue;}
                if(!committedAck && owned.released() && failed instanceof MongoException mongo
                        && !mongo.hasErrorLabel("UnknownTransactionCommitResult")
                        && !(mongo instanceof com.mongodb.MongoOperationTimeoutException)
                        && (mongo.hasErrorLabel("TransientTransactionError")
                            || attempt<2 && ErrorCategory.fromErrorCode(mongo.getCode())==ErrorCategory.DUPLICATE_KEY)) {
                    budget.remainingMillis();
                    continue;
                }
                if(retryCommit)throw claimFenced();
                if(failed instanceof Error error)throw error;
                throw (RuntimeException)failed;
            }
        }
    }

    private static final class UnresolvedTransaction extends RuntimeException {
        private final Throwable original;
        private UnresolvedTransaction(Throwable original){super(null,null,false,false);this.original=original;}
    }

    /** Ends the callback after abort instead of asking the driver to immediately replay it. */
    private static final class RenewalWriteConflict extends RuntimeException {
        private RenewalWriteConflict() { super(null, null, false, false); }
    }

    private void requireScope(PipelineIntentScope original) {
        Objects.requireNonNull(original, "scope");
        if (validation != null) throw claimFenced();
        if (!scope.equals(new CacheBusinessScope(original.clusterId(), original.bindingVersion(), original.cacheIncarnation())))
            throw MongoCacheBusinessIdentity.businessFenced();
    }
    private void requireKey(WorkloadClaimKey key) {
        if (!scope.clusterId().equals(Objects.requireNonNull(key, "key").clusterId())) throw MongoCacheBusinessIdentity.businessFenced();
        if (validation != null && !key.equals(io.tapstate.spi.store.ValidationClaims.key(scope.clusterId(), validation.subject().pipelineId()))) throw claimFenced();
    }

    private void requireValidationAuthority() {
        if (validation != null) { validation.require(false); MongoValidationSchema.requireReady(database, false); }
    }

    static void requireCollection(MongoDatabase database) {
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS;
        Document found = database.listCollections().filter(new Document("name", table.collectionName())).first();
        if (found == null || !"collection".equals(found.get("type"))) throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "cache-business", "field", "workload-claims"), null);
    }
}
