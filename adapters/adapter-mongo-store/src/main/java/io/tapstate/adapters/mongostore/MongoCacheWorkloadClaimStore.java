package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
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
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
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
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final MongoWorkloadClaimStore raw;
    private final MongoValidationPreparations.Authority validation;
    private final MongoClaimOperations operations;

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this(client, database, scope, requireActive, null);
    }

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive,
            MongoValidationPreparations.Authority validation) {
        this(client,database,scope,requireActive,validation,new MongoClaimOperations(client));
    }

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive,
            MongoValidationPreparations.Authority validation, MongoClaimOperations operations) {
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

    @Override public void awaitQuiescence(long deadline) { StoreIo.run(() -> operations.await(deadline)); }

    @Override public WorkloadClaimAttempt acquire(WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        requireKey(key);
        if (key.type() == WorkloadClaimType.PIPELINE_ACTUATION) throw MongoCacheBusinessIdentity.businessFenced();
        return transaction(true, (store, session) -> {
            if (key.type() == WorkloadClaimType.VALIDATION) {
                var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
                requireValidationClaim(session, row, key);
                return store.acquireExisting(key, owner, topologyRevision, ttl);
            }
            if(key.type()==WorkloadClaimType.CAPTURE) {
                var row=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
                if(row==null)throw MongoCapturePreparationFacts.history(key.resourceId());
                MongoCaptureCachePreparations.requirePreparation(row,key,scope);return store.acquireExisting(key,owner,topologyRevision,ttl);
            }
            return store.acquire(key,owner,topologyRevision,ttl);
        });
    }
    @Override public WorkloadClaimAttempt acquirePipeline(PipelineIntentScope original, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        requireScope(original);
        var key = pipelineKey(original);
        return transaction(true, original, (store, session) -> store.acquireExisting(key, owner, topologyRevision, ttl));
    }
    @Override public Optional<WorkloadClaimReading> readPipeline(PipelineIntentScope original) {
        requireScope(original);
        var key = pipelineKey(original);
        return transaction(false, original, (store, session) -> {
            var retained = storedScope(session, key);
            if (retained != null && !retained.equals(original)) throw claimFenced();
            return store.read(key);
        });
    }
    @Override public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        return transaction(false, null, (store, session) -> {
            var renewed = store.renewBeforeReads(expected, ttl);
            if (renewed.isPresent()) {
                var retained = storedScope(session, expected.key());
                if (retained != null) validateState(session, retained);
            } else validateMutationScope(session, store, expected);
            return renewed;
        }, true);
    }
    @Override public boolean release(WorkloadClaim expected) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        return transaction(false, (store, session) -> {
            if (knownRetiredPipeline(session, expected.key())) return false;
            var retained = storedScope(session, expected.key());
            boolean released = store.release(expected);
            if (released && retained != null) fenceProgress(session, retained);
            return released;
        });
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
        return transaction(false, (store, session) -> {
            storedScope(session, key);
            return store.read(key);
        });
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
        access.updateState(session, access.state(session), new Document(), false);
    }

    private void validateState(ClientSession session, PipelineIntentScope retained) {
        var access = new MongoCachePipelineAccess(client, database, retained, null, requireActive);
        access.verifyStateStructure(operations.budget()); access.state(session);
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
            var decoded = new PipelineIntentScope(value.getString("clusterId"), binding, value.getString("cacheIncarnation"), value.getString("pipelineId"), generation);
            if (!value.equals(scopeDocument(decoded)) || !key.resourceId().equals(decoded.pipelineId()) || !key.clusterId().equals(decoded.clusterId())
                    || !scope.equals(new CacheBusinessScope(decoded.clusterId(), decoded.bindingVersion(), decoded.cacheIncarnation()))) throw unreadableScope();
            if (MongoCachePipelinePreparations.phase(row, decoded) != io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED) throw claimFenced();
            return decoded;
        } catch (NullPointerException | ClassCastException | IllegalArgumentException invalid) { throw unreadableScope(); }
    }

    private static Document scopeDocument(PipelineIntentScope value) {
        return new Document("clusterId", value.clusterId()).append("bindingVersion", value.bindingVersion())
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
        return StoreIo.call(() -> {
            try {
                if(operations.budget()==null)operations.requireAdmission();
                requireActive.run();
                requireValidationAuthority();
                var budget = operations.budget();
                if (budget != null) return boundedTransaction(admission,pipeline,operation,renewalFirst,budget);
                for (int attempt = 0; ; attempt++) {
                    try (var session = client.startSession()) {
                        var committed = session.withTransaction(() -> attempt(session,admission,pipeline,operation,renewalFirst,null), TRANSACTION);
                        requireValidationAuthority();
                        return committed;
                    } catch (MongoException conflict) {
                        // A duplicate-key error aborts its transaction; retry the original callback,
                        // never keep using that session or adopt another scope.
                        if (attempt >= 2 || ErrorCategory.fromErrorCode(conflict.getCode()) != ErrorCategory.DUPLICATE_KEY) throw conflict;
                    }
                }
            } catch (RuntimeException failed) {
                // A known closed/revoked owner is diagnosed before a driver's closed-client failure.
                requireActive.run();
                throw failed;
            }
        });
    }

    private <T> T attempt(ClientSession session, boolean admission, PipelineIntentScope pipeline,
            BiFunction<MongoWorkloadClaimStore,ClientSession,T> operation, boolean renewalFirst, MongoReadBudget budget) {
        if(budget!=null)operations.check();
        requireActive.run();
        var identity = new MongoCacheBusinessIdentity(database, scope);
        if (!renewalFirst) {
            if (admission) identity.admitClaim(session); else identity.requireAdmissionState(session);
        }
        requireCollection(budget==null?database:budget.database(database));
        var store = raw.inStrictSession(session);
        if (pipeline != null) {
            var access = new MongoCachePipelineAccess(client, database, pipeline, null, requireActive);
            access.verifyStateStructure(budget);
            var current = access.state(session);
            // A real per-Pipeline write serializes admission with controlled state replacement.
            if (admission) access.updateState(session, current, new Document(), false);
        }
        T result = operation.apply(store, session);
        // Renewal reads follow the exact lease mutation in the original transaction snapshot.
        if (renewalFirst) identity.requireAdmissionState(session);
        requireActive.run();
        if(budget!=null)operations.check();
        return result;
    }

    private <T> T boundedTransaction(boolean admission, PipelineIntentScope pipeline,
            BiFunction<MongoWorkloadClaimStore,ClientSession,T> operation, boolean renewalFirst, MongoReadBudget budget) {
        for(int attempt=0;;attempt++) {
            operations.check();
            var session=client.startSession();
            var owned=operations.own(session);
            boolean committedAck=false;
            try {
                var result=session.withTransaction(()->{
                    budget.remainingMillis();
                    owned.beginAttempt();
                    try {
                        var value=attempt(session,admission,pipeline,operation,renewalFirst,budget);
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
                operations.committed(owned);
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
