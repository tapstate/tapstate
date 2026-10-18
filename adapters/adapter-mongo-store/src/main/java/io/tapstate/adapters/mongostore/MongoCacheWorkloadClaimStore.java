package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** One fixed business scope; no claim transition uses another client or an unbound transaction. */
final class MongoCacheWorkloadClaimStore implements WorkloadClaimStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final MongoWorkloadClaimStore raw;

    MongoCacheWorkloadClaimStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client");
        this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
    }

    @Override public WorkloadClaimAttempt acquire(WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        requireKey(key);
        if (key.type() == WorkloadClaimType.PIPELINE_ACTUATION) throw MongoCacheBusinessIdentity.businessFenced();
        return transaction(true, store -> store.acquire(key, owner, topologyRevision, ttl));
    }
    @Override public WorkloadClaimAttempt acquirePipeline(PipelineIntentScope original, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        Objects.requireNonNull(original, "scope");
        if (!scope.equals(new CacheBusinessScope(original.clusterId(), original.bindingVersion(), original.cacheIncarnation())))
            throw MongoCacheBusinessIdentity.businessFenced();
        var key = new WorkloadClaimKey(original.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, original.pipelineId());
        return transaction(true, original, store -> store.acquire(key, owner, topologyRevision, ttl));
    }
    @Override public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        return transaction(false, store -> store.renew(expected, ttl));
    }
    @Override public boolean release(WorkloadClaim expected) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        return transaction(false, store -> store.release(expected));
    }
    @Override public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision, Set<String> nodeIds) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        return transaction(false, store -> store.advanceExecution(expected, topologyRevision, nodeIds));
    }
    @Override public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean afterMemberLoss) {
        requireKey(Objects.requireNonNull(expected, "expected").key());
        return transaction(false, store -> store.recordExecutionFailure(expected, afterMemberLoss));
    }
    @Override public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
        requireKey(key);
        return transaction(false, store -> store.read(key));
    }
    @Override public Map<WorkloadClaimKey, WorkloadClaimReading> readAll(Collection<WorkloadClaimKey> keys) {
        Objects.requireNonNull(keys, "keys");
        var captured = java.util.List.copyOf(keys);
        if (captured.isEmpty()) return Map.of();
        captured.forEach(this::requireKey);
        return transaction(false, store -> store.readAll(captured));
    }

    private <T> T transaction(boolean admission, Function<MongoWorkloadClaimStore, T> operation) {
        return transaction(admission, null, operation);
    }

    private <T> T transaction(boolean admission, PipelineIntentScope pipeline, Function<MongoWorkloadClaimStore, T> operation) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                for (int attempt = 0; ; attempt++) {
                    try (var session = client.startSession()) {
                        return session.withTransaction(() -> {
                            requireActive.run();
                            var identity = new MongoCacheBusinessIdentity(database, scope);
                            if (admission) identity.admitClaim(session); else identity.requireAdmissionState(session);
                            requireCollection(database);
                            if (pipeline != null) {
                                var access = new MongoCachePipelineAccess(client, database, pipeline, null, requireActive);
                                access.verifyStructures();
                                var current = access.state(session);
                                // A real per-Pipeline write serializes admission with state replacement,
                                // including a first claim absent from the replacement's claim snapshot.
                                access.updateState(session, current, new Document(), false);
                            }
                            T result = operation.apply(raw.inSession(session));
                            requireActive.run();
                            return result;
                        }, TRANSACTION);
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

    private void requireKey(WorkloadClaimKey key) {
        if (!scope.clusterId().equals(Objects.requireNonNull(key, "key").clusterId())) throw MongoCacheBusinessIdentity.businessFenced();
    }

    static void requireCollection(MongoDatabase database) {
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS;
        Document found = database.listCollections().filter(new Document("name", table.collectionName())).first();
        if (found == null || !"collection".equals(found.get("type"))) throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "cache-business", "field", "workload-claims"), null);
    }
}
