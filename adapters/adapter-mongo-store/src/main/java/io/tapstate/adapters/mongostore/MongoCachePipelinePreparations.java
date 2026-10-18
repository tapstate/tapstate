package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.FirstPipelineCreation;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelinePreparationReceipt;
import io.tapstate.spi.store.PipelineStatePreparations;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/** Insert-once first preparation and existing-only continuation on one original CACHE client. */
final class MongoCachePipelinePreparations implements PipelineCachePreparations {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope business;
    private final Runnable requireActive;
    private final MongoCollection<Document> claims;
    private final MongoCollection<Document> states;

    MongoCachePipelinePreparations(MongoClient client, MongoDatabase database, CacheBusinessScope business, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.business = Objects.requireNonNull(business, "business"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        this.states = SystemCollections.CACHE_PIPELINE_STATE.on(database);
    }

    @Override public Reservation reserveFirst(FirstPipelineCreation invocation, Duration ttl) {
        // Consumption precedes every metadata callback, guard, validation and IO in this logical call.
        FirstPipelineCreation original = MongoPipelineStatePreparations.consumeFirst(invocation);
        PipelinePreparation preparation = original.preparation();
        requireIssued(preparation); positive(ttl); requireScope(preparation.scope());
        return transaction(preparation.scope(), true, original::requireActive, session -> {
            WorkloadClaimFence node = preparation.issuerNode();
            Date serverNow = proveNode(session, node);
            if (claimRow(session, preparation.scope()) != null || stateRow(session, preparation.scope()) != null) throw rebuild(preparation.scope());
            var key = pipelineKey(preparation.scope());
            Document row = new Document("_id", MongoWorkloadClaimStore.id(key)).append("clusterId", key.clusterId())
                    .append("resourceType", key.type().name()).append("resourceId", key.resourceId())
                    .append("ownerNodeId", node.owner().nodeId()).append("ownerBootId", node.owner().bootId())
                    .append("claimGeneration", 1L).append("executionGeneration", 0L).append("topologyRevision", node.topologyRevision())
                    .append("leaseUntil", Date.from(serverNow.toInstant().plusMillis(ttl.toMillis())))
                    .append("pipelineScope", MongoPipelinePreparationFacts.scope(preparation.scope()))
                    .append("pipelinePreparation", marker(preparation.preparationId(), Phase.RESERVED));
            claims.insertOne(session, row);
            return new Reservation(preparation.scope(), preparation.preparationId(), Phase.RESERVED,
                    WorkloadClaimFence.from(raw(session).read(key).orElseThrow().claim()));
        });
    }

    @Override public Reservation resume(PipelinePreparation original, WorkloadClaimFence currentNode, Duration ttl) {
        requireIssued(original); requireScope(original.scope()); positive(ttl); requireNode(currentNode);
        return transaction(original.scope(), false, () -> {}, session -> {
            Document row = requireOriginal(session, original.scope(), original.preparationId());
            Phase phase = phase(row, original.scope());
            WorkloadClaimReading retained = raw(session).read(pipelineKey(original.scope())).orElseThrow();
            if (phase == Phase.PREPARED) {
                verifyInitial(session, original.scope(), retained.claim());
                return new Reservation(original.scope(), original.preparationId(), phase, WorkloadClaimFence.from(retained.claim()));
            }
            if (stateRow(session, original.scope()) != null) throw rebuild(original.scope());
            new MongoCacheBusinessIdentity(database, business).admitClaim(session);
            proveNode(session, currentNode);
            WorkloadClaim claim = retained.claim();
            requireInitialClaim(claim, original.scope());
            if (claim.claimGeneration() >= Long.MAX_VALUE - 1) throw rebuild(original.scope());
            // This helper cannot create a missing key. A concurrent delete aborts or yields no match.
            var acquired = raw(session).acquireExisting(claim.key(), currentNode.owner(), currentNode.topologyRevision(), ttl);
            if (!acquired.acquired()) throw claimFenced();
            requireInitialClaim(acquired.claim(), original.scope());
            return new Reservation(original.scope(), original.preparationId(), Phase.RESERVED, WorkloadClaimFence.from(acquired.claim()));
        });
    }

    @Override public PipelinePreparationReceipt seed(Reservation original, PipelineStatePreparations configuration, Instant touchTime) {
        Objects.requireNonNull(original, "original"); Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(touchTime, "touchTime"); requireScope(original.scope());
        requireActive.run();
        // Cross-domain validation is deliberately outside the CACHE transaction and its retry callback.
        configuration.requireIssued(original.scope(), original.preparationId());
        transaction(original.scope(), false, () -> {}, session -> {
            Document row = requireOriginal(session, original.scope(), original.preparationId());
            WorkloadClaim claim = raw(session).read(pipelineKey(original.scope())).orElseThrow().claim();
            if (phase(row, original.scope()) == Phase.PREPARED) {
                if (!original.claim().equals(WorkloadClaimFence.from(claim))) throw claimFenced();
                verifyInitial(session, original.scope(), claim);
                return null;
            }
            requireInitialClaim(claim, original.scope());
            if (stateRow(session, original.scope()) != null) throw rebuild(original.scope());
            Document filter = WorkloadClaimDocuments.live(original.claim())
                    .append("pipelineScope", MongoPipelinePreparationFacts.scope(original.scope()))
                    .append("pipelinePreparation", marker(original.preparationId(), Phase.RESERVED));
            if (claims.updateOne(session, filter, new Document("$set", new Document("pipelinePreparation", marker(original.preparationId(), Phase.PREPARED))))
                    .getMatchedCount() != 1) throw claimFenced();
            states.insertOne(session, new Document("_id", original.scope().pipelineId())
                    .append("scope", MongoPipelinePreparationFacts.scope(original.scope())).append("stateJson", StateJson.of(PipelineState.NEW))
                    .append("epoch", 0L).append("touchMillis", touchTime.toEpochMilli()).append("runtimeWriteFence", 1L));
            return null;
        });
        requireActive.run();
        return new Receipt(original.scope(), original.preparationId());
    }

    /** Only the verification path above can construct a production completion receipt. */
    private record Receipt(PipelineIntentScope scope, String preparationId) implements PipelinePreparationReceipt {}
    static void requireVerifiedReceipt(PipelinePreparationReceipt receipt) {
        if (!(receipt instanceof Receipt)) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
    }

    private Document requireOriginal(ClientSession session, PipelineIntentScope scope, String preparationId) {
        Document row = claimRow(session, scope);
        if (row == null) throw rebuild(scope);
        raw(session).read(pipelineKey(scope)).orElseThrow();
        try {
            if (!(row.get("pipelineScope") instanceof Document stored) || !scope.equals(MongoPipelinePreparationFacts.scope(stored))) throw rebuild(scope);
            phase(row, scope);
            if (!preparationId.equals(row.get("pipelinePreparation", Document.class).getString("preparationId"))) throw rebuild(scope);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw rebuild(scope); }
        return row;
    }

    static Phase phase(Document row, PipelineIntentScope scope) {
        try {
            if (!(row.get("pipelinePreparation") instanceof Document marker)
                    || !marker.keySet().equals(Set.of("preparationId", "phase"))) throw rebuild(scope);
            String id = marker.getString("preparationId");
            if (!UUID.fromString(id).toString().equals(id)) throw rebuild(scope);
            return Phase.valueOf(marker.getString("phase"));
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw rebuild(scope); }
    }

    private void verifyInitial(ClientSession session, PipelineIntentScope scope, WorkloadClaim claim) {
        requireInitialClaim(claim, scope);
        Document row = stateRow(session, scope);
        if (row == null || !MongoPipelinePreparationFacts.scope(scope).equals(row.get("scope"))
                || !Long.valueOf(0).equals(row.get("epoch")) || !Long.valueOf(1).equals(row.get("runtimeWriteFence"))
                || !StateJson.of(PipelineState.NEW).equals(row.get("stateJson")) || !(row.get("touchMillis") instanceof Long)) throw rebuild(scope);
    }
    private void requireInitialClaim(WorkloadClaim claim, PipelineIntentScope scope) {
        if (claim.executionGeneration() != 0 || claim.contextExecutionGeneration() != 0 || claim.executionClaimGeneration() != 0
                || !claim.executionNodeIds().isEmpty() || claim.failureClaimGeneration() != 0 || claim.failureAfterMemberLoss()
                || claim.claimGeneration() >= Long.MAX_VALUE - 1 || claim.topologyRevision() == Long.MAX_VALUE) throw rebuild(scope);
    }

    /** Strict original node proof and an actual write serialize preparation with node revocation. */
    private Date proveNode(ClientSession session, WorkloadClaimFence node) {
        requireNode(node);
        var reading = raw(session).read(node.key()).orElseThrow(MongoCachePipelinePreparations::claimFenced);
        if (!node.equals(WorkloadClaimFence.from(reading.claim())) || !reading.leased()
                || node.claimGeneration() >= Long.MAX_VALUE - 1 || node.executionGeneration() == Long.MAX_VALUE
                || node.topologyRevision() == Long.MAX_VALUE) throw claimFenced();
        if (claims.updateOne(session, WorkloadClaimDocuments.live(node),
                new Document("$inc", new Document("fencedPreparationWrites", 1L))).getMatchedCount() != 1) throw claimFenced();
        // This query's $$NOW is server time from the same CACHE client/session, never client wall time.
        Document time = claims.aggregate(session, List.of(new Document("$match", new Document("_id", MongoWorkloadClaimStore.id(node.key()))),
                new Document("$project", new Document("_id", 0).append("serverNow", "$$NOW")))).first();
        if (time == null || !(time.get("serverNow") instanceof Date now)) throw claimFenced();
        return now;
    }
    private void requireNode(WorkloadClaimFence node) {
        Objects.requireNonNull(node, "node");
        if (node.key().type() != WorkloadClaimType.NODE_SESSION || !business.clusterId().equals(node.key().clusterId())
                || !node.key().resourceId().equals(node.owner().nodeId())) throw claimFenced();
    }
    private void requireScope(PipelineIntentScope scope) {
        if (!business.equals(new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()))) throw MongoCacheBusinessIdentity.businessFenced();
    }
    private static void requireIssued(PipelinePreparation original) {
        Objects.requireNonNull(original, "original");
        if (original.phase() != PipelinePreparation.Phase.ISSUED) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
    }
    private Document claimRow(ClientSession session, PipelineIntentScope scope) { return claims.find(session, new Document("_id", MongoWorkloadClaimStore.id(pipelineKey(scope)))).first(); }
    private Document stateRow(ClientSession session, PipelineIntentScope scope) { return states.find(session, new Document("_id", scope.pipelineId())).first(); }
    private MongoWorkloadClaimStore raw(ClientSession session) { return new MongoWorkloadClaimStore(claims).inStrictSession(session); }
    private static WorkloadClaimKey pipelineKey(PipelineIntentScope scope) { return new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId()); }
    static Document marker(String preparationId, Phase phase) { return new Document("preparationId", preparationId).append("phase", phase.name()); }
    private static void positive(Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero() || ttl.toMillis() < 1) throw new IllegalArgumentException("preparation ttl must be at least one millisecond");
    }
    static TapstateException rebuild(PipelineIntentScope scope) { return new TapstateException(IoError.PIPELINE_STATE_REBUILD_REQUIRED, Map.of("pipeline", scope.pipelineId()), null); }
    private static TapstateException claimFenced() { return MongoCachePipelineAccess.claimFenced(); }

    private <T> T transaction(PipelineIntentScope scope, boolean admission, Runnable originalActive, Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run(); originalActive.run();
                try (var session = client.startSession()) {
                    T result = session.withTransaction(() -> {
                        requireActive.run(); originalActive.run();
                        var identity = new MongoCacheBusinessIdentity(database, business);
                        if (admission) identity.admitClaim(session); else identity.requireAdmissionState(session);
                        MongoCacheWorkloadClaimStore.requireCollection(database);
                        new MongoCachePipelineAccess(client, database, scope, null, requireActive).verifyStateStructure();
                        T value = action.apply(session);
                        requireActive.run(); originalActive.run();
                        return value;
                    }, TRANSACTION);
                    requireActive.run(); originalActive.run();
                    return result;
                }
            } catch (RuntimeException failure) { requireActive.run(); originalActive.run(); throw failure; }
        });
    }
}
