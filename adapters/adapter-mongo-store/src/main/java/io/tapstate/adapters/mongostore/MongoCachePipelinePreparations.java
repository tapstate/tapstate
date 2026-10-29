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
import io.tapstate.spi.store.PipelinePreparationInputs;
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
        var inputs = Objects.requireNonNull(original.inputs(),"inputs");
        for (var channel : inputs.directChannels()) if (!channel.context().scope().equals(preparation.scope())) throw rebuild(preparation.scope());
        requireIssued(preparation); positive(ttl); requireScope(preparation.scope());
        var retirement=MongoPipelineStatePreparations.retirement(original);
        var recovery=MongoPipelineStatePreparations.cacheRecovery(original);
        return transaction(preparation.scope(), true, original::requireActive, session -> {
            WorkloadClaimFence node = preparation.issuerNode();
            Date serverNow = proveNode(session, node);
            if (recovery.isPresent()) {
                if (!recovery.orElseThrow().targetScope().equals(preparation.scope())) throw rebuild(preparation.scope());
            } else if(retirement.isPresent()) {
                var result=retirement.orElseThrow();
                if(result.scope().stateGeneration()+1!=preparation.scope().stateGeneration() || !result.scope().pipelineId().equals(preparation.pipelineId()))throw rebuild(preparation.scope());
                if(MongoPipelineRetirementFacts.sameBusiness(result.scope(),preparation.scope()) && result.cache().isPresent())
                    return reserveRetired(session,preparation,inputs,result.cache().orElseThrow(),node,serverNow,ttl);
            } else if(preparation.scope().stateGeneration()!=1)throw rebuild(preparation.scope());
            if (claimRow(session, preparation.scope()) != null || stateRow(session, preparation.scope()) != null || inventoryExists(session, preparation.scope())) throw rebuild(preparation.scope());
            MongoPipelineSourcePreparations.requireEmpty(session,database,preparation.scope());
            MongoSinkPreparationStore.requireEmpty(session,database,preparation.scope());
            var key = pipelineKey(preparation.scope());
            Document row = new Document("_id", MongoWorkloadClaimStore.id(key)).append("clusterId", key.clusterId())
                    .append("resourceType", key.type().name()).append("resourceId", key.resourceId())
                    .append("ownerNodeId", node.owner().nodeId()).append("ownerBootId", node.owner().bootId())
                    .append("claimGeneration", 1L).append("executionGeneration", 0L).append("topologyRevision", node.topologyRevision())
                    .append("leaseUntil", Date.from(serverNow.toInstant().plusMillis(ttl.toMillis())))
                    .append("pipelineScope", MongoPipelinePreparationFacts.scope(preparation.scope()))
                    .append("pipelinePreparation", marker(preparation.preparationId(), Phase.RESERVED))
                    .append("pipelineSources",MongoPipelineSourcePreparations.encode(inputs))
                    .append("sinkPreparations",MongoSinkPreparationStore.initial(preparation.preparationId()));
            claims.insertOne(session, row);
            return new Reservation(preparation.scope(), preparation.preparationId(), Phase.RESERVED,
                    WorkloadClaimFence.from(raw(session).read(key).orElseThrow().claim()));
        });
    }

    @Override public Reservation resume(PipelinePreparation original, WorkloadClaimFence currentNode, Duration ttl) {
        requireIssued(original); requireScope(original.scope()); positive(ttl); requireNode(currentNode);
        return transaction(original.scope(), false, () -> {}, session -> {
            Document row = requireOriginal(session, original.scope(), original.preparationId());
            var inputs = MongoPipelineSourcePreparations.retained(session,database,original.scope());
            Phase phase = phase(row, original.scope());
            WorkloadClaimReading retained = raw(session).read(pipelineKey(original.scope())).orElseThrow();
            if (phase == Phase.PREPARED) {
                verifyInitial(session, original.scope(), retained.claim());
                return new Reservation(original.scope(), original.preparationId(), phase, WorkloadClaimFence.from(retained.claim()));
            }
            requireUnseeded(session,original.scope(),row);
            MongoSinkPreparationStore.requireEmpty(session,database,original.scope());
            MongoPipelineSourcePreparations.requireEmpty(session,database,original.scope());
            new MongoCacheBusinessIdentity(database, business).admitClaim(session);
            proveNode(session, currentNode);
            WorkloadClaim claim = retained.claim();
            requireInitialClaim(session,claim, original.scope());
            if (claim.claimGeneration() >= Long.MAX_VALUE - 1) throw rebuild(original.scope());
            // This helper cannot create a missing key. A concurrent delete aborts or yields no match.
            var acquired = raw(session).acquireExisting(claim.key(), currentNode.owner(), currentNode.topologyRevision(), ttl);
            if (!acquired.acquired()) throw claimFenced();
            requireInitialClaim(session,acquired.claim(), original.scope());
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
            var inputs = MongoPipelineSourcePreparations.retained(session,database,original.scope());
            WorkloadClaim claim = raw(session).read(pipelineKey(original.scope())).orElseThrow().claim();
            if (phase(row, original.scope()) == Phase.PREPARED) {
                if (!original.claim().equals(WorkloadClaimFence.from(claim))) throw claimFenced();
                verifyInitial(session, original.scope(), claim);
                return null;
            }
            requireInitialClaim(session,claim, original.scope());
            requireUnseeded(session,original.scope(),row);
            MongoSinkPreparationStore.requireEmpty(session,database,original.scope());
            Document filter = WorkloadClaimDocuments.live(original.claim())
                    .append("pipelineScope", MongoPipelinePreparationFacts.scope(original.scope()))
                    .append("pipelinePreparation", marker(original.preparationId(), Phase.RESERVED));
            if (claims.updateOne(session, filter, new Document("$set", new Document("pipelinePreparation", marker(original.preparationId(), Phase.PREPARED))))
                    .getMatchedCount() != 1) throw claimFenced();
            var prior=retiredBirth(row,original.scope());
            var initial=new Document("_id",original.scope().pipelineId()).append("scope",MongoPipelinePreparationFacts.scope(original.scope()))
                    .append("stateJson",StateJson.of(PipelineState.NEW)).append("epoch",prior.map(value -> value.epoch()+1).orElse(0L))
                    .append("touchMillis",touchTime.toEpochMilli()).append("runtimeWriteFence",prior.map(value -> value.writeFence()+1).orElse(1L));
            if(prior.isEmpty())states.insertOne(session,initial);
            else {
                var old=prior.orElseThrow();
                if(states.replaceOne(session,new Document("_id",original.scope().pipelineId()).append("scope",MongoPipelinePreparationFacts.scope(old.scope()))
                        .append("epoch",old.epoch()).append("runtimeWriteFence",old.writeFence()),initial).getMatchedCount()!=1)throw rebuild(original.scope());
            }
            MongoOperatorInventory.prepareInitial(session, database, original.scope(), original.preparationId());
            MongoPipelineSourcePreparations.seedInitial(session,database,original.scope(),inputs);
            MongoSnapshotStateStore.seedInitial(session,database,original.scope());
            return null;
        });
        requireActive.run();
        return new Receipt(original.scope(), original.preparationId());
    }

    @Override public void verifySources(PipelineIntentScope scope,PipelinePreparationInputs expected) {
        Objects.requireNonNull(expected,"expected"); requireScope(scope);
        var access = new MongoCachePipelineAccess(client,database,scope,null,requireActive);
        access.metadataTransaction(SystemCollections.DIRECT_CHANNEL_OFFSETS,false,session -> {
            new MongoCacheMetadataStructure(database,MongoRateHistoryStore.DEFAULT_RETENTION).verifyDirect();
            var original = MongoPipelineSourcePreparations.retained(session,database,scope);
            if (!original.sameRecovery(expected)) throw MongoPipelineSourcePreparations.identityUnproven(scope);
            var added = new java.util.TreeSet<String>();
            expected.selectedTables().forEach((source,tables) -> tables.forEach(table -> {
                if (!original.selectedTables().getOrDefault(source,java.util.Set.of()).contains(table))
                    added.add(source + "." + table);
            }));
            if (!added.isEmpty()) throw new TapstateException(IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED,
                    Map.of("pipeline",scope.pipelineId(),"tables",String.join(", ",added)),null);
            MongoPipelineSourcePreparations.verifyAll(session,client,database,scope,original,requireActive);
            MongoSnapshotStateStore.order(session,database,scope); return null;
        });
    }

    /** Only the verification path above can construct a production completion receipt. */
    private record Receipt(PipelineIntentScope scope, String preparationId) implements PipelinePreparationReceipt {}
    static void requireVerifiedReceipt(PipelinePreparationReceipt receipt) {
        if (!(receipt instanceof Receipt)) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
    }

    private Document requireOriginal(ClientSession session, PipelineIntentScope scope, String preparationId) {
        Document row = claimRow(session, scope);
        if (row == null) throw rebuild(scope);
        if (raw(session).read(pipelineKey(scope)).isEmpty()) throw new java.util.NoSuchElementException("No value present");
        try {
            if (!(row.get("pipelineScope") instanceof Document stored) || !scope.equals(MongoPipelinePreparationFacts.scope(stored))) throw rebuild(scope);
            phase(row, scope);
            MongoSinkPreparationStore.requireInitial(row,preparationId,scope);
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
        requireInitialClaim(session,claim, scope);
        Document row = stateRow(session, scope);
        var prior=retiredBirth(claimRow(session,scope),scope);
        if (row == null || !MongoPipelinePreparationFacts.scope(scope).equals(row.get("scope"))
                || !prior.map(value -> value.epoch()+1).orElse(0L).equals(row.get("epoch"))
                || !prior.map(value -> value.writeFence()+1).orElse(1L).equals(row.get("runtimeWriteFence"))
                || !StateJson.of(PipelineState.NEW).equals(row.get("stateJson")) || !(row.get("touchMillis") instanceof Long)) throw rebuild(scope);
        MongoSinkPreparationStore.requireEmpty(session,database,scope);
        var inputs = MongoPipelineSourcePreparations.retained(session,database,scope);
        MongoPipelineSourcePreparations.verifyInitial(session,database,scope,inputs);
        MongoSnapshotStateStore.verifyInitial(session,database,scope);
        try { MongoOperatorInventory.requireInitial(session, database, scope,
                claimRow(session, scope).get("pipelinePreparation", Document.class).getString("preparationId")); }
        catch (TapstateException invalid) { throw rebuild(scope); }
    }
    private void requireInitialClaim(ClientSession session,WorkloadClaim claim, PipelineIntentScope scope) {
        // Reuse preserves the complete execution history; the captured terminal predecessor proves it.
        var row=claimRow(session,scope);var prior=retiredBirth(row,scope);
            if(prior.isPresent()) {
                var old=prior.orElseThrow();
                if(claim.claimGeneration()<=old.claim().claimGeneration() || claim.claimGeneration()>=Long.MAX_VALUE-1
                        || claim.executionGeneration()!=old.claim().executionGeneration())throw rebuild(scope);
                return;
            }
        if (claim.executionGeneration() != 0 || claim.contextExecutionGeneration() != 0 || claim.executionClaimGeneration() != 0
                || !claim.executionNodeIds().isEmpty() || claim.failureClaimGeneration() != 0 || claim.failureAfterMemberLoss()
                || claim.claimGeneration() >= Long.MAX_VALUE - 1 || claim.topologyRevision() == Long.MAX_VALUE) throw rebuild(scope);
    }

    private Reservation reserveRetired(ClientSession session,PipelinePreparation preparation,PipelinePreparationInputs inputs,
            io.tapstate.spi.store.PipelineRetiredState previous,WorkloadClaimFence node,Date now,Duration ttl) {
        var scope=preparation.scope();new MongoCachePipelineRetirements(client,database,business,requireActive).verifyRetired(session,previous);
        if(previous.claim().claimGeneration()>=Long.MAX_VALUE-2 || previous.epoch()>=Long.MAX_VALUE-2 || previous.writeFence()>=Long.MAX_VALUE-2
                || previous.scope().stateGeneration()>=scope.stateGeneration() || inventoryExists(session,scope))throw rebuild(scope);
        MongoPipelineSourcePreparations.requireEmpty(session,database,scope);MongoSinkPreparationStore.requireEmpty(session,database,scope);
        var expected=WorkloadClaimDocuments.live(previous.claim());expected.remove("$expr");
        expected.append("pipelineScope",MongoPipelinePreparationFacts.scope(previous.scope())).append("pipelineRetirement",MongoPipelineRetirementFacts.cache(previous))
                .append("pipelinePreparation",marker(previous.preparationId(),Phase.RETIRED));
        var values=new Document("ownerNodeId",node.owner().nodeId()).append("ownerBootId",node.owner().bootId())
                .append("claimGeneration",previous.claim().claimGeneration()+1).append("leaseUntil",Date.from(now.toInstant().plusMillis(ttl.toMillis())))
                .append("pipelineScope",MongoPipelinePreparationFacts.scope(scope)).append("pipelinePreparation",marker(preparation.preparationId(),Phase.RESERVED))
                .append("pipelineSources",MongoPipelineSourcePreparations.encode(inputs)).append("sinkPreparations",MongoSinkPreparationStore.initial(preparation.preparationId()));
        if(claims.updateOne(session,expected,new Document("$set",values)).getMatchedCount()!=1)throw claimFenced();
        return new Reservation(scope,preparation.preparationId(),Phase.RESERVED,WorkloadClaimFence.from(raw(session).read(pipelineKey(scope)).orElseThrow().claim()));
    }
    private java.util.Optional<io.tapstate.spi.store.PipelineRetiredState> retiredBirth(Document row,PipelineIntentScope scope) {
        if(row==null || row.get("pipelineRetirement")==null)return java.util.Optional.empty();
        var retired=MongoPipelineRetirementFacts.cache(row.get("pipelineRetirement",Document.class));
        if(!MongoPipelineRetirementFacts.sameBusiness(scope,retired.scope()) || !scope.pipelineId().equals(retired.scope().pipelineId())
                || scope.stateGeneration()<=retired.scope().stateGeneration())throw rebuild(scope);
        return java.util.Optional.of(retired);
    }
    private void requireUnseeded(ClientSession session,PipelineIntentScope scope,Document claim) {
        if(inventoryExists(session,scope))throw rebuild(scope);
        var previous=retiredBirth(claim,scope);var state=stateRow(session,scope);
        if(previous.isEmpty()){if(state!=null)throw rebuild(scope);return;}
        var old=previous.orElseThrow();MongoOperatorInventory.requireDeleted(session,database,old);
        if(state==null || !MongoPipelinePreparationFacts.scope(old.scope()).equals(state.get("scope"))
                || !Long.valueOf(old.epoch()).equals(state.get("epoch")) || !Long.valueOf(old.writeFence()).equals(state.get("runtimeWriteFence"))
                || !StateJson.of(PipelineState.STOPPED).equals(state.get("stateJson")))throw rebuild(scope);
    }

    /** Strict original node proof and an actual write serialize preparation with node revocation. */
    private Date proveNode(ClientSession session, WorkloadClaimFence node) {
        return proveNode(session, database, business, node);
    }
    static Date proveNode(ClientSession session, MongoDatabase database, CacheBusinessScope business, WorkloadClaimFence node) {
        Objects.requireNonNull(node, "node");
        if (node.key().type() != WorkloadClaimType.NODE_SESSION || !business.clusterId().equals(node.key().clusterId())
                || !node.key().resourceId().equals(node.owner().nodeId())) throw claimFenced();
        var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        var reading = new MongoWorkloadClaimStore(claims).inStrictSession(session).read(node.key()).orElseThrow(MongoCachePipelinePreparations::claimFenced);
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
    private boolean inventoryExists(ClientSession session, PipelineIntentScope scope) {
        return SystemCollections.CACHE_OPERATOR_CLEANUP.on(database).find(session, new Document("_id", MongoPipelinePreparationFacts.scope(scope))).first() != null;
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
                        new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).verifyFacet(SystemCollections.CACHE_OPERATOR_CLEANUP);
                        new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).verifyDirect();
                        new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).verifyFacet(SystemCollections.CACHE_SINK_PREPARATIONS);
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
