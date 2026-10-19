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
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    final MongoDatabase database;
    final PipelineIntentScope scope;
    private final MongoClient client;
    private final WorkloadClaimFence original;
    private final Runnable requireActive;
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
        this.operatorDatabases = operatorDatabases;
        this.rateRetention = MongoCacheMetadataStructure.requireRetention(rateRetention);
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.original = original;
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (original != null && (original.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !scope.clusterId().equals(original.key().clusterId()) || !scope.pipelineId().equals(original.key().resourceId()))) throw fenced();
    }

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

    <T> T metadataTransaction(SystemCollections table, boolean mutation, Function<ClientSession, T> action) {
        return metadataTransaction(table, database, mutation, action);
    }

    <T> T metadataTransaction(SystemCollections table, MongoDatabase target, boolean mutation, Function<ClientSession, T> action) {
        T result = transaction(session -> {
            Document current = state(session);
            if (!database.getName().equals(target.getName())) {
                String preparation = MongoCacheOperatorDatabases.require(session, target, operatorDatabases);
                MongoOperatorInventory.requireDatabase(MongoOperatorInventory.require(session, database, scope), target.getName(), preparation, scope);
            }
            new MongoCacheMetadataStructure(target, rateRetention).verifyFacet(table);
            if (mutation) { proveClaim(session); updateState(session, current, new Document(), false); }
            return action.apply(session);
        });
        requireActive.run(); return result;
    }

    <T> T operatorPreparation(Function<ClientSession, T> action) {
        var local = TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
        return transaction(false, local, session -> {
            Document current = state(session); proveClaim(session); updateState(session, current, new Document(), false);
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

    void requirePipeline(String pipelineId) { if (!scope.pipelineId().equals(pipelineId)) throw fenced(); }

    Document state(ClientSession session) {
        Document state = SystemCollections.CACHE_PIPELINE_STATE.on(database).find(session, new Document("_id", scope.pipelineId())).first();
        if (state == null) throw unreadable("checkpoint");
        requireScope(state);
        if (!(state.get("epoch") instanceof Long epoch) || epoch < 0 || epoch == Long.MAX_VALUE
                || !(state.get("runtimeWriteFence") instanceof Long revision) || revision < 1 || revision == Long.MAX_VALUE
                || !(state.get("stateJson") instanceof String) || !(state.get("touchMillis") instanceof Long)) throw unreadable("checkpoint");
        requirePreparedClaim(session);
        return state;
    }

    private void requirePreparedClaim(ClientSession session) {
        MongoCacheWorkloadClaimStore.requireCollection(database);
        var key = new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
        var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        Document row = table.find(session, new Document("_id", MongoWorkloadClaimStore.id(key))).first();
        if (row == null) throw MongoCachePipelinePreparations.rebuild(scope);
        new MongoWorkloadClaimStore(table).inStrictSession(session).decodeStored(row);
        try {
            if (!(row.get("pipelineScope") instanceof Document stored)) throw unreadable("pipeline-claim-scope");
            var decoded = MongoPipelinePreparationFacts.scope(stored);
            if (!scope.pipelineId().equals(decoded.pipelineId()) || !new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation())
                    .equals(new CacheBusinessScope(decoded.clusterId(), decoded.bindingVersion(), decoded.cacheIncarnation()))) throw unreadable("pipeline-claim-scope");
            if (!scope.equals(decoded)) throw claimFenced();
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable("pipeline-claim-scope"); }
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

    void proveClaim(ClientSession session) { proveClaim(session,"fencedPipelineWrites"); }

    void proveConsumerClaim(ClientSession session) { proveClaim(session,"fencedConsumerWrites"); }

    private void proveClaim(ClientSession session,String counter) {
        if (original == null) throw fenced();
        MongoCacheWorkloadClaimStore.requireCollection(database);
        if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session, WorkloadClaimDocuments.live(original)
                        .append("pipelineScope", MongoPipelinePreparationFacts.scope(scope)).append("pipelinePreparation.phase", "PREPARED"),
                new Document("$inc",new Document(counter,1L))).getMatchedCount() != 1) throw claimFenced();
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

    void verifyStateStructure() { verifyStructure(SystemCollections.CACHE_PIPELINE_STATE); }

    private void verifyStructure(SystemCollections table) {
        var found = database.listCollections().filter(new Document("name", table.collectionName())).first();
        if (found == null || !"collection".equals(found.get("type"))) throw unreadable("structure");
        var options = found.get("options", Document.class);
        if (options != null && (Boolean.TRUE.equals(options.get("capped")) || options.containsKey("timeseries"))) throw unreadable("structure");
        var indexes = table.on(database).listIndexes().into(new ArrayList<>());
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
