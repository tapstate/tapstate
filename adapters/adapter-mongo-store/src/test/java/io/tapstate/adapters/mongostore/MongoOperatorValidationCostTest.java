package io.tapstate.adapters.mongostore;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.client.*;
import com.mongodb.client.result.UpdateResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Counts the actual validation calls without substituting the scoped operator implementation. */
class MongoOperatorValidationCostTest {
    @Test void hotReadsReuseTheSameStructureProof() {
        var f = new Fixture();
        f.store.load(f.at.mapName(), "one");
        int descriptions = f.descriptionReads;
        var indexes = Map.copyOf(f.indexReads);
        assertThat(descriptions).isPositive();
        assertThat(indexes).containsKeys("pipeline_state", "nest_state", "nest_shapes", "operator_cleanup");

        f.store.load(f.at.mapName(), "two");

        assertThat(f.descriptionReads).isEqualTo(descriptions);
        assertThat(f.indexReads).isEqualTo(indexes);
    }

    @Test void retryRechecksEveryIndexAndReadsFreshDescriptions() {
        var oneAttempt = new Fixture();
        oneAttempt.store.load(oneAttempt.at.mapName(), "one");
        var retried = new Fixture();
        retried.attempts = 2;
        retried.store.load(retried.at.mapName(), "one");

        assertThat(retried.descriptionReads).isGreaterThan(oneAttempt.descriptionReads);
        oneAttempt.indexReads.forEach((name, count) ->
                assertThat(retried.indexReads).containsEntry(name, count * 2));
    }

    @Test void aWriteAfterAReadMustProveTheCollectionStillExists() {
        var f = new Fixture();
        f.store.load(f.at.mapName(), "one");
        f.missingCollection = "nest_state";
        assertThatThrownBy(() -> f.store.save(f.at.mapName(), "two", new byte[]{1}))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void aRetryCannotReuseACollectionDescriptionFromTheFailedAttempt() {
        var f = new Fixture();
        f.attempts = 2;
        f.afterAttempt = () -> f.missingCollection = "nest_shapes";
        assertThatThrownBy(() -> f.store.load(f.at.mapName(), "one"))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void aHotBatchDoesNotReopenTheClaimAndInventoryProof() {
        var f = new Fixture();
        assertThat(f.store.loadAll(f.at.mapName(), List.of("one", "two"))).isEmpty();
        var coldFinds = Map.copyOf(f.finds);
        assertThat(coldFinds).containsKeys("workload_claims", "operator_cleanup", "nest_shapes", "nest_state");

        assertThat(f.store.loadAll(f.at.mapName(), List.of("three", "four"))).isEmpty();

        assertThat(f.finds).containsEntry("workload_claims", coldFinds.get("workload_claims"))
                .containsEntry("operator_cleanup", coldFinds.get("operator_cleanup"))
                .containsEntry("nest_shapes", coldFinds.get("nest_shapes"))
                .containsEntry("nest_state", coldFinds.get("nest_state") + 1);
    }

    @Test void everyRetryRevalidatesTheClaimAndInventory() {
        var oneAttempt = new Fixture();
        oneAttempt.store.loadAll(oneAttempt.at.mapName(), List.of("one", "two"));
        var retried = new Fixture();
        retried.attempts = 2;
        retried.store.loadAll(retried.at.mapName(), List.of("one", "two"));

        assertThat(retried.finds).containsEntry("workload_claims", 2 * oneAttempt.finds.get("workload_claims"))
                .containsEntry("operator_cleanup", 2 * oneAttempt.finds.get("operator_cleanup"));
    }

    @Test void aNewAttemptCannotReuseAnEarlierPreparation() {
        var f = new Fixture();
        f.attempts = 2;
        f.afterAttempt = () -> f.rows.get("workload_claims").put("pipelinePreparation",
                new Document("preparationId", UUID.randomUUID().toString()).append("phase", "PREPARED"));
        assertThatThrownBy(() -> f.store.load(f.at.mapName(), "one"))
                .isInstanceOfSatisfying(TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void aWriteAfterAReadMustProveTheIndexStillExists() {
        var f = new Fixture();
        f.store.load(f.at.mapName(), "one");
        f.missingIndex = "operator_placement";
        assertThatThrownBy(() -> f.store.save(f.at.mapName(), "two", new byte[]{1}))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test void hotWritesKeepTheLiveClaimAndInventoryFence() {
        var f = new Fixture();
        f.store.saveAll(f.at.mapName(), Map.of("one", new byte[]{1}, "two", new byte[]{2}));
        var coldUpdates = Map.copyOf(f.updates);
        int inventoryReads = f.finds.get("operator_cleanup");
        assertThat(coldUpdates).containsKeys("workload_claims", "pipeline_state");

        f.store.save(f.at.mapName(), "three", new byte[]{3});

        assertThat(f.updates).containsEntry("workload_claims", coldUpdates.get("workload_claims") + 1)
                .containsEntry("pipeline_state", coldUpdates.get("pipeline_state"));
        assertThat(f.finds).containsEntry("operator_cleanup", inventoryReads + 1);
    }

    private static final class Fixture {
        private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
        private static final String PREPARATION = "c51674fb-2469-4ebc-b4e9-5c34a41c2817";
        final MongoDatabase db = mock(MongoDatabase.class);
        final MongoClient client = mock(MongoClient.class);
        final ClientSession session = mock(ClientSession.class);
        final Map<String, Document> rows = new HashMap<>();
        final Map<String, Integer> finds = new HashMap<>(), indexReads = new HashMap<>(), updates = new HashMap<>();
        final Map<String, MongoCollection<Document>> collections = new HashMap<>();
        final OperatorPlacement at = new OperatorPlacement(SCOPE, "cache-db", OperatorPlacement.Resource.NEST_STATE,
                "nest", "root", "state");
        final MongoCacheOperatorStateStore store;
        int attempts = 1;
        int descriptionReads;
        Runnable afterAttempt = () -> {};
        String missingIndex;
        String missingCollection;

        @SuppressWarnings("unchecked")
        Fixture() {
            var fence = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                    new WorkloadOwner("node", "boot"), 1, 0, 0);
            rows.put("cache_identity", new Document("clusterId", "cluster").append("bindingVersion", 1L)
                    .append("cacheIncarnation", "cache").append("layoutId", MongoConfigurationDomain.LAYOUT)
                    .append("businessPhase", "READY").append("businessPreparationId", PREPARATION).append("claimAdmissionFence", 1L));
            rows.put("schema_migrations", new Document("installedVersion", 1).append("phase", "READY")
                    .append("operationId", PREPARATION).append("epoch", 1L));
            rows.put("pipeline_state", new Document("_id", "orders").append("scope", MongoPipelinePreparationFacts.scope(SCOPE))
                    .append("epoch", 0L).append("runtimeWriteFence", 1L).append("stateJson", "{}").append("touchMillis", 0L));
            rows.put("workload_claims", WorkloadClaimDocuments.stored(fence)
                    .append("_id", MongoWorkloadClaimStore.id(fence.key())).append("leaseUntil", new Date(Long.MAX_VALUE))
                    .append("pipelineScope", MongoPipelinePreparationFacts.scope(SCOPE))
                    .append("pipelinePreparation", new Document("preparationId", PREPARATION).append("phase", "PREPARED")));
            rows.put("operator_cleanup", new Document("_id", MongoPipelinePreparationFacts.scope(SCOPE)).append("formatVersion", 1)
                    .append("preparationId", PREPARATION).append("revision", 2L).append("databases", List.of())
                    .append("shapes", List.of(new Document("database", "cache-db").append("stepId", OperatorStateText.encode("nest"))))
                    .append("placements", List.of(MongoOperatorInventory.location(at))));
            rows.put("nest_shapes", new Document("_id", new Document("scope", MongoPipelinePreparationFacts.scope(SCOPE))
                    .append("stepId", OperatorStateText.encode("nest"))).append("formatVersion", 1)
                    .append("paths", List.of(OperatorStateText.encode("root"))));
            when(db.getName()).thenReturn("cache-db");
            when(db.getCollection(anyString())).thenAnswer(call -> collections.computeIfAbsent(call.getArgument(0), this::collection));
            when(db.listCollections()).thenAnswer(call -> {
                descriptionReads++;
                ListCollectionsIterable<Document> result = mock(ListCollectionsIterable.class);
                var descriptions = Arrays.stream(SystemCollections.values()).flatMap(t -> t.physicalNames().stream()).distinct()
                        .filter(n -> !n.equals(missingCollection))
                        .map(n -> new Document("name", n).append("type", "collection")).toList();
                when(result.iterator()).thenAnswer(ignored -> cursor(descriptions));
                when(result.filter(any(Bson.class))).thenAnswer(filtered -> {
                    String name = ((Document) filtered.getArgument(0)).getString("name");
                    when(result.first()).thenReturn(new Document("name", name).append("type", "collection"));
                    return result;
                });
                return result;
            });
            when(client.startSession()).thenReturn(session);
            when(client.startSession(any(ClientSessionOptions.class))).thenReturn(session);
            when(session.withTransaction(any(TransactionBody.class), any(TransactionOptions.class))).thenAnswer(call -> {
                Object result = null;
                for (int i = 0; i < attempts; i++) { result = ((TransactionBody<?>) call.getArgument(0)).execute(); afterAttempt.run(); }
                return result;
            });
            store = new MongoCacheOperatorStateStore(new MongoCachePipelineAccess(client, db, SCOPE, fence, () -> {}), at);
        }

        @SuppressWarnings("unchecked")
        private MongoCollection<Document> collection(String name) {
            MongoCollection<Document> result = mock(MongoCollection.class);
            when(result.withReadPreference(any())).thenReturn(result);
            when(result.withReadConcern(any())).thenReturn(result);
            when(result.withWriteConcern(any())).thenReturn(result);
            when(result.find(any(ClientSession.class), any(Bson.class))).thenAnswer(call -> {
                finds.merge(name, 1, Integer::sum);
                FindIterable<Document> read = mock(FindIterable.class);
                when(read.projection(any(Bson.class))).thenAnswer(projection -> {
                    if (projection.getArgument(0) instanceof Document fields && fields.containsKey("remaining")) {
                        FindIterable<Document> clock = mock(FindIterable.class);
                        when(clock.first()).thenReturn(new Document("remaining", 60_000L));
                        return clock;
                    }
                    return read;
                });
                when(read.first()).thenAnswer(ignored -> rows.get(name));
                when(read.iterator()).thenAnswer(ignored -> cursor(rows.containsKey(name) ? List.of(rows.get(name)) : List.of()));
                return read;
            });
            when(result.listIndexes()).thenAnswer(call -> {
                indexReads.merge(name, 1, Integer::sum);
                ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
                when(indexes.into(any())).thenAnswer(into -> {
                    Collection<Document> target = into.getArgument(0);
                    target.add(new Document("name", "_id_").append("key", new Document("_id", 1)));
                    var table = Arrays.stream(SystemCollections.values()).filter(t -> t.collectionName().equals(name)).findFirst().orElseThrow();
                    for (var spec : table.indexes()) {
                        if (spec.indexName().equals(missingIndex)) continue;
                        var key = new Document(); spec.keys().forEach(k -> key.append(k, 1));
                        var index = new Document("name", spec.indexName()).append("key", key).append("unique", spec.unique());
                        if (spec.partialFilterJson() != null) index.append("partialFilterExpression", Document.parse(spec.partialFilterJson()));
                        target.add(index);
                    }
                    return target;
                });
                return indexes;
            });
            when(result.updateOne(any(ClientSession.class), any(Bson.class), any(Bson.class))).thenAnswer(call -> {
                updates.merge(name, 1, Integer::sum); return UpdateResult.acknowledged(1, 1L, null);
            });
            return result;
        }

        @SuppressWarnings("unchecked")
        private static MongoCursor<Document> cursor(List<Document> rows) {
            var values = rows.iterator(); MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(cursor.hasNext()).thenAnswer(call -> values.hasNext());
            when(cursor.next()).thenAnswer(call -> values.next());
            return cursor;
        }
    }
}
