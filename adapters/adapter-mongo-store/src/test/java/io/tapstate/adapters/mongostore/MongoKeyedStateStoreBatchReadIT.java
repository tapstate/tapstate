package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses that the batch read is one, against a real Mongo. Three separate things can be wrong here
 * and only one of them shows up as a wrong answer, which is why this looks at what was sent as well as
 * at what came back.
 *
 * <ul>
 *   <li><b>It could answer correctly, one round trip per key.</b> The answer is right and the run is
 *       merely slow - three orders of magnitude on a large recompute - so nothing reports anything and
 *       the natural diagnosis is that the store is slow. Witnessed by counting the commands the driver
 *       actually sent, not by timing anything.
 *   <li><b>It could match on a path inside the id.</b> The index is on the id and not on a path within
 *       it, so {@code _id.k: {$in: [...]}} reads every document in the collection to answer. Right
 *       answer, whole-collection cost. Witnessed by explaining the filter the store itself sent.
 *   <li><b>It could build the id sub-document field-first-wrong.</b> Sub-document equality in BSON is
 *       field-order sensitive, so an id written {@code {k, ns}} matches zero documents - and zero
 *       matches is indistinguishable from "these keys have no state yet", which reads as an operator
 *       starting fresh. Witnessed by the ordinary correctness cases below.
 * </ul>
 */
@RequiresDocker
class MongoKeyedStateStoreBatchReadIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    private static final String NAMESPACE = "join.orders_wide.dimension.customers";
    private static final String SIBLING = "join.orders_wide.fact.orders";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void aBatchAnswersExactlyWhatTheKeyAtATimeReadsWould() {
        withStore((store, finds) -> {
            store.save(NAMESPACE, "C1", bytes("state of C1"));
            store.save(NAMESPACE, "C2", bytes("state of C2"));

            Map<String, byte[]> batched = store.loadAll(NAMESPACE, List.of("C1", "C2", "never-saved"));

            Map<String, byte[]> oneAtATime = new LinkedHashMap<>();
            for (String key : List.of("C1", "C2", "never-saved")) {
                store.load(NAMESPACE, key).ifPresent(state -> oneAtATime.put(key, state));
            }
            assertThat(batched).containsExactlyInAnyOrderEntriesOf(oneAtATime);
            assertThat(batched).containsOnlyKeys("C1", "C2");
        });
    }

    /**
     * The case the id's two halves exist for. Business keys repeat across tables, so one key name is a
     * key in more namespaces than one; a batch that reached across would hand an operator another's
     * state, and nothing about that is an error - the row would simply be built out of the wrong values.
     */
    @Test
    void aBatchNeverAnswersWithAnotherNamespacesKeyOfTheSameName() {
        withStore((store, finds) -> {
            store.save(NAMESPACE, "shared", bytes("mine"));
            store.save(SIBLING, "shared", bytes("theirs"));

            assertThat(store.loadAll(NAMESPACE, List.of("shared")))
                    .containsEntry("shared", bytes("mine"));
        });
    }

    /** A key rendering can hold anything a business key can, separators and braces included. */
    @Test
    void aKeyOfAnyShapeSurvivesTheRoundTripThroughABatch() {
        withStore((store, finds) -> {
            store.save(NAMESPACE, "", bytes("empty"));
            store.save(NAMESPACE, "{\"looks\": \"like a document\"}", bytes("and is not one"));

            assertThat(store.loadAll(NAMESPACE, List.of("", "{\"looks\": \"like a document\"}")))
                    .containsEntry("", bytes("empty"))
                    .containsEntry("{\"looks\": \"like a document\"}", bytes("and is not one"));
        });
    }

    @Test
    void askingForNothingSendsNoCommandAtAll() {
        withStore((store, finds) -> {
            finds.clear();

            assertThat(store.loadAll(NAMESPACE, List.of())).isEmpty();

            assertThat(finds).isEmpty();
        });
    }

    /**
     * The one that catches an answer arrived at one key at a time. Nothing about the returned map would
     * differ, so this counts what the driver was actually asked to send.
     */
    @Test
    void oneBatchIsOneRoundTripRatherThanOnePerKey() {
        withStore((store, finds) -> {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                String key = "C" + i;
                keys.add(key);
                store.save(NAMESPACE, key, bytes("state of " + key));
            }
            finds.clear();

            assertThat(store.loadAll(NAMESPACE, keys)).hasSize(200);

            assertThat(finds).hasSize(1);
        });
    }

    /**
     * More keys than one request may carry. The batch is split because a request has a hard size limit
     * that a large enough set of keys reaches - so the split has to exist, and every key still has to
     * come back.
     */
    @Test
    void everyKeyComesBackWhenThereAreMoreThanOneRequestMayCarry() {
        withStore((store, finds) -> {
            int keyCount = MongoKeyedStateStore.MAX_KEYS_PER_READ + 7;
            List<String> keys = new ArrayList<>(keyCount);
            for (int i = 0; i < keyCount; i++) {
                String key = "C" + i;
                keys.add(key);
                store.save(NAMESPACE, key, bytes("state of " + key));
            }
            finds.clear();

            Map<String, byte[]> loaded = store.loadAll(NAMESPACE, keys);

            assertThat(loaded).hasSize(keyCount);
            assertThat(loaded).containsEntry("C0", bytes("state of C0"));
            assertThat(loaded).containsEntry("C" + (keyCount - 1), bytes("state of C" + (keyCount - 1)));
            // Split, and split no more than it had to be.
            assertThat(finds).hasSize(2);
        });
    }

    /**
     * The one that catches a filter matching on a path inside the id. It explains the filter the store
     * itself sent rather than one this test rebuilt: a store that stopped using that filter would leave
     * this case explaining something nothing runs, which is a green that measured nothing.
     */
    @Test
    void theBatchReadsByIndexRatherThanByReadingTheWholeCollection() {
        withStore((store, finds) -> {
            for (int i = 0; i < 500; i++) {
                store.save(SIBLING, "noise" + i, bytes("not asked for"));
            }
            store.save(NAMESPACE, "C1", bytes("state of C1"));
            store.save(NAMESPACE, "C2", bytes("state of C2"));
            finds.clear();

            store.loadAll(NAMESPACE, List.of("C1", "C2"));

            BsonDocument filter = finds.get(0).getDocument("filter");
            Document stats = explain(filter).get("executionStats", Document.class);
            // Two keys asked for, two documents read. A filter on a path inside the id answers the same
            // two keys after reading all 502, and the only visible difference is the time.
            assertThat(stats.getInteger("nReturned")).isEqualTo(2);
            assertThat(stats.getInteger("totalDocsExamined")).isLessThanOrEqualTo(2);
        });
    }

    /**
     * Positive control for the case above: the reading it calls a scan really is one, on this server,
     * measured the same way. Without it, a {@code totalDocsExamined} that is small for some unrelated
     * reason - an explain that measured nothing, a planner that answered from a cache - passes the ban
     * while the thing it bans would have passed too.
     */
    @Test
    void aFilterOnAPathInsideTheIdIsTheScanTheBanAbovePointsAt() {
        withStore((store, finds) -> {
            for (int i = 0; i < 500; i++) {
                store.save(SIBLING, "noise" + i, bytes("not asked for"));
            }
            store.save(NAMESPACE, "C1", bytes("state of C1"));
            store.save(NAMESPACE, "C2", bytes("state of C2"));

            BsonDocument insideTheId = BsonDocument.parse(
                    "{\"_id.ns\": \"" + NAMESPACE + "\", \"_id.k\": {\"$in\": [\"C1\", \"C2\"]}}");
            Document stats = explain(insideTheId).get("executionStats", Document.class);

            assertThat(stats.getInteger("nReturned")).isEqualTo(2);
            assertThat(stats.getInteger("totalDocsExamined")).isEqualTo(502);
        });
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static Document explain(BsonDocument filter) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            return client.getDatabase("tapstate").getCollection(MongoStorePort.OPERATOR_STATE)
                    .find(filter).explain(ExplainVerbosity.EXECUTION_STATS);
        }
    }

    /**
     * A store on a client that records every find it is asked to send. The recording is what turns "the
     * answer is right" into "the answer was arrived at the way it claims", which is the whole question
     * here.
     *
     * <p><b>The command is copied inside the callback, which is not tidiness.</b> The document a command
     * event carries is a view onto the driver's own buffer and is only readable while the callback runs;
     * a reference kept past it reads whatever that buffer was reused for. Measured: a filter read
     * afterwards decoded as a negative document length, and before that failure was reached the same
     * stale read had explained as a whole-collection scan - which is exactly the finding this case
     * exists to report, arrived at with nothing wrong with the store at all.
     */
    private static void withStore(BiConsumer<MongoKeyedStateStore, List<BsonDocument>> test) {
        List<BsonDocument> finds = new ArrayList<>();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        if ("find".equals(event.getCommandName())) {
                            finds.add(event.getCommand().clone());
                        }
                    }
                })
                .build();
        try (MongoClient client = MongoClients.create(settings)) {
            MongoCollection<Document> collection =
                    client.getDatabase("tapstate").getCollection(MongoStorePort.OPERATOR_STATE);
            collection.drop();
            test.accept(new MongoKeyedStateStore(collection), finds);
        }
    }
}
