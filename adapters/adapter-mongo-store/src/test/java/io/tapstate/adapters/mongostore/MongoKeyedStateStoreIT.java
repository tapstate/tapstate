package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the cold layer under a stateful operator against a real Mongo: what was saved comes back
 * byte for byte, a namespace answers only for its own keys, a delete removes only what it names, and a
 * namespace can be dropped whole without anything having listed what was in it.
 *
 * <p>The namespace cases are the ones that matter most. Two vertices file under keys they each chose,
 * and business keys repeat across tables - so "customer 1" is a key in more namespaces than one. A store
 * that let them meet would answer one vertex with another's state, which is not an error anywhere: the
 * document would be built, out of the wrong rows. The key rendering can contain anything a business key
 * can, spaces and separators included, which is why one of the cases uses a key that would break a store
 * that joined the two halves into a single string.
 *
 * <p>Where Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a
 * green build that ran nothing.
 */
@RequiresDocker
class MongoKeyedStateStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    private static final String NAMESPACE = "nest.orders_to_docs.assemble.policies";
    private static final String SIBLING = "nest.orders_to_docs.assemble.orders";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void whatWasSavedComesBackAsItWasSaved() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("the state of C1"));

            assertThat(store.load(NAMESPACE, "[\"C1\"]~s")).contains(bytes("the state of C1"));
        });
    }

    @Test
    void aKeyThatWasNeverSavedIsAbsentRatherThanEmpty() {
        withStore(store -> assertThat(store.load(NAMESPACE, "[\"never\"]~s")).isEmpty());
    }

    @Test
    void aSavedKeyIsReplacedRatherThanAccumulated() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("first"));
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("second"));

            assertThat(store.load(NAMESPACE, "[\"C1\"]~s")).contains(bytes("second"));
        });
    }

    @Test
    void oneNamespaceNeverAnswersForAnothersKey() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("belongs to policies"));

            assertThat(store.load(SIBLING, "[\"C1\"]~s")).isEmpty();
            assertThat(store.load(NAMESPACE, "[\"C1\"]~s")).contains(bytes("belongs to policies"));
        });
    }

    /**
     * A key holding what a joined id would use as its boundary. A store that joined the namespace and the
     * key into one string could read this pair as a different pair; two fields cannot be misread that way.
     */
    @Test
    void aKeyThatLooksLikeItSpansTheBoundaryIsStillItsOwnKey() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"a b\"]~s", bytes("one key with a space in it"));
            store.save(NAMESPACE + " [\"a", "b\"]~s", bytes("a different namespace entirely"));

            assertThat(store.load(NAMESPACE, "[\"a b\"]~s")).contains(bytes("one key with a space in it"));
            assertThat(store.load(NAMESPACE + " [\"a", "b\"]~s"))
                    .contains(bytes("a different namespace entirely"));
        });
    }

    @Test
    void aDeleteRemovesOnlyTheKeyItNames() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("one"));
            store.save(NAMESPACE, "[\"C2\"]~s", bytes("two"));

            store.delete(NAMESPACE, "[\"C1\"]~s");

            assertThat(store.load(NAMESPACE, "[\"C1\"]~s")).isEmpty();
            assertThat(store.load(NAMESPACE, "[\"C2\"]~s")).contains(bytes("two"));
        });
    }

    @Test
    void deletingAKeyThatIsNotThereIsNotAFailure() {
        withStore(store -> store.delete(NAMESPACE, "[\"never\"]~s"));
    }

    @Test
    void droppingANamespaceTakesAllOfItAndOnlyIt() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("one"));
            store.save(NAMESPACE, "[\"C2\"]~s", bytes("two"));
            store.save(SIBLING, "[\"C1\"]~s", bytes("someone else's"));

            store.dropNamespace(NAMESPACE);

            assertThat(store.load(NAMESPACE, "[\"C1\"]~s")).isEmpty();
            assertThat(store.load(NAMESPACE, "[\"C2\"]~s")).isEmpty();
            assertThat(store.load(SIBLING, "[\"C1\"]~s"))
                    .describedAs("dropping one pipeline's state must not take another's with it")
                    .contains(bytes("someone else's"));
        });
    }

    /**
     * Counting is the one question about a namespace as a whole that gets asked while a run is going, and
     * it is answered over a range of the id rather than by matching the namespace half of it. The range is
     * where this can go wrong without looking wrong: bounds that are too tight count nothing and read as an
     * empty namespace, bounds that are too loose count the neighbours and read as a namespace that grew.
     * So a sibling is always present here, holding a different number.
     */
    @Test
    void aNamespaceCountsItsOwnEntriesAndNoneOfItsNeighbours() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("one"));
            store.save(NAMESPACE, "[\"C2\"]~s", bytes("two"));
            store.save(NAMESPACE, "[\"C3\"]~s", bytes("three"));
            store.save(SIBLING, "[\"C1\"]~s", bytes("someone else's"));

            assertThat(store.count(NAMESPACE)).isEqualTo(3L);
            assertThat(store.count(SIBLING))
                    .describedAs("a range that took in the neighbours would answer both with the total")
                    .isEqualTo(1L);
        });
    }

    @Test
    void aNamespaceNothingWasEverSavedUnderCountsNone() {
        withStore(store -> {
            store.save(SIBLING, "[\"C1\"]~s", bytes("someone else's"));

            assertThat(store.count(NAMESPACE)).isZero();
        });
    }

    @Test
    void whatIsDeletedStopsBeingCounted() {
        withStore(store -> {
            store.save(NAMESPACE, "[\"C1\"]~s", bytes("one"));
            store.save(NAMESPACE, "[\"C2\"]~s", bytes("two"));

            store.delete(NAMESPACE, "[\"C1\"]~s");

            assertThat(store.count(NAMESPACE)).isEqualTo(1L);
        });
    }

    /**
     * A key whose rendering is longer than any other, against bounds that have to hold for every key a
     * business key can produce rather than for the short ones a test reaches for first.
     */
    @Test
    void aKeyOfAnyShapeIsStillInsideTheRangeItsNamespaceIsCountedOver() {
        withStore(store -> {
            store.save(NAMESPACE, "", bytes("empty"));
            store.save(NAMESPACE, "~", bytes("past the letters"));
            store.save(NAMESPACE, "{\"looks\": \"like a document\"}", bytes("and is not one"));

            assertThat(store.count(NAMESPACE)).isEqualTo(3L);
        });
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static void withStore(Consumer<MongoKeyedStateStore> test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection =
                    client.getDatabase("tapstate").getCollection(MongoStorePort.OPERATOR_STATE);
            collection.drop();
            test.accept(new MongoKeyedStateStore(collection));
        }
    }
}
