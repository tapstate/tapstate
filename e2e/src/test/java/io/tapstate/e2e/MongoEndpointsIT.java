package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Mongo endpoint driver, checked against a real Mongo and nothing else - no product, no connector.
 *
 * <p>The sibling SQL driver has had a lane of its own from the start; this one was only ever reached
 * through the specifications that happen to name a Mongo store, all of which need connector jars and
 * therefore stay out of the default build. So the driver whose readings every such specification
 * trusts had nothing holding it on its own - and the two drivers are supposed to answer the same
 * specification the same way, which is a claim only a pair of tests can carry.
 *
 * <p>Rows are read back over a client this test opens itself, so the driver cannot agree with itself.
 */
class MongoEndpointsIT {

    private static final String TABLE = "orders";

    private static String uri;

    private final MongoEndpoints endpoints = new MongoEndpoints();

    @BeforeAll
    static void takeADatabase() {
        DockerGate.require();
        uri = SharedMongo.replicaSetUrl("e2e_mongo_endpoints");
    }

    @BeforeEach
    void emptyTheCollection() {
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase("e2e_mongo_endpoints").getCollection(TABLE).drop();
        }
    }

    @AfterEach
    void releaseTheDriver() {
        endpoints.close();
    }

    @Test
    void seedingWritesTheRowsNumberedFromOne() {
        endpoints.seed(at(), TABLE, SeedRows.generated(3));

        assertThat(rowsReadBackIndependently()).containsExactly("1,1", "2,2", "3,3");
        assertThat(endpoints.count(at(), TABLE)).isEqualTo(3L);
    }

    /**
     * Seeding nothing is how a specification says the collection holds nothing. The driver rejects a
     * write of no documents outright, so the empty case has to be recognised rather than passed on.
     */
    @Test
    void seedingNoRowsLeavesAnEmptyCollection() {
        endpoints.seed(at(), TABLE, SeedRows.generated(0));

        assertThat(endpoints.count(at(), TABLE)).isZero();
    }

    @Test
    void insertingAppendsRowsAfterTheHighestIdTheCollectionHolds() {
        endpoints.seed(at(), TABLE, SeedRows.generated(3));

        endpoints.cdc(at(), TABLE, CdcOp.INSERT, 2);

        assertThat(rowsReadBackIndependently()).containsExactly("1,1", "2,2", "3,3", "4,4", "5,5");
    }

    @Test
    void deletingRemovesTheLowestIdsAndLowersTheCount() {
        endpoints.seed(at(), TABLE, SeedRows.generated(4));

        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 2);

        assertThat(rowsReadBackIndependently()).containsExactly("3,3", "4,4");
    }

    /**
     * "The lowest ids" stops meaning "the low numbers" the moment a document is deleted.
     *
     * <p>Every other case here seeds ids 1..N and changes them straight away, so a driver reading
     * "lowest two" as {@code id <= 2} would pass all of them. It would then quietly do nothing to a
     * collection whose surviving ids start at three, and a specification asserting an unchanged count
     * would still be green, because doing nothing changes no count either. The SQL driver selects by
     * order for exactly this reason, and one specification is supposed to mean the same change against
     * either store.
     */
    @Test
    void changesReachTheDocumentsThatAreActuallyLowestNotTheLowNumbers() {
        endpoints.seed(at(), TABLE, SeedRows.generated(4));
        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 2);

        endpoints.cdc(at(), TABLE, CdcOp.UPDATE, 2);

        assertThat(touchedIds()).containsExactly(3L, 4L);
    }

    /** A delete after a delete is the same story: the two lowest survivors, not the two lowest numbers. */
    @Test
    void deletingTwiceRemovesTheSurvivorsNotTheLowNumbers() {
        endpoints.seed(at(), TABLE, SeedRows.generated(4));
        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 2);

        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 1);

        assertThat(rowsReadBackIndependently()).containsExactly("4,4");
    }

    /**
     * An insert continues from the highest id present, not from the count - after a delete the two
     * differ, and numbering by count would collide with a document that is still there.
     */
    @Test
    void insertingAfterADeleteContinuesFromTheHighestIdNotTheCount() {
        endpoints.seed(at(), TABLE, SeedRows.generated(4));
        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 2);

        endpoints.cdc(at(), TABLE, CdcOp.INSERT, 1);

        assertThat(rowsReadBackIndependently()).containsExactly("3,3", "4,4", "5,5");
    }

    /**
     * Identity is a plain field here rather than the store's own key, so nothing guarantees a document
     * carries it. A document the product landed without one is a real reading, and an insert that has
     * nothing to continue from must say so rather than surface as an unboxing of null.
     */
    @Test
    void insertingWhereTheHighestDocumentHasNoIdRefusesAndSaysSo() {
        writeDirectly(new Document("seq", 1L));

        assertThatThrownBy(() -> endpoints.cdc(at(), TABLE, CdcOp.INSERT, 1))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining(TABLE)
                .hasMessageContaining("nothing to continue from");
    }

    /**
     * Re-emission is a no-op here, and that is a decision rather than an omission - so it is pinned.
     * The executor calls it on any stalled await, and a driver that quietly rewrote the collection
     * instead would be mutating the data under an assertion that is still being made. What this test
     * refuses is the silent middle ground: a redelivery that does something partial.
     */
    @Test
    void reEmittingLeavesTheCollectionExactlyAsItWas() {
        endpoints.seed(at(), TABLE, SeedRows.generated(3));
        endpoints.cdc(at(), TABLE, CdcOp.DELETE, 1);

        endpoints.redeliver(at(), TABLE);

        assertThat(rowsReadBackIndependently()).containsExactly("2,2", "3,3");
    }

    @Test
    void fetchAnswersEmptyForNoMatchAndRefusesMoreThanOne() {
        endpoints.seed(at(), TABLE, List.of(
                Map.of("id", 1L, "name", "twin"),
                Map.of("id", 2L, "name", "twin")));

        assertThat(endpoints.fetch(at(), TABLE, Map.of("id", 9L))).isEmpty();
        assertThatThrownBy(() -> endpoints.fetch(at(), TABLE, Map.of("name", "twin")))
                .hasMessageContaining("more than one");
    }

    /** A collection nobody created reads as empty, so a wait for a first write has something to wait on. */
    @Test
    void aCollectionNoOneHasCreatedYetCountsZero() {
        assertThat(endpoints.count(at(), "never_created")).isZero();
    }

    private static EndpointAddress at() {
        return new EndpointAddress("tgt_mongo", Map.of("uri", uri));
    }

    private void writeDirectly(Document document) {
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase("e2e_mongo_endpoints").getCollection(TABLE).insertOne(document);
        }
    }

    /**
     * Reads the collection over a client this test opens itself, so the driver cannot agree with
     * itself: a driver reporting what it meant to write rather than what the store holds would pass
     * every count assertion and fail here.
     */
    private List<String> rowsReadBackIndependently() {
        List<String> rows = new ArrayList<>();
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase("e2e_mongo_endpoints")
                    .getCollection(TABLE)
                    .find()
                    .sort(new Document("id", 1))
                    .forEach(document -> rows.add(document.get("id") + "," + document.get("seq")));
        }
        return rows;
    }

    private List<Long> touchedIds() {
        List<Long> ids = new ArrayList<>();
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase("e2e_mongo_endpoints")
                    .getCollection(TABLE)
                    .find(new Document("touched", true))
                    .sort(new Document("id", 1))
                    .forEach(document -> ids.add(document.getLong("id")));
        }
        return ids;
    }
}
