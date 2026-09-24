package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Witnesses the channel holding what a nest could never assemble against a real Mongo: a discarded row comes
 * back as it went in, the same row discarded twice leaves one record, a namespace answers only for its own,
 * the newest is read first, and a namespace can be dropped whole.
 *
 * <p>Filing per element is the case worth the container. A replay hands the same discarded changes over
 * again - which is ordinary, not exceptional - and a channel that appended instead would grow by an hour of
 * discards every time an hour was replayed, with none of the copies saying anything the first did not.
 *
 * <p>Where Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a
 * green build that ran nothing.
 */
@RequiresDocker
class MongoNestDeadLetterStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    private static final String NAMESPACE = "nest.orders_to_docs.assemble.policies";
    private static final String SIBLING = "nest.orders_to_docs.assemble.orders";

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void whatWasDiscardedComesBackAsItWent() {
        withStore(store -> {
            store.record(new NestDeadLetterRecord(NAMESPACE, "[\"policies\"]#[7]~i", "mysql-a", "1:42",
                    180_000L, 9_000L, Map.of("id", 7, "policy_no", "P-7")));

            NestDeadLetterRecord kept = store.read(NAMESPACE, 10).get(0);
            assertThat(kept.namespace()).isEqualTo(NAMESPACE);
            assertThat(kept.element()).isEqualTo("[\"policies\"]#[7]~i");
            assertThat(kept.chain()).isEqualTo("mysql-a");
            assertThat(kept.order()).isEqualTo("1:42");
            assertThat(kept.heldForMillis()).isEqualTo(180_000L);
            assertThat(kept.discardedAt()).isEqualTo(9_000L);
            assertThat(kept.row()).containsEntry("id", 7).containsEntry("policy_no", "P-7");
        });
    }

    /**
     * A deletion carries no row, and reading it back as a row with no fields would tell whoever is looking a
     * different and wrong thing: that a row was discarded and happened to be empty.
     */
    @Test
    void aDiscardedDeletionComesBackAsADeletion() {
        withStore(store -> {
            store.record(new NestDeadLetterRecord(NAMESPACE, "[\"policies\"]#[7]~i", "mysql-a", "1:42",
                    0L, 9_000L, null));

            assertThat(store.read(NAMESPACE, 10).get(0).deletion()).isTrue();
        });
    }

    @Test
    void theSameElementDiscardedAgainLeavesOneRecordRatherThanTwo() {
        withStore(store -> {
            store.record(record("[\"policies\"]#[7]~i", 9_000L));
            store.record(record("[\"policies\"]#[7]~i", 20_000L));

            List<NestDeadLetterRecord> kept = store.read(NAMESPACE, 10);
            assertThat(kept).hasSize(1);
            // The later hand-over wins: what is filed is the most recent time the row was found undeliverable.
            assertThat(kept.get(0).discardedAt()).isEqualTo(20_000L);
        });
    }

    /**
     * Business keys repeat across tables, so "policy 7" is an element in more namespaces than one. A channel
     * that let them meet would file one vertex's discarded row over another's - not an error anywhere, just
     * a record that quietly stops being about the row it names.
     */
    @Test
    void aNamespaceAnswersOnlyForItsOwn() {
        withStore(store -> {
            store.record(record("[\"policies\"]#[7]~i", 9_000L));
            store.record(new NestDeadLetterRecord(SIBLING, "[\"policies\"]#[7]~i", "mysql-a", "1:42",
                    0L, 9_000L, Map.of("id", 7)));

            assertThat(store.read(NAMESPACE, 10)).hasSize(1);
            assertThat(store.read(SIBLING, 10)).hasSize(1);
        });
    }

    /**
     * Newest first, because a reader looking into a channel is asking what is happening now. Ordered by
     * element instead, a channel that had been discarding for a week would answer every question with the
     * same rows from the first hour.
     */
    @Test
    void theMostRecentlyDiscardedIsReadFirst() {
        withStore(store -> {
            store.record(record("[\"policies\"]#[1]~i", 1_000L));
            store.record(record("[\"policies\"]#[3]~i", 3_000L));
            store.record(record("[\"policies\"]#[2]~i", 2_000L));

            assertThat(store.read(NAMESPACE, 10).stream().map(NestDeadLetterRecord::discardedAt))
                    .containsExactly(3_000L, 2_000L, 1_000L);
            assertThat(store.read(NAMESPACE, 2).stream().map(NestDeadLetterRecord::discardedAt))
                    .containsExactly(3_000L, 2_000L);
        });
    }

    @Test
    void aNamespaceThatDiscardedNothingReadsEmptyRatherThanFailing() {
        withStore(store -> assertThat(store.read("nest.never.ran", 10)).isEmpty());
    }

    @Test
    void droppingANamespaceTakesOnlyItsOwn() {
        withStore(store -> {
            store.record(record("[\"policies\"]#[7]~i", 9_000L));
            store.record(new NestDeadLetterRecord(SIBLING, "[\"orders\"]#[1]~i", "mysql-a", "1:1",
                    0L, 9_000L, Map.of("id", 1)));

            store.dropNamespace(NAMESPACE);

            assertThat(store.read(NAMESPACE, 10)).isEmpty();
            assertThat(store.read(SIBLING, 10)).hasSize(1);
        });
    }

    /**
     * An element rendering can contain anything a business key can, separators included, and it is the half
     * of a two-field id - so a store that joined the halves into one string would let one namespace's record
     * be read as another's. The same bound has to hold for the range a namespace is read over.
     */
    @Test
    void anElementOfAnyShapeStaysInsideItsOwnNamespacesRange() {
        withStore(store -> {
            store.record(record("#", 1L));
            store.record(record("{\"looks\": \"like a document\"}", 2L));
            store.record(record("[\"policies\"]#[\"a value with spaces and a ~ in it\"]~s", 3L));

            assertThat(store.read(NAMESPACE, 10)).hasSize(3);
        });
    }

    private static NestDeadLetterRecord record(String element, long discardedAt) {
        return new NestDeadLetterRecord(NAMESPACE, element, "mysql-a", "1:42", 0L, discardedAt,
                Map.of("id", 7));
    }

    @Test
    void aDiscardedRowKeepsTheTypesItsColumnsWereDeclaredAs() {
        withStore(store -> {
            store.record(new NestDeadLetterRecord(NAMESPACE, "[\"policies\"]#[7]~i", "mongo-a", "1:42",
                    180_000L, 9_000L,
                    Map.of("_id", new ConvertedValue("650f1a2b3c4d5e6f70819200", "OBJECT_ID"))));

            assertThat(store.read(NAMESPACE, 10).get(0).row())
                    .as("this channel exists to be looked at later, and a row whose declared types were "
                            + "dropped on the way in is not the row that was discarded")
                    .containsEntry("_id", new ConvertedValue("650f1a2b3c4d5e6f70819200", "OBJECT_ID"));
        });
    }

    private static void withStore(Consumer<MongoNestDeadLetterStore> test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate")
                    .getCollection(MongoStorePort.NEST_DEAD_LETTERS);
            collection.drop();
            test.accept(new MongoNestDeadLetterStore(collection));
        }
    }
}
