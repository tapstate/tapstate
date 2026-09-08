package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Witnesses the discovered-schema store against a real Mongo replica-set: a saved discovery envelope
 * (connector id, discovery time, and the source model's tables, fields including one with no resolved
 * type, primary key, and both a unique and a non-unique index) reads back equal through the real bson
 * encode / decode, an absent connection reads back empty, and a re-discovery of the same connection
 * replaces the stored envelope in place (last write wins) rather than accumulating documents. Where
 * Docker is absent this aborts on a developer machine and fails in CI, where a skip would be a green
 * build that ran nothing.
 */
@RequiresDocker
class MongoSchemaStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static SourceModel ordersModel() {
        return new SourceModel(List.of(
                new SourceTable(
                        "orders",
                        List.of(new SourceField("id", "bigint"), new SourceField("note", null)),
                        List.of("id"),
                        List.of(
                                new SourceIndex("pk_orders", List.of("id"), true),
                                new SourceIndex("by_note", List.of("note"), false))),
                new SourceTable("customers", List.of(new SourceField("email", "varchar")), List.of("email"), List.of())));
    }

    @Test
    void savedEnvelopeReadsBackEqualThroughRealBson() {
        withStore((store, collection) -> {
            DiscoveredSourceModel envelope =
                    new DiscoveredSourceModel("orders-db", "mysql", 1783998000000L, ordersModel());
            store.save(envelope);

            Optional<DiscoveredSourceModel> read = store.get("orders-db");
            assertThat(read).contains(envelope);
        });
    }

    @Test
    void getReturnsEmptyForAnAbsentConnection() {
        withStore((store, collection) -> assertThat(store.get("never-discovered")).isEmpty());
    }

    @Test
    void reDiscoveryReplacesTheStoredEnvelopeInPlace() {
        withStore((store, collection) -> {
            store.save(new DiscoveredSourceModel("orders-db", "mysql", 1L, ordersModel()));
            DiscoveredSourceModel rediscovered = new DiscoveredSourceModel(
                    "orders-db",
                    "mysql",
                    2L,
                    new SourceModel(List.of(
                            new SourceTable("orders", List.of(new SourceField("id", "bigint")), List.of("id"), List.of()))));
            store.save(rediscovered);

            // The envelope and the one table it now names - a re-discovery replaces both rather than
            // accumulating a second copy of either.
            assertThat(collection.countDocuments()).isEqualTo(2);
            assertThat(store.get("orders-db")).contains(rediscovered);
        });
    }

    /**
     * A connection with an ordinary large schema cannot have its discovery stored, because every table
     * it has shares one document and therefore one 16MB ceiling.
     *
     * <p>Measured 2026-09-08, the ceiling falls at roughly 150,000 columns summed across the whole
     * connection - 80,000 columns encode to 8.5MB, 158,200 to 16.9MB against a 16,793,600 byte limit.
     * Neither dimension has to be remarkable to reach it: 1500 tables of 120 columns is an unexceptional
     * ERP schema and does not fit. It is the sum over the database that is capped, not any one table,
     * which is why no author looking at their own tables would see this coming.
     *
     * <p>Unlike the derived side nothing accumulates here: this is a single {@code save} of a single
     * discovery, so the size of the database alone decides it.
     *
     * <p><b>The consequence is that the connection can never be discovered.</b> Every expression check
     * and every target-table decision reads that discovery, so a source this size is not slow, it is
     * unusable - and re-discovering, the one repair this store offers, fails the same way.
     *
     * <p>This is red until the envelope is stored per table rather than as one document per connection.
     */
    @Test
    void aConnectionsTablesDoNotShareOneDocumentCeiling() {
        withStore((store, collection) -> {
            store.save(new DiscoveredSourceModel("wide-db", "mysql", 1783998000000L, wideModel(1500, 120)));

            assertThat(store.get("wide-db")).isPresent();
        });
    }

    /** A model of {@code tables} tables of {@code columns} columns, with the names an author meets. */
    private static SourceModel wideModel(int tables, int columns) {
        List<SourceTable> model = new ArrayList<>();
        for (int table = 0; table < tables; table++) {
            List<SourceField> fields = new ArrayList<>();
            for (int column = 0; column < columns; column++) {
                fields.add(new SourceField(
                        "customer_shipping_address_line_detail_" + column, "decimal(18,4)"));
            }
            model.add(new SourceTable("warehouse_movement_history_" + table, fields,
                    List.of("customer_shipping_address_line_detail_0"), List.of()));
        }
        return new SourceModel(model);
    }

    /**
     * A re-discovery becomes visible in one step: the tables of the previous discovery are gone and
     * the new ones are all there, with nothing of the two mixed together.
     *
     * <p>The tables live in documents of their own now, so no single write replaces them. What makes
     * the swap atomic is that the envelope naming the current generation is written last and is one
     * document; this is the case that fails if that ordering is ever inverted.
     */
    @Test
    void aReDiscoveryReplacesEveryTableAndLeavesNoneOfThePreviousOne() {
        withStore((store, collection) -> {
            store.save(new DiscoveredSourceModel("orders-db", "mysql", 1L, ordersModel()));
            store.save(new DiscoveredSourceModel("orders-db", "mysql", 2L, new SourceModel(List.of(
                    new SourceTable("shipments", List.of(new SourceField("id", "bigint")), List.of("id"),
                            List.of())))));

            DiscoveredSourceModel read = store.get("orders-db").orElseThrow();
            assertThat(read.model().tables()).extracting(SourceTable::name).containsExactly("shipments");
            // The superseded generation's documents are swept, not merely hidden - left behind they
            // would grow the collection by a whole model on every discovery.
            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void aModelWrittenBeforeTheTablesMovedOutOfTheEnvelopeReadsBackAsNotDiscovered() {
        withStore((store, collection) -> {
            // The shape the previous build wrote: tables inline, no generation, and its own stamp.
            collection.insertOne(new Document("_id", "old-db")
                    .append("modelVersion", 1)
                    .append("connectorId", "mysql")
                    .append("discoveredAt", 1L)
                    .append("tables", List.of(new Document("name", "orders").append("fields", List.of()))));

            // Not an error and not an empty database: "discover this connection" is the one action
            // that fixes it, and reporting no tables would instead read as a database with none.
            assertThat(store.get("old-db")).isEmpty();
        });
    }

    @Test
    void anEnvelopeCarryingTheCurrentStampButNoGenerationIsReportedAsCorruption() {
        withStore((store, collection) -> {
            collection.insertOne(new Document("_id", "torn")
                    .append("modelVersion", 2)
                    .append("connectorId", "mysql")
                    .append("discoveredAt", 1L));

            assertThatThrownBy(() -> store.get("torn"))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code().code()).isEqualTo("io.document-unreadable"));
        });
    }

    @Test
    void discoveryOrderSurvivesEvenWhenItDisagreesWithTableNameOrder() {
        withStore((store, collection) -> {
            // Documents come back in key order, which is table-name order; these two names are chosen
            // so that the two orders disagree, which is what makes the assertion discriminating.
            store.save(new DiscoveredSourceModel("orders-db", "mysql", 1L, new SourceModel(List.of(
                    new SourceTable("zulu", List.of(), List.of(), List.of()),
                    new SourceTable("alpha", List.of(), List.of(), List.of())))));

            assertThat(store.get("orders-db").orElseThrow().model().tables())
                    .extracting(SourceTable::name).containsExactly("zulu", "alpha");
        });
    }

    private interface StoreTest {
        void run(MongoSchemaStore store, MongoCollection<Document> collection);
    }

    /** Runs a test body against a fresh schema store over a clean collection on the real replica-set. */
    private static void withStore(StoreTest test) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("source_schemas");
            collection.drop();
            test.run(new MongoSchemaStore(collection), collection);
        }
    }
}
