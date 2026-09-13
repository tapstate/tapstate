package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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
    void declaredStringAttributesSurviveActualBsonStorage() {
        var string = new io.tapstate.core.common.StringType(36L, false, true, 1L, 2);
        var bounded = new SourceField("id", "varchar(36)", io.tapstate.core.common.TapstateType.STRING, null, null, string);
        var unspecified = new SourceField("note", "text", io.tapstate.core.common.TapstateType.STRING, null, null,
                new io.tapstate.core.common.StringType(null, null, null, null, null));
        var model = new SourceModel(List.of(new SourceTable("orders", List.of(bounded, unspecified), List.of("id"), List.of())));
        withStore((store, collection) -> {
            var observation = new DiscoveredSourceModel("strings", "mysql", 1L, model);
            store.save(observation);
            assertThat(store.get("strings")).contains(observation);
        });
    }

    @Test
    void declaredNumericAttributesSurviveActualBsonStorage() {
        var number = new io.tapstate.core.common.NumericType(128, true, false, true, new java.math.BigDecimal("-99999999999999.9999"), new java.math.BigDecimal("99999999999999.9999"), 18, 4);
        var field = new SourceField("amount", "decimal(18,4)", io.tapstate.core.common.TapstateType.DECIMAL, null, number);
        var model = new SourceModel(List.of(new SourceTable("orders", List.of(field), List.of(), List.of())));
        withStore((store, collection) -> {
            var observation = new DiscoveredSourceModel("numeric-db", "mysql", 1L, model);
            store.save(observation);
            assertThat(store.get("numeric-db")).contains(observation);
        });
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

    /**
     * A discovery that fails partway through leaves the previous one intact and readable, rather than
     * a model holding none of the tables it should.
     *
     * <p>This is what the write ordering inside a save buys, and it is invisible to every other case
     * here: with the envelope written last, a failure before it lands means the new generation is
     * never named and the previous discovery is still the current one. Written the other way round,
     * the envelope names a generation whose tables never arrived, and the connection reads back as a
     * database with no tables at all - a wrong answer in the shape of a right one, and the shape a
     * reader would act on.
     *
     * <p>The failure is injected at the table write rather than simulated by a crash, so the case is
     * deterministic and needs no second thread.
     */
    @Test
    void aDiscoveryThatFailsPartWayThroughLeavesThePreviousOneReadable() {
        withStore((store, collection) -> {
            store.save(new DiscoveredSourceModel("orders-db", "mysql", 1L, ordersModel()));

            MongoCollection<Document> failingTableWrites = failing(collection, "insertMany");
            assertThatThrownBy(() -> new MongoSchemaStore(failingTableWrites).save(
                    new DiscoveredSourceModel("orders-db", "mysql", 2L, new SourceModel(List.of(
                            new SourceTable("shipments", List.of(), List.of(), List.of()))))))
                    .isInstanceOf(RuntimeException.class);

            DiscoveredSourceModel read = store.get("orders-db").orElseThrow();
            assertThat(read.discoveredAt()).isEqualTo(1L);
            assertThat(read.model().tables()).extracting(SourceTable::name)
                    .containsExactly("orders", "customers");
        });
    }

    @Test
    void aReaderRetriesWhenItsGenerationIsReclaimedBeforeTheTableRead() {
        withStore((store, collection) -> {
            DiscoveredSourceModel before = observation(1, "old");
            DiscoveredSourceModel after = observation(2, "new");
            store.save(before);
            MongoCollection<Document> interrupted = interleave(collection,
                    call -> call.name().equals("find") && call.arguments()[0] instanceof Document filter
                            && filter.containsKey("generation"),
                    () -> new MongoSchemaStore(collection).save(after), false);

            assertThat(new MongoSchemaStore(interrupted).get("orders-db")).contains(after);
        });
    }

    @Test
    void continuousPublicationFailsAfterEightAttemptsAndALaterReadCanRecover() {
        withStore((store, collection) -> {
            store.save(observation(0, "initial"));
            AtomicInteger reads = new AtomicInteger();
            AtomicInteger publications = new AtomicInteger();
            AtomicBoolean publishing = new AtomicBoolean(true);
            MongoCollection<Document> interrupted = interleave(collection,
                    call -> {
                        if (!isTableRead(call)) {
                            return false;
                        }
                        reads.incrementAndGet();
                        return publishing.get();
                    },
                    () -> store.save(observation(publications.incrementAndGet(), "replacement")),
                    false, 9);
            MongoSchemaStore reader = new MongoSchemaStore(interrupted);

            // The ninth publication is finite: the unbounded implementation eventually returns,
            // so this regression fails on its missing diagnostic without hanging the test process.
            Throwable failure = catchThrowable(() -> reader.get("orders-db"));

            assertThat(failure).isInstanceOf(TapstateException.class);
            TapstateException coded = (TapstateException) failure;
            assertThat(coded.code().code()).isEqualTo("io.schema-read-contention");
            assertThat(coded.args()).containsExactlyEntriesOf(Map.of("connectionId", "orders-db"));
            assertThat(reads.get()).isEqualTo(8);
            assertThat(publications.get()).isEqualTo(8);

            publishing.set(false);
            assertThat(reader.get("orders-db")).contains(observation(8, "replacement"));
            assertThat(reads.get()).isEqualTo(9);
        });
    }

    @Test
    void aReaderCanSucceedOnItsLastAllowedAttempt() {
        withStore((store, collection) -> {
            store.save(observation(0, "initial"));
            AtomicInteger reads = new AtomicInteger();
            AtomicInteger publications = new AtomicInteger();
            MongoCollection<Document> interrupted = interleave(collection,
                    call -> {
                        if (!isTableRead(call)) {
                            return false;
                        }
                        reads.incrementAndGet();
                        return true;
                    },
                    () -> store.save(observation(publications.incrementAndGet(), "replacement")),
                    false, 7);

            assertThat(new MongoSchemaStore(interrupted).get("orders-db"))
                    .contains(observation(7, "replacement"));
            assertThat(reads.get()).isEqualTo(8);
            assertThat(publications.get()).isEqualTo(7);
        });
    }

    @Test
    void aWriterCannotDeleteAnotherWritersUnpublishedTables() {
        withStore((store, collection) -> {
            store.save(observation(1, "old"));
            DiscoveredSourceModel last = observation(3, "last");
            MongoCollection<Document> interrupted = interleave(collection,
                    call -> call.name().equals("insertMany"),
                    () -> new MongoSchemaStore(collection).save(observation(2, "middle")), true);

            new MongoSchemaStore(interrupted).save(last);

            assertThat(store.get("orders-db")).contains(last);
            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void aDelayedSweepCannotDeleteTheNewCurrentGeneration() {
        withStore((store, collection) -> {
            store.save(observation(1, "old"));
            DiscoveredSourceModel last = observation(3, "last");
            MongoCollection<Document> interrupted = interleave(collection,
                    call -> call.name().equals("deleteMany"),
                    () -> new MongoSchemaStore(collection).save(last), false);

            new MongoSchemaStore(interrupted).save(observation(2, "middle"));

            assertThat(store.get("orders-db")).contains(last);
            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void duplicateTableNamesRetainTheirDistinctObservationsAndOrder() {
        withStore((store, collection) -> {
            DiscoveredSourceModel duplicateNames = new DiscoveredSourceModel("orders-db", "mysql", 1,
                    new SourceModel(List.of(
                            new SourceTable("orders", List.of(new SourceField("first", "int")),
                                    List.of(), List.of()),
                            new SourceTable("orders", List.of(new SourceField("second", "varchar")),
                                    List.of(), List.of()))));
            store.save(duplicateNames);

            assertThat(store.get("orders-db")).contains(duplicateNames);
            assertThat(collection.countDocuments()).isEqualTo(3);
        });
    }

    private static DiscoveredSourceModel observation(long time, String table) {
        return new DiscoveredSourceModel("orders-db", "mysql", time,
                new SourceModel(List.of(new SourceTable(table, List.of(), List.of(), List.of()))));
    }

    private record Call(String name, Object[] arguments) { }

    private static boolean isTableRead(Call call) {
        return call.name().equals("find") && call.arguments()[0] instanceof Document filter
                && filter.containsKey("generation");
    }

    /** Pauses one driver operation while an independent store completes a real Mongo write. */
    private static MongoCollection<Document> interleave(MongoCollection<Document> delegate,
            Predicate<Call> match, Runnable action, boolean after) {
        return interleave(delegate, match, action, after, 1);
    }

    /** Finite interleavings let contention regressions fail without an unbounded test fixture. */
    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> interleave(MongoCollection<Document> delegate,
            Predicate<Call> match, Runnable action, boolean after, int maximum) {
        AtomicInteger fired = new AtomicInteger();
        return (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(), new Class<?>[] {MongoCollection.class},
                (proxy, invoked, args) -> {
                    boolean run = match.test(new Call(invoked.getName(), args))
                            && fired.getAndIncrement() < maximum;
                    if (run && !after) {
                        action.run();
                    }
                    Object result;
                    try {
                        result = invoked.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (run && after) {
                        action.run();
                    }
                    return result;
                });
    }

    /** The collection, with one named method failing the way a lost connection would. */
    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> failing(MongoCollection<Document> delegate, String method) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(
                MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class},
                (proxy, invoked, args) -> {
                    if (invoked.getName().equals(method)) {
                        throw new MongoException("injected failure");
                    }
                    try {
                        return invoked.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
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
