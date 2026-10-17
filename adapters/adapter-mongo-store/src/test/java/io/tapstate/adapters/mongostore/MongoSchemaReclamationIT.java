package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.BiPredicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Real Mongo witnesses for durable writer leases, including loss without exception cleanup. */
@RequiresDocker
class MongoSchemaReclamationIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void aDeadWritersTablesSurviveUntilItsLeaseExpiresAndAreThenReclaimed() {
        withCollection(collection -> {
            MongoSchemaStore store = new MongoSchemaStore(collection);
            store.save(observation("old"));
            Error processDeath = new AssertionError("injected process death without lease release");
            MongoCollection<Document> dying = interrupt(collection, "insertMany", true,
                    () -> { throw processDeath; });
            assertThatThrownBy(() -> new MongoSchemaStore(dying).save(observation("abandoned")))
                    .isSameAs(processDeath);
            assertThat(store.get("orders-db")).contains(observation("old"));

            new MongoSchemaStore(collection).save(observation("middle"));
            assertThat(collection.countDocuments(new Document("name", "abandoned"))).isEqualTo(1);
            expireWriters(collection);
            new MongoSchemaStore(collection).save(observation("current"));

            assertThat(store.get("orders-db")).contains(observation("current"));
            assertThat(collection.countDocuments()).isEqualTo(2);
            assertThat(envelope(collection).get("_writers", Document.class)).isEmpty();
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aRevokedWriterCannotPublishEvenIfItsInsertFinishesAfterReclamation(boolean afterInsert) {
        withCollection(collection -> {
            MongoSchemaStore store = new MongoSchemaStore(collection);
            store.save(observation("old"));
            MongoCollection<Document> delayed = interrupt(collection, "insertMany", afterInsert, () -> {
                expireWriters(collection);
                new MongoSchemaStore(collection).save(observation("current"));
            });

            Throwable failure = catchThrowable(() -> new MongoSchemaStore(delayed).save(observation("late")));
            assertThat(failure).isInstanceOf(TapstateException.class);
            TapstateException coded = (TapstateException) failure;
            assertThat(coded.code()).isEqualTo(IoError.SCHEMA_WRITE_CONTENTION);
            assertThat(coded.args()).containsExactlyEntriesOf(Map.of("connectionId", "orders-db"));
            assertThat(store.get("orders-db")).contains(observation("current"));

            // An insert already sent by a revoked process can arrive after the sweep. Its UUID
            // remains unpublishable, and another sweep must still find those late documents.
            new MongoSchemaStore(collection).save(observation("next"));
            assertThat(store.get("orders-db")).contains(observation("next"));
            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void aLaterDiscoveryRetriesCleanupAfterTheSweeperDies() {
        withCollection(collection -> {
            MongoSchemaStore store = new MongoSchemaStore(collection);
            store.save(observation("old"));
            Error processDeath = new AssertionError("injected process death before deletion");
            MongoCollection<Document> dying = interrupt(collection, "deleteMany", false,
                    () -> { throw processDeath; });
            assertThatThrownBy(() -> new MongoSchemaStore(dying).save(observation("middle")))
                    .isSameAs(processDeath);
            assertThat(store.get("orders-db")).contains(observation("middle"));
            assertThat(collection.countDocuments(new Document("name", "old"))).isEqualTo(1);

            new MongoSchemaStore(collection).save(observation("current"));
            assertThat(store.get("orders-db")).contains(observation("current"));
            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void publicationDuringCandidateCollectionInvalidatesTheSweep() {
        withCollection(collection -> {
            MongoSchemaStore store = new MongoSchemaStore(collection);
            store.save(observation("old"));
            MongoCollection<Document> delayed = interrupt(collection,
                    (method, args) -> method.equals("find")
                            && ((Document) args[0]).get("_id") instanceof Document,
                    false, () -> new MongoSchemaStore(collection).save(observation("current")));

            new MongoSchemaStore(delayed).save(observation("middle"));

            assertThat(store.get("orders-db")).contains(observation("current"));
            assertThat(collection.countDocuments()).isEqualTo(2);
        });
    }

    private static DiscoveredSourceModel observation(String table) {
        return new DiscoveredSourceModel("orders-db", "mysql", 1L,
                new SourceModel(List.of(new SourceTable(table, List.of(), List.of(), List.of()))));
    }

    private static Document envelope(MongoCollection<Document> collection) {
        return collection.find(new Document("_id", "orders-db")).first();
    }

    /** Advances only persisted lease timestamps, so neither sleeps nor client clocks decide expiry. */
    private static void expireWriters(MongoCollection<Document> collection) {
        Document timestamps = new Document();
        envelope(collection).get("_writers", Document.class).keySet().forEach(
                generation -> timestamps.append("_writers." + generation, new Date(0)));
        assertThat(timestamps).isNotEmpty();
        collection.updateOne(new Document("_id", "orders-db"), new Document("$set", timestamps)
                .append("$inc", new Document("_revision", 1L)));
    }

    private static MongoCollection<Document> interrupt(MongoCollection<Document> collection, String method,
            boolean after, Runnable action) {
        return interrupt(collection, (name, args) -> name.equals(method), after, action);
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> interrupt(MongoCollection<Document> collection,
            BiPredicate<String, Object[]> match, boolean after, Runnable action) {
        AtomicBoolean fired = new AtomicBoolean();
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class}, (proxy, invoked, args) -> {
                    boolean run = match.test(invoked.getName(), args) && fired.compareAndSet(false, true);
                    if (run && !after) {
                        action.run();
                    }
                    Object result;
                    try {
                        result = invoked.invoke(collection, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (run && after) {
                        action.run();
                    }
                    return result;
                });
    }

    private static void withCollection(Consumer<MongoCollection<Document>> test) {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoCollection<Document> collection = client.getDatabase("tapstate").getCollection("source_schemas");
            collection.drop();
            test.accept(collection);
        }
    }
}
