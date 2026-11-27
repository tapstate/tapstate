package io.tapstate.adapters.mongostore;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoActuationStructureProofTest {
    @Test void delayedSimpleCollationAuditCannotOverrideNewerRefusal() throws Exception {
        var oldScanStarted = new CountDownLatch(1);
        var releaseOldScan = new CountDownLatch(1);
        var scans = new AtomicInteger();
        var collections = driverStub(ListCollectionsIterable.class, (self, method, args) -> {
            if (!method.getName().equals("iterator")) throw new AssertionError(method);
            if (scans.incrementAndGet() == 1) {
                oldScanStarted.countDown();
                assertThat(releaseOldScan.await(5, TimeUnit.SECONDS)).as("release the older metadata scan").isTrue();
                return collationCursor("simple");
            }
            return collationCursor("en");
        });
        var bindings = bindingsWithCollections(collections);
        try (var worker = Executors.newSingleThreadExecutor()) {
            var older = worker.submit(bindings::supportsActuationBatch);
            try {
                assertThat(oldScanStarted.await(5, TimeUnit.SECONDS)).as("start the older metadata scan").isTrue();
                boolean fresh = bindings.supportsActuationBatch();
                assertThat(fresh).as("fresh non-simple verdict").isFalse();
                releaseOldScan.countDown();
                boolean delayed = older.get(5, TimeUnit.SECONDS);
                boolean next = bindings.supportsActuationBatch();
                assertThat(scans).hasValue(2);
                assertThat(next)
                        .as("fresh non-simple verdict=%s; delayed result=%s; next caller admits batch=%s; scans=%s",
                                fresh, delayed, next, scans.get())
                        .isFalse();
            } finally {
                releaseOldScan.countDown();
            }
        }
    }

    @Test void deniedCollationAuditDisablesBatchReads() {
        var failure = new MongoCommandException(BsonDocument.parse("{ok: 0, code: 13}"), new ServerAddress());
        assertThat(bindingsWithFailedAudit(failure).supportsActuationBatch()).isFalse();
    }

    @Test void otherCollationAuditErrorsPropagate() {
        var failure = new MongoCommandException(BsonDocument.parse("{ok: 0, code: 2}"), new ServerAddress());
        assertThatThrownBy(bindingsWithFailedAudit(failure)::supportsActuationBatch).isSameAs(failure);
    }

    private static MongoStorageBindingStore bindingsWithFailedAudit(MongoCommandException failure) {
        return bindingsWithCollections(driverStub(ListCollectionsIterable.class, (self, method, args) -> {
            if (method.getName().equals("iterator")) throw failure;
            throw new AssertionError(method);
        }));
    }

    private static MongoStorageBindingStore bindingsWithCollections(ListCollectionsIterable<?> collections) {
        var collection = driverStub(MongoCollection.class, (self, method, args) -> switch (method.getName()) {
            case "withReadConcern", "withWriteConcern" -> self;
            default -> throw new AssertionError(method);
        });
        var database = driverStub(MongoDatabase.class, (self, method, args) -> switch (method.getName()) {
            case "getCollection" -> collection;
            case "listCollections" -> collections;
            default -> throw new AssertionError(method);
        });
        var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(new byte[32]));
        return new MongoStorageBindingStore(replicaSetClient(), database, ciphers);
    }

    private static MongoClient replicaSetClient() {
        var primary = ServerDescription.builder().address(new ServerAddress()).type(ServerType.REPLICA_SET_PRIMARY)
                .state(ServerConnectionState.CONNECTED).ok(true).maxWireVersion(13).build();
        var topology = new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.REPLICA_SET, List.of(primary));
        return driverStub(MongoClient.class, (self, method, args) -> {
            if (method.getName().equals("getClusterDescription")) return topology;
            throw new AssertionError(method);
        });
    }

    private static MongoCursor<?> collationCursor(String locale) {
        var rows = List.of(new Document("name", "pipeline_intents")
                .append("options", new Document("collation", new Document("locale", locale)))).iterator();
        return driverStub(MongoCursor.class, (self, method, args) -> switch (method.getName()) {
            case "hasNext" -> rows.hasNext();
            case "next" -> rows.next();
            case "close" -> null;
            default -> throw new AssertionError(method);
        });
    }

    private static <T> T driverStub(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
