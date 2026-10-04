package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * Real MongoDB preparation controls, including collection identity, physical indexes and reopening.
 * The declarative vocabulary cannot inspect a collection UUID or reopen a writer with a receipt
 * while deliberately withholding a source checkpoint.
 */
@RequiresDocker
class MongoTargetPreparationPoliciesIT {
    private static final PipelineNode NODE = new PipelineNode("mongo_policies", "sink");
    private static final TargetTable TARGET = new TargetTable("orders", List.of(
            new TargetField("id", "source_integer", true, TapstateType.INT64),
            new TargetField("seq", "source_integer", false, TapstateType.INT64)),
            List.of(new TargetIndex(List.of("id"), true)));
    private static final Map<String, ConnectorRef> connectors = new HashMap<>();

    @BeforeAll
    static void requireRealTarget() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
        Path jar;
        try (var paths = Files.list(Path.of(System.getProperty("tapstate.e2e.connectors-dir")))) {
            jar = paths.filter(path -> path.getFileName().toString().startsWith("mongodb")
                    && path.toString().endsWith(".jar")).findFirst().orElseThrow();
        }
        var inspected = new ConnectorIntrospector().introspect(List.of(jar));
        connectors.put("mongodb", new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(),
                null, inspected.spec()));
    }

    @Test
    void clearKeepsCollectionIdentityAndUserIndexesAndDoesNotRepeatOnRecovery() throws Exception {
        String database = database();
        String uri = SharedMongo.replicaSetUrl(database);
        try (MongoClient client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                var orders = mongo.getCollection("orders");
                orders.insertOne(new Document("id", 99L).append("seq", 99L));
                orders.createIndex(new Document("seq", -1), new IndexOptions().name("user_seq").unique(true));
                List<Document> indexes = orders.listIndexes().into(new ArrayList<>());
                Object uuid = mongo.listCollections().filter(new Document("name", "orders"))
                        .first().get("info", Document.class).get("uuid");
                State state = new State();

                write(uri, database, state, OnFullLoad.CLEAR, 1, 10);

                assertThat(orders.find().projection(new Document("_id", 0)).into(new ArrayList<>()))
                        .as("clear must remove old documents before the first full-load row lands")
                        .containsExactly(new Document("id", 1L).append("seq", 10L));
                assertAll("clear must preserve collection metadata",
                        () -> assertThat(orders.listIndexes().into(new ArrayList<>())).containsAll(indexes),
                        () -> assertThat(mongo.listCollections().filter(new Document("name", "orders"))
                                .first().get("info", Document.class).get("uuid")).isEqualTo(uuid));

                write(uri, database, state, OnFullLoad.CLEAR, 2, 20);
                assertThat(orders.find().projection(new Document("_id", 0)).into(new ArrayList<>()))
                        .as("a preparation receipt must preserve rows already delivered on recovery")
                        .containsExactlyInAnyOrder(new Document("id", 1L).append("seq", 10L),
                                new Document("id", 2L).append("seq", 20L));
            } finally {
                mongo.drop();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("emptyTargets")
    void aMissingOrEmptyCollectionAcceptsItsFirstFullLoad(OnFullLoad policy, boolean exists) throws Exception {
        String database = database();
        String uri = SharedMongo.replicaSetUrl(database);
        try (MongoClient client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                if (exists) {
                    mongo.createCollection("orders");
                }
                write(uri, database, new State(), policy, 1, 10);
                assertThat(mongo.getCollection("orders").find().projection(new Document("_id", 0))
                        .into(new ArrayList<>())).containsExactly(new Document("id", 1L).append("seq", 10L));
            } finally {
                mongo.drop();
            }
        }
    }

    private static Stream<Arguments> emptyTargets() {
        return Stream.of(Arguments.of(OnFullLoad.CLEAR, false), Arguments.of(OnFullLoad.CLEAR, true),
                Arguments.of(OnFullLoad.FAIL, false), Arguments.of(OnFullLoad.FAIL, true));
    }

    private static void write(String uri, String database, State state, OnFullLoad policy, long id, long seq)
            throws Exception {
        try (SinkWriter writer = new PdkSinkPort(connectors::get, state).open(new SinkConfig(
                "mongodb", Map.of("uri", uri, "database", database), WriteMode.UPSERT, DdlPolicy.FAIL,
                TARGET, NODE, policy, true))) {
            assertThat(writer.write(List.of(Envelope.insert(1L, "orders", Map.of("id", id, "seq", seq), null)))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
        }
    }

    private static String database() {
        return "e2e_mongo_policies_" + UUID.randomUUID().toString().replace("-", "");
    }

    private static final class State implements KeyedStateStore {
        private final Map<String, byte[]> entries = new HashMap<>();
        public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(entries.get(namespace + "/" + key)); }
        public void save(String namespace, String key, byte[] value) { entries.put(namespace + "/" + key, value); }
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) {
            return Optional.ofNullable(entries.putIfAbsent(namespace + "/" + key, value));
        }
        public void delete(String namespace, String key) { entries.remove(namespace + "/" + key); }
        public void dropNamespace(String namespace) { entries.keySet().removeIf(key -> key.startsWith(namespace + "/")); }
        public long count(String namespace) { return entries.keySet().stream().filter(key -> key.startsWith(namespace + "/")).count(); }
    }
}
