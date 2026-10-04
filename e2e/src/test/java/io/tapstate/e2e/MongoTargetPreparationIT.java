package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkPreparationNamespace;
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
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Real MongoDB readback for a full load into a non-empty target collection.
 * Java is required to assert a rejected sink write and inspect the untouched collection directly.
 */
@RequiresDocker
class MongoTargetPreparationIT {
    private static final PipelineNode NODE = new PipelineNode("mongo_preparation", "sink");
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
    void aFullLoadFailRefusesAnExistingNonEmptyCollection() {
        String database = "e2e_mongo_preparation_" + UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl(database);
        Map<String, Object> config = Map.of("uri", uri, "database", database);
        TargetTable target = new TargetTable("orders", List.of(
                new TargetField("id", "source_integer", true, TapstateType.INT64),
                new TargetField("seq", "source_integer", false, TapstateType.INT64)),
                List.of(new TargetIndex(List.of("id"), true)));
        State state = new State();

        try (var client = MongoClients.create(uri)) {
            var mongo = client.getDatabase(database);
            try {
                var orders = mongo.getCollection("orders");
                Document existing = new Document("id", 99L).append("seq", 99L);
                orders.insertOne(existing);
                assertThat(orders.countDocuments()).isEqualTo(1);
                assertThat(state.count(SinkPreparationNamespace.of(NODE))).isZero();

                Throwable failure = catchThrowable(() -> {
                    try (SinkWriter writer = new PdkSinkPort(connectors::get, state).open(new SinkConfig(
                            "mongodb", config, WriteMode.UPSERT, DdlPolicy.FAIL, target, NODE, OnFullLoad.FAIL, true))) {
                        assertThat(writer.write(List.of(Envelope.insert(
                                1L, "orders", Map.of("id", 1L, "seq", 10L), null)))
                                .toCompletableFuture().get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
                    }
                });

                List<Document> rows = orders.find().projection(new Document("_id", 0)).into(new ArrayList<>());
                assertThat(failure)
                        .as("on_full_load fail must refuse a non-empty collection before writing; target rows: %s", rows)
                        .isInstanceOf(TapstateException.class)
                        .hasRootCauseInstanceOf(IllegalStateException.class)
                        .hasStackTraceContaining("orders").hasStackTraceContaining("not empty");
                assertThat(rows).containsExactly(new Document("id", 99L).append("seq", 99L));
            } finally {
                mongo.drop();
            }
        }
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
