package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.sink.WriteResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreviewMaterializerSinkWriterFactoryTest {

    private static HazelcastInstance member;

    @BeforeAll
    static void startMember() {
        Config config = new Config();
        config.setClusterName("preview-materializer-test-" + UUID.randomUUID());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void stopMember() {
        member.shutdown();
    }

    @Test
    void appendsPayloadsFromRowsAndDoesNotRequireKeys() throws Exception {
        String mapName = mapName();
        SinkWriter writer = factory(mapName, Map.of(), WriteMode.APPEND).getEx();

        WriteResult result = writer.write(List.of(
                Envelope.insert(1L, "orders", Map.of("id", 1, "name", "first"), null),
                Envelope.update(2L, "orders", Map.of("id", 1), Map.of("id", 1, "name", "second"), null),
                Envelope.delete(3L, "orders", Map.of("id", 1, "name", "deleted"), null),
                Envelope.ddl(4L, "orders", Map.of("table", "orders")),
                new Envelope(Op.UPDATE, 5L, "orders", null, null, null)))
                .toCompletableFuture().join();

        Map<String, String> rows = member.getMap(mapName);
        assertThat(result.written()).isEqualTo(3);
        assertThat(rows.keySet()).containsExactlyInAnyOrder("append:00000000000000000000", "append:00000000000000000001",
                "append:00000000000000000002");
        assertThat(PreviewDocumentStorage.decode(rows.get("append:00000000000000000002")))
                .containsEntry("id", 1L).containsEntry("name", "deleted");
        writer.close();
        assertThatThrownBy(() -> writer.write(List.of()).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void upsertsMergeRemovedFieldsAndHandleChangedAndDeletedKeys() {
        String mapName = mapName();
        SinkWriter writer = factory(mapName, Map.of("orders", List.of("tenant", "id")), WriteMode.UPSERT).getEx();
        Map<String, String> rows = member.getMap(mapName);

        writer.write(List.of(Envelope.insert(1L, "orders",
                Map.of("tenant", "a", "id", 1, "name", "first", "obsolete", true), null)))
                .toCompletableFuture().join();
        writer.write(List.of(Envelope.update(2L, "orders",
                Map.of("tenant", "a", "id", 1), Map.of("tenant", "a", "id", 1, "name", "updated"), null)
                .withRemoved(Set.of("obsolete"))))
                .toCompletableFuture().join();

        Map<String, Object> updated = PreviewDocumentStorage.decode(rows.values().iterator().next());
        assertThat(updated).containsEntry("name", "updated").doesNotContainKey("obsolete");

        writer.write(List.of(Envelope.update(3L, "orders", Map.of("tenant", "a", "id", 1),
                Map.of("tenant", "a", "id", 2, "name", "moved"), null))).toCompletableFuture().join();
        assertThat(rows).hasSize(1);
        assertThat(PreviewDocumentStorage.decode(rows.values().iterator().next()))
                .containsEntry("id", 2L).containsEntry("name", "moved");

        writer.write(List.of(Envelope.delete(4L, "orders", Map.of("tenant", "a", "id", 2), null)))
                .toCompletableFuture().join();
        assertThat(rows).isEmpty();
        writer.close();
    }

    @Test
    void rejectsUnresolvedAndMissingOutputIdentityWithCodedFailures() {
        String mapName = mapName();
        SinkWriter writer = factory(mapName, Map.of("orders", List.of("id")), WriteMode.UPSERT).getEx();

        assertThatThrownBy(() -> writer.write(List.of(Envelope.insert(1L, "unknown", Map.of("id", 1), null)))
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThatThrownBy(() -> writer.write(List.of(Envelope.insert(2L, "orders", Map.of("name", "missing"), null)))
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(io.tapstate.core.common.TapstateException.class);
        writer.close();
    }

    private static PreviewMaterializerSinkWriterFactory factory(
            String mapName, Map<String, List<String>> keys, WriteMode mode) {
        return new PreviewMaterializerSinkWriterFactory(mapName, keys, mode);
    }

    private static String mapName() {
        return "preview-materializer-" + UUID.randomUUID();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) {
        return (Map<String, Object>) JsonReader.parse(json);
    }
}
