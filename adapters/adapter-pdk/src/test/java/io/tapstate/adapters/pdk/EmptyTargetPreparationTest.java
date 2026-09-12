package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.KeyedStateStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmptyTargetPreparationTest {
    private static final TargetTable TARGET = new TargetTable("target", List.of(new TargetField("id", "int", true)));
    private static final PipelineNode NODE = new PipelineNode("pipeline", "sink");

    @Test
    void anEmptySnapshotClearsEverySelectedTableBeforeAnyBatchAndOnlyOnce(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        var port = port(dir, trace, true, new State());
        TargetTable second = new TargetTable("second", TARGET.fields());
        try (var writer = port.open(config(OnFullLoad.CLEAR, true),
                Map.of("source", TARGET, "alias", TARGET, "empty", second))) {
            assertThat(Files.readAllLines(trace)).containsExactlyInAnyOrder(
                    "create:target", "clear:target", "create:second", "clear:second");
        }
    }

    @Test
    void anEmptySnapshotRejectsANonemptyTargetAndStopsTheConnector(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        var port = port(dir, trace, true, new State());
        assertThatThrownBy(() -> port.open(config(OnFullLoad.FAIL, true)))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code()).isEqualTo(ConnectorError.WRITE_FAILED);
                    assertThat(e.getMessage()).contains("not empty");
                });
        assertThat(Files.readAllLines(trace)).containsExactly("create:target", "count:target", "stop");
    }

    @Test
    void anEmptySnapshotCreatesItsAbsentTable(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        try (var writer = port(dir, trace, false, new State()).open(config(OnFullLoad.FAIL, true))) {
            assertThat(Files.readAllLines(trace)).containsExactly("create:target");
        }
    }

    @Test
    void reopeningWithAReceiptDoesNotClearAgain(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        var port = port(dir, trace, true, new State());
        try (var writer = port.open(config(OnFullLoad.CLEAR, true))) { }
        try (var writer = port.open(config(OnFullLoad.CLEAR, true))) { }
        assertThat(Files.readAllLines(trace)).containsExactly(
                "create:target", "clear:target", "stop", "create:target", "stop");
    }

    @Test
    void aCdcOnlyStartNeverClears(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        try (var writer = port(dir, trace, true, new State()).open(config(OnFullLoad.CLEAR, false))) {
            assertThat(Files.readAllLines(trace)).containsExactly("create:target");
        }
    }

    private static SinkConfig config(OnFullLoad policy, boolean fullLoad) {
        return new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, TARGET, NODE, policy, fullLoad);
    }

    private static PdkSinkPort port(Path dir, Path trace, boolean exists, State state) {
        Path jar = Synthetic.preparationSink(dir, trace, exists);
        return new PdkSinkPort(id -> new ConnectorRef(List.of(jar), "synthetic.PreparationSink", "2.0.8", null), state);
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
