package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The writers of one sink on one member share one connector only where the connector's artifact is certified
 * for it: then the first writer to open starts the instance, all of them write through it, and it stops only
 * when the last of them closes. Every other artifact - not certified, or certified for other bytes - runs an
 * instance per writer, and so does a write that names no sink.
 */
class ASinksWritersShareAConnectorOnlyWhenItsArtifactIsCertifiedTest {

    private static final PipelineNode SINK = new PipelineNode("p1", "to_target");

    @Test
    void writersOfACertifiedArtifactShareOneInstanceUntilTheLastLetsGo(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        PdkSinkPort port = port(dir, trace, "h1", true);

        List<SinkWriter> writers = open(port, SINK, 4);
        writeThroughEach(writers);
        for (SinkWriter writer : writers.subList(0, 3)) {
            writer.close();
        }

        assertThat(Files.readAllLines(trace)).as("started once, written by all four, still running for the last")
                .containsExactly("init", "write", "write", "write", "write");

        writers.get(3).close();

        assertThat(Files.readAllLines(trace)).endsWith("stop").filteredOn("stop"::equals).hasSize(1);
    }

    @Test
    void writersOfAnArtifactNotCertifiedEachRunTheirOwn(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        PdkSinkPort port = port(dir, trace, "h1", false);

        List<SinkWriter> writers = open(port, SINK, 4);
        for (SinkWriter writer : writers) {
            writer.close();
        }

        assertThat(Files.readAllLines(trace)).filteredOn("init"::equals).as("one instance per writer").hasSize(4);
        assertThat(Files.readAllLines(trace)).filteredOn("stop"::equals).hasSize(4);
    }

    @Test
    void anArtifactCertifiedForOtherBytesIsNotSharedWithTheWritersOfTheseOnes(@TempDir Path dir)
            throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        SharedSinkConnectors sharing = new SharedSinkConnectors();
        Path jar = Synthetic.lifecycleSink(dir, trace);

        SinkWriter onFirstBytes = new PdkSinkPort(id -> certified(jar, "h1"), null, sharing)
                .openPrepared(config(SINK), Map.of());
        SinkWriter onSecondBytes = new PdkSinkPort(id -> certified(jar, "h2"), null, sharing)
                .openPrepared(config(SINK), Map.of());

        assertThat(Files.readAllLines(trace)).as("an instance for each artifact").containsExactly("init", "init");
        onFirstBytes.close();
        onSecondBytes.close();
    }

    @Test
    void aWriteNamingNoSinkRunsItsOwnInstanceEvenOfACertifiedArtifact(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        PdkSinkPort port = port(dir, trace, "h1", true);

        List<SinkWriter> writers = open(port, null, 2);
        for (SinkWriter writer : writers) {
            writer.close();
        }

        assertThat(Files.readAllLines(trace)).filteredOn("init"::equals).hasSize(2);
    }

    private static PdkSinkPort port(Path dir, Path trace, String contentHash, boolean shareSafe) {
        Path jar = Synthetic.lifecycleSink(dir, trace);
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.LifecycleSink", "2.0.8", null, null,
                contentHash, shareSafe);
        return new PdkSinkPort(id -> ref, null, new SharedSinkConnectors());
    }

    private static ConnectorRef certified(Path jar, String contentHash) {
        return new ConnectorRef(List.of(jar), "synthetic.LifecycleSink", "2.0.8", null, null, contentHash, true);
    }

    private static List<SinkWriter> open(PdkSinkPort port, PipelineNode node, int count) {
        List<SinkWriter> writers = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            writers.add(port.openPrepared(config(node), Map.of()));
        }
        return writers;
    }

    private static void writeThroughEach(List<SinkWriter> writers) throws Exception {
        int id = 0;
        for (SinkWriter writer : writers) {
            writer.write(List.of(Envelope.insert(++id, "t1", Map.of("id", id), null)))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    private static SinkConfig config(PipelineNode node) {
        return new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null, node);
    }
}
