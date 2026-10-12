package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.SyntheticSinkJars;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
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
 * The four writers a sink opens on a member each run a connector of their own - unless the connector's artifact
 * is certified to serve several writers at once, and then they share one: started by the first, stopped by the
 * last. Either way the sink's tables are prepared once, on a connector opened for that alone.
 */
class ASinksWritersOwnAConnectorEachUntilItsArtifactIsCertifiedTest {

    private static final PipelineNode SINK = new PipelineNode("p1", "to_target");

    @Test
    void writersOfAnArtifactNobodyCertifiedEachStartAConnector(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));

        runFourWriters(dir, trace, false);

        List<String> lines = Files.readAllLines(trace);
        assertThat(lines).filteredOn("init"::equals).as("one to prepare, then one per writer").hasSize(5);
        assertThat(lines).filteredOn("stop"::equals).hasSize(5);
        assertThat(lines).filteredOn("write"::equals).hasSize(4);
    }

    @Test
    void writersOfACertifiedArtifactShareOneConnectorUntilTheLastLetsGo(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));

        runFourWriters(dir, trace, true);

        assertThat(Files.readAllLines(trace)).as("one to prepare, then one the four writers share")
                .containsExactly("init", "stop", "init", "write", "write", "write", "write", "stop");
    }

    /** Prepares the sink once, opens four writers, writes a row through each and closes them all. */
    private static void runFourWriters(Path dir, Path trace, boolean certified) throws Exception {
        ConnectorRef ref = new ConnectorRef(List.of(SyntheticSinkJars.lifecycleSink(dir, trace)),
                "synthetic.LifecycleSink", "2.0.8", null, null, "h1", certified);
        HazelcastInstance member = memberWith(connectorId -> ref);
        try {
            PdkSinkWriterFactory factory = new PdkSinkWriterFactory("demo", Map.of(), WriteMode.UPSERT,
                    DdlPolicy.FAIL, Map.<String, TargetTable>of(), SINK, OnFullLoad.APPEND, true);
            factory.prepareTargets(member);
            List<SinkWriter> writers = new ArrayList<>();
            for (int writer = 0; writer < 4; writer++) {
                writers.add(factory.getEx());
            }
            int id = 0;
            for (SinkWriter writer : writers) {
                writer.write(List.of(Envelope.insert(++id, "t1", Map.of("id", id), null)))
                        .toCompletableFuture().get(30, TimeUnit.SECONDS);
            }
            for (SinkWriter writer : writers) {
                writer.close();
            }
        } finally {
            member.shutdown();
        }
    }

    private static HazelcastInstance memberWith(ConnectorProvisioner provisioner) {
        return new HazelcastConfiguration().hazelcastMember(new HazelcastProperties(), null, provisioner, null,
                new InMemoryKeyedStateStore(), NestSettings.defaults(), null, null);
    }
}
