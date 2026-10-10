package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.SyntheticSinkJars;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The factory a sink's vertex carries prepares the sink's tables once, when asked to for a run, and every writer
 * it opens afterwards only writes - however many it opens. A writer asked for before the run prepared anything
 * is refused, with no connector opened.
 *
 * <p>A full load clears its tables. Four writers each preparing would clear each table four times, and a clear
 * landing after another writer had started writing would take that writer's rows with it; a writer preparing
 * for itself where the run had not would clear a table the others were not waiting on.
 */
class ASinksWritersWriteOnlyTablesItsRunPreparedTest {

    private static final PipelineNode SINK = new PipelineNode("p1", "to_target");
    private static final TargetTable TARGET = new TargetTable("target", List.of(new TargetField("id", "int", true)));

    @Test
    void theRunPreparesOnceAndEveryWriterItOpensOnlyWrites(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        HazelcastInstance member = memberWith(provisioner(dir, trace));
        try {
            PdkSinkWriterFactory factory = clearingFactory();

            factory.prepareTargets(member);
            for (int writer = 0; writer < 3; writer++) {
                try (SinkWriter ignored = factory.getEx()) {
                    // Opening is the whole of what is observed: whatever a writer prepares, it does on open.
                }
            }

            assertThat(Files.readAllLines(trace))
                    .containsExactly("create:target", "clear:target", "stop", "stop", "stop", "stop");
        } finally {
            member.shutdown();
        }
    }

    @Test
    void aWriterAskedForBeforeTheRunPreparedIsRefusedWithNoConnectorOpened(@TempDir Path dir) throws Exception {
        Path trace = Files.createFile(dir.resolve("trace"));
        HazelcastInstance member = memberWith(provisioner(dir, trace));
        try {
            PdkSinkWriterFactory factory = clearingFactory();

            assertThatThrownBy(factory::getEx)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("target").hasMessageContaining("not prepared");
            assertThat(Files.readAllLines(trace)).isEmpty();
        } finally {
            member.shutdown();
        }
    }

    /** A sink whose one table is cleared for a full load. */
    private static PdkSinkWriterFactory clearingFactory() {
        return new PdkSinkWriterFactory("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL,
                Map.of("source", TARGET), SINK, OnFullLoad.CLEAR, true);
    }

    private static HazelcastInstance memberWith(ConnectorProvisioner provisioner) {
        return new HazelcastConfiguration().hazelcastMember(new HazelcastProperties(), null, provisioner, null,
                new InMemoryKeyedStateStore(), NestSettings.defaults(), null, null);
    }

    /** Resolves every connector id to one synthetic sink whose tables exist, tracing what is done to them. */
    private static ConnectorProvisioner provisioner(Path dir, Path trace) {
        ConnectorRef ref = new ConnectorRef(List.of(SyntheticSinkJars.preparationSink(dir, trace, true)),
                "synthetic.PreparationSink", "2.0.8", null);
        return connectorId -> ref;
    }
}
