package io.tapstate.adapters.pdk;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which pipeline node a connector was opened for reaches the connector handle, on both sides.
 *
 * <p>A connector keeps notes for itself and has to find them again on a later drive. Where they belong
 * is decided by the node that opened it — the pipeline and the source or sink element — and that pair
 * is resolved far above here, where a pipeline starts its sources and where a topology binds its sinks.
 * Between there and the connector it travels on the config alone, so these cases pin the last leg: the
 * ports derive a scope from the node the config names and hand it to the open.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>A dropped node is not visible from the rows.</b> A port that ignored the node would open the
 *       connector, read or write every row correctly, and scope its notes to nothing — so a case reading
 *       the data would pass either way. These read the scope the handle was opened under instead.</li>
 *   <li><b>Naming no node is a real answer, not a missing one.</b> The read-only drives live for a
 *       single call and legitimately scope nothing, so "no node" and "the node went missing" arrive as
 *       the same null. What separates them is that a config built without a node is asserted to produce
 *       no scope, and one built with a node is asserted to produce that node's — the pair, not either
 *       alone.</li>
 * </ul>
 */
class AConnectorIsOpenedForTheNodeThatAskedForItTest {

    private static ConnectorProvisioner provisioner(Path jar, String className) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), className, "2.0.8", null);
        return connectorId -> ref;
    }

    // ---- the name a node's notes are filed under -------------------------------------------------

    @Test
    void theNamespaceIsBuiltFromBothIdsOfTheNode() {
        assertThat(ConnectorStateNamespace.of(new PipelineNode("p-orders", "src_pg_main")))
                .isEqualTo("pdk.state.p-orders.src_pg_main");
    }

    /**
     * Two nodes never share a name. Sharing one is what would hand a second pipeline the first one's
     * replication slot — the notes would be found, be well-formed, and be somebody else's.
     */
    @Test
    void twoNodesNeverNameTheSameNamespace() {
        String a = ConnectorStateNamespace.of(new PipelineNode("p1", "src_a"));
        String b = ConnectorStateNamespace.of(new PipelineNode("p1", "src_b"));
        String c = ConnectorStateNamespace.of(new PipelineNode("p2", "src_a"));

        assertThat(List.of(a, b, c)).doesNotHaveDuplicates();
    }

    @Test
    void aDriveThatNamesNoNodeFilesNowhere() {
        assertThat(ConnectorStateNamespace.of(null)).isNull();
    }

    // ---- the read side ---------------------------------------------------------------------------

    @Test
    void aSnapshotReadOpensItsConnectorUnderTheNodesScope(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.emittingSource(dir);
        PdkCapturePort port = new PdkCapturePort(provisioner(jar, "synthetic.EmittingSource"));

        CaptureConfig config = new CaptureConfig("demo", Map.of(), List.of("t1"))
                .at(new PipelineNode("p1", "src_a"));

        try (CaptureBatch batch = port.snapshot(config)) {
            assertThat(((PdkCaptureBatch) batch).connector().stateNamespace())
                    .isEqualTo("pdk.state.p1.src_a");
        }
    }

    /**
     * The other half of the pair. Without it the case above passes over a port that hard-coded any
     * non-null name, and over one that scoped every drive whether or not it had a node to scope by.
     */
    @Test
    void aSnapshotReadForNoNodeOpensItsConnectorUnscoped(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.emittingSource(dir);
        PdkCapturePort port = new PdkCapturePort(provisioner(jar, "synthetic.EmittingSource"));

        try (CaptureBatch batch = port.snapshot(new CaptureConfig("demo", Map.of(), List.of("t1")))) {
            assertThat(((PdkCaptureBatch) batch).connector().stateNamespace()).isNull();
        }
    }

    // ---- the write side --------------------------------------------------------------------------

    @Test
    void aWriteOpensItsConnectorUnderTheNodesScope(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.countingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.CountingSink"));

        SinkConfig config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null,
                new PipelineNode("p1", "to_mongo"));

        try (SinkWriter writer = port.open(config)) {
            assertThat(((PdkSinkWriter) writer).connector().stateNamespace())
                    .isEqualTo("pdk.state.p1.to_mongo");
        }
    }

    @Test
    void aWriteForNoNodeOpensItsConnectorUnscoped(@TempDir Path dir) throws Exception {
        Path jar = Synthetic.countingSink(dir);
        PdkSinkPort port = new PdkSinkPort(provisioner(jar, "synthetic.CountingSink"));

        try (SinkWriter writer = port.open(new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL))) {
            assertThat(((PdkSinkWriter) writer).connector().stateNamespace()).isNull();
        }
    }
}
