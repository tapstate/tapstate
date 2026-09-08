package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapdata.entity.utils.cache.KVMap;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.KeyedStateStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The second map the plugin contract hands a connector: the one it calls global.
 *
 * <p>A connector reaches two maps off its driving context. One holds what it keeps for itself at a
 * single pipeline node; this is the other, and the contract's own assembly gives every connector in the
 * deployment the same one — the connectors that use it are writing something that is true of the
 * installation rather than of one node's run, and they read it expecting an answer another pipeline may
 * have written.
 *
 * <p>This host handed out a fresh in-memory map per open instead, so the map called global was the least
 * shared of the two: not shared between pipelines, not between members, not even between two opens of
 * one node.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>Shared and durable are different claims, and only one of them a per-node map fails.</b> A
 *       global map wired to the node's own namespace would outlive the handle and still be wrong, so
 *       the reader here is opened for a <em>different pipeline</em> than the writer. Reading back under
 *       the same node would pass against that wiring.</li>
 *   <li><b>Bleed in either direction is a failure, so both directions are read.</b> A global map that
 *       resolved to the node's namespace would make a connector's private notes globally visible, which
 *       is the same defect seen from the other end and no assertion on one map alone sees it.</li>
 *   <li><b>Nothing here asserts on a map the host kept.</b> What is read is the map reached off the
 *       context, which is the object the connector itself is handed and the only one it can reach.</li>
 * </ul>
 */
class TheGlobalNotepadIsSharedByEveryPipelineTest {

    /** One store behind every open in a case: two connectors sharing a map is the whole question. */
    private final KeyedStateStore store = new HeapKeyedState();

    private static ConnectorRef ref(Path dir) {
        return new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null);
    }

    private PdkConnector openFor(Path dir, String pipelineId, String nodeId) {
        return PdkConnector.open(
                "demo", ref(dir), Map.of(), ConnectorStateNamespace.of(new PipelineNode(pipelineId, nodeId)), store);
    }

    @Test
    void whatOnePipelineFilesGloballyAnotherPipelineReads(@TempDir Path dir) {
        try (PdkConnector writer = openFor(dir, "p1", "src_a")) {
            writer.context().getGlobalStateMap().put("epoch", "7");
        }

        // A different pipeline entirely, and a sink node rather than a source: nothing about the reader
        // matches the writer except the deployment they are both running in.
        try (PdkConnector reader = openFor(dir, "p2", "to_mongo")) {
            assertThat(reader.context().getGlobalStateMap().get("epoch"))
                    .as("what one pipeline filed in the global map, read from another")
                    .isEqualTo("7");
        }
    }

    @Test
    void theGlobalNotepadAndANodesOwnNotesDoNotBleedIntoEachOther(@TempDir Path dir) {
        try (PdkConnector connector = openFor(dir, "p1", "src_a")) {
            connector.context().getGlobalStateMap().put("epoch", "7");
            connector.context().getStateMap().put("SERVER_NAME", "ff38f23a");

            assertThat(connector.context().getStateMap().get("epoch"))
                    .as("a node's own notes do not show what was filed globally")
                    .isNull();
            assertThat(connector.context().getGlobalStateMap().get("SERVER_NAME"))
                    .as("the global map does not show a node's own notes")
                    .isNull();
        }
    }

    /**
     * Taking one pipeline down for good drops the namespaces that pipeline owns. The global map is not
     * one of them, and it is not enough for that to be true of the cleanup code: the name itself has to
     * sit outside every pipeline, or a cleanup that is correct today becomes wrong the first time
     * someone drops a namespace by prefix.
     */
    @Test
    void droppingANodesOwnNotesLeavesTheGlobalNotepadStanding(@TempDir Path dir) {
        try (PdkConnector connector = openFor(dir, "p1", "src_a")) {
            connector.context().getGlobalStateMap().put("epoch", "7");
            connector.context().getStateMap().put("SERVER_NAME", "ff38f23a");

            // What a pipeline being taken down does to this node: the namespace, whole.
            connector.context().getStateMap().clear();

            assertThat(connector.context().getStateMap().get("SERVER_NAME"))
                    .as("the node's own notes after its namespace was dropped")
                    .isNull();
            assertThat(connector.context().getGlobalStateMap().get("epoch"))
                    .as("the global map after one pipeline's node was dropped")
                    .isEqualTo("7");
        }
    }

    /**
     * The read-only drives name no node and are handed no store. They still touch the map during init,
     * so it has to be there; what it cannot be is durable, since there is nowhere to put it.
     */
    @Test
    void withNoStoreTheGlobalNotepadStillWorksAndLivesWithTheHandle(@TempDir Path dir) {
        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of())) {
            KVMap<Object> global = connector.context().getGlobalStateMap();
            assertThat(global).as("driving context global state map with no store").isNotNull();
            global.put("epoch", "7");
            assertThat(global.get("epoch")).isEqualTo("7");
        }

        try (PdkConnector next = PdkConnector.open("demo", ref(dir), Map.of())) {
            assertThat(next.context().getGlobalStateMap().get("epoch"))
                    .as("a store-less handle keeps nothing for the next one")
                    .isNull();
        }
    }
}
