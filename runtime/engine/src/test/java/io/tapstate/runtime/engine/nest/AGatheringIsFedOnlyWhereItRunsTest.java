package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * That every edge into a gathering vertex is routed to the one processor that vertex runs.
 *
 * <p>A gathering exists so the level below it sees a single edge per stream, which is why it runs one
 * processor in the whole cluster rather than one per member. That pin puts the processor on the member
 * owning the vertex's name and leaves every other member running a stand-in which throws on the first
 * item handed to it - so an edge that delivers to whatever is local kills the job as soon as a producer
 * on any other member emits.
 *
 * <p>This is asserted on the drawn graph rather than by running one, because running one on a single
 * member cannot fail: there the local processor is the only place input can come from, so a
 * locally-routed edge and a correctly-routed one behave identically. The property is a property of the
 * edge, and the edge is the thing looked at.
 */
class AGatheringIsFedOnlyWhereItRunsTest {

    @Test
    void bothProducersOfAnAliasReachTheGatheringOnTheMemberItIsPinnedTo() {
        DAG dag = new DAG();
        Vertex left = dag.newVertex("customers", Processors.noopP());
        Vertex right = dag.newVertex("customers_eu", Processors.noopP());
        NestTopology topology =
                NestTopology.compile("p", "doc", nest("customer", List.of("customer_id")), tables());

        NestDag.attach(dag, topology, "doc", "customer", "doc",
                alias -> List.of(left, right), null, vertex -> 0, null);

        List<Edge> into = dag.getInboundEdges("doc");
        assertThat(into)
                .describedAs("one edge per producer, which is what the gathering is for")
                .hasSize(2);
        for (Edge edge : into) {
            assertThat(edge.isDistributed())
                    .describedAs("%s is local, so nothing it carries ever leaves its own member",
                            edge.getSourceName())
                    .isTrue();
            assertThat(edge.getPartitioner().getConstantPartitioningKey())
                    .describedAs("%s must route to the member the gathering is pinned to, which is the "
                            + "one owning the gathering's own name", edge.getSourceName())
                    .isEqualTo("doc");
        }
    }
}
