package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.NodeWidth;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * How many processors a state-carrying nest vertex runs is the width its node was worked out to run at, not the
 * engine's own answer.
 *
 * <p>The engine's answer is the member's core count, which is the budget for work that computes. These
 * vertices do not compute; each of them declares itself non-cooperative and holds a thread of its own for
 * the life of the job, waiting on a state map. So a wider machine buys nothing here and costs a thread per
 * vertex per core - and since one tree may compile to as many state-carrying vertices as the limit on them
 * allows, the two numbers multiply, giving a worst case that grows with the box rather than with the work.
 *
 * <p><b>Nothing observable goes wrong when a vertex is left to the engine, which is why it is asserted rather
 * than reviewed.</b> Every document still assembles and every count is still right; what changes is a thread
 * count nobody is looking at, on a machine that is not the one the author was using.
 *
 * <p>A node run as one processor for the whole cluster pins each of its state-carrying vertices to one member and
 * routes every edge into them there, because every other member runs only a stand-in that refuses input. And the
 * node's width never reaches the vertices that gather an alias's several producers into one: those exist to be a
 * single lane and are pinned to one instance in the whole cluster, so a second answer for them is not a slower
 * job but a refused one.
 */
class HowManyInstancesANestVertexRunsIsDecidedNotInheritedTest {

    /** Two producers of one alias, so the graph has a gathering vertex to leave alone as well. */
    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"),
                    embed("claim", "item_id", "item_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))),
            embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null));

    private static final List<String> SOURCES = List.of("order", "item", "claim", "claim-2", "customer");

    @Test
    void everyVertexThatKeepsStateRunsTheWidthItsNodeWasWorkedOutFor() {
        NestTopology topology = NestTopology.compile("p", "doc", TREE, tables());
        DAG dag = draw(topology, new NodeWidth("doc", 3, 1, null));

        assertThat(keepingState(topology))
                .describedAs("all three kinds are in the tree - a resolver, the assembler, and the vertex "
                        + "filing the rows the root points at - so none of them is covered by an "
                        + "assertion that had nothing to walk")
                .hasSize(3)
                .allSatisfy(name -> {
                    assertThat(dag.getVertex(name).getLocalParallelism())
                            .describedAs("%s runs the node's width on each member, rather than inheriting "
                                    + "the count meant for work that computes", name)
                            .isEqualTo(3);
                    assertThat(dag.getInboundEdges(name)).isNotEmpty().allSatisfy(edge ->
                            assertThat(edge.getPartitioner().getConstantPartitioningKey())
                                    .describedAs("%s -> %s is routed by the key of the state it changes",
                                            edge.getSourceName(), name)
                                    .isNull());
                });
    }

    @Test
    void aNodeRunAsOneProcessorPinsEveryVertexThatKeepsStateAndSendsEveryEdgeThere() {
        NestTopology topology = NestTopology.compile("p", "doc", TREE, tables());
        DAG dag = draw(topology, NodeWidth.totalOne("doc", null));

        assertThat(keepingState(topology)).hasSize(3).allSatisfy(name -> {
            Vertex vertex = dag.getVertex(name);
            assertThat(vertex.getLocalParallelism())
                    .describedAs("%s leaves its count to the one-processor pin", name)
                    .isEqualTo(Vertex.LOCAL_PARALLELISM_USE_DEFAULT);
            assertThat(vertex.getMetaSupplier().preferredLocalParallelism())
                    .describedAs("%s runs one processor for the whole cluster", name)
                    .isEqualTo(1);
            assertThat(dag.getInboundEdges(name)).isNotEmpty().allSatisfy(edge ->
                    assertThat(edge.getPartitioner().getConstantPartitioningKey())
                            .describedAs("%s -> %s delivers to the member the vertex is pinned to",
                                    edge.getSourceName(), name)
                            .isEqualTo(name));
        });
    }

    @Test
    void theVertexThatGathersAnAliasIsLeftAtTheOneInstanceItMustRun() {
        NestTopology topology = NestTopology.compile("p", "doc", TREE, tables());
        DAG dag = draw(topology, new NodeWidth("doc", 3, 1, null));

        // Whatever the nest drew that is neither one of its state-carrying vertices nor a source: named
        // after the vertex it feeds and the alias it gathers, which is not a name to match on.
        List<String> known = new ArrayList<>(SOURCES);
        known.addAll(keepingState(topology));
        List<Vertex> gathering = new ArrayList<>();
        dag.forEach(vertex -> {
            if (!known.contains(vertex.getName())) {
                gathering.add(vertex);
            }
        });

        assertThat(gathering)
                .describedAs("the alias with two producers compiled to a gathering vertex, so there is one "
                        + "to check - without it this case passes over an empty list")
                .isNotEmpty();
        assertThat(gathering).allSatisfy(vertex -> assertThat(vertex.getLocalParallelism())
                .describedAs("%s is a single lane by construction and carries the engine's own answer for "
                        + "that; a count set here instead would be refused at submission", vertex.getName())
                .isEqualTo(Vertex.LOCAL_PARALLELISM_USE_DEFAULT));
    }

    private static List<String> keepingState(NestTopology topology) {
        List<String> names = new ArrayList<>();
        topology.vertices().forEach(vertex -> names.add(vertex.name()));
        topology.lookups().forEach(lookup -> names.add(lookup.name()));
        return names;
    }

    private static DAG draw(NestTopology topology, NodeWidth width) {
        DAG dag = new DAG();
        Map<String, List<Vertex>> sources = new LinkedHashMap<>();
        for (String alias : SOURCES) {
            sources.put(alias, List.of(dag.newVertex(alias, Processors.noopP())));
        }
        // One alias arriving from two places, which is what a gathering vertex is drawn for.
        sources.put("claim", List.of(sources.get("claim").get(0), sources.get("claim-2").get(0)));
        // Ordinals are handed out as asked for: the stream doing the pointing leaves its source three
        // times - once towards its document and twice to be recorded against what it points at - and a
        // graph answering zero every time refuses the second edge.
        Map<Vertex, Integer> handedOut = new java.util.HashMap<>();
        NestDag.attach(dag, topology, "doc", "order", "doc", sources::get,
                new NestBinding(tables(), HeapNestStores.onHeap(), (from, released) -> { }),
                vertex -> handedOut.merge(vertex, 1, Integer::sum) - 1, null, width);
        return dag;
    }
}
