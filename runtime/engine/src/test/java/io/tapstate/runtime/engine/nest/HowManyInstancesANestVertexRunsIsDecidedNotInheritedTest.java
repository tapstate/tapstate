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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * How many instances of a state-carrying nest vertex a member runs is written down, not taken from the
 * engine.
 *
 * <p>The engine's answer is the member's core count, which is the budget for work that computes. These
 * vertices do not compute; each of them declares itself non-cooperative and holds a thread of its own for
 * the life of the job, waiting on a state map. So a wider machine buys nothing here and costs a thread per
 * vertex per core - and since one tree may compile to as many state-carrying vertices as the limit on them
 * allows, the two numbers multiply, giving a worst case that grows with the box rather than with the work.
 *
 * <p><b>Nothing observable goes wrong when this is left off, which is why it is asserted rather than
 * reviewed.</b> Every document still assembles and every count is still right; what changes is a thread
 * count nobody is looking at, on a machine that is not the one the author was using.
 *
 * <p>Two things beyond the number itself are pinned here. It is above one, because a member running a
 * single instance of each vertex puts both sides of every hand-over between instances inside one of them,
 * where releasing what is held and merely forgetting it locally stop being distinguishable - so the paths
 * that carry a subtree from one key to another would go on being written and stop being reachable on a
 * single machine. And it does not reach the vertices that gather an alias's several producers into one:
 * those exist to be a single lane and are pinned to one instance in the whole cluster, so a second answer
 * for them is not a slower job but a refused one.
 */
class HowManyInstancesANestVertexRunsIsDecidedNotInheritedTest {

    /** Two producers of one alias, so the graph has a gathering vertex to leave alone as well. */
    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"),
                    embed("claim", "item_id", "item_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))),
            embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null));

    private static final List<String> SOURCES = List.of("order", "item", "claim", "claim-2", "customer");

    @Test
    void everyVertexThatKeepsStateRunsTheNumberOfInstancesThisDecidedOn() {
        NestTopology topology = NestTopology.compile("p", "doc", TREE, tables());
        DAG dag = draw(topology);

        List<String> keepingState = new ArrayList<>();
        topology.vertices().forEach(vertex -> keepingState.add(vertex.name()));
        topology.lookups().forEach(lookup -> keepingState.add(lookup.name()));

        assertThat(keepingState)
                .describedAs("all three kinds are in the tree - a resolver, the assembler, and the vertex "
                        + "filing the rows the root points at - so none of them is covered by an "
                        + "assertion that had nothing to walk")
                .hasSize(3);
        assertThat(keepingState).allSatisfy(name -> assertThat(dag.getVertex(name).getLocalParallelism())
                .describedAs("%s says how many instances of it a member runs, rather than inheriting the "
                        + "count meant for work that computes", name)
                .isEqualTo(NestDag.STATE_VERTEX_LOCAL_PARALLELISM));

        assertThat(NestDag.STATE_VERTEX_LOCAL_PARALLELISM)
                .describedAs("above one, so that what one instance hands to another still happens between "
                        + "two of them on a single machine rather than inside one")
                .isGreaterThan(1);
    }

    @Test
    void theVertexThatGathersAnAliasIsLeftAtTheOneInstanceItMustRun() {
        NestTopology topology = NestTopology.compile("p", "doc", TREE, tables());
        DAG dag = draw(topology);

        // Whatever the nest drew that is neither one of its state-carrying vertices nor a source: named
        // after the vertex it feeds and the alias it gathers, which is not a name to match on.
        List<String> known = new ArrayList<>(SOURCES);
        topology.vertices().forEach(vertex -> known.add(vertex.name()));
        topology.lookups().forEach(lookup -> known.add(lookup.name()));
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

    private static DAG draw(NestTopology topology) {
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
                vertex -> handedOut.merge(vertex, 1, Integer::sum) - 1, null);
        return dag;
    }
}
