package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.Processor;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Every vertex a nest tree compiles to declares itself non-cooperative, whatever kind it is.
 *
 * <p>A cooperative processor promises never to block, and shares one thread with every other cooperative
 * vertex in the job. These vertices cannot make that promise: each event they handle reads and writes a
 * state map, and a map that is not purely on the heap can go to a disk on the way. One that blocked while
 * claiming to be cooperative would stall every unrelated vertex sharing its thread - and nothing would say
 * so. Nothing throws, no counter moves, the pipeline stays healthy and simply falls behind, which is why
 * this is pinned rather than left to a reviewer to notice.
 *
 * <p>The tree is walked rather than a single vertex named, so a kind that is added later has to be given
 * an answer here too instead of inheriting whatever the engine's default happens to be.
 */
class NoNestVertexRunsOnACooperativeThreadTest {

    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("item", "order_id", "order_id", EmbedAs.ARRAY, "items", List.of("item_id"),
                    embed("claim", "item_id", "item_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))),
            // Pointed at rather than gathered - the column the root joins on is what identifies a
            // customer - so this compiles to the third kind of vertex.
            embed("customer", "customer_id", "customer_id", EmbedAs.OBJECT, "customer", null));

    @Test
    void everyVertexOfACompiledTreeRefusesTheCooperativePool() {
        NestTopology topology = NestTopology.compile("p", "doc", TREE, tables());

        assertThat(topology.vertices())
                .describedAs("both assembling kinds are present - a resolver per non-leaf embed and one "
                        + "assembler - so that neither is checked by an assertion that had nothing to walk")
                .hasSize(2)
                .allSatisfy(vertex -> assertThat(processorFor(topology, vertex).isCooperative())
                        .describedAs("%s must not share a cooperative thread", vertex.name())
                        .isFalse());
        assertThat(topology.lookups())
                .describedAs("and the kind that files the rows a level points at, which reaches the same "
                        + "state layer as the other two and arrived long after them. It is walked here "
                        + "rather than left to inherit whatever the engine defaults to, which is the "
                        + "cooperative pool it must not be in")
                .isNotEmpty()
                .allSatisfy(lookup -> assertThat(processorFor(lookup).isCooperative())
                        .describedAs("%s must not share a cooperative thread", lookup.name())
                        .isFalse());
    }

    private static Processor processorFor(NestLookup lookup) {
        return new LookupProcessor(lookup, new HeapNestStore<>(), new HeapNestStore<>(), 1L, null);
    }

    private static Processor processorFor(NestTopology topology, NestVertex vertex) {
        return vertex.isAssembler()
                ? new AssemblerProcessor(vertex, topology.slots(), new HeapNestStore<>(), "doc")
                : new ResolverProcessor(vertex, new HeapNestStore<>(), (from, released) -> { });
    }
}
