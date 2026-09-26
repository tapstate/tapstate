package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What reaches a vertex, as the graph is drawn: every chain whose bounds it has to wait on, and - apart from
 * that - the chains whose own rows reach it, which is narrower. A nest or a join assembles rows of its own out
 * of the chains it reads: their bounds travel on, but a row it emits is never a row one of those tables' sources
 * read, so a sink below it receives none of their rows.
 */
class PipelineChainsTest {

    @Test
    void aTablesOwnRowsReachPastEveryStepButOneThatAssemblesRowsOfItsOwn() {
        PipelineChains chains = new PipelineChains();
        chains.source("orders_src", "orders");
        chains.source("items_src", "items");
        chains.source("logs_src", "logs");
        chains.derived("filtered", List.of("orders_src"));
        chains.assembled("joined", List.of("filtered", "items_src"));
        chains.derived("gathered", List.of("joined", "logs_src"));

        assertThat(chains.unassembled(List.of("filtered")))
                .as("a step reshaping rows one at a time passes the table's own rows on")
                .containsExactly("orders");
        assertThat(chains.unassembled(List.of("joined"))).as("an assembling step's rows are its own").isEmpty();
        assertThat(chains.unassembled(List.of("gathered"))).containsExactly("logs");
        assertThat(chains.union(List.of("gathered")))
                .as("while every chain behind it still reaches it, bounds and all")
                .containsExactlyInAnyOrder("orders", "items", "logs");
    }
}
