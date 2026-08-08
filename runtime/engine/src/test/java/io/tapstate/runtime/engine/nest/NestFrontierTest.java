package io.tapstate.runtime.engine.nest;

import io.tapstate.runtime.engine.ChainAxes;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a nest node is told about the chains behind each of its aliases. The per-producer shape is the
 * load-bearing part: where an alias resolves to several producers they are gathered by one vertex first,
 * and that vertex is the only place that knows which chain arrives on which of its own edges.
 */
class NestFrontierTest {

    private static final ChainAxes AXES = ChainAxes.assign(List.of("customers", "items", "orders"));

    @Test
    void theAliasCarriesEveryChainItsProducersCarryBetweenThem() {
        NestFrontier frontier = new NestFrontier(AXES, Map.of(
                "lines", List.of(List.of("items"), List.of("orders")))::get);

        assertThat(frontier.chainsOfAlias("lines")).containsExactly("items", "orders");
    }

    @Test
    void aChainTwoProducersBothCarryIsNamedOnceInTheAliasSet() {
        NestFrontier frontier = new NestFrontier(AXES, Map.of(
                "lines", List.of(List.of("orders"), List.of("orders", "items")))::get);

        // The alias's set says which chains a level must wait on, and a chain named twice would be waited
        // on twice over - while the per-producer lists it is derived from stay apart, because that is what
        // says which edge of the gathering each one arrives on.
        assertThat(frontier.chainsOfAlias("lines")).containsExactly("orders", "items");
    }
}
