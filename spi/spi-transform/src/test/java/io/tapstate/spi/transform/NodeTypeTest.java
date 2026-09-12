package io.tapstate.spi.transform;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** The transform node kinds are a closed, fixed set. */
class NodeTypeTest {

    @Test
    void nodeTypesAreTheClosedSetOfSeven() {
        assertThat(NodeType.values())
                .containsExactly(
                        NodeType.FILTER,
                        NodeType.MAP,
                        NodeType.JS,
                        NodeType.UNWIND,
                        NodeType.UNION,
                        NodeType.NEST,
                        NodeType.JOIN);
    }
}
