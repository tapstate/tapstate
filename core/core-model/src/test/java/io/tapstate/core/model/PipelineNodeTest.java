package io.tapstate.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.jupiter.api.Test;

class PipelineNodeTest {

    @Test
    void bothIdsAreRequired() {
        assertThatNullPointerException().isThrownBy(() -> new PipelineNode(null, "src_a"));
        assertThatNullPointerException().isThrownBy(() -> new PipelineNode("p1", null));
    }

    /**
     * A blank id is refused rather than accepted, because what is built from this pair is a namespace
     * name: two nodes whose ids differ only in that one of them is blank would otherwise be told apart
     * by nothing, and whichever ran second would read the other's notes.
     */
    @Test
    void neitherIdMayBeBlank() {
        assertThatIllegalArgumentException().isThrownBy(() -> new PipelineNode(" ", "src_a"));
        assertThatIllegalArgumentException().isThrownBy(() -> new PipelineNode("p1", ""));
    }

    @Test
    void twoNodesWithTheSameIdsAreEqual() {
        assertThat(new PipelineNode("p1", "src_a")).isEqualTo(new PipelineNode("p1", "src_a"));
    }

    /**
     * The pair is ordered, not a set: the same two strings the other way round is a different node.
     * Nothing downstream re-checks which id is which, so a swapped pair resolves to a namespace that
     * exists and belongs to somebody else.
     */
    @Test
    void theTwoIdsAreNotInterchangeable() {
        assertThat(new PipelineNode("p1", "src_a")).isNotEqualTo(new PipelineNode("src_a", "p1"));
    }
}
