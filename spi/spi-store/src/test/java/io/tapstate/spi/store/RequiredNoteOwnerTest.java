package io.tapstate.spi.store;

import io.tapstate.core.model.PipelineNode;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequiredNoteOwnerTest {
    @Test
    void aSharedCaptureCannotBorrowPrivateViewOrSyncNotes() {
        var shared = new RequiredNoteScope(new RequiredNoteOwner(1, "pdk.chain.chain-a",
                RequiredNoteOwner.Kind.CHAIN, "chain-a", 0), 1);
        PipelineNode node = new PipelineNode("p1", "shared");
        for (var purpose : new PipelineNode.Purpose[]{PipelineNode.Purpose.VIEW, PipelineNode.Purpose.SYNC}) {
            var sink = new RequiredNoteScope(RequiredNoteOwner.forNode(1, node, purpose, 1), 1);
            assertThatThrownBy(() -> new RequiredNoteCarry(shared, sink)).isInstanceOf(IllegalArgumentException.class);
        }
        var capture = new RequiredNoteScope(RequiredNoteOwner.forNode(1, node, PipelineNode.Purpose.CAPTURE, 1), 1);
        assertThat(new RequiredNoteCarry(shared, capture).source()).isEqualTo(capture);
    }

    @Test
    void aPipelineOwnerRequiresTheFixedPurposeAndANonemptyNodeId() {
        assertThatThrownBy(() -> new RequiredNoteOwner(1, "pdk.state.p1.node", RequiredNoteOwner.Kind.PIPELINE,
                "p1", 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RequiredNoteOwner(1, "pdk.state.p1.capture.", RequiredNoteOwner.Kind.PIPELINE,
                "p1", 1)).isInstanceOf(IllegalArgumentException.class);
    }
}
