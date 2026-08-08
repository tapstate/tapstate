package io.tapstate.spi.store;

import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.ChainPosition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerOffsetTest {

    @Test
    void holdsThePipelineCursorAndAckedPosition() {
        ConsumerOffset offset = new ConsumerOffset("orders-pipeline", Map.of("orders", 42L, "items", 7L), new ChainPosition(new SourceOrder(1, 100), "gtid:aaa-1:100"));
        assertThat(offset.pipelineId()).isEqualTo("orders-pipeline");
        assertThat(offset.perTableSeq()).containsEntry("orders", 42L).containsEntry("items", 7L);
        assertThat(offset.sinkAckedSrcpos()).isEqualTo("gtid:aaa-1:100");
    }

    @Test
    void allowsANullAckedPositionBeforeAnythingIsAcked() {
        // A consumer that has read from the ring but not yet had any change durably acked by its sink:
        // its per-table read cursor exists, but the acked source position that gates offset advance is
        // still absent.
        ConsumerOffset offset = new ConsumerOffset("orders-pipeline", Map.of("orders", 5L), null);
        assertThat(offset.sinkAckedSrcpos()).isNull();
        assertThat(offset.perTableSeq()).containsEntry("orders", 5L);
    }

    @Test
    void copiesThePerTableCursorSoALaterMutationDoesNotLeakIn() {
        Map<String, Long> live = new HashMap<>();
        live.put("orders", 1L);
        ConsumerOffset offset = new ConsumerOffset("p", live, null);
        live.put("orders", 999L);
        assertThat(offset.perTableSeq()).containsEntry("orders", 1L);
    }

    @Test
    void rejectsMutationOfTheReturnedCursor() {
        ConsumerOffset offset = new ConsumerOffset("p", Map.of("orders", 1L), null);
        assertThatThrownBy(() -> offset.perTableSeq().put("items", 2L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsABlankPipelineId() {
        assertThatThrownBy(() -> new ConsumerOffset("  ", Map.of(), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsANullPerTableCursor() {
        assertThatThrownBy(() -> new ConsumerOffset("p", null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
