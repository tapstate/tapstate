package io.tapstate.spi.store;

import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.ChainPosition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    void holdsTheTablesThisPipelineHasFinishedLoading() {
        ConsumerOffset offset = new ConsumerOffset(
                "orders-pipeline", Map.of("orders", 42L), null, List.of("orders", "order_items"));
        assertThat(offset.snapshotCompletedTables()).containsExactly("orders", "order_items");
    }

    /**
     * The three-argument shape leaves the completion set empty rather than absent.
     *
     * <p>It is the shape a consumer has before its sink confirms anything, and the two writers that create
     * a consumer record -- the read cursor and the sink-ack -- both use it. Empty is the correct reading
     * for them: a pipeline that has confirmed nothing owes every table it selected.
     */
    @Test
    void theThreeArgConstructorLeavesNoTableFinished() {
        ConsumerOffset offset = new ConsumerOffset("p", Map.of("orders", 5L), null);
        assertThat(offset.snapshotCompletedTables()).isEmpty();
    }

    @Test
    void copiesTheCompletedTablesSoALaterMutationDoesNotLeakIn() {
        List<String> live = new ArrayList<>();
        live.add("orders");
        ConsumerOffset offset = new ConsumerOffset("p", Map.of(), null, live);
        live.clear();
        assertThat(offset.snapshotCompletedTables()).containsExactly("orders");
    }

    @Test
    void rejectsMutationOfTheReturnedCompletedTables() {
        ConsumerOffset offset = new ConsumerOffset("p", Map.of(), null, List.of("orders"));
        assertThatThrownBy(() -> offset.snapshotCompletedTables().add("items"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsANullCompletedTables() {
        assertThatThrownBy(() -> new ConsumerOffset("p", Map.of(), null, null))
                .isInstanceOf(IllegalArgumentException.class);
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
