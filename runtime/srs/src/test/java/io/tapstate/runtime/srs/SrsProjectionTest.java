package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Projects a {@link SrsItem} out of the change ring into the transform-facing {@link Envelope}: the
 * position token enters the envelope currency here, the stream name is injected from the source vertex
 * the reader is bound to (the ring is per-table, so the item does not carry it), and schema stays null
 * in the lean tier (the item points at schema history by version rather than repeating it).
 */
class SrsProjectionTest {

    private static SrsItem insert(String token, Map<String, Object> after) {
        return new SrsItem(new SourcePosition(token), Op.INSERT, 100L, null, after, 0L);
    }

    @Test
    void projectsTheSourcePositionTokenIntoTheEnvelope() {
        Envelope e = SrsProjection.toEnvelope(insert("gtid:aaa:99", Map.of("id", 1)), "orders", new SourceOrder(1L, 5L));
        assertThat(e.position().token()).isEqualTo("gtid:aaa:99");
    }

    @Test
    void injectsTheStreamNameTheItemDoesNotCarry() {
        Envelope e = SrsProjection.toEnvelope(insert("p1", Map.of("id", 1)), "orders", new SourceOrder(1L, 5L));
        assertThat(e.src()).isEqualTo("orders");
    }

    @Test
    void carriesOpTsAndTheAfterImageForAnInsert() {
        Envelope e = SrsProjection.toEnvelope(insert("p1", Map.of("id", 7)), "orders", new SourceOrder(1L, 5L));
        assertThat(e.op()).isEqualTo(Op.INSERT);
        assertThat(e.ts()).isEqualTo(100L);
        assertThat(e.after()).containsEntry("id", 7);
        assertThat(e.before()).isNull();
    }

    @Test
    void carriesBothRowImagesForAnUpdate() {
        SrsItem item = new SrsItem(new SourcePosition("p1"), Op.UPDATE, 1L,
                Map.of("v", "old"), Map.of("v", "new"), 0L);
        Envelope e = SrsProjection.toEnvelope(item, "orders", new SourceOrder(1L, 5L));
        assertThat(e.op()).isEqualTo(Op.UPDATE);
        assertThat(e.before()).containsEntry("v", "old");
        assertThat(e.after()).containsEntry("v", "new");
    }

    @Test
    void carriesTheBeforeImageForADelete() {
        SrsItem item = new SrsItem(new SourcePosition("p1"), Op.DELETE, 1L, Map.of("id", 7), null, 0L);
        Envelope e = SrsProjection.toEnvelope(item, "orders", new SourceOrder(1L, 5L));
        assertThat(e.op()).isEqualTo(Op.DELETE);
        assertThat(e.before()).containsEntry("id", 7);
        assertThat(e.after()).isNull();
    }

    @Test
    void projectsTheEngineAssignedOrderAlongsideTheConnectorsToken() {
        Envelope e = SrsProjection.toEnvelope(
                insert("gtid:aaa:99", Map.of("id", 1)), "orders", new SourceOrder(7L, 42L));

        // Two quantities, two questions. The token says where to resume a read and the engine never parses
        // it; the order says which of two changes is later and is the only one any comparison uses. This is
        // where both enter the currency, so they are always the pair belonging to one change.
        assertThat(e.position().order()).isEqualTo(new SourceOrder(7L, 42L));
        assertThat(e.position().token()).isEqualTo("gtid:aaa:99");
    }

    @Test
    void leavesSchemaNullInTheLeanTier() {
        Envelope e = SrsProjection.toEnvelope(insert("p1", Map.of("id", 1)), "orders", new SourceOrder(1L, 5L));
        assertThat(e.schema()).isNull();
    }
}
