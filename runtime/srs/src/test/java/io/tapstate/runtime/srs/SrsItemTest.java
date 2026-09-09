package io.tapstate.runtime.srs;

import io.tapstate.core.event.Op;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SrsItemTest {

    private static final SourcePosition POS = new SourcePosition("gtid:aaa-1:100");

    @Test
    void holdsTheChangeSlotsForAnUpdate() {
        SrsItem item = new SrsItem(POS, Op.UPDATE, 1720000000000L,
                Map.of("id", 1, "name", "old"), Map.of("id", 1, "name", "new"), 3L);
        assertThat(item.srcPos()).isEqualTo(POS);
        assertThat(item.op()).isEqualTo(Op.UPDATE);
        assertThat(item.ts()).isEqualTo(1720000000000L);
        assertThat(item.before()).containsEntry("name", "old");
        assertThat(item.after()).containsEntry("name", "new");
        assertThat(item.schemaVer()).isEqualTo(3L);
    }

    @Test
    void allowsAnAbsentBeforeForAnInsert() {
        SrsItem item = new SrsItem(POS, Op.INSERT, 1L, null, Map.of("id", 1), 0L);
        assertThat(item.before()).isNull();
        assertThat(item.after()).containsEntry("id", 1);
    }

    @Test
    void allowsAnAbsentAfterForADelete() {
        SrsItem item = new SrsItem(POS, Op.DELETE, 1L, Map.of("id", 1), null, 0L);
        assertThat(item.before()).containsEntry("id", 1);
        assertThat(item.after()).isNull();
    }

    @Test
    void copiesTheRowMapsSoALaterMutationDoesNotLeakIn() {
        Map<String, Object> after = new HashMap<>();
        after.put("id", 1);
        SrsItem item = new SrsItem(POS, Op.INSERT, 1L, null, after, 0L);
        after.put("id", 999);
        assertThat(item.after()).containsEntry("id", 1);
    }

    @Test
    void rejectsMutationOfTheReturnedRowMap() {
        SrsItem item = new SrsItem(POS, Op.INSERT, 1L, null, Map.of("id", 1), 0L);
        assertThatThrownBy(() -> item.after().put("name", "x"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsASnapshotReadOpBecauseSnapshotRowsNeverEnterTheRing() {
        // The change ring holds only cdc mutations (i/u/d/ddl); a snapshot read (op r) goes straight to
        // the sink and must never be buffered here.
        assertThatThrownBy(() -> new SrsItem(POS, Op.READ, 1L, null, Map.of("id", 1), 0L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void acceptsAChangeTheSourceStatedNoPositionAt() {
        // A source names one position for a run of changes -- "everything up to here has been handed
        // over" -- so only the change closing that run carries one. Requiring one on every change would
        // force the runtime to fill the others in, which says a change was read that was not; a run
        // interrupted between them would then resume past changes it never delivered. Nothing here ranks
        // by the position anyway: ordering is the ring's own sequence paired with its generation.
        SrsItem item = new SrsItem(null, Op.INSERT, 1L, null, Map.of("id", 1), 0L);

        assertThat(item.srcPos()).isNull();
    }

    @Test
    void rejectsANullOp() {
        assertThatThrownBy(() -> new SrsItem(POS, null, 1L, null, Map.of("id", 1), 0L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsANegativeSchemaVersion() {
        assertThatThrownBy(() -> new SrsItem(POS, Op.INSERT, 1L, null, Map.of("id", 1), -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
