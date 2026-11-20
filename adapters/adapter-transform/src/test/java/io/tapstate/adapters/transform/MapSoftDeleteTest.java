package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A map naming a soft-delete column turns each delete into an update that marks the row, and marks
 * every other row as live.
 *
 * <p>The target never receives a delete from such a step, so the delete has to arrive as something a
 * target keeps: the row as it last was, with the marker set. A nest fed by the step keeps the element
 * in its array for the same reason. The cases assert whole rows, because a marker that lands on one
 * side of an update and not the other, or a rule that stops running on the removed row, is exactly
 * what would leave a target holding a row that reads live.
 */
class MapSoftDeleteTest {

    private static final String MARKER = "is_deleted";

    private static TransformPort map(Map<String, FieldRule> fields, String softDelete) {
        return StatelessTransforms.map(MapSpec.from(new TransformBody.MapProjection(fields, softDelete)));
    }

    private static Map<String, Object> row(Object id, String email) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", id);
        r.put("email", email);
        return r;
    }

    /** A row through a map with no rules: the marker comes first, then every column passed through. */
    private static Map<String, Object> marked(Map<String, Object> row, boolean deleted) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put(MARKER, deleted);
        r.putAll(row);
        return r;
    }

    @Test
    @DisplayName("a delete leaves as an update of the removed row, marked deleted")
    void aDeleteBecomesAMarkedUpdate() {
        List<Envelope> out = map(Map.of(), MARKER)
                .transform(new Envelope(Op.DELETE, 7L, "emails", row(1, "a@x"), null, null));

        assertThat(out).hasSize(1);
        Envelope update = out.get(0);
        assertThat(update.op()).isEqualTo(Op.UPDATE);
        assertThat(update.ts()).isEqualTo(7L);
        assertThat(update.src()).isEqualTo("emails");
        assertThat(update.before()).containsExactlyEntriesOf(marked(row(1, "a@x"), false));
        assertThat(update.after()).containsExactlyEntriesOf(marked(row(1, "a@x"), true));
    }

    @Test
    @DisplayName("inserts, reads and updates carry the marker as false on every row they hold")
    void liveRowsAreMarkedLive() {
        TransformPort port = map(Map.of(), MARKER);

        Envelope insert = port.transform(Envelope.insert(1L, "emails", row(1, "a@x"), null)).get(0);
        Envelope read = port.transform(new Envelope(Op.READ, 1L, "emails", null, row(1, "a@x"), null)).get(0);
        Envelope update = port.transform(
                new Envelope(Op.UPDATE, 2L, "emails", row(1, "a@x"), row(1, "b@x"), null)).get(0);

        assertThat(insert.op()).isEqualTo(Op.INSERT);
        assertThat(insert.after()).containsExactlyEntriesOf(marked(row(1, "a@x"), false));
        assertThat(read.op()).isEqualTo(Op.READ);
        assertThat(read.after()).containsExactlyEntriesOf(marked(row(1, "a@x"), false));
        assertThat(update.op()).isEqualTo(Op.UPDATE);
        assertThat(update.before()).containsExactlyEntriesOf(marked(row(1, "a@x"), false));
        assertThat(update.after()).containsExactlyEntriesOf(marked(row(1, "b@x"), false));
    }

    /**
     * Delete then insert of the same key is how a source restores a row. The insert must read as
     * live again rather than keep the marker the delete left on the target.
     */
    @Test
    @DisplayName("a row inserted again after a delete reads as live")
    void reinsertAfterDeleteIsLive() {
        TransformPort port = map(Map.of(), MARKER);

        Envelope deleted = port.transform(new Envelope(Op.DELETE, 1L, "emails", row(1, "a@x"), null, null)).get(0);
        Envelope back = port.transform(Envelope.insert(2L, "emails", row(1, "a@x"), null)).get(0);

        assertThat(deleted.after()).containsEntry(MARKER, true);
        assertThat(back.after()).containsEntry(MARKER, false);
    }

    @Test
    @DisplayName("the rules run on the removed row, so the marked row has the projected shape")
    void rulesRunOnTheRemovedRow() {
        Map<String, FieldRule> fields = new LinkedHashMap<>();
        fields.put("email_id", FieldRule.rename("id"));
        fields.put("contact", FieldRule.computed("'mail:' + after.email"));
        Envelope out = map(fields, MARKER)
                .transform(new Envelope(Op.DELETE, 1L, "emails", row(1, "a@x"), null, null)).get(0);

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("email_id", 1);
        expected.put("contact", "mail:a@x");
        expected.put(MARKER, true);
        expected.put("email", "a@x");
        assertThat(out.after()).containsExactlyEntriesOf(expected);
    }

    /**
     * A source that sends only the identifying columns on a delete gives an update of just those and
     * the marker. Nothing is invented for the columns it did not send.
     */
    @Test
    @DisplayName("a key-only delete becomes an update of the key and the marker")
    void keyOnlyDelete() {
        Envelope out = map(Map.of(), MARKER)
                .transform(new Envelope(Op.DELETE, 1L, "emails", Map.of("id", 1), null, null)).get(0);

        assertThat(out.op()).isEqualTo(Op.UPDATE);
        assertThat(out.after()).containsExactlyEntriesOf(marked(Map.of("id", 1), true));
    }

    @Test
    @DisplayName("a source column of the marker's name is replaced, not carried beside it")
    void sourceColumnOfTheSameNameIsReplaced() {
        Map<String, Object> source = row(1, "a@x");
        source.put(MARKER, "N");

        Envelope out = map(Map.of(), MARKER)
                .transform(new Envelope(Op.DELETE, 1L, "emails", source, null, null)).get(0);

        assertThat(out.after()).containsEntry(MARKER, true).hasSize(3);
    }

    @Test
    @DisplayName("without soft_delete a delete stays a delete and gains no column")
    void withoutTheOptionDeletesStayDeletes() {
        Envelope out = map(Map.of("email", FieldRule.rename("email")), null)
                .transform(new Envelope(Op.DELETE, 1L, "emails", row(1, "a@x"), null, null)).get(0);

        assertThat(out.op()).isEqualTo(Op.DELETE);
        assertThat(out.before()).doesNotContainKey(MARKER);
    }

    @Test
    @DisplayName("a schema change passes through untouched")
    void ddlPassesThrough() {
        Envelope ddl = new Envelope(Op.DDL, 1L, "emails", null, null, Map.of("columns", List.of()));

        assertThat(map(Map.of(), MARKER).transform(ddl)).containsExactly(ddl);
    }

    @Test
    @DisplayName("the fields an earlier step removed still travel with the marked update")
    void removedFieldsAreCarried() {
        Envelope delete = new Envelope(Op.DELETE, 1L, "emails", row(1, "a@x"), null, null)
                .withRemoved(Set.of("legacy"));

        Envelope out = map(Map.of(), MARKER).transform(delete).get(0);

        assertThat(out.removed()).containsExactly("legacy");
    }

    /**
     * An earlier step that removed a column of the marker's name said so of its own column. This step
     * writes the marker on every row, so that removal must not travel on and strip it at the target.
     */
    @Test
    @DisplayName("a removal of the marker's own name is not carried past the step that writes it")
    void aRemovalOfTheMarkerIsDropped() {
        TransformPort port = map(Map.of(), MARKER);
        Envelope insert = Envelope.insert(1L, "emails", row(1, "a@x"), null).withRemoved(Set.of(MARKER, "legacy"));
        Envelope delete = new Envelope(Op.DELETE, 1L, "emails", row(1, "a@x"), null, null)
                .withRemoved(Set.of(MARKER, "legacy"));

        assertThat(port.transform(insert).get(0).removed()).containsExactly("legacy");
        assertThat(port.transform(delete).get(0).removed()).containsExactly("legacy");
    }
}
