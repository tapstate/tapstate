package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * One row carrying a list becoming one row per element, op by op.
 *
 * <p>Almost everything here is a branch, and a branch an end-to-end case cannot reach: a run walks
 * one of them per event, so the shapes a real source actually sends - an empty list, a column the
 * document happens not to have, a change stream with no earlier row - are only ever all covered
 * here. The two that matter most are the ones whose wrong answer is silence. An expansion that
 * passes a delete through untouched leaves every row it once produced standing in the target, and
 * an expansion that pairs an update by element value instead of by key leaves the old rows behind
 * whenever a parent's key changes; both run green over any data that never exercises them.
 */
class UnwindPortTest {

    /** The rows arriving here are keyed on the parent's own key; the expansion adds to it. */
    private static final List<String> PARENT_KEY = List.of("o_id");

    private static TransformBody.Unwind unwind(
            String includeArrayIndex, Boolean preserve, String elementKey) {
        return new TransformBody.Unwind("items", includeArrayIndex, preserve, elementKey, null);
    }

    private static UnwindPort port(TransformBody.Unwind body) {
        return new UnwindPort(UnwindSpec.from(body, PARENT_KEY), coded -> { });
    }

    /** The ordinary shape: the element carries its own identity. */
    private static UnwindPort bySku() {
        return port(unwind(null, null, "sku"));
    }

    private static Map<String, Object> row(Object items, Object... rest) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("o_id", 7L);
        r.put("region", "eu");
        if (items != ABSENT) {
            r.put("items", items);
        }
        for (int i = 0; i < rest.length; i += 2) {
            r.put((String) rest[i], rest[i + 1]);
        }
        return r;
    }

    /** Distinguishes "the column is not in the document" from "the column is there holding null". */
    private static final Object ABSENT = new Object();

    private static Map<String, Object> element(String sku, long qty) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("sku", sku);
        e.put("qty", qty);
        return e;
    }

    private static Envelope insert(Map<String, Object> after) {
        return new Envelope(Op.INSERT, 1L, "orders", null, after, null);
    }

    private static Envelope delete(Map<String, Object> before) {
        return new Envelope(Op.DELETE, 1L, "orders", before, null, null);
    }

    private static Envelope update(Map<String, Object> before, Map<String, Object> after) {
        return new Envelope(Op.UPDATE, 1L, "orders", before, after, null);
    }

    private static List<Object> itemsOf(List<Envelope> out) {
        List<Object> values = new ArrayList<>();
        out.forEach(e -> values.add((e.after() == null ? e.before() : e.after()).get("items")));
        return values;
    }

    // ---- what is in the column, and what that makes of the row -------------------------

    @Test
    @DisplayName("one row per element, the list's field replaced by the element in each")
    void oneRowPerElement() {
        List<Envelope> out = bySku().transform(
                insert(row(List.of(element("a", 1), element("b", 2), element("c", 3)))));

        assertThat(out).hasSize(3);
        assertThat(itemsOf(out)).containsExactly(element("a", 1), element("b", 2), element("c", 3));
        // The parent's other columns travel on every row: an implementation that emitted only the
        // element would pass a count assertion and lose the rest of the order.
        assertThat(out.get(0).after()).containsEntry("o_id", 7L).containsEntry("region", "eu");
        assertThat(out).allSatisfy(e -> assertThat(e.op()).isEqualTo(Op.INSERT));
    }

    @Test
    @DisplayName("an empty, null or absent list produces no rows at all by default")
    void nothingToExpandProducesNothing() {
        assertThat(bySku().transform(insert(row(List.of())))).isEmpty();
        assertThat(bySku().transform(insert(row(null)))).isEmpty();
        assertThat(bySku().transform(insert(row(ABSENT)))).isEmpty();
    }

    @Test
    @DisplayName("preserving them keeps one row instead, the list's field left empty")
    void preservingKeepsOneEmptyRow() {
        UnwindPort keeping = port(unwind(null, true, "sku"));

        for (Object nothing : new Object[] {List.of(), null, ABSENT}) {
            List<Envelope> out = keeping.transform(insert(row(nothing)));

            assertThat(out).hasSize(1);
            assertThat(out.get(0).after()).containsEntry("items", null).containsEntry("o_id", 7L);
        }
    }

    /**
     * A value that is not a list at all is one element, not an error. Whether a column holds a list
     * is a fact about the row rather than the declaration, so the check could only ever happen at
     * runtime - and stopping a pipeline over one dirty row is a worse answer than expanding it as
     * the single thing it is.
     */
    @Test
    @DisplayName("a value that is not a list is expanded as the one element it is")
    void aScalarIsOneElement() {
        List<Envelope> out = bySku().transform(insert(row("just-this")));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).after()).containsEntry("items", "just-this");
    }

    // ---- the ordinal column -------------------------------------------------------------

    @Test
    @DisplayName("the element's position in the list is carried as its own column")
    void theOrdinalIsCarried() {
        UnwindPort numbered = port(unwind("item_no", null, "sku"));

        List<Envelope> out = numbered.transform(insert(row(List.of(element("a", 1), element("b", 2)))));

        assertThat(out).hasSize(2);
        assertThat(out.get(0).after()).containsEntry("item_no", 0L);
        assertThat(out.get(1).after()).containsEntry("item_no", 1L);
    }

    @Test
    @DisplayName("a row kept for an empty list has no position to carry")
    void thePreservedRowHasNoOrdinal() {
        UnwindPort numbered = port(unwind("item_no", true, "sku"));

        List<Envelope> out = numbered.transform(insert(row(List.of())));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).after()).containsEntry("item_no", null);
    }

    // ---- op by op -----------------------------------------------------------------------

    @Test
    @DisplayName("a snapshot read expands the same way an insert does")
    void aReadExpands() {
        List<Envelope> out = bySku().transform(
                new Envelope(Op.READ, 1L, "orders", null, row(List.of(element("a", 1), element("b", 2))), null));

        assertThat(out).hasSize(2);
        assertThat(out).allSatisfy(e -> assertThat(e.op()).isEqualTo(Op.READ));
    }

    /**
     * The one op whose wrong answer leaves the target permanently wrong. Passing a delete through
     * as one event removes at most the parent's own row, and every row the expansion once produced
     * stays behind with nothing left that refers to it.
     */
    @Test
    @DisplayName("a delete expands too, so every row the parent produced goes with it")
    void aDeleteExpandsIntoOneDeletePerElement() {
        List<Envelope> out = bySku().transform(
                delete(row(List.of(element("a", 1), element("b", 2)))));

        assertThat(out).hasSize(2);
        assertThat(out).allSatisfy(e -> {
            assertThat(e.op()).isEqualTo(Op.DELETE);
            assertThat(e.after()).isNull();
        });
        assertThat(itemsOf(out)).containsExactly(element("a", 1), element("b", 2));
    }

    @Test
    @DisplayName("a schema change carries no row and passes through as it came")
    void aDdlPassesThrough() {
        Envelope ddl = new Envelope(Op.DDL, 1L, "orders", null, null, Map.of("items", "array"));

        assertThat(bySku().transform(ddl)).containsExactly(ddl);
    }

    // ---- pairing an update by key, not by element value ---------------------------------

    @Test
    @DisplayName("an element dropped from the list is deleted, the rest updated")
    void anUpdatePairsByKey() {
        List<Envelope> out = bySku().transform(update(
                row(List.of(element("a", 1), element("b", 2))),
                row(List.of(element("a", 9)))));

        assertThat(out).hasSize(2);
        assertThat(out).filteredOn(e -> e.op() == Op.DELETE)
                .singleElement()
                .satisfies(e -> assertThat(e.before()).containsEntry("items", element("b", 2)));
        assertThat(out).filteredOn(e -> e.op() == Op.UPDATE)
                .singleElement()
                .satisfies(e -> assertThat(e.after()).containsEntry("items", element("a", 9)));
    }

    /**
     * The case element-value pairing gets wrong while passing every other test here. The list is
     * untouched and only the parent's key moves, so a pairing that compares elements sees nothing
     * to do and leaves the rows under the old key behind for good.
     */
    @Test
    @DisplayName("a parent that changes its key takes its old rows with it")
    void aParentKeyChangeDeletesTheOldRows() {
        Map<String, Object> was = row(List.of(element("a", 1), element("b", 2)));
        Map<String, Object> now = row(List.of(element("a", 1), element("b", 2)));
        now.put("o_id", 8L);

        List<Envelope> out = bySku().transform(update(was, now));

        assertThat(out).filteredOn(e -> e.op() == Op.DELETE).hasSize(2);
        assertThat(out).filteredOn(e -> e.op() != Op.DELETE).hasSize(2);
        assertThat(out).filteredOn(e -> e.op() == Op.DELETE)
                .allSatisfy(e -> assertThat(e.before()).containsEntry("o_id", 7L));
    }

    @Test
    @DisplayName("an element the list did not have before arrives as an insert")
    void anAddedElementIsAnInsert() {
        List<Envelope> out = bySku().transform(update(
                row(List.of(element("a", 1))),
                row(List.of(element("a", 1), element("b", 2)))));

        assertThat(out).filteredOn(e -> e.op() == Op.INSERT)
                .singleElement()
                .satisfies(e -> assertThat(e.after()).containsEntry("items", element("b", 2)));
        assertThat(out).filteredOn(e -> e.op() == Op.DELETE).isEmpty();
    }

    // ---- the earlier row has to be a whole row ------------------------------------------

    private static TapstateException refusedFor(Envelope event) {
        return catchThrowableOfType(TapstateException.class, () -> bySku().transform(event));
    }

    /**
     * Three shapes of half a row, and they must be told apart from a whole one by what the row
     * carries rather than by whether it has the expanded column. The three are separate cases on
     * purpose: an implementation testing only for a null earlier row passes the last two and goes
     * on silently swallowing every delete, and those two are what real sources actually send - a
     * change stream with no pre-image configured sends an empty row, and a relational source under
     * its default replica identity sends the key columns and nothing else.
     */
    @Test
    @DisplayName("no earlier row at all is refused rather than read as an empty list")
    void aDeleteWithNoEarlierRowIsRefused() {
        TapstateException refusal = refusedFor(new Envelope(Op.DELETE, 1L, "orders", null, null, null));

        assertThat(refusal.code()).isEqualTo(TransformError.UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE);
    }

    @Test
    @DisplayName("an empty earlier row is refused")
    void aDeleteWithAnEmptyEarlierRowIsRefused() {
        assertThat(refusedFor(delete(new LinkedHashMap<>())).code())
                .isEqualTo(TransformError.UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE);
    }

    @Test
    @DisplayName("an earlier row carrying only the key columns is refused")
    void aDeleteWithOnlyTheKeyIsRefused() {
        Map<String, Object> keyOnly = new LinkedHashMap<>();
        keyOnly.put("o_id", 7L);

        assertThat(refusedFor(delete(keyOnly)).code())
                .isEqualTo(TransformError.UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE);
    }

    /**
     * The false positive the rule is shaped to avoid. A whole document that simply has no such
     * column is a legitimate row - an optional field in a document store is an ordinary thing - and
     * refusing it would stop the pipeline on the same source and the same column that a snapshot
     * read is required to accept and discard.
     */
    @Test
    @DisplayName("a whole earlier row that happens to have no list is accepted, not refused")
    void aCompleteEarlierRowWithoutTheListIsAccepted() {
        Map<String, Object> whole = new LinkedHashMap<>();
        whole.put("o_id", 7L);
        whole.put("region", "eu");

        assertThatCode(() -> bySku().transform(delete(whole))).doesNotThrowAnyException();
        assertThat(bySku().transform(delete(whole))).isEmpty();
    }

    @Test
    @DisplayName("an update is judged on its earlier row the same way a delete is")
    void anUpdateNeedsTheEarlierRowToo() {
        Map<String, Object> keyOnly = new LinkedHashMap<>();
        keyOnly.put("o_id", 7L);

        assertThat(refusedFor(update(keyOnly, row(List.of(element("a", 1))))).code())
                .isEqualTo(TransformError.UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE);
    }

    /**
     * An insert has no earlier row by definition, so the rule must not reach it. A check written
     * over every op would stop every pipeline on its first row.
     */
    @Test
    @DisplayName("an insert is not judged on an earlier row it never has")
    void anInsertIsNotJudgedOnAnEarlierRow() {
        assertThatCode(() -> bySku().transform(insert(row(List.of(element("a", 1))))))
                .doesNotThrowAnyException();
    }
}
