package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * An element's own identifying field is written beside the element, as a column of the output row.
 *
 * <p>The expansion puts one element where the list was, so a field of that element is addressable
 * only by reaching inside the value in that column. That is enough to tell one output row from
 * another here, in memory, and not enough anywhere else: a key is matched at the target by column,
 * a store builds no column for a name reaching into a value, and the model published for the table
 * passes over a key column it cannot find rather than refusing. The whole of that failure is
 * silent - rows land, the table fills, and every expanded row of one parent overwrites the last.
 *
 * <p><b>So the field is lifted, and it is lifted here rather than left to a projection the author
 * writes after.</b> A projection can move it, but nothing makes the author write one, and a
 * pipeline that omits it is the collapse above with no signal anywhere. The ordinal column is
 * already written exactly this way; this is the same act for the other of the two locators.
 *
 * <p>Both sides get it, not just the new row: a delete carries the row that is going, and a target
 * matches that row by key like any other, so a delete without the key column is a row the target
 * cannot find.
 */
class UnwindElementKeyColumnTest {

    private static TransformPort port(String includeArrayIndex, String elementKey) {
        return StatelessTransforms.unwind(UnwindSpec.from(
                new TransformBody.Unwind("items", includeArrayIndex, true, elementKey, null),
                List.of("o_id")));
    }

    private static Map<String, Object> element(String sku) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("sku", sku);
        e.put("qty", 2);
        return e;
    }

    private static Map<String, Object> row(Object items) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("o_id", 7L);
        r.put("items", items);
        return r;
    }

    @Test
    @DisplayName("each expanded row carries the element's key field as a column of its own")
    void theElementsKeyFieldBecomesAColumn() {
        List<Envelope> out = port(null, "sku").transform(
                new Envelope(Op.INSERT, 1L, "orders", null, row(List.of(element("a"), element("b"))),
                        null));

        assertThat(out).hasSize(2);
        assertThat(out.get(0).after()).containsEntry("sku", "a");
        assertThat(out.get(1).after()).containsEntry("sku", "b");
        // The element itself still sits where the list was. Lifting the key is not flattening the
        // element, and an author who wants the rest of its fields at the top level still says so.
        assertThat(out.get(0).after()).containsEntry("items", element("a"));
    }

    @Test
    @DisplayName("the rows a delete produces carry it too, so the target can find them")
    void aDeletesRowsCarryTheKeyColumn() {
        List<Envelope> out = port(null, "sku").transform(
                new Envelope(Op.DELETE, 1L, "orders", row(List.of(element("a"), element("b"))), null,
                        null));

        assertThat(out).hasSize(2);
        assertThat(out).allSatisfy(e -> assertThat(e.op()).isEqualTo(Op.DELETE));
        assertThat(out).extracting(e -> e.before().get("sku")).containsExactly("a", "b");
    }

    /**
     * A list of scalars has no field to lift, and a row kept only because empties are preserved has
     * no element at all. Both produce the column holding nothing rather than no column: a target
     * builds its table from the model, and a row missing a column the model declares is a write
     * that fails on some stores and silently writes a default on others.
     */
    @Test
    @DisplayName("an element with no such field still produces the column, empty")
    void anElementWithoutTheFieldStillProducesTheColumn() {
        List<Envelope> fromScalars = port(null, "sku").transform(
                new Envelope(Op.INSERT, 1L, "orders", null, row(List.of("a", "b")), null));

        assertThat(fromScalars).hasSize(2);
        assertThat(fromScalars.get(0).after()).containsEntry("sku", null);

        List<Envelope> fromEmpty = port(null, "sku").transform(
                new Envelope(Op.INSERT, 1L, "orders", null, row(List.of()), null));

        assertThat(fromEmpty).hasSize(1);
        assertThat(fromEmpty.get(0).after()).containsEntry("sku", null);
    }

    /**
     * The reverse guard. An expansion told apart by its ordinal alone adds that column and no
     * other - a lift that fired whenever a field name was to hand would put a column on rows of
     * every pipeline that never asked for one.
     */
    @Test
    @DisplayName("an expansion keyed on its ordinal alone adds no other column")
    void anOrdinalOnlyExpansionAddsNoOtherColumn() {
        List<Envelope> out = port("item_no", null).transform(
                new Envelope(Op.INSERT, 1L, "orders", null, row(List.of(element("a"))), null));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).after()).containsOnlyKeys("o_id", "items", "item_no");
    }

    /**
     * Declaring both writes both columns, and the pairing still turns on the element's own field.
     * Removing the first of two elements moves the second one's ordinal; if the ordinal were what
     * rows were told apart by, that would read as one row deleted and one changed rather than as
     * the one deletion it is.
     */
    @Test
    @DisplayName("with both declared, the ordinal moves and the element's key still pairs the rows")
    void bothColumnsAreWrittenAndTheElementsFieldStillPairs() {
        List<Envelope> out = port("item_no", "sku").transform(
                new Envelope(Op.UPDATE, 1L, "orders", row(List.of(element("a"), element("b"))),
                        row(List.of(element("b"))), null));

        assertThat(out).hasSize(2);
        assertThat(out).filteredOn(e -> e.op() == Op.DELETE).singleElement()
                .satisfies(e -> assertThat(e.before()).containsEntry("sku", "a"));
        assertThat(out).filteredOn(e -> e.op() == Op.UPDATE).singleElement()
                .satisfies(e -> assertThat(e.after())
                        .containsEntry("sku", "b").containsEntry("item_no", 0L));
    }
}
