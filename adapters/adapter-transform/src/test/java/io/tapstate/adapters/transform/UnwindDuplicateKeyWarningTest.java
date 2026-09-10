package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.Severity;
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
 * Two elements of one row landing on the same key: said out loud, and both rows sent anyway.
 *
 * <p>The offline check refuses a declaration that names nothing varying per element. It cannot
 * refuse this one: whether two elements of a particular row happen to carry the same identity is a
 * property of the data, and the declaration that produced them is correct for every other row. So
 * the run carries on and says so, which is the only reason anyone ever finds out - at the target
 * the two rows are one row, and no count, no error and no lag anywhere records that a row was
 * overwritten.
 *
 * <p><b>Three things are asserted together, because each has its own wrong implementation.</b> One
 * that warns and drops the duplicate row is this operator silently deciding which of an author's
 * elements counts; one that sends both and says nothing is today's behaviour with no benefit; one
 * that reports it at error severity stops a pipeline over data that is merely ambiguous.
 */
class UnwindDuplicateKeyWarningTest {

    private final List<TapstateException> said = new ArrayList<>();

    private UnwindPort port(String elementKey, String includeArrayIndex) {
        return new UnwindPort(
                UnwindSpec.from(
                        new TransformBody.Unwind("items", includeArrayIndex, null, elementKey, null),
                        List.of("o_id")),
                said::add);
    }

    private static Map<String, Object> element(String sku, long qty) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("sku", sku);
        e.put("qty", qty);
        return e;
    }

    private static Envelope insert(List<Map<String, Object>> items) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("o_id", 7L);
        row.put("items", items);
        row.put("region", "eu");
        return new Envelope(Op.INSERT, 1L, "orders", null, row, null);
    }

    @Test
    @DisplayName("the clash is reported at warning severity and both rows are still sent")
    void bothRowsGoOutAndTheClashIsSaid() {
        List<Envelope> out = port("sku", null)
                .transform(insert(List.of(element("a", 1), element("a", 2))));

        assertThat(out).hasSize(2);
        assertThat(said).singleElement().satisfies(coded -> {
            assertThat(coded.code()).isEqualTo(TransformError.UNWIND_ROWS_SHARE_A_KEY);
            assertThat(coded.code().severity()).isEqualTo(Severity.WARNING);
            assertThat(coded.args()).containsEntry("path", "items");
        });
    }

    @Test
    @DisplayName("elements that differ are not reported")
    void distinctElementsSayNothing() {
        List<Envelope> out = port("sku", null)
                .transform(insert(List.of(element("a", 1), element("b", 2))));

        assertThat(out).hasSize(2);
        assertThat(said).isEmpty();
    }

    /**
     * Numbering the elements gives every one of them a different key by construction, so the same
     * two elements stop clashing. Held here because it is the fix the message recommends, and a
     * recommendation nothing checks is a recommendation that can quietly stop working.
     */
    @Test
    @DisplayName("keying on the position instead makes identical elements distinct")
    void theOrdinalTellsIdenticalElementsApart() {
        List<Envelope> out = port(null, "item_no")
                .transform(insert(List.of(element("a", 1), element("a", 2))));

        assertThat(out).hasSize(2);
        assertThat(said).isEmpty();
    }
}
