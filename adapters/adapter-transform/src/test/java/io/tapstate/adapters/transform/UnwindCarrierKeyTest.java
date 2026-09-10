package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
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
 * Keys compared with the carrier taken off, which every other case here is blind to.
 *
 * <p>A value the source connector converted for travel arrives wrapped, carrying the name its own
 * schema gave the column so the write side can rebuild the driver's own type. A carrier never
 * equals the plain value inside it, so a comparison that skips the unwrap is not merely imprecise -
 * it is a comparison that can never hold. Both consequences look exactly like working software:
 * pairing sees every key as new and rewrites the whole list on every update, and the duplicate
 * check sees two identical keys as different and never says anything.
 *
 * <p>Nothing else in this suite reaches it, because every other case keys on a plain scalar. The
 * side that is wrapped and the side that is not is also the realistic shape: a change stream's
 * earlier row and its new row do not always come through the same conversion.
 */
class UnwindCarrierKeyTest {

    private static final List<String> PARENT_KEY = List.of("o_id");

    private final List<TapstateException> said = new ArrayList<>();

    private UnwindPort port() {
        return new UnwindPort(
                UnwindSpec.from(new TransformBody.Unwind("items", null, null, "sku", null), PARENT_KEY),
                said::add);
    }

    /** An element whose identity travelled as a converted value rather than as the value itself. */
    private static Map<String, Object> carried(String sku, long qty) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("sku", new ConvertedValue(sku, "objectId"));
        e.put("qty", qty);
        return e;
    }

    private static Map<String, Object> plain(String sku, long qty) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("sku", sku);
        e.put("qty", qty);
        return e;
    }

    private static Map<String, Object> row(List<Map<String, Object>> items) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("o_id", 7L);
        r.put("region", "eu");
        r.put("items", items);
        return r;
    }

    /**
     * The list is untouched and only the wrapping differs, so nothing should be removed. Left
     * wrapped, not one key matches: the earlier row's two keys are all "missing" from the new row
     * and the new row's two are all new, which is two deletes and two inserts for an update that
     * changed nothing. What the target ends up holding is still right, which is exactly why no
     * count and no error anywhere catches this.
     */
    @Test
    @DisplayName("a key wrapped on one side and bare on the other is the same key")
    void aCarriedKeyMatchesThePlainOne() {
        List<Envelope> out = port().transform(new Envelope(Op.UPDATE, 1L, "orders",
                row(List.of(plain("a", 1), plain("b", 2))),
                row(List.of(carried("a", 1), carried("b", 2))),
                null));

        assertThat(out).filteredOn(e -> e.op() == Op.DELETE).isEmpty();
        assertThat(out).filteredOn(e -> e.op() == Op.INSERT).isEmpty();
        assertThat(out).hasSize(2).allSatisfy(e -> assertThat(e.op()).isEqualTo(Op.UPDATE));
        // The rows still carry the wrapper onward: only the comparison unwraps, because the write
        // side is owed the name the source declared for the column.
        assertThat(out.get(0).after()).extracting("items")
                .isEqualTo(carried("a", 1));
    }

    /**
     * The duplicate check reads the same comparison, so it goes blind in the same place. Two
     * elements whose identities are equal but wrapped separately are two distinct carriers; left
     * wrapped, the row that gets overwritten at the target is overwritten in silence.
     */
    @Test
    @DisplayName("two elements carrying the same wrapped key are still the same key")
    void twoCarriedElementsSharingAKeyAreStillNoticed() {
        List<Envelope> out = port().transform(new Envelope(Op.INSERT, 1L, "orders", null,
                row(List.of(carried("a", 1), carried("a", 2))), null));

        assertThat(said).singleElement().satisfies(coded ->
                assertThat(coded.code()).isEqualTo(TransformError.UNWIND_ROWS_SHARE_A_KEY));
        assertThat(out).hasSize(2);
    }
}
