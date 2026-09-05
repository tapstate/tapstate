package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.spi.transform.TransformPort;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@code js} port: the full-power escape hatch. A script declares {@code process(record, ctx)}
 * (required) and optionally {@code filter(record)}; the record is the event as a mutable JS object
 * (op as its wire symbol, ts / src scalars, before / after / schema objects). Unlike filter / map,
 * js sees every event including ddl (the escape hatch is intentionally unfiltered). Output is what the script emits through
 * {@code ctx.emit(record)} followed by the return value if it is non-null; returning null drops.
 */
class JsTransformTest {

    private static TransformPort js(String script) {
        return StatelessTransforms.js(script);
    }

    private static Map<String, Object> after(Envelope out) {
        return out.after();
    }

    @Test
    @DisplayName("a value a connector converted reaches the script as the value, not as a host object")
    void aCarriedValueReachesTheScriptAsTheValue() {
        TransformPort js = js(
                "function process(r, ctx) { r.after.hit = (r.after._id === '64f0c0de'); return r; }");
        Envelope row = Envelope.insert(1L, "orders", new LinkedHashMap<>(
                Map.of("_id", new ConvertedValue("64f0c0de", "the-driver-object"))), null);

        // A guest cannot see into a host object it was not taught about, so the comparison is false for
        // every row - and a script that neither throws nor logs is indistinguishable from data that
        // genuinely did not match. Unlike the expression seam, a script rebuilds the whole record, so
        // this is also where every field stops carrying what a target would use to write it back.
        assertThat(after(js.transform(row).get(0)))
                .containsEntry("hit", true)
                .containsEntry("_id", "64f0c0de");
    }

    @Test
    @DisplayName("process mutates the record and returns it (the enrich case)")
    void processReturnsMutatedRecord() {
        TransformPort js = js(
                "function process(r, ctx) { r.after.full = r.after.first + ' ' + r.after.last; return r; }");
        Envelope row = Envelope.insert(1L, "people",
                new LinkedHashMap<>(Map.of("first", "ada", "last", "lovelace")), null);

        List<Envelope> out = js.transform(row);

        assertThat(out).hasSize(1);
        assertThat(after(out.get(0)))
                .containsEntry("full", "ada lovelace")
                .containsEntry("first", "ada")
                .containsEntry("last", "lovelace");
    }

    @Test
    @DisplayName("preserves op, ts, src, before and schema across a process that only touches after")
    void preservesEnvelopeSpine() {
        TransformPort js = js("function process(r, ctx) { r.after.stage = 'prod'; return r; }");
        Envelope update = Envelope.update(7L, "orders",
                new LinkedHashMap<>(Map.of("id", 1)),
                new LinkedHashMap<>(Map.of("id", 1)),
                Map.of("v", 2));

        Envelope out = js.transform(update).get(0);

        assertThat(out.op()).isEqualTo(Op.UPDATE);
        assertThat(out.ts()).isEqualTo(7L);
        assertThat(out.src()).isEqualTo("orders");
        assertThat(out.before()).containsEntry("id", 1);
        assertThat(out.schema()).containsEntry("v", 2);
        assertThat(out.after()).containsEntry("stage", "prod").containsEntry("id", 1);
    }

    @Test
    @DisplayName("an optional filter drops the event when it returns false, before process runs")
    void filterDropsBeforeProcess() {
        TransformPort js = js(
                "function process(r, ctx) { return r; } function filter(r) { return r.after.keep === true; }");
        Envelope kept = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("keep", true)), null);
        Envelope dropped = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("keep", false)), null);

        assertThat(js.transform(kept)).hasSize(1);
        assertThat(js.transform(dropped)).isEmpty();
    }

    @Test
    @DisplayName("with no filter declared every event reaches process")
    void noFilterProcessesEvery() {
        TransformPort js = js("function process(r, ctx) { return r; }");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("id", 1)), null);

        assertThat(js.transform(row)).hasSize(1);
    }

    @Test
    @DisplayName("returning null drops the event")
    void returningNullDrops() {
        TransformPort js = js("function process(r, ctx) { return null; }");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("id", 1)), null);

        assertThat(js.transform(row)).isEmpty();
    }

    @Test
    @DisplayName("ctx.emit fans one event out to several")
    void emitFansOut() {
        TransformPort js = js(
                "function process(r, ctx) {"
                        + "  ctx.emit(r);"
                        + "  ctx.emit({ op: r.op, ts: r.ts, src: r.src, before: null, after: { id: 2 }, schema: null });"
                        + "}");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("id", 1L)), null);

        List<Envelope> out = js.transform(row);

        assertThat(out).hasSize(2);
        assertThat(after(out.get(0))).containsEntry("id", 1L);
        assertThat(after(out.get(1))).containsEntry("id", 2L);
    }

    @Test
    @DisplayName("the return value is a trailing emit: emitted records come first, then the return")
    void returnIsTrailingEmit() {
        TransformPort js = js(
                "function process(r, ctx) {"
                        + "  ctx.emit({ op: 'i', ts: 1, src: 't', before: null, after: { n: 1 }, schema: null });"
                        + "  return { op: 'i', ts: 1, src: 't', before: null, after: { n: 2 }, schema: null };"
                        + "}");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("n", 0L)), null);

        List<Envelope> out = js.transform(row);

        assertThat(out).hasSize(2);
        assertThat(after(out.get(0))).containsEntry("n", 1L);
        assertThat(after(out.get(1))).containsEntry("n", 2L);
    }

    @Test
    @DisplayName("emitting the same record after each mutation captures each state, not just the last")
    void emitSnapshotsAtEmitTime() {
        TransformPort js = js(
                "function process(r, ctx) { for (var i = 0; i < 3; i++) { r.after.seq = i; ctx.emit(r); } }");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("id", 1L)), null);

        List<Envelope> out = js.transform(row);

        assertThat(out).hasSize(3);
        assertThat(after(out.get(0))).containsEntry("seq", 0L);
        assertThat(after(out.get(1))).containsEntry("seq", 1L);
        assertThat(after(out.get(2))).containsEntry("seq", 2L);
    }

    @Test
    @DisplayName("a script with no process function surfaces a coded transform.script-no-process")
    void codesScriptWithoutProcess() {
        assertThatThrownBy(() -> js("function filter(r) { return true; }"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("transform.script-no-process"));
    }

    @Test
    @DisplayName("a script that does not compile surfaces a coded transform.script-compile-failed")
    void codesUncompilableScript() {
        assertThatThrownBy(() -> js("function process(r, ctx) { return "))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("transform.script-compile-failed"));
    }

    @Test
    @DisplayName("a script that throws at runtime surfaces a coded transform.script-failed")
    void codesScriptRuntimeFailure() {
        TransformPort js = js("function process(r, ctx) { throw new Error('boom'); }");
        Envelope row = Envelope.insert(1L, "t", Map.of("id", 1), null);

        assertThatThrownBy(() -> js.transform(row))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException e = (TapstateException) thrown;
                    assertThat(e.code().code()).isEqualTo("transform.script-failed");
                    assertThat(String.valueOf(e.args().get("detail"))).contains("boom");
                });
    }

    @Test
    @DisplayName("a script that returns a non-record surfaces a coded transform.script-output-invalid")
    void codesInvalidScriptOutput() {
        TransformPort js = js("function process(r, ctx) { return 42; }");
        Envelope row = Envelope.insert(1L, "t", Map.of("id", 1), null);

        assertThatThrownBy(() -> js.transform(row))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("transform.script-output-invalid"));
    }

    @Test
    @DisplayName("a script that emits an unknown op symbol surfaces a coded transform.script-output-invalid")
    void codesUnknownOutputOp() {
        // 'insert' is not a wire op symbol (the symbols are i / u / d / r / ddl); an author-set op the
        // envelope cannot parse is invalid output, coded like any other bad output shape.
        TransformPort js = js("function process(r, ctx) { r.op = 'insert'; return r; }");
        Envelope row = Envelope.insert(1L, "t", Map.of("id", 1), null);

        assertThatThrownBy(() -> js.transform(row))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("transform.script-output-invalid"));
    }

    @Test
    @DisplayName("a script whose output before is not an object surfaces a coded transform.script-output-invalid")
    void codesNonObjectOutputBefore() {
        TransformPort js = js("function process(r, ctx) { r.before = 'oops'; return r; }");
        Envelope row = Envelope.update(1L, "t", Map.of("id", 1), Map.of("id", 2), null);

        assertThatThrownBy(() -> js.transform(row))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code().code())
                        .isEqualTo("transform.script-output-invalid"));
    }

    @Test
    @DisplayName("js sees a ddl event and can act on it (no non-row bypass, unlike filter / map)")
    void seesDdlEvent() {
        TransformPort js = js(
                "function process(r, ctx) { if (r.op === 'ddl') { r.schema.touched = true; } return r; }");
        Envelope ddl = Envelope.ddl(3L, "orders", new LinkedHashMap<>(Map.of("kind", "add-column")));

        Envelope out = js.transform(ddl).get(0);

        assertThat(out.op()).isEqualTo(Op.DDL);
        assertThat(out.schema()).containsEntry("kind", "add-column").containsEntry("touched", true);
    }

    @Test
    @DisplayName("op is bound as its wire symbol so a script can branch on change kind")
    void opBoundAsWireSymbol() {
        TransformPort dropDeletes = js(
                "function process(r, ctx) { return r; } function filter(r) { return r.op !== 'd'; }");
        Envelope delete = Envelope.delete(1L, "t", new LinkedHashMap<>(Map.of("id", 1)), null);
        Envelope insert = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("id", 1)), null);

        assertThat(dropDeletes.transform(delete)).isEmpty();
        assertThat(dropDeletes.transform(insert)).hasSize(1);
    }

    @Test
    @DisplayName("a delete (before only, no after) round-trips through process untouched")
    void deletePassesThrough() {
        TransformPort js = js("function process(r, ctx) { return r; }");
        Envelope delete = Envelope.delete(5L, "t", new LinkedHashMap<>(Map.of("id", 9)), null);

        Envelope out = js.transform(delete).get(0);

        assertThat(out.op()).isEqualTo(Op.DELETE);
        assertThat(out.before()).containsEntry("id", 9);
        assertThat(out.after()).isNull();
    }

    @Test
    @DisplayName("a computed integer round-trips as an integer, not a double")
    void computedIntegerRoundTrips() {
        TransformPort js = js("function process(r, ctx) { r.after.doubled = r.after.n * 2; return r; }");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("n", 21L)), null);

        assertThat(after(js.transform(row).get(0))).containsEntry("doubled", 42L);
    }

    @Test
    @DisplayName("a field the script never touches keeps its exact Java type (no Long/Double narrowing)")
    void passThroughPreservesJavaType() {
        // Only 'tag' is written; id (BIGINT -> Long), price (DOUBLE) and a nested Long must survive with
        // their exact Java types, not be narrowed to Integer by the round-trip through the JS engine.
        TransformPort js = js("function process(r, ctx) { r.after.tag = 'x'; return r; }");
        LinkedHashMap<String, Object> after = new LinkedHashMap<>();
        after.put("id", 100L);
        after.put("price", 19.0d);
        after.put("qty", 5);
        after.put("nested", new LinkedHashMap<>(Map.of("big", 9000000000L)));
        Envelope row = Envelope.insert(1L, "t", after, null);

        Map<String, Object> out = after(js.transform(row).get(0));

        assertThat(out.get("id")).isEqualTo(100L);
        assertThat(out.get("price")).isEqualTo(19.0d);
        assertThat(out.get("qty")).isEqualTo(5);
        assertThat(out.get("nested")).isEqualTo(Map.of("big", 9000000000L));
        assertThat(out).containsEntry("tag", "x");
    }

    @Test
    @DisplayName("a whole number the script computes leaves in the row's own integer width")
    void aComputedWholeNumberLeavesAsTheSixtyFourBitInteger() {
        // The escape hatch writes into the same rows every other step reads, so it owes them the same
        // currency: one integer width, the one the type namespace names. Handing back a narrower box
        // for the small values and a wider one for the large would make a column's type depend on the
        // magnitude that happened to flow through it.
        TransformPort js = js("function process(r, ctx) { r.after.small = 5; r.after.big = 9000000000; return r; }");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("id", 1L)), null);

        Map<String, Object> out = after(js.transform(row).get(0));

        assertThat(out.get("small")).isEqualTo(5L).isInstanceOf(Long.class);
        assertThat(out.get("big")).isEqualTo(9000000000L);
    }

    @Test
    @DisplayName("nested objects and arrays the script builds round-trip to Map and List")
    void nestedValuesRoundTrip() {
        TransformPort js = js(
                "function process(r, ctx) { r.after.tags = ['a', 'b']; r.after.meta = { k: 1 }; return r; }");
        Envelope row = Envelope.insert(1L, "t", new LinkedHashMap<>(Map.of("id", 1)), null);

        Map<String, Object> after = after(js.transform(row).get(0));

        assertThat(after.get("tags")).isEqualTo(List.of("a", "b"));
        assertThat(after.get("meta")).isEqualTo(Map.of("k", 1L));
    }
}
