package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.spi.transform.TransformPort;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What a row expression may compute on, and what it must leave alone.
 *
 * <p>The rows here are the rows the chain actually carries: a driver's own boxing became the value
 * model at the boundary where the column's type was resolved, so an integral column reaches an
 * expression as the one integer width and nothing downstream re-decides that. Which boxes widen into
 * which is that boundary's contract, pinned where it happens, not here.
 *
 * <p>What is left for this seam is the pair of things the expression runtime itself owes: a value it
 * cannot represent must be refused by name rather than quietly turned into a different one, and a
 * value that merely travels through an expression must leave as the kind of value the row holds —
 * above all an exact fixed-point column, which has to arrive and leave bit for bit as the source
 * stated it.
 */
class RowExpressionValueBindingTest {

    @Test
    @DisplayName("a value a connector converted is compared as the value, not as what carries it")
    void aCarriedValueIsBoundAsTheValueInside() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("_id", new ConvertedValue("64f0c0de", "the-driver-object"));
        row.put("tags", List.of(new ConvertedValue("eu", "the-driver-object")));

        // The failure this pins is the quiet one: an expression over a carrier neither fails nor warns,
        // it is simply false for every row - so a filter drops everything and a computed flag is never
        // set, both looking exactly like data that did not match.
        assertThat(compute("hit", "after._id == \'64f0c0de\'", row).get("hit")).isEqualTo(true);
        assertThat(compute("first", "after.tags[0]", row).get("first")).isEqualTo("eu");
    }

    private static TransformPort map(String field, FieldRule rule) {
        LinkedHashMap<String, FieldRule> fields = new LinkedHashMap<>();
        fields.put(field, rule);
        return StatelessTransforms.map(MapSpec.from(new TransformBody.MapProjection(fields)));
    }

    private static Map<String, Object> compute(String field, String expr, Map<String, Object> row) {
        Envelope event = Envelope.insert(1L, "t", new LinkedHashMap<>(row), null);
        return map(field, FieldRule.computed(expr)).transform(event).get(0).after();
    }

    @Test
    @DisplayName("computes on an integral column, which the row holds as the one integer width")
    void computesOnIntegralColumn() {
        Map<String, Object> after = compute("doubled", "after.qty * 2", Map.of("qty", 7L));

        assertThat(after).containsEntry("doubled", 14L);
    }

    @Test
    @DisplayName("carries an exact fixed-point column through an expression bit for bit, scale included")
    void carriesDecimalThroughUnchanged() {
        // An expression that only moves a decimal is allowed to run: moving one loses nothing, and the
        // apply-time gate refuses only computing on one. So this is the path where an adaptation that
        // normalised every number into one representation would lose digits with every gate still
        // green. equals - not compareTo - because only equals sees a scale that was rounded away.
        //
        // The value discriminates two ways, because there are two ways to lose it. It carries more
        // significant digits than a binary float holds, so a value that went through one comes back
        // with different digits even if something wraps it back up afterwards; and it ends in a zero
        // that only the stated scale keeps, which a round trip drops while the number stays equal in
        // magnitude.
        BigDecimal amount = new BigDecimal("123456789012345678.90");

        Map<String, Object> after = compute("moved", "after.amount", Map.of("amount", amount));

        assertThat(after.get("moved")).isEqualTo(amount);
    }

    @Test
    @DisplayName("refuses an integral value too large for an int64 rather than wrapping it")
    void refusesIntegralValueBeyondInt64() {
        // A value this wide is one the value model names no lossless target for, so it reaches the
        // expression exactly as the source holds it. The gate types the column, not the value, so a
        // column whose type is computable can still hold a value the expression language cannot
        // represent. Refusing names it; the obvious adaptation - asking any Number for its long -
        // would silently hand the expression a different number and let a corrupted value through as
        // a success.
        BigInteger beyond = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);

        assertThatThrownBy(() -> compute("next", "after.id + 1", Map.of("id", beyond)))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("transform.expression-failed");
    }

    @Test
    @DisplayName("computes on an approximate numeric column, which the row holds as a double")
    void computesOnApproximateNumericColumn() {
        Map<String, Object> after = compute("scaled", "after.ratio * 2.0", Map.of("ratio", 1.5d));

        assertThat(after).containsEntry("scaled", 3.0d);
    }

    @Test
    @DisplayName("computes on a binary column")
    void computesOnBinaryColumn() {
        Map<String, Object> after = compute("width", "size(after.blob)", Map.of("blob", new byte[] {1, 2, 3}));

        assertThat(after).containsEntry("width", 3L);
    }

    @Test
    @DisplayName("carries a binary column through an expression as the bytes it arrived as")
    void carriesBinaryThroughAsBytes() {
        // Making bytes computable means handing the expression the byte string its language uses. That
        // representation is this seam's own and must not ride back out on the result: a sink is owed
        // the row's own bytes, not the expression runtime's wrapper for them. It is also why bytes are
        // wrapped here rather than upstream - the wrapper is a language detail, not part of the row.
        byte[] blob = {1, 2, 3};

        Map<String, Object> after = compute("moved", "after.blob", Map.of("blob", blob));

        assertThat(after.get("moved")).isInstanceOf(byte[].class).isEqualTo(blob);
    }

    @Test
    @DisplayName("computes on an integral value nested inside a document")
    void computesOnIntegerInsideNestedDocument() {
        Map<String, Object> after =
                compute("next", "after.doc.qty + 1", Map.of("doc", Map.of("qty", 7L)));

        assertThat(after).containsEntry("next", 8L);
    }

    @Test
    @DisplayName("computes on an integral element inside an array")
    void computesOnIntegerInsideArray() {
        Map<String, Object> after =
                compute("next", "after.tags[0] + 1", Map.of("tags", List.of(7L)));

        assertThat(after).containsEntry("next", 8L);
    }

    @Test
    @DisplayName("reports an int64 arithmetic overflow as a coded diagnostic, not a bare crash")
    void reportsInt64OverflowAsCodedDiagnostic() {
        // Integer arithmetic is the path the apply-time gate deliberately lets through, so the one way
        // it can still fail once running has to arrive as a diagnostic naming the expression. Letting
        // an author past the gate and then failing the pipeline with a raw stack is the reason the
        // gate lets unresolved columns through at all.
        Map<String, Object> row = Map.of("counter", Long.MAX_VALUE);

        assertThatThrownBy(() -> compute("next", "after.counter + 1", row))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("transform.expression-failed");
    }
}
