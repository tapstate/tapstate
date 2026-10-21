package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.TapstateType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The typed half of the row-expression compilation surface: the same envelope, but with the row's
 * columns carrying the types a discovered source resolved them to, instead of every member access
 * being dyn.
 *
 * <p>Two entry points, in the order a caller uses them. {@link RowExpressions#rowColumns} answers
 * which columns an expression reads at all — a caller with no columns to type needs no source model.
 * {@link RowExpressions#typedValueError} / {@link RowExpressions#typedPredicateError} then judge the
 * expression against those columns' types, so an operation that cannot be evaluated without losing
 * what the column holds fails to compile rather than reaching the runtime.
 */
class RowExpressionTypedCheckTest {

    private static Map<String, TapstateType> columns() {
        Map<String, TapstateType> types = new LinkedHashMap<>();
        types.put("id", TapstateType.INT64);
        types.put("amount", TapstateType.DECIMAL);
        types.put("name", TapstateType.STRING);
        types.put("created", TapstateType.DATETIME);
        return types;
    }

    // ---- rowColumns: which columns does the expression read ----------------------------

    @Test
    @DisplayName("rowColumns is empty for an expression that reads no row field")
    void rowColumnsEmptyWithoutRowAccess() {
        assertThat(RowExpressions.rowColumns("op == 'i'")).isEmpty();
        assertThat(RowExpressions.rowColumns("ts > 0 && src == 'orders'")).isEmpty();
        assertThat(RowExpressions.rowColumns("now()")).isEmpty();
    }

    @Test
    @DisplayName("rowColumns does not count schema, which is envelope metadata rather than row data")
    void rowColumnsIgnoresSchema() {
        assertThat(RowExpressions.rowColumns("schema.table == 'orders'")).isEmpty();
    }

    @Test
    @DisplayName("rowColumns collects every column reached through after and before alike")
    void rowColumnsCollectsBothSides() {
        assertThat(RowExpressions.rowColumns("after.id * 2")).containsExactly("id");
        assertThat(RowExpressions.rowColumns("before.id == after.id")).containsExactly("id");
        assertThat(RowExpressions.rowColumns("after.id > 0 && after.name != ''"))
                .containsExactlyInAnyOrder("id", "name");
    }

    @Test
    @DisplayName("rowColumns counts a presence test, which reads the column as much as a value does")
    void rowColumnsCountsPresenceTest() {
        assertThat(RowExpressions.rowColumns("has(after.name)")).containsExactly("name");
    }

    // ---- typed check: an exactly representable column behaves as itself -----------------

    @Test
    @DisplayName("arithmetic on a 64-bit integer column type-checks")
    void int64ArithmeticIsAccepted() {
        assertThat(RowExpressions.typedValueError("after.id * 2", columns())).isNull();
    }

    @Test
    @DisplayName("a string column keeps string semantics")
    void stringColumnIsAccepted() {
        assertThat(RowExpressions.typedValueError("after.name + '!'", columns())).isNull();
        assertThat(RowExpressions.typedPredicateError("after.name == 'widget'", columns())).isNull();
    }

    @Test
    @DisplayName("a column typed as text refuses arithmetic, which the untyped envelope let through")
    void stringColumnRefusesArithmetic() {
        assertThat(RowExpressions.valueError("after.name * 2")).isNull();
        assertThat(RowExpressions.typedValueError("after.name * 2", columns())).isNotNull();
    }

    // ---- typed check: a column with no exact CEL counterpart ---------------------------

    @Test
    @DisplayName("arithmetic on an exact fixed-point column is refused, naming the type")
    void decimalArithmeticIsRefused() {
        String error = RowExpressions.typedValueError("after.amount * 2", columns());

        assertThat(error).isNotNull().contains("tapstate.decimal");
    }

    @Test
    @DisplayName("comparing an exact fixed-point column is refused too, not only arithmetic")
    void decimalComparisonIsRefused() {
        assertThat(RowExpressions.typedPredicateError("after.amount > 0", columns())).isNotNull();
    }

    @Test
    @DisplayName("carrying an exact fixed-point column through unchanged is not a loss, so it passes")
    void decimalCarriedUnchangedIsAccepted() {
        assertThat(RowExpressions.typedValueError("after.amount", columns())).isNull();
    }

    @Test
    void aComputedDecimalResultNeedsABareColumnRead() {
        Map<String, TapstateType> decimals = Map.of("a", TapstateType.DECIMAL, "b", TapstateType.DECIMAL);
        for (String expression : List.of(
                "has(after.a) ? after.a : after.b",
                "op == 'i' ? after.a : before.b",
                "has(after.a) ? after.a : (has(after.b) ? after.b : before.a)",
                "[after.a, after.b][0]")) {
            assertThat(RowExpressions.typedValueType(expression, decimals))
                    .as("the computed result of %s", expression).isEqualTo(TapstateType.DECIMAL);
            assertThat(RowExpressions.typedValueError(expression, decimals))
                    .as("a decimal without a source column's descriptor: %s", expression).isNotNull();
        }
        assertThat(RowExpressions.typedValueError("(after.a)", decimals)).isNull();
        assertThat(RowExpressions.typedValueError("before.b", decimals)).isNull();
        assertThat(RowExpressions.typedValueError("has(after.a)", decimals)).isNull();
        assertThat(RowExpressions.typedValueError("has(after.a) ? after.a : after.b",
                Map.of("a", TapstateType.INT64, "b", TapstateType.INT64))).isNull();
    }

    @Test
    @DisplayName("a date-and-time column has no exact counterpart either, so operating on it is refused")
    void datetimeOperationIsRefused() {
        assertThat(RowExpressions.typedPredicateError("after.created > 0", columns())).isNotNull();
        assertThat(RowExpressions.typedValueError("after.created", columns())).isNull();
    }

    // ---- a column the source model does not carry --------------------------------------

    @Test
    @DisplayName("a column absent from the model stays dyn and compiles, since a source may hold it anyway")
    void columnAbsentFromTheModelStaysDyn() {
        assertThat(RowExpressions.typedValueError("after.absent * 2", columns())).isNull();
        assertThat(RowExpressions.typedPredicateError("after.absent == 1", columns())).isNull();
        assertThat(RowExpressions.typedPredicateError("has(after.absent)", columns())).isNull();
    }

    @Test
    @DisplayName("an absent column alongside a refused one does not rescue the expression")
    void anAbsentColumnDoesNotRescueARefusedOne() {
        assertThat(RowExpressions.typedPredicateError(
                "after.absent == 1 && after.amount * 2 > 0", columns())).isNotNull();
    }

    // ---- the envelope itself is unchanged by typing the row -----------------------------

    @Test
    @DisplayName("typing the row leaves the envelope scalars and builtins alone")
    void envelopeIsUnchanged() {
        assertThat(RowExpressions.typedPredicateError("op == 'i'", columns())).isNull();
        assertThat(RowExpressions.typedPredicateError("ts > 0", columns())).isNull();
        assertThat(RowExpressions.typedValueError("now()", columns())).isNull();
        assertThat(RowExpressions.typedPredicateError("schema.table == 'orders'", columns())).isNull();
    }

    @Test
    @DisplayName("a predicate must still yield bool once the row is typed")
    void predicateMustStillYieldBool() {
        assertThat(RowExpressions.typedPredicateError("after.name", columns())).isNotNull();
    }

    @Test
    @DisplayName("with no columns to type, the row falls back to dyn and nothing is refused")
    void noColumnsMeansNothingIsTyped() {
        assertThat(RowExpressions.typedValueError("after.amount * 2", Map.of())).isNull();
    }
}
