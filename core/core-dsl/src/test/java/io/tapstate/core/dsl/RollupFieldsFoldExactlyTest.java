package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.dsl.aggregate.RollupElement;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.RollupScope;
import io.tapstate.core.dsl.aggregate.RollupSpec;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.model.RollupField;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.RollupPick;
import io.tapstate.core.model.RollupResultType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The function library as a rollup applies it: SQL null semantics, a deterministic first and last, and
 * numbers that are exact or refused. Checked against a reference fold written independently of the
 * library, and against every order the same elements could arrive in.
 */
class RollupFieldsFoldExactlyTest {

    private static final RollupScope ITEMS = new RollupScope(Map.of(
            "id", ValueType.INT64,
            "qty", ValueType.INT64,
            "price", ValueType.decimal(2),
            "weight", ValueType.DOUBLE,
            "sku", ValueType.of(TapstateType.STRING),
            "day", ValueType.of(TapstateType.DATE),
            "kind", ValueType.of(TapstateType.STRING)), true, Map.of(), Map.of(), Map.of());

    // ---- empty sets and nulls --------------------------------------------------------------------

    @Test
    void anEmptyLevelCountsZeroAndLeavesEveryOtherFunctionNull() {
        Map<String, Object> out = fold(Map.of(
                "n", field(RollupFunction.COUNT, true),
                "d", field(RollupFunction.COUNT_DISTINCT, "$sku"),
                "s", field(RollupFunction.SUM, "$qty"),
                "a", field(RollupFunction.AVG, "$qty"),
                "lo", field(RollupFunction.MIN, "$sku"),
                "hi", field(RollupFunction.MAX, "$day"),
                "f", field(RollupFunction.FIRST, new RollupPick("$sku", List.of("$day")))), List.of());
        assertThat(out).containsEntry("n", 0L).containsEntry("d", 0L)
                .containsEntry("s", null).containsEntry("a", null).containsEntry("lo", null)
                .containsEntry("hi", null).containsEntry("f", null);
    }

    @Test
    void nullArgumentsAreSkippedAndAllNullIsTheEmptySet() {
        List<RollupElement> elements = List.of(element(1, row("qty", null)), element(2, row("qty", null)));
        Map<String, Object> out = fold(Map.of(
                "rows", field(RollupFunction.COUNT, true),
                "values", field(RollupFunction.COUNT, "$qty"),
                "total", field(RollupFunction.SUM, "$qty"),
                "withDefault", RollupField.of(RollupFunction.SUM, "$qty", null, null, 0)), elements);
        assertThat(out).containsEntry("rows", 2L).containsEntry("values", 0L)
                .containsEntry("total", null).containsEntry("withDefault", 0L);
    }

    @Test
    void aDefaultIsTypedAgainstTheResultAndAnIntegerWidensToTheDecimalScale() {
        Map<String, Object> out = fold(Map.of(
                "total", RollupField.of(RollupFunction.SUM, "$price", null, null, 0)), List.of());
        assertThat(out.get("total")).isEqualTo(new BigDecimal("0.00"));
        assertThatThrownBy(() -> compile("x", RollupField.of(RollupFunction.SUM, "$weight", null, null, 0)))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(RollupError.EXPRESSION_INVALID))
                .satisfies(e -> assertThat(((TapstateException) e).args()).containsEntry("part", "default"));
        assertThatThrownBy(() -> compile("x", RollupField.of(RollupFunction.COUNT, true, null, null, "none")))
                .satisfies(e -> assertThat(((TapstateException) e).args()).containsEntry("part", "default"));
    }

    @Test
    void aFilterDecidesWhichElementsContributeToItsFieldOnly() {
        List<RollupElement> elements = List.of(
                element(1, row("qty", 2L, "kind", "reward")),
                element(2, row("qty", 5L, "kind", "cash")),
                element(3, row("qty", 7L, "kind", "reward")));
        Map<String, Object> out = fold(Map.of(
                "all", field(RollupFunction.SUM, "$qty"),
                "rewards", RollupField.of(RollupFunction.SUM, "$qty", "=after.kind == 'reward'", null, null),
                "none", RollupField.of(RollupFunction.SUM, "$qty", "=after.kind == 'other'", null, null)), elements);
        assertThat(out).containsEntry("all", 14L).containsEntry("rewards", 9L).containsEntry("none", null);
    }

    // ---- first and last ---------------------------------------------------------------------------

    @Test
    void lastBreaksATieOnItsFirstOrderingWithTheNextAndFallsBackWhenTheWinnerGoes() {
        RollupField latest = field(RollupFunction.LAST,
                new RollupPick("$qty", List.of("$day", "$kind")));
        List<RollupElement> three = List.of(
                element(1, row("day", LocalDate.parse("2026-03-01"), "kind", "A", "qty", 700L)),
                element(2, row("day", LocalDate.parse("2026-03-05"), "kind", "A", "qty", 650L)),
                element(3, row("day", LocalDate.parse("2026-03-05"), "kind", "B", "qty", 690L)));
        for (List<RollupElement> order : permutations(three)) {
            assertThat(fold(Map.of("latest", latest), order)).containsEntry("latest", 690L);
        }
        assertThat(fold(Map.of("latest", latest), three.subList(0, 2))).containsEntry("latest", 650L);
    }

    @Test
    void anElementWithANullOrderingIsNoCandidateButANullWinningValueIsTheResult() {
        RollupField latest = field(RollupFunction.LAST, new RollupPick("$qty", List.of("$day")));
        List<RollupElement> elements = List.of(
                element(1, row("day", LocalDate.parse("2026-01-01"), "qty", 1L)),
                element(2, row("day", null, "qty", 99L)),
                element(3, row("day", LocalDate.parse("2026-02-01"), "qty", null)));
        assertThat(fold(Map.of("latest", latest), elements)).containsEntry("latest", null);
        assertThat(fold(Map.of("latest", RollupField.of(RollupFunction.LAST,
                new RollupPick("$qty", List.of("$day")), "=has(after.qty)", null, null)), elements))
                .containsEntry("latest", 1L);
    }

    @Test
    void aTieThatSurvivesEveryOrderingGoesToTheSmallestKeyForFirstAndTheGreatestForLast() {
        List<RollupElement> tied = List.of(
                element(3, row("day", LocalDate.parse("2026-01-01"), "qty", 30L)),
                element(1, row("day", LocalDate.parse("2026-01-01"), "qty", 10L)),
                element(2, row("day", LocalDate.parse("2026-01-01"), "qty", 20L)));
        Map<String, Object> out = fold(Map.of(
                "first", field(RollupFunction.FIRST, new RollupPick("$qty", List.of("$day"))),
                "last", field(RollupFunction.LAST, new RollupPick("$qty", List.of("$day")))), tied);
        assertThat(out).containsEntry("first", 10L).containsEntry("last", 30L);
    }

    @Test
    void numbersCompareByValueAndStringsByCodePoint() {
        List<RollupElement> elements = List.of(
                element(1, row("sku", "😀")), element(2, row("sku", "￿")), element(3, row("sku", "Z")));
        assertThat(fold(Map.of("hi", field(RollupFunction.MAX, "$sku")), elements))
                .containsEntry("hi", "😀");
        List<RollupElement> prices = List.of(element(1, row("price", new BigDecimal("2.50"))),
                element(2, row("price", new BigDecimal("2.5"))), element(3, row("price", new BigDecimal("1.25"))));
        assertThat(fold(Map.of("distinct", field(RollupFunction.COUNT_DISTINCT, "$price")), prices))
                .containsEntry("distinct", 2L);
    }

    // ---- the numeric contract ---------------------------------------------------------------------

    @Test
    void aDecimalSumKeepsItsScaleAndEveryDigitADoubleWouldRound() {
        List<RollupElement> elements = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            elements.add(element(i, row("price", new BigDecimal("0.10"))));
        }
        elements.add(element(10, row("price", new BigDecimal("9999999999999999.99"))));
        Object total = fold(Map.of("total", field(RollupFunction.SUM, "$price")), elements).get("total");
        assertThat(total).isEqualTo(new BigDecimal("10000000000000000.99"));
        assertThat(((BigDecimal) total).scale()).isEqualTo(2);
        assertThat(BigDecimal.valueOf(9999999999999999.99 + 1.0)).isNotEqualTo(total);
    }

    @Test
    void aDecimalSumPastThirtyEightDigitsAndAnIntegerSumPastSixtyFourBitsStopWithACode() {
        BigDecimal huge = new BigDecimal("999999999999999999999999999999999999.99");
        assertOverflow(field(RollupFunction.SUM, "$price"),
                List.of(element(1, row("price", huge)), element(2, row("price", huge))), "decimal(38,2)");
        assertOverflow(field(RollupFunction.SUM, "$qty"),
                List.of(element(1, row("qty", Long.MAX_VALUE)), element(2, row("qty", 1L))), "int64");
    }

    @Test
    void aDecimalAverageRoundsHalfEvenAtTheLargerOfItsScaleAndSix() {
        List<RollupElement> elements = List.of(element(1, row("price", new BigDecimal("0.00"))),
                element(2, row("price", new BigDecimal("0.00"))), element(3, row("price", new BigDecimal("0.01"))));
        Object avg = fold(Map.of("avg", field(RollupFunction.AVG, "$price")), elements).get("avg");
        assertThat(avg).isEqualTo(new BigDecimal("0.01").divide(new BigDecimal(3), 6, RoundingMode.HALF_EVEN));
        assertThat(avg).isEqualTo(new BigDecimal("0.003333"));
        List<RollupElement> ints = List.of(element(1, row("qty", 1L)), element(2, row("qty", 2L)));
        assertThat(fold(Map.of("avg", field(RollupFunction.AVG, "$qty")), ints).get("avg"))
                .isEqualTo(new BigDecimal("1.500000"));
    }

    @Test
    void aTypeConversionHappensOnlyWhenItIsExact() {
        RollupField asInt = RollupField.of(RollupFunction.SUM, "$price", null, RollupResultType.INT64, null);
        assertThat(fold(Map.of("n", asInt), List.of(element(1, row("price", new BigDecimal("2.00"))),
                element(2, row("price", new BigDecimal("1.00")))))).containsEntry("n", 3L);
        assertThatThrownBy(() -> fold(Map.of("n", asInt), List.of(element(1, row("price", new BigDecimal("2.50"))),
                element(2, row("price", new BigDecimal("1.25"))))))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(RollupError.RESULT_NOT_CONVERTIBLE))
                .satisfies(e -> assertThat(((TapstateException) e).args())
                        .containsEntry("field", "n").containsEntry("root", "doc-1").containsEntry("to", "int64"));
        assertThatThrownBy(() -> compile("s", RollupField.of(RollupFunction.MAX, "$sku", null, RollupResultType.INT64, null)))
                .satisfies(e -> assertThat(((TapstateException) e).args()).containsEntry("part", "type"));
    }

    @Test
    void aValueOfAnotherTypeThanItsColumnIsRefusedRatherThanCoerced() {
        assertThatThrownBy(() -> fold(Map.of("total", field(RollupFunction.SUM, "$price")),
                List.of(element(1, row("price", 2.5d)))))
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(RollupError.VALUE_TYPE_MISMATCH))
                .satisfies(e -> assertThat(((TapstateException) e).args())
                        .containsEntry("declared", "decimal(38,2)").containsEntry("actual", "double"));
    }

    @Test
    void aSumOverAColumnNothingTypedIsRefusedWhenThePipelineIsApplied() {
        assertThatThrownBy(() -> compile("t", field(RollupFunction.SUM, "$undiscovered")))
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(RollupError.TYPE_UNKNOWN))
                .satisfies(e -> assertThat(((TapstateException) e).args()).containsEntry("expression", "$undiscovered"));
        assertThatThrownBy(() -> compile("t", field(RollupFunction.SUM, "=after.price * 2")))
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(RollupError.EXPRESSION_INVALID));
    }

    @Test
    void aDoubleSumIsFoldedInKeyOrderSoEveryArrivalOrderGivesTheSameBits() {
        List<RollupElement> elements = new ArrayList<>();
        Random random = new Random(7);
        for (int i = 0; i < 40; i++) {
            elements.add(element(i, row("weight", random.nextDouble() * Math.pow(10, random.nextInt(12)))));
        }
        Object expected = fold(Map.of("w", field(RollupFunction.SUM, "$weight")), elements).get("w");
        for (int round = 0; round < 20; round++) {
            Collections.shuffle(elements, random);
            assertThat(fold(Map.of("w", field(RollupFunction.SUM, "$weight")), elements).get("w")).isEqualTo(expected);
        }
    }

    // ---- against a reference fold -----------------------------------------------------------------

    @Test
    void everyFunctionAgreesWithAReferenceFoldWhateverTheArrivalOrder() {
        Random random = new Random(11);
        for (int round = 0; round < 200; round++) {
            List<RollupElement> elements = new ArrayList<>();
            int size = random.nextInt(8);
            for (int i = 0; i < size; i++) {
                Long qty = random.nextInt(4) == 0 ? null : (long) random.nextInt(21) - 10;
                BigDecimal price = random.nextInt(4) == 0 ? null
                        : BigDecimal.valueOf(random.nextInt(2001) - 1000, 2);
                elements.add(element(i, row("qty", qty, "price", price, "kind", random.nextBoolean() ? "a" : "b")));
            }
            Map<String, RollupField> fields = new LinkedHashMap<>();
            fields.put("n", field(RollupFunction.COUNT, "$qty"));
            fields.put("d", field(RollupFunction.COUNT_DISTINCT, "$qty"));
            fields.put("s", field(RollupFunction.SUM, "$qty"));
            fields.put("p", field(RollupFunction.SUM, "$price"));
            fields.put("a", field(RollupFunction.AVG, "$price"));
            fields.put("lo", field(RollupFunction.MIN, "$price"));
            fields.put("hi", field(RollupFunction.MAX, "$qty"));
            fields.put("fa", RollupField.of(RollupFunction.SUM, "$qty", "=after.kind == 'a'", null, null));
            Map<String, Object> expected = reference(elements);
            List<RollupElement> shuffled = new ArrayList<>(elements);
            Collections.shuffle(shuffled, random);
            assertThat(fold(fields, elements)).isEqualTo(expected);
            assertThat(fold(fields, shuffled)).isEqualTo(expected);
        }
    }

    /** The SQL meaning of each field, computed without the library. */
    private static Map<String, Object> reference(List<RollupElement> elements) {
        List<Long> qty = new ArrayList<>();
        List<BigDecimal> price = new ArrayList<>();
        List<Long> qtyA = new ArrayList<>();
        for (RollupElement e : elements) {
            Object q = e.rendered().get("qty");
            if (q != null) {
                qty.add((Long) q);
                if ("a".equals(e.rendered().get("kind"))) {
                    qtyA.add((Long) q);
                }
            }
            Object p = e.rendered().get("price");
            if (p != null) {
                price.add((BigDecimal) p);
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("n", (long) qty.size());
        out.put("d", qty.stream().distinct().count());
        out.put("s", qty.isEmpty() ? null : qty.stream().mapToLong(Long::longValue).sum());
        out.put("p", price.isEmpty() ? null : price.stream().reduce(BigDecimal.ZERO.setScale(2), BigDecimal::add));
        out.put("a", price.isEmpty() ? null : price.stream().reduce(BigDecimal.ZERO, BigDecimal::add)
                .divide(BigDecimal.valueOf(price.size()), 6, RoundingMode.HALF_EVEN));
        out.put("lo", price.stream().min(BigDecimal::compareTo).orElse(null));
        out.put("hi", qty.stream().max(Long::compareTo).orElse(null));
        out.put("fa", qtyA.isEmpty() ? null : qtyA.stream().mapToLong(Long::longValue).sum());
        return out;
    }

    // ---- references ---------------------------------------------------------------------------------

    @Test
    void aFieldReadingAReferenceAnElementNamesButCouldNotResolveIsNullNotPartialNorItsDefault() {
        RollupScope scope = new RollupScope(Map.of("amount", ValueType.INT64), true,
                Map.of("prize", RollupScope.ofColumns(Map.of("value", ValueType.INT64), true)), Map.of(), Map.of());
        RollupField priced = RollupField.of(RollupFunction.SUM, "=after.amount * after.prize.value", null, null, 0);
        RollupField counted = RollupField.of(RollupFunction.SUM, "$amount", null, null, 0);
        List<RollupSpec> specs = List.of(RollupCompiler.compile("amount_total", priced, scope),
                RollupCompiler.compile("qty", counted, scope));
        assertThat(specs.getFirst().references()).containsExactly("prize");
        List<RollupElement> complete = List.of(
                new RollupElement(List.of(1), row("amount", 2L, "prize", Map.of("value", 5L)), Set.of()),
                new RollupElement(List.of(2), row("amount", 1L, "prize", Map.of("value", 3L)), Set.of()));
        assertThat(RollupFold.apply(specs, complete, "t1")).containsEntry("amount_total", 13L).containsEntry("qty", 3L);
        List<RollupElement> missing = List.of(complete.getFirst(),
                new RollupElement(List.of(2), row("amount", 1L), Set.of("prize")));
        assertThat(RollupFold.apply(specs, missing, "t1")).containsEntry("amount_total", null).containsEntry("qty", 3L);
    }

    @Test
    void aMisspeltReferenceColumnIsRefusedWhenThePipelineIsApplied() {
        RollupScope scope = new RollupScope(Map.of("amount", ValueType.INT64), true,
                Map.of("prize", RollupScope.ofColumns(Map.of("value", ValueType.INT64), true)), Map.of(), Map.of());
        assertThatThrownBy(() -> RollupCompiler.compile("x",
                RollupField.of(RollupFunction.SUM, "=after.amount * after.prize.valeu", null, null, null), scope))
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(RollupError.EXPRESSION_INVALID))
                .satisfies(e -> assertThat(((TapstateException) e).args()).containsEntry("part", "argument"));
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private static void assertOverflow(RollupField field, List<RollupElement> elements, String type) {
        assertThatThrownBy(() -> fold(Map.of("total", field), elements))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(RollupError.OVERFLOW))
                .satisfies(e -> assertThat(((TapstateException) e).args())
                        .containsEntry("type", type).containsEntry("field", "total").containsEntry("root", "doc-1"));
    }

    private static RollupField field(RollupFunction function, Object argument) {
        return RollupField.of(function, argument, null, null, null);
    }

    private static RollupSpec compile(String name, RollupField field) {
        return RollupCompiler.compile(name, field, ITEMS);
    }

    private static Map<String, Object> fold(Map<String, RollupField> fields, List<RollupElement> elements) {
        List<RollupSpec> specs = new ArrayList<>();
        new LinkedHashMap<>(fields).forEach((name, field) -> specs.add(compile(name, field)));
        return RollupFold.apply(specs, elements, "doc-1");
    }

    private static RollupElement element(int key, Map<String, Object> row) {
        return new RollupElement(List.of(key), row, Set.of());
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            row.put((String) kv[i], kv[i + 1]);
        }
        return row;
    }

    private static <T> List<List<T>> permutations(List<T> items) {
        if (items.size() <= 1) {
            return List.of(items);
        }
        List<List<T>> out = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            List<T> rest = new ArrayList<>(items);
            T head = rest.remove(i);
            for (List<T> tail : permutations(rest)) {
                List<T> p = new ArrayList<>();
                p.add(head);
                p.addAll(tail);
                out.add(p);
            }
        }
        return out;
    }
}
