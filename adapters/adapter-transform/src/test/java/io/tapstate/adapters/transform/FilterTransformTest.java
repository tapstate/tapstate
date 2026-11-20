package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.spi.transform.TransformPort;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Row membership changes reach downstream sinks while explicit operation predicates still apply.
 * Non-row {@code ddl} events bypass the predicate to preserve schema evolution.
 */
class FilterTransformTest {

    @Test
    @DisplayName("keeps a row whose predicate is true")
    void keepsRowWhenPredicateTrue() {
        TransformPort filter = StatelessTransforms.filter("after.deleted == 0");
        Envelope row = Envelope.insert(1L, "orders", Map.of("id", 1, "deleted", 0), null);

        assertThat(filter.transform(row)).containsExactly(row);
    }

    @Test
    @DisplayName("drops a row whose predicate is false")
    void dropsRowWhenPredicateFalse() {
        TransformPort filter = StatelessTransforms.filter("after.deleted == 0");
        Envelope row = Envelope.insert(1L, "orders", Map.of("id", 1, "deleted", 1), null);

        assertThat(filter.transform(row)).isEmpty();
    }

    @Test
    @DisplayName("filters decimal column using user reported compound predicate")
    void filtersDecimalColumnWithUserReportedExpression() {
        TransformPort filter = StatelessTransforms.filter("after.i_name != 'cK6J3' && after.i_price > 25");
        Envelope match = Envelope.insert(1L, "bmsql_item",
                Map.of("i_name", "validItem", "i_price", new java.math.BigDecimal("25.01")), null);
        Envelope wrongName = Envelope.insert(2L, "bmsql_item",
                Map.of("i_name", "cK6J3", "i_price", new java.math.BigDecimal("30.00")), null);
        Envelope lowPrice = Envelope.insert(3L, "bmsql_item",
                Map.of("i_name", "validItem", "i_price", new java.math.BigDecimal("25.00")), null);

        assertThat(filter.transform(match)).containsExactly(match);
        assertThat(filter.transform(wrongName)).isEmpty();
        assertThat(filter.transform(lowPrice)).isEmpty();
    }

    @Test
    @DisplayName("retracts a row when a decimal update causes it to stop matching a threshold filter")
    void emitsADeleteWhenDecimalFieldStopsMatchingFilter() {
        TransformPort filter = StatelessTransforms.filter("after.amount > 25");
        Map<String, Object> matching = Map.of("id", 1, "amount", new java.math.BigDecimal("30.00"));
        Map<String, Object> notMatching = Map.of("id", 1, "amount", new java.math.BigDecimal("20.00"));

        Envelope insert = Envelope.insert(1L, "orders", matching, null);
        assertThat(filter.transform(insert)).containsExactly(insert);

        Envelope dropsBelow = Envelope.update(2L, "orders", matching, notMatching, null);
        assertThat(filter.transform(dropsBelow))
                .as("stopping to match must emit a DELETE retraction with before image")
                .singleElement().satisfies(retraction -> {
                    assertThat(retraction.op()).isEqualTo(Op.DELETE);
                    assertThat(retraction.before()).isEqualTo(matching);
                    assertThat(retraction.after()).isNull();
                });

        Envelope staysBelow = Envelope.update(3L, "orders", notMatching,
                Map.of("id", 1, "amount", new java.math.BigDecimal("15.00")), null);
        assertThat(filter.transform(staysBelow)).isEmpty();

        Envelope recovers = Envelope.update(4L, "orders", notMatching, matching, null);
        assertThat(filter.transform(recovers)).containsExactly(recovers);
    }

    @Test
    @DisplayName("decimal equality in filter ignores trailing scale zeros")
    void decimalEqualityIgnoresTrailingScaleZerosInFilter() {
        TransformPort filter = StatelessTransforms.filter("after.amount == 25");
        Envelope exactZeroScale = Envelope.insert(1L, "orders", Map.of("id", 1, "amount", new java.math.BigDecimal("25")), null);
        Envelope exactMultiScale = Envelope.insert(2L, "orders", Map.of("id", 2, "amount", new java.math.BigDecimal("25.0000")), null);
        Envelope differentValue = Envelope.insert(3L, "orders", Map.of("id", 3, "amount", new java.math.BigDecimal("25.0001")), null);

        assertThat(filter.transform(exactZeroScale)).containsExactly(exactZeroScale);
        assertThat(filter.transform(exactMultiScale)).containsExactly(exactMultiScale);
        assertThat(filter.transform(differentValue)).isEmpty();
    }

    @Test
    @DisplayName("decimal column compared with other decimal and double columns in same row")
    void decimalComparedWithOtherColumns() {
        TransformPort filter = StatelessTransforms.filter("after.price > after.cost && after.price > after.factor");
        Envelope match = Envelope.insert(1L, "items", Map.of(
                "price", new java.math.BigDecimal("100.00"),
                "cost", new java.math.BigDecimal("80.50"),
                "factor", 99.5), null);
        Envelope noMatch = Envelope.insert(2L, "items", Map.of(
                "price", new java.math.BigDecimal("100.00"),
                "cost", new java.math.BigDecimal("105.00"),
                "factor", 99.5), null);

        assertThat(filter.transform(match)).containsExactly(match);
        assertThat(filter.transform(noMatch)).isEmpty();
    }

    @Test
    @DisplayName("decimal column compared with fractional double literal behaves exactly")
    void filtersDecimalColumnWithFractionalLiteral() {
        TransformPort filterGe = StatelessTransforms.filter("after.amount >= 25.1");
        TransformPort filterEq = StatelessTransforms.filter("after.amount == 25.1");
        TransformPort filterGt = StatelessTransforms.filter("after.amount > 25.1");

        Envelope exact = Envelope.insert(1L, "orders", Map.of("id", 1, "amount", new java.math.BigDecimal("25.1000")), null);
        Envelope slightlyAbove = Envelope.insert(2L, "orders", Map.of("id", 2, "amount", new java.math.BigDecimal("25.1001")), null);
        Envelope slightlyBelow = Envelope.insert(3L, "orders", Map.of("id", 3, "amount", new java.math.BigDecimal("25.0999")), null);

        assertThat(filterGe.transform(exact)).containsExactly(exact);
        assertThat(filterEq.transform(exact)).containsExactly(exact);
        assertThat(filterGt.transform(exact)).isEmpty();

        assertThat(filterGe.transform(slightlyAbove)).containsExactly(slightlyAbove);
        assertThat(filterEq.transform(slightlyAbove)).isEmpty();
        assertThat(filterGt.transform(slightlyAbove)).containsExactly(slightlyAbove);

        assertThat(filterGe.transform(slightlyBelow)).isEmpty();
        assertThat(filterEq.transform(slightlyBelow)).isEmpty();
        assertThat(filterGt.transform(slightlyBelow)).isEmpty();
    }

    @Test
    @DisplayName("decimal filter supports BigInteger column values")
    void filtersDecimalColumnWithBigIntegerRepresentation() {
        TransformPort filterGt = StatelessTransforms.filter("after.amount > 24");
        TransformPort filterEq = StatelessTransforms.filter("after.amount == 25");
        TransformPort filterLt = StatelessTransforms.filter("after.amount < 25");

        Envelope row = Envelope.insert(1L, "orders", Map.of("id", 1, "amount", new java.math.BigInteger("25")), null);

        assertThat(filterGt.transform(row)).containsExactly(row);
        assertThat(filterEq.transform(row)).containsExactly(row);
        assertThat(filterLt.transform(row)).isEmpty();
    }

    @Test
    void aDecimalComparisonUsesTheOriginalFractionalLiteralInEitherOperandOrder() {
        Envelope exact = decimalRow("1.0000000000000001");
        Envelope rounded = decimalRow("1.0");
        assertFilter("after.amount == 1.0000000000000001", exact, rounded);
        assertFilter("1.0000000000000001 == after.amount", exact, rounded);
        assertFilter("1.0000000000000001 > after.amount", rounded, exact);
        assertFilter("after.amount >= 10000000000000001e-16", exact, rounded);
        assertFilter("after.amount == - 1.0000000000000001", decimalRow("-1.0000000000000001"),
                decimalRow("-1.0"));
    }

    @Test
    void aDoubleRowOperandUsesItsActualBinaryValueInEitherOperandOrder() {
        Envelope equal = withFactor("-9223372036854775808", -0x1.0p63);
        Envelope higher = withFactor("-9223372036854775807", -0x1.0p63);
        Envelope lower = withFactor("-9223372036854775809", -0x1.0p63);
        assertFilter("after.amount == after.factor && after.factor == after.amount", equal, higher);
        assertFilter("after.amount >= after.factor && after.factor <= after.amount", equal, lower);
        assertFilter("after.amount > after.factor && after.factor < after.amount", higher, equal);
        assertFilter("after.amount < after.factor && after.factor > after.amount", lower, equal);
        assertFilter("after.amount != after.factor && after.factor != after.amount", lower, equal);
        assertFilter("after.factor == 1.0000000000000001", withFactor("1", 1.0), withFactor("1", 1.5));
    }

    @Test
    void aDecimalCanBeComparedWithAnIntegerLiteralEmittedByAMap() {
        TransformResource resource = (TransformResource) new DslParser().parse("""
                version: tapstate/v1
                kind: transform
                id: threshold
                type: map
                fields: { limit: 25 }
                """);
        TransformPort map = StatelessTransforms.map(MapSpec.from((TransformBody.MapProjection) resource.body()));
        Envelope match = map.transform(decimalRow("30.00")).getFirst();
        Envelope noMatch = map.transform(decimalRow("25.00")).getFirst();
        assertThat(match.after().get("limit")).isInstanceOf(Integer.class);
        assertFilter("after.amount > after.limit && after.limit < after.amount", match, noMatch);
    }

    @Test
    void aBareComputedReadKeepsABigInteger() {
        BigInteger original = new BigInteger("18446744073709551615");
        Envelope row = Envelope.insert(1L, "orders", Map.of("amount", original), null);
        TransformPort map = StatelessTransforms.map(MapSpec.from(new TransformBody.MapProjection(
                Map.of("out", FieldRule.computed("after.amount")))));
        assertThat(map.transform(row).getFirst().after()).containsEntry("out", original);
    }

    @Test
    void decimal128SpecialValuesAreOrderedWithoutFailingTheFilter() {
        Envelope nan = decimal128Special(Double.NaN);
        Envelope infinity = decimal128Special(Double.POSITIVE_INFINITY);
        Envelope negativeInfinity = decimal128Special(Double.NEGATIVE_INFINITY);
        assertFilter("after.amount > 25 && 25 < after.amount", infinity, nan);
        assertFilter("after.amount < 25 && 25 > after.amount", negativeInfinity, nan);
        assertFilter("after.amount != 25", nan, decimalRow("25.00"));
    }

    @Test
    void equalityRecursesIntoListsAndMapsAndPreservesCelKeyEquality() {
        Envelope equal = withFactor("25.000", new BigDecimal("25.00"));
        Envelope different = withFactor("25.001", new BigDecimal("25.00"));
        assertFilter("[after.amount, {1: after.factor}] == [after.factor, {1u: after.amount}]",
                equal, different);
        assertFilter("{'v': [after.amount]} != {'v': [after.factor]}", different, equal);
        assertFilter("[after.amount] == [after.factor]", equal, different);
    }

    private static Envelope decimalRow(String amount) {
        return Envelope.insert(1L, "orders", Map.of("amount", new BigDecimal(amount)), null);
    }

    private static Envelope withFactor(String amount, Number factor) {
        return Envelope.insert(1L, "orders", Map.of("amount", new BigDecimal(amount), "factor", factor), null);
    }

    private static Envelope decimal128Special(double number) {
        return Envelope.insert(1L, "orders", Map.of("amount", new ConvertedValue(number, "DECIMAL128")), null);
    }

    private static void assertFilter(String expression, Envelope match, Envelope noMatch) {
        TransformPort filter = StatelessTransforms.filter(expression);
        assertThat(filter.transform(match)).as(expression).containsExactly(match);
        assertThat(filter.transform(noMatch)).as(expression).isEmpty();
    }

    @Test
    @DisplayName("removes a previously matching shipment from the view when its status changes")
    void emitsADeleteWhenAShipmentStopsMatchingTheViewFilter() {
        TransformPort filter = StatelessTransforms.filter("after.status == 'in_transit'");
        Map<String, Object> inTransit = Map.of("id", 2, "status", "in_transit");
        Envelope shipment2 = Envelope.read(1L, "shipments", inTransit, null);
        Envelope shipment6 = Envelope.read(2L, "shipments", Map.of("id", 6, "status", "in_transit"), null);
        Envelope newShipment = Envelope.insert(3L, "shipments", Map.of("id", 7, "status", "in_transit"), null);

        assertThat(filter.transform(shipment2)).containsExactly(shipment2);
        assertThat(filter.transform(shipment6)).containsExactly(shipment6);
        assertThat(filter.transform(newShipment)).containsExactly(newShipment);

        Envelope delivered = Envelope.update(4L, "shipments", inTransit,
                Map.of("id", 2, "status", "delivered"), null);

        assertThat(filter.transform(delivered))
                .as("the view needs a delete for shipment 2 instead of retaining its old in_transit row")
                .singleElement().satisfies(removal -> {
                    assertThat(removal.op()).isEqualTo(Op.DELETE);
                    assertThat(removal.before()).containsEntry("id", 2);
                    assertThat(removal.after()).isNull();
                });
    }

    @ParameterizedTest
    @CsvSource({"in_transit,in_transit,true", "delivered,in_transit,true", "delivered,delivered,false"})
    void keepsMatchingUpdatesAndDropsUpdatesThatNeverMatched(String before, String after, boolean kept) {
        TransformPort filter = StatelessTransforms.filter("after.status == 'in_transit'");
        Envelope update = Envelope.update(1L, "shipments", Map.of("id", 2, "status", before),
                Map.of("id", 2, "status", after), null);

        assertThat(filter.transform(update)).containsExactly(kept ? new Envelope[] {update} : new Envelope[0]);
    }

    @Test
    void aRetractionKeepsTheOldKeyAndEventMetadataAndUsesTheOriginalOperationForEvaluation() {
        TransformPort filter = StatelessTransforms.filter("op == 'u' && after.status == 'in_transit'");
        Envelope update = Envelope.update(42L, "shipments", Map.of("id", 2, "status", "in_transit"),
                        Map.of("id", 20, "status", "delivered"), Map.of("id", "int"))
                .withPositions(Map.of("shipments", new ChainPosition(new SourceOrder(1, 5), "offset-5")))
                .withRemoved(Set.of("obsolete"));

        assertThat(filter.transform(update)).containsExactly(new Envelope(
                Op.DELETE, update.ts(), update.src(), update.before(), null,
                update.schema(), update.positions(), update.removed()));
    }

    @Test
    void sourceDeletesKeepTheirAbsentAfterImageDuringPredicateEvaluation() {
        TransformPort filter = StatelessTransforms.filter("op != 'd' || !has(after.id)");
        Envelope insert = Envelope.insert(1L, "shipments", Map.of("id", 2, "status", "in_transit"), null);
        Envelope delete = Envelope.delete(2L, "shipments", insert.after(), null);

        assertThat(filter.transform(insert)).containsExactly(insert);
        assertThat(filter.transform(delete)).containsExactly(delete);
    }

    @Test
    void operationPredicatesStillExcludeUpdatesAndDeletesWhenTheyReadRowFields() {
        TransformPort filter = StatelessTransforms.filter("op == 'i' && after.status == 'in_transit'");
        Map<String, Object> matching = Map.of("id", 2, "status", "in_transit");
        Envelope update = Envelope.update(1L, "shipments", matching, Map.of("id", 2, "status", "delivered"), null);

        assertThat(filter.transform(update)).isEmpty();
        assertThat(filter.transform(Envelope.delete(2L, "shipments", matching, null))).isEmpty();
    }

    @Test
    void aMissingEarlierPredicateFieldStillSurfacesTheExistingCodedEvaluationFailure() {
        TransformPort filter = StatelessTransforms.filter("after.status == 'in_transit'");
        Envelope update = Envelope.update(1L, "shipments", Map.of("id", 2),
                Map.of("id", 2, "status", "delivered"), null);

        assertThatThrownBy(() -> filter.transform(update))
                .isInstanceOf(TapstateException.class)
                .satisfies(failure -> assertThat(((TapstateException) failure).code().code())
                        .isEqualTo("transform.expression-failed"));
    }

    @Test
    @DisplayName("binds op as its wire symbol so a predicate can select by change kind")
    void bindsOpAsWireSymbol() {
        TransformPort insertsOnly = StatelessTransforms.filter("op == 'i'");
        Envelope insert = Envelope.insert(1L, "t", Map.of("id", 1), null);
        Envelope update = Envelope.update(1L, "t", Map.of("id", 1), Map.of("id", 2), null);

        assertThat(insertsOnly.transform(insert)).containsExactly(insert);
        assertThat(insertsOnly.transform(update)).isEmpty();
    }

    @Test
    @DisplayName("evaluates a delete like any other row event (before is bound)")
    void evaluatesDeleteAsRow() {
        TransformPort dropDeletes = StatelessTransforms.filter("op != 'd'");
        Envelope delete = Envelope.delete(1L, "t", Map.of("id", 1), null);
        Envelope insert = Envelope.insert(1L, "t", Map.of("id", 1), null);

        assertThat(dropDeletes.transform(delete)).isEmpty();
        assertThat(dropDeletes.transform(insert)).containsExactly(insert);
    }

    @Test
    @DisplayName("bypasses a ddl event without evaluating the predicate")
    void bypassesDdlEvent() {
        TransformPort filter = StatelessTransforms.filter("after.deleted == 0");
        Envelope ddl = Envelope.ddl(1L, "orders", Map.of("kind", "add-column"));

        assertThat(filter.transform(ddl)).containsExactly(ddl);
    }

    @Test
    @DisplayName("a row expression that fails to evaluate surfaces a coded transform.expression-failed")
    void codesRowExpressionEvaluationFailure() {
        // A time zone read from the row type-checks as a string but names no zone, so the predicate
        // compiles and then fails at eval — the operator must see a coded diagnostic naming the
        // expression, not a bare crash.
        TransformPort filter = StatelessTransforms.filter("today(after.zone) == today(after.zone)");
        Envelope row = Envelope.insert(1L, "orders", Map.of("id", 1, "zone", "Nowhere/Here"), null);

        assertThatThrownBy(() -> filter.transform(row))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException e = (TapstateException) thrown;
                    assertThat(e.code().code()).isEqualTo("transform.expression-failed");
                    assertThat(e.args().get("expr")).isEqualTo("today(after.zone) == today(after.zone)");
                    assertThat(e.args()).containsKey("detail");
                });
    }
}
