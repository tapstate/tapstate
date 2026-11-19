package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapstate.adapters.pdk.TapEventCodec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ValidationPredicateCompiler;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.validation.ValidationPredicate;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DecimalFilterParityTest {
    private static final ValidationValues.Limits ROW = new ValidationValues.Limits(32768, 512, 32);

    @Test
    void mongoSignedZerosCompareAsDecimalsAndBinarySignedZeroRetainsCelSemantics() {
        Envelope negative = decode(Decimal128.NEGATIVE_ZERO);
        Envelope positive = decode(Decimal128.POSITIVE_ZERO);
        assertThat(((ConvertedValue) negative.after().get("amount")).value()).isEqualTo(-0.0d);
        for (String expression : List.of("after.amount == 0.0", "0.0 == after.amount",
                "after.amount < 1e-400", "1e-400 > after.amount",
                "after.amount > -1e-400", "after.amount == 0")) {
            var filter = StatelessTransforms.filter(expression);
            assertThat(filter.transform(negative)).as(expression).containsExactly(negative);
            assertThat(filter.transform(positive)).as(expression).containsExactly(positive);
        }
        Envelope binary = Envelope.insert(1, "orders", Map.of("amount", -0.0d), null);
        assertThat(StatelessTransforms.filter("after.amount == 0.0").transform(binary)).isEmpty();
        assertThat(StatelessTransforms.filter("after.amount < 1e-400").transform(binary)).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({
        "after.amount == 1.0000000000000001, 1.0000000000000001, 1.0",
        "1.0000000000000001 == after.amount, 1.0000000000000001, 1.0",
        "after.amount < 1e-400, 0, 1e-400",
        "after.amount < 1e-20000, 0, 1e-500",
        "after.amount == - 1.0000000000000001, -1.0000000000000001, -1.0"
    })
    void validationAndRuntimeSelectTheSameDecimalRows(String expression, String match, String miss) {
        var filter = StatelessTransforms.filter(expression);
        for (boolean compatible : new boolean[] {false, true}) {
            var snapshot = ValidationPredicateCompiler.compile("filter", expression,
                    new ValidationPredicate.Limits(ROW, 10000, compatible));
            for (String value : List.of(match, miss)) {
                Map<String, Object> row = Map.of("amount", new BigDecimal(value));
                boolean expected = value.equals(match);
                var event = Envelope.insert(1, "orders", row, null);
                assertThat(!filter.transform(event).isEmpty()).as("runtime: %s", value).isEqualTo(expected);
                assertThat(snapshot.evaluate(ValidationValues.ownRow(row, ROW)))
                        .as("snapshot: %s, compatible: %s", value, compatible)
                        .isEqualTo(new ValidationPredicate.Result(expected, null));
            }
        }
    }

    @Test
    void anExponentOutsideDecimalRangeKeepsItsBinaryComparison() {
        String expression = "after.ratio < 1e999999999999999999999";
        Map<String, Object> row = Map.of("ratio", 0.5d);
        var event = Envelope.insert(1, "orders", row, null);
        var filter = StatelessTransforms.filter(expression);
        assertThat(filter.transform(event)).containsExactly(event);
        var snapshot = ValidationPredicateCompiler.compile("filter", expression,
                new ValidationPredicate.Limits(ROW, 10000, false));
        assertThat(snapshot.evaluate(ValidationValues.ownRow(row, ROW)))
                .isEqualTo(new ValidationPredicate.Result(true, null));
        Map<String, Object> decimal = Map.of("ratio", new BigDecimal("0.5"));
        assertThatThrownBy(() -> filter.transform(Envelope.insert(1, "orders", decimal, null)))
                .isInstanceOf(TapstateException.class)
                .hasStackTraceContaining("outside the exact decimal comparison range");
        var decimalRow = ValidationValues.ownRow(decimal, ROW);
        assertThat(snapshot.evaluate(decimalRow)).isEqualTo(new ValidationPredicate.Result(null,
                new ValidationPredicate.Problem(ValidationPredicate.ProblemKind.UNSUPPORTED_VALUE, List.of("ratio"))));
    }

    private static Envelope decode(Decimal128 amount) {
        var codecs = new TapCodecsRegistry();
        codecs.registerToTapValue(Decimal128.class,
                (value, type) -> new TapNumberValue(((Decimal128) value).doubleValue()));
        return TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("orders")
                .referenceTime(1L).after(Map.of("amount", amount)), codecs, Map.of());
    }
}
