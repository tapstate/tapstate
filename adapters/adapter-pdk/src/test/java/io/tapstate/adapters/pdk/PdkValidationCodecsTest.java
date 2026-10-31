package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapJson;
import io.tapdata.entity.schema.value.TapJsonValue;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PdkValidationCodecsTest {
    private record DriverValue(String value) {}

    @Test void declaredJsonParsesTheCustomResultOnceWithoutReplacingItsMeaning() {
        var registry = new Registry();
        AtomicInteger calls = new AtomicInteger();
        registry.registerToTapValue(DriverValue.class, (value, model) -> {
            calls.incrementAndGet();
            assertThat(model).isInstanceOf(TapJson.class);
            return new TapJsonValue("{\"n\":9007199254740993,\"child\":[1.2300,null]}");
        });
        var selector = codecs(registry);
        var selected = selector.select("document", new DriverValue("not-json"), new TapJson());
        var parsed = (Map<?, ?>) selector.jsonText("document", selected, new ValidationValues.Limits(8192, 100, 12));
        assertThat(parsed.get("n")).isEqualTo(new BigInteger("9007199254740993"));
        assertThat(((List<?>)parsed.get("child")).getFirst()).isEqualTo(new BigDecimal("1.2300"));
        assertThat(calls).hasValue(1);
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void jsonTextLaneKeepsOrdinaryTextSeparateAndPreservesValidNull() {
        var selector = codecs(new Registry());
        var limits = new ValidationValues.Limits(8192, 100, 12);
        var text = selector.select("text", "{\"a\":1}", new TapString());
        assertThat(text.value()).isEqualTo("{\"a\":1}");
        for (Object value : List.of(new TapStringValue("{\"a\":1}"),
                new ValidationValues.Typed("text", "varchar", "binding-v1", "{}"), Map.of("a", 1))) {
            assertThatThrownBy(() -> selector.jsonText("json", selector.select("json", value, new TapJson()), limits))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
        }
        assertThat(selector.jsonText("json", selector.select("json", null, new TapJson()), limits)).isNull();
        assertThat(selector.jsonText("json", selector.select("json", new TapJsonValue(null), new TapJson()), limits)).isNull();
        assertThat(selector.jsonText("json", selector.select("json", new ConvertedValue("null", "jsonb"), new TapJson()), limits)).isNull();
        assertThatThrownBy(() -> selector.jsonText("json",
                new PdkValidationCodecs.Selected("{}", PdkValidationCodecs.Origin.BUILTIN, "stale"), limits))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
    }

    @Test void jsonWrapperFailuresKeepTheirCauseAndFatalErrorsEscape() {
        var selector = codecs(new Registry());
        var limits = new ValidationValues.Limits(8192, 100, 12);
        var cause = new IllegalStateException("broken JSON value");
        TapJsonValue broken = new TapJsonValue() {
            @Override public String getValue() { throw cause; }
        };
        assertThatThrownBy(() -> selector.jsonText("json", selector.select("json", broken, new TapJson()), limits))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ValidationError.CODEC_FAILED);
                    assertThat(failure.getCause()).isSameAs(cause);
                });
        var fatal = new AssertionError("fatal JSON value");
        TapJsonValue fatalValue = new TapJsonValue() {
            @Override public String getValue() { throw fatal; }
        };
        assertThatThrownBy(() -> selector.jsonText("json", selector.select("json", fatalValue, new TapJson()), limits))
                .isSameAs(fatal);
    }

    @Test void exactNumericLaneRefusesCollapsedCustomDoublesWithoutTryingAnotherCodec() {
        var registry = new Registry();
        registry.registerToTapValue(DriverValue.class, (value, model) ->
                new TapNumberValue(new BigDecimal(((DriverValue)value).value()).doubleValue()));
        var selector = codecs(registry);
        var model = new TapNumber().fixed(true).precision(34).scale(9);
        var first = selector.select("amount", new DriverValue("9007199254740992"), model);
        var second = selector.select("amount", new DriverValue("9007199254740993"), model);
        assertThat(((TapValue<?, ?>)first.value()).getValue()).isEqualTo(((TapValue<?, ?>)second.value()).getValue());
        for (var selected : List.of(first, second)) {
            assertThatThrownBy(() -> selector.exactNumber("amount", selected))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
        }
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void exactNumericLanePreservesLargePortableNumbersAndValidNull() {
        var selector = codecs(new Registry());
        var model = new TapNumber().fixed(true).precision(38);
        for (Number value : List.of(1, 9007199254740993L, new BigInteger("18446744073709551616"),
                new BigDecimal("123456789.123456789"))) {
            assertThat(selector.exactNumber("amount", selector.select("amount", value, model))).isSameAs(value);
        }
        var decimal = new BigDecimal("1.00");
        var converted = new ConvertedValue(decimal, "NUMERIC");
        assertThat(selector.exactNumber("amount", selector.select("amount", converted, model))).isSameAs(decimal);
        var typed = new ValidationValues.Typed("exact-number", "NUMERIC", "binding-v1", decimal);
        assertThat(selector.exactNumber("amount", selector.select("amount", typed, model))).isSameAs(decimal);
        assertThat(selector.exactNumber("amount", selector.select("amount", null, model))).isNull();
        assertThat(selector.exactNumber("amount", selector.select("amount", new TapNumberValue(null), model))).isNull();
    }

    @Test void exactNumericLaneNeverGuessesTextBooleansOrFloatingRepresentations() {
        var selector = codecs(new Registry());
        for (Object value : List.of("1", true, 1.0d, 1.0f, Double.NaN, Double.POSITIVE_INFINITY,
                new TapStringValue("123456789.123456789"))) {
            var selected = selector.select("amount", value, new TapNumber().fixed(true));
            assertThatThrownBy(() -> selector.exactNumber("amount", selected)).isInstanceOf(TapstateException.class);
        }
        // Floating data remains available to its own declared lane; this is not a global float ban.
        assertThat(selector.select("ratio", 1.25d, new TapNumber().fixed(false)).value()).isEqualTo(1.25d);
    }

    @Test void exactCustomPayloadIsUsedAsReturnedAndWrapperFailureKeepsItsCause() {
        var registry = new Registry();
        BigDecimal output = new BigDecimal("123456789.123456789");
        registry.registerToTapValue(DriverValue.class, (value, model) -> new ExactWrapper(output, null));
        var selector = codecs(registry);
        assertThat(selector.exactNumber("amount", selector.select("amount", new DriverValue("opaque"),
                new TapNumber().fixed(true)))).isSameAs(output);
        var cause = new IllegalStateException("fixture wrapper failed");
        var broken = selector.select("amount", new ExactWrapper(output, cause), new TapNumber().fixed(true));
        assertThatThrownBy(() -> selector.exactNumber("amount", broken))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ValidationError.CODEC_FAILED);
                    assertThat(failure.getCause()).isSameAs(cause);
                });
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void cyclicNumericWrapperIsBoundedRatherThanUnwrappedForever() {
        var wrapper = new TapValue<Object, TapNumber>() {
            @Override public TapType createDefaultTapType() { return new TapNumber(); }
            @Override public Class<? extends TapType> tapTypeClass() { return TapNumber.class; }
        };
        wrapper.setValue(wrapper);
        var selector = codecs(new Registry());
        assertThatThrownBy(() -> selector.exactNumber("amount", selector.select("amount", wrapper, null)))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }

    private static final class ExactWrapper extends TapValue<BigDecimal, TapNumber> {
        private final RuntimeException failure;
        private ExactWrapper(BigDecimal value, RuntimeException failure) { this.value = value; this.failure = failure; }
        @Override public BigDecimal getValue() { if (failure != null) throw failure; return value; }
        @Override public TapType createDefaultTapType() { return new TapNumber(); }
        @Override public Class<? extends TapType> tapTypeClass() { return TapNumber.class; }
    }

    @Test void exactNumericLaneRejectsOldBindingsAndContradictoryTypedDomains() {
        var selector = codecs(new Registry());
        var old = new PdkValidationCodecs.Selected(BigDecimal.ONE, PdkValidationCodecs.Origin.BUILTIN, "old-binding");
        assertThatThrownBy(() -> selector.exactNumber("amount", old)).isInstanceOf(TapstateException.class);
        var wrongDomain = new ValidationValues.Typed("text", "VARCHAR", "binding-v1", 1);
        assertThatThrownBy(() -> selector.exactNumber("amount", selector.select("amount", wrongDomain, null)))
                .isInstanceOf(TapstateException.class);
    }

    @Test void customCodecWinsBeforeTheBuiltinLaneAndReceivesTheActualModel() {
        var registry = new Registry();
        AtomicReference<TapType> seenModel = new AtomicReference<>();
        var semanticResult = new TapStringValue("custom-domain");
        registry.registerToTapValue(Long.class, (value, model) -> {
            seenModel.set(model);
            return semanticResult;
        });
        var model = new TapNumber().fixed(true).precision(30);
        var selected = codecs(registry).select("id", 9007199254740993L, model);
        assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.CUSTOM);
        assertThat(selected.value()).isSameAs(semanticResult);
        assertThat(selected.codecBinding()).isEqualTo("binding-v1");
        assertThat(seenModel.get()).isSameAs(model);
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void exactJavaNumbersNeverNarrowThroughTheDefaultNumberCodec() {
        var registry = new Registry();
        registry.fallback = (value, model) -> new TapNumberValue(((Number) value).doubleValue());
        var selector = codecs(registry);
        for (Object value : List.of(9007199254740993L, new BigInteger("18446744073709551616"),
                new BigDecimal("9007199254740993.123456789"), 1.5f)) {
            var selected = selector.select("number", value, new TapNumber());
            assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.BUILTIN);
            assertThat(selected.value()).isSameAs(value);
        }
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void aCustomNullResultDoesNotInventANotApplicableConvention() {
        var registry = new Registry();
        registry.registerToTapValue(DriverValue.class, (value, model) -> null);
        registry.fallback = (value, model) -> new TapStringValue("fallback");
        assertThatThrownBy(() -> codecs(registry).select("amount", new DriverValue("private-value"), null))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE);
                    assertThat(failure.args()).containsExactlyInAnyOrderEntriesOf(
                            Map.of("connector", "connector", "field", "amount"));
                    assertThat(failure.getMessage()).doesNotContain("private-value");
                });
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void aNonNullWrapperWithNullPayloadIsAValidCustomResult() {
        var registry = new Registry();
        var nullResult = new TapStringValue(null);
        registry.registerToTapValue(DriverValue.class, (value, model) -> nullResult);
        var selected = codecs(registry).select("field", new DriverValue("value"), null);
        assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.CUSTOM);
        assertThat(selected.value()).isSameAs(nullResult);
        assertThat(((TapValue<?, ?>) selected.value()).getValue()).isNull();
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void customFailureIsNotReplacedWithNullOrDefaultConversion() {
        var registry = new Registry();
        var original = new IllegalStateException("fixture codec failure");
        registry.registerToTapValue(DriverValue.class, (value, model) -> { throw original; });
        assertThatThrownBy(() -> codecs(registry).select("field", new DriverValue("value"), null))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ValidationError.CODEC_FAILED);
                    assertThat(failure.getCause()).isSameAs(original);
                });
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void defaultIsUsedOnlyWithoutCustomOrSafeBuiltinInterpretation() {
        var registry = new Registry();
        var model = new TapString();
        var expected = new TapStringValue("default-domain");
        AtomicReference<TapType> seen = new AtomicReference<>();
        registry.fallback = (value, type) -> { seen.set(type); return expected; };
        var selected = codecs(registry).select("field", new DriverValue("value"), model);
        assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.DEFAULT);
        assertThat(selected.value()).isSameAs(expected);
        assertThat(seen.get()).isSameAs(model);
        assertThat(registry.defaultCalls).hasValue(1);
    }

    @Test void convertedValuesAreNeverPassedThroughTheRegistryAgain() {
        var registry = new Registry();
        var selector = codecs(registry);
        for (Object value : List.of(new TapStringValue("converted"), new ConvertedValue("converted", "native"),
                new ValidationValues.Typed("text", "native", "binding-v1", "converted"),
                new ValidationValues.Binary(4, new byte[]{1}))) {
            var selected = selector.select("field", value, null);
            assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.ALREADY_CONVERTED);
            assertThat(selected.value()).isSameAs(value);
        }
        assertThat(registry.customCalls).hasValue(0);
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void rawCarrierIsNotPermissionToCompareDriverObjects() {
        var registry = new Registry();
        registry.fallback = (value, model) -> new TapRawValue(value);
        var selector = codecs(registry);
        for (Object value : List.of(new DriverValue("value"), new TapRawValue("raw"))) {
            assertThatThrownBy(() -> selector.select("field", value, null))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
        }
    }

    @Test void unsupportedValueDoesNotFallBackToToString() {
        var registry = new Registry();
        assertThatThrownBy(() -> codecs(registry).select("field", new DriverValue("001"), null))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
    }

    @Test void containersStillRequireIndependentChildDecodingAndOwnership() {
        var registry = new Registry();
        var row = new LinkedHashMap<String, Object>();
        row.put("child", new DriverValue("child"));
        var selected = codecs(registry).select("object", row, null);
        assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.BUILTIN);
        assertThat(selected.value()).isSameAs(row);
        assertThat(registry.defaultCalls).hasValue(0);
    }

    @Test void nullDoesNotCallAnyCodecAndFatalErrorsRemainFatal() {
        var registry = new Registry();
        var selected = codecs(registry).select("field", null, null);
        assertThat(selected.origin()).isEqualTo(PdkValidationCodecs.Origin.NULL);
        assertThat(selected.value()).isNull();
        assertThat(registry.customCalls).hasValue(0);
        var fatal = new AssertionError("fixture fatal failure");
        registry.registerToTapValue(DriverValue.class, (value, model) -> { throw fatal; });
        assertThatThrownBy(() -> codecs(registry).select("field", new DriverValue("value"), null)).isSameAs(fatal);
    }

    @Test void convertedFactsFromAnotherCodecBindingAreNotRelabelledAsCurrent() {
        var old = new ValidationValues.Typed("text", "native", "binding-old", "converted");
        var registry = new Registry();
        assertThatThrownBy(() -> codecs(registry).select("field", old, null))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
        assertThat(registry.customCalls).hasValue(0);
        assertThat(registry.defaultCalls).hasValue(0);
    }

    private static PdkValidationCodecs codecs(TapCodecsRegistry registry) {
        return new PdkValidationCodecs("connector", "binding-v1", registry);
    }

    private static final class Registry extends TapCodecsRegistry {
        private final AtomicInteger customCalls = new AtomicInteger();
        private final AtomicInteger defaultCalls = new AtomicInteger();
        private ToTapValueCodec<?> fallback;
        @Override public ToTapValueCodec<?> getCustomToTapValueCodec(Class<?> type) {
            customCalls.incrementAndGet();
            return super.getCustomToTapValueCodec(type);
        }
        @Override public ToTapValueCodec<?> getToTapValueCodec(Class<?> type) {
            defaultCalls.incrementAndGet();
            return fallback;
        }
    }
}
