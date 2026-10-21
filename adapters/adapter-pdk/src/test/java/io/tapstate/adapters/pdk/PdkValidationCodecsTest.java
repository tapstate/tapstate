package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.type.TapNumber;
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
