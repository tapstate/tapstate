package io.tapstate.adapters.pdk;

import static io.tapstate.core.validation.ValidationRootHintBatch.ChangeKind.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapBinary;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.ByteData;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationValues;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PdkValidationHintKeysTest {
    private static final ValidationValues.Limits LIMITS = new ValidationValues.Limits(131072, 500, 16);
    private record DriverId(String text) {}

    @Test
    void actualOldAndNewSelectedKeysUseCustomCodecOnceWithoutVisitingBusinessValues() {
        var registry = new TapCodecsRegistry(); var calls = new AtomicInteger();
        registry.registerToTapValue(DriverId.class, (value, type) -> {
            assertThat(type).isInstanceOf(TapNumber.class); calls.incrementAndGet();
            return new ExactNumber(new BigDecimal(((DriverId) value).text()));
        });
        var extractor = numeric(registry);
        var before = selectedOnly(new DriverId("9007199254740993"));
        var after = selectedOnly(new DriverId("9007199254740994"));
        var result = extractor.extract(UPDATE, before, after);
        assertThat(result.before()).isEqualTo(key(9007199254740993L));
        assertThat(result.after()).isEqualTo(key(9007199254740994L));
        assertThat(calls).hasValue(2);
        assertThat(extractor.workReservationBytes()).isGreaterThan(LIMITS.bytes());
    }

    @Test
    void oldImageMissingOrNullKeyFailsWithoutSubstitutionFromAfter() {
        var extractor = numeric(new TapCodecsRegistry());
        assertThatThrownBy(() -> extractor.extract(UPDATE, null, Map.of("id", 2L)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("hint.image-missing");
        assertThatThrownBy(() -> extractor.extract(UPDATE, Map.of(), Map.of("id", 2L)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("before:[id]").hasMessageContaining("missing_field");
        var withNull = new LinkedHashMap<String, Object>(); withNull.put("id", null);
        assertThatThrownBy(() -> extractor.extract(UPDATE, withNull, Map.of("id", 2L)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("null_component");
        assertThatThrownBy(() -> extractor.extract(UPDATE, Map.of("id", 1L), Map.of()))
                .isInstanceOf(TapstateException.class).hasMessageContaining("after:[id]");
        var unchanged = extractor.extract(UPDATE, Map.of("id", 1L), Map.of("id", 1L));
        assertThat(unchanged.before()).isEqualTo(key(1));
        assertThat(unchanged.after()).isEqualTo(key(1));
        assertThat(extractor.extract(DELETE, Map.of("id", 1L), null).before()).isEqualTo(key(1));
        assertThat(extractor.extract(INSERT, null, Map.of("id", 2L)).after()).isEqualTo(key(2));
    }

    @Test
    void alreadyConvertedFactsAreNotSentThroughTheCustomCodecAgain() {
        var registry = new TapCodecsRegistry(); var calls = new AtomicInteger();
        registry.registerToTapValue(BigDecimal.class, (value, type) -> { calls.incrementAndGet(); throw new AssertionError("second codec"); });
        var result = numeric(registry).extract(INSERT, null,
                Map.of("id", new ConvertedValue(new BigDecimal("9007199254740993.2500"), "NUMERIC")));
        assertThat(result.after().parts().getFirst().value()).isEqualTo(new BigDecimal("9007199254740993.25"));
        assertThat(calls).hasValue(0);
    }

    @Test
    void lossyNumericCodecIsRejectedEvenWhenItsRoundedValueLooksIntegral() {
        var registry = new TapCodecsRegistry(); var calls = new AtomicInteger();
        registry.registerToTapValue(DriverId.class, (value, type) -> {
            calls.incrementAndGet(); return new TapNumberValue(new BigDecimal(((DriverId) value).text()).doubleValue());
        });
        assertThatThrownBy(() -> numeric(registry).extract(UPDATE,
                Map.of("id", new DriverId("9007199254740993")), Map.of("id", new DriverId("9007199254740994"))))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
        assertThat(calls).hasValue(1);
    }

    @Test
    void compoundPhysicalNamesAndTextRemainLiteralAndOrdered() {
        var table = new TapTable("orders").add(new TapField("tenant.id", "BIGINT").tapType(new TapNumber()))
                .add(new TapField("code", "VARCHAR").tapType(new TapString()));
        var binding = new ValidationKeyBinding("key", List.of(
                new ValidationKeyBinding.Component(List.of("tenant.id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number")),
                new ValidationKeyBinding.Component(List.of("code"), ValidationKey.Kind.TEXT, Set.of("text"))), Set.of(), LIMITS);
        var extractor = new PdkValidationHintKeys("orders", table, codecs(new TapCodecsRegistry()), binding,
                Map.of("tenant.id", PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER),
                        "code", PdkValidationRows.Rule.of(PdkValidationRows.Shape.TEXT)), LIMITS);
        var actual = extractor.extract(INSERT, null, Map.of("tenant.id", 7L, "code", " 001 "));
        assertThat(actual.after()).isEqualTo(new ValidationKey(List.of(ValidationKey.Part.number(7), ValidationKey.Part.text(" 001 "))));
        assertThatThrownBy(() -> extractor.extract(INSERT, null, Map.of("tenant.id", 7L)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("after:[code]");
    }

    @Test
    void mutableBinaryKeysAreOwnedBeforeReturning() {
        var table = new TapTable("orders").add(new TapField("id", "BINARY").tapType(new TapBinary()));
        var binding = new ValidationKeyBinding("key", List.of(
                new ValidationKeyBinding.Component(List.of("id"), ValidationKey.Kind.BINARY, Set.of("binary"))), Set.of(), LIMITS);
        var extractor = new PdkValidationHintKeys("orders", table, codecs(new TapCodecsRegistry()), binding,
                Map.of("id", PdkValidationRows.Rule.of(PdkValidationRows.Shape.BINARY)), LIMITS);
        byte[] bytes = {1, 2, 3};
        assertThatThrownBy(() -> extractor.extract(INSERT, null, Map.of("id", bytes)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("binary subtype unavailable");
        var actual = extractor.extract(INSERT, null, Map.of("id", new TapBinaryValue(new ByteData((byte) 4, bytes)))).after();
        bytes[0] = 99;
        assertThat(actual).isEqualTo(new ValidationKey(List.of(ValidationKey.Part.binary(4, new byte[] {1, 2, 3}))));
    }

    @Test
    void unknownNestedAndContainerKeyRulesAreRejectedBeforeReading() {
        var registry = new TapCodecsRegistry(); var table = table();
        var binding = numericBinding();
        assertThatThrownBy(() -> new PdkValidationHintKeys("orders", table, codecs(registry), binding,
                Map.of("id", PdkValidationRows.Rule.AUTO), LIMITS)).isInstanceOf(TapstateException.class)
                .hasMessageContaining("explicit-key-shape-required");
        assertThatThrownBy(() -> new PdkValidationHintKeys("orders", table, codecs(registry), binding,
                Map.of("id", PdkValidationRows.Rule.of(PdkValidationRows.Shape.OBJECT)), LIMITS)).isInstanceOf(TapstateException.class);
        var nested = new ValidationKeyBinding("key", List.of(
                new ValidationKeyBinding.Component(List.of("parent", "id"), ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), LIMITS);
        assertThatThrownBy(() -> new PdkValidationHintKeys("orders", table, codecs(registry), nested, Map.of(), LIMITS))
                .isInstanceOf(TapstateException.class).hasMessageContaining("scalar-column-required");
        assertThatThrownBy(() -> new PdkValidationHintKeys("orders", new TapTable("missing"), codecs(registry), binding,
                Map.of("id", PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER)), LIMITS))
                .isInstanceOf(TapstateException.class).hasMessageContaining("key-model-missing");
    }

    @Test
    void scalarKeyContractNeverWalksAnUnexpectedContainer() {
        Map<String, Object> container = new AbstractMap<>() {
            @Override public Set<Entry<String, Object>> entrySet() { throw new AssertionError("container traversal"); }
        };
        assertThatThrownBy(() -> numeric(new TapCodecsRegistry()).extract(INSERT, null, Map.of("id", container)))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ValidationError.CODEC_VALUE_UNAVAILABLE));
    }

    @Test
    void badCodecAndFatalErrorsPreserveTheirActualCause() {
        var registry = new TapCodecsRegistry(); var cause = new IllegalStateException("codec failed");
        registry.registerToTapValue(DriverId.class, (value, type) -> { throw cause; });
        assertThatThrownBy(() -> numeric(registry).extract(INSERT, null, Map.of("id", new DriverId("1"))))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ValidationError.CODEC_FAILED); assertThat(failure.getCause()).isSameAs(cause);
                });
        var fatal = new AssertionError("fatal codec");
        registry.registerToTapValue(DriverId.class, (value, type) -> { throw fatal; });
        assertThatThrownBy(() -> numeric(registry).extract(INSERT, null, Map.of("id", new DriverId("1")))).isSameAs(fatal);
    }

    private static Map<String, Object> selectedOnly(Object value) {
        return new AbstractMap<>() {
            @Override public boolean containsKey(Object name) {
                if (!"id".equals(name)) throw new AssertionError("unrelated field accessed");
                return true;
            }
            @Override public Object get(Object name) {
                if (!"id".equals(name)) throw new AssertionError("unrelated field accessed");
                return value;
            }
            @Override public Set<Entry<String, Object>> entrySet() { throw new AssertionError("whole row traversal"); }
        };
    }
    private static ValidationKey key(long n) { return new ValidationKey(List.of(ValidationKey.Part.number(n))); }
    private static TapTable table() { return new TapTable("orders").add(new TapField("id", "NUMERIC").tapType(new TapNumber().fixed(true).precision(38))); }
    private static ValidationKeyBinding numericBinding() {
        return new ValidationKeyBinding("key", List.of(new ValidationKeyBinding.Component(List.of("id"),
                ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), LIMITS);
    }
    private static PdkValidationCodecs codecs(TapCodecsRegistry registry) { return new PdkValidationCodecs("fixture", "binding", registry); }
    private static PdkValidationHintKeys numeric(TapCodecsRegistry registry) {
        return new PdkValidationHintKeys("orders", table(), codecs(registry), numericBinding(),
                Map.of("id", PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER)), LIMITS);
    }
    private static final class ExactNumber extends TapValue<BigDecimal, TapNumber> {
        private ExactNumber(BigDecimal number) { this.value = number; }
        @Override public TapType createDefaultTapType() { return new TapNumber(); }
        @Override public Class<? extends TapType> tapTypeClass() { return TapNumber.class; }
    }
}
