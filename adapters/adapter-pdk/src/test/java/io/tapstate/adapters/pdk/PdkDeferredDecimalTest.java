package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapstate.core.common.NumericType;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkDeferredDecimalTest {
    private static final TapNumber SOURCE = new TapNumber().fixed(true).precision(18).scale(4)
            .minValue(new BigDecimal("-99999999999999.9999"))
            .maxValue(new BigDecimal("99999999999999.9999"));
    private static final NumericType NUMBER = PdkTypeMapping.numericType(SOURCE);
    private static final TargetTable TARGET = new TargetTable("orders", List.of(
            new TargetField("id", "bigint", true), TargetField.deferredDecimal("amount", NUMBER)));

    @Test
    void untouchedDecimalsRecoverTheirExactSourceDescriptorWithoutChangingTheDeclaredSchema() {
        var declared = TargetTapTable.build(TARGET).getNameFieldMap();
        var carried = carried(new BigDecimal("12345678901234.5678"), SOURCE);

        var resolved = PdkDeferredDecimal.fields(TARGET, declared, Map.of("id", 1L, "amount", carried));

        assertThat(declared).containsOnlyKeys("id");
        assertThat(resolved).containsOnlyKeys("id", "amount");
        assertThat(resolved.get("id")).isSameAs(declared.get("id"));
        assertThat(PdkTypeMapping.numericType(resolved.get("amount").getTapType())).isEqualTo(NUMBER);
        assertThat(TargetTapTable.build(TARGET).primaryKeys()).containsExactly("id");
    }

    @Test
    void absentAndChangedTypesKeepDynamicInferenceAndDoNotCreateDecimalColumns() {
        Map<String, TapField> declared = TargetTapTable.build(TARGET).getNameFieldMap();
        assertThat(PdkDeferredDecimal.fields(TARGET, declared, null)).isSameAs(declared);
        for (var row : List.<Map<String, Object>>of(Map.of("id", 1L), Map.of("amount", "text"), Map.of("amount", 17L),
                Map.of("amount", new ConvertedValue("text", "VARCHAR", "TapStringValue", null, null)))) {
            assertThat(PdkDeferredDecimal.fields(TARGET, declared, row)).isSameAs(declared);
        }
        assertThat(PdkDeferredDecimal.fields(new TargetTable("plain", List.of(new TargetField("id", "bigint", true))),
                declared, Map.of("id", 1L))).isSameAs(declared);
    }

    @Test
    void decimalValuesRequireTheMatchingOriginalNumericCarrier() {
        BigDecimal value = new BigDecimal("1.2500");
        var declared = TargetTapTable.build(TARGET).getNameFieldMap();
        var otherType = new TapNumber().fixed(true).precision(10).scale(2)
                .minValue(new BigDecimal("-99999999.99")).maxValue(new BigDecimal("99999999.99"));
        var invalid = List.of(value, new ConvertedValue(value, "DECIMAL128"),
                new ConvertedValue(value, "DECIMAL128", "TapStringValue", description(SOURCE), null),
                new ConvertedValue(value, "DECIMAL128", "TapNumberValue", null, null), carried(value, otherType));

        for (Object untrusted : invalid) {
            var row = Map.of("amount", untrusted);
            assertThatThrownBy(() -> PdkDeferredDecimal.fields(TARGET, declared, row))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("amount", "orders", "matching source numeric descriptor");
        }
        assertThat(declared).containsOnlyKeys("id");
    }

    @Test
    void anUpdateUsesSeparateDescriptorsWhenTheScriptChangesTheAfterType() {
        BigDecimal decimal = new BigDecimal("12345678901234.5678");
        var before = Map.<String, Object>of("id", 1L, "amount", carried(decimal, SOURCE));
        var after = Map.<String, Object>of("id", 1L, "amount", "changed", "added", 7L);
        var declared = TargetTapTable.build(TARGET).getNameFieldMap();
        var numericBefore = PdkDeferredDecimal.fields(TARGET, declared, before);
        var dynamicAfter = PdkDeferredDecimal.fields(TARGET, declared, after);
        var registry = new TapCodecsRegistry().registerFromTapValueV2(TapNumberValue.class, (value, field) -> {
            if (field != null && field.getName().equals("amount")) {
                assertThat(PdkTypeMapping.numericType(field.getTapType())).isEqualTo(NUMBER);
            }
            return value.getValue();
        });

        var encoded = (TapUpdateRecordEvent) TapEventCodec.encode(Envelope.update(1L, "orders", before, after, null),
                new PdkValueConversions(registry), numericBefore, dynamicAfter);

        assertThat(encoded.getBefore()).containsEntry("amount", decimal);
        assertThat(encoded.getAfter()).containsEntry("amount", "changed").containsEntry("added", 7L);
        assertThat(dynamicAfter).doesNotContainKey("amount");
    }

    private static ConvertedValue carried(BigDecimal value, TapNumber type) {
        return new ConvertedValue(value, "DECIMAL128", "TapNumberValue", description(type), null);
    }

    private static String description(TapNumber type) {
        return PdkJsonProvider.parser().toJson(type);
    }
}
