package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.List;
import io.tapstate.core.event.ConvertedValue;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FullPdkConversionTest {
    private static final Instant PRECISE = Instant.parse("2021-11-22T00:08:03.849711123Z");

    private record DriverCell(TapValue<?, ?> converted) { }

    @Test
    void aPositionalNestedArrayDoesNotInheritItsOuterArraysDeclaration() {
        TapTable table = new TapTable("values");
        table.add(new TapField("rows", "outer-array").tapType(new io.tapdata.entity.schema.type.TapArray()));
        var decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("rows", List.of(List.of(1L)))), new TapCodecsRegistry(), PdkRowSchema.of(table));
        ConvertedValue outer = (ConvertedValue) decoded.after().get("rows");
        ConvertedValue inner = (ConvertedValue) ((List<?>) outer.value()).getFirst();
        assertThat(outer.originType()).isEqualTo("outer-array");
        assertThat(inner.originType()).isNull();
    }

    @Test
    void anAlreadyConvertedSourceValueInheritsItsDiscoveredPrecisionWithoutMutatingTheSource() {
        TapDateTimeValue value = new TapDateTimeValue(new DateTime(PRECISE));
        TapTable table = new TapTable("values");
        table.add(new TapField("stamp", "timestamp(9)").tapType(new TapDateTime().fraction(9)));
        var decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("stamp", value)), new TapCodecsRegistry(), PdkRowSchema.of(table));
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, restored -> {
            assertThat(restored.getTapType()).isNotNull();
            assertThat(restored.getTapType().getFraction()).isEqualTo(9);
            return restored.getValue().toInstant();
        });
        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(decoded, target)).getAfter().get("stamp")).isEqualTo(PRECISE);
        assertThat(value.getTapType()).isNull();
    }

    @Test
    void alreadyConvertedArrayFieldsInheritTheirOwnSchemaRatherThanTheContainerType() {
        TapDateTimeValue value = new TapDateTimeValue(new DateTime(PRECISE));
        TapTable table = new TapTable("values");
        table.add(new TapField("rows", "array").tapType(new io.tapdata.entity.schema.type.TapArray()));
        table.add(new TapField("rows.stamp", "timestamp(9)").tapType(new TapDateTime().fraction(9)));
        var decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("rows", List.of(Map.of("stamp", value)))), new TapCodecsRegistry(), PdkRowSchema.of(table));
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, restored -> {
            assertThat(restored.getTapType()).isNotNull();
            assertThat(restored.getTapType().getFraction()).isEqualTo(9);
            return restored.getValue().toInstant();
        });
        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(decoded, target)).getAfter())
                .isEqualTo(Map.of("rows", List.of(Map.of("stamp", PRECISE))));
    }

    @Test
    void preservesZeroDatetimeAndTimeTextForTheTargetCodec() {
        DateTime invalidDatetime = new DateTime("0-0-0-0-0-0", DateTime.DATETIME_TYPE);
        DateTime invalidTime = new DateTime("0-0-0", DateTime.TIME_TYPE);
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, value -> {
            assertThat(value.getValue().isContainsIllegal()).isTrue();
            return value.getValue().getIllegalDate();
        });
        target.registerFromTapValue(TapTimeValue.class, value -> {
            assertThat(value.getValue().isContainsIllegal()).isTrue();
            return value.getValue().getIllegalDate();
        });
        assertThat(roundTrip(new TapDateTimeValue(invalidDatetime).tapType(new TapDateTime()), target))
                .isEqualTo(invalidDatetime.getIllegalDate());
        assertThat(roundTrip(new TapTimeValue(invalidTime).tapType(new TapTime()), target))
                .isEqualTo(invalidTime.getIllegalDate());
    }

    @Test
    void nullTargetMapAndArrayConversionsCannotDropArrayElements() {
        Map<String, Object> row = Map.of("array", List.of(Map.of("x", 1L), List.of(2L)));
        var decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values").after(row),
                new TapCodecsRegistry(), Map.of());
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(io.tapdata.entity.schema.value.TapMapValue.class, value -> null);
        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(decoded, target)).getAfter()).isEqualTo(row);
        TapCodecsRegistry arrayTarget = new TapCodecsRegistry();
        arrayTarget.registerFromTapValue(io.tapdata.entity.schema.value.TapArrayValue.class, value -> null);
        assertThat(((TapInsertRecordEvent) TapEventCodec.encode(decoded, arrayTarget)).getAfter()).isEqualTo(row);
    }

    @Test
    void preservesDateValueKindForTheTargetsRegisteredCodec() {
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateValue.class, value -> "date");
        target.registerFromTapValue(TapDateTimeValue.class, value -> "datetime");
        assertThat(roundTrip(new TapDateValue(new DateTime(PRECISE)).tapType(new TapDate()), target))
                .isEqualTo("date");
    }

    @Test
    void preservesTimeValueKindForTheTargetsRegisteredCodec() {
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapTimeValue.class, value -> "time");
        target.registerFromTapValue(TapDateTimeValue.class, value -> "datetime");
        assertThat(roundTrip(new TapTimeValue(new DateTime(PRECISE)).tapType(new TapTime().fraction(9)), target))
                .isEqualTo("time");
    }

    @Test
    void preservesYearValueKindForTheTargetsRegisteredCodec() {
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapYearValue.class, value -> "year");
        target.registerFromTapValue(TapDateTimeValue.class, value -> "datetime");
        assertThat(roundTrip(new TapYearValue(2026).tapType(new TapYear()), target)).isEqualTo("year");
    }

    @Test
    void givesTheTargetDatetimePrecisionAndTheUnroundedInstant() {
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, value -> {
            assertThat(value.getTapType()).as("the source datetime description reaches the target").isNotNull();
            assertThat(value.getTapType().getFraction()).isEqualTo(9);
            return value.getValue().toInstant();
        });
        assertThat(roundTrip(new TapDateTimeValue(new DateTime(PRECISE)).tapType(new TapDateTime().fraction(9)), target))
                .isEqualTo(PRECISE);
    }

    @Test
    void preservesTemporalOriginFractionZoneAndBytesForCustomTargetCodecs() {
        DateTime original = new DateTime(PRECISE.atZone(java.time.ZoneId.of("Asia/Shanghai")));
        original.setOriginBytes(new byte[] {1, 2, 3});
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, value -> {
            DateTime restored = value.getValue();
            assertThat(restored.getOriginType()).isEqualTo(original.getOriginType());
            assertThat(restored.getFraction()).isEqualTo(original.getFraction());
            assertThat(restored.getTimeZone()).isEqualTo(original.getTimeZone());
            assertThat(restored.getOriginBytes()).containsExactly(original.getOriginBytes());
            return restored.toInstant();
        });
        assertThat(roundTrip(new TapDateTimeValue(original).tapType(new TapDateTime().fraction(9).withTimeZone(true)), target))
                .isEqualTo(PRECISE);
    }

    @Test
    void givesTheTargetTheCompleteNumericDescription() {
        TapNumber type = new TapNumber().precision(18).scale(4).fixed(true).unsigned(true).bit(64)
                .minValue(BigDecimal.ZERO).maxValue(new BigDecimal("99999999999999.9999"));
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapNumberValue.class, value -> {
            assertThat(value.getTapType()).as("numeric metadata reaches the target codec").isNotNull();
            assertThat(value.getTapType()).usingRecursiveComparison().isEqualTo(type);
            return value.getValue();
        });
        assertThat(roundTrip(new TapNumberValue(6.25).tapType(type), target)).isEqualTo(6.25);
    }

    @Test
    void leavesAnUnconvertedExactDecimalExact() {
        BigDecimal exact = new BigDecimal("12345678901234.5678901234");
        var event = TapInsertRecordEvent.create().table("values").after(Map.of("value", exact));
        var decoded = TapEventCodec.decodeChange(event, new TapCodecsRegistry(), Map.of("value", "decimal(24,10)"));
        assertThat(decoded.after().get("value")).isEqualTo(exact);
    }

    @Test
    void thePdkSchemaCodecPreservesEachTimestampPrecisionInBothPhases() {
        for (int fraction : List.of(0, 3, 6, 9)) {
            long factor = (long) Math.pow(10, fraction);
            long raw = PRECISE.getEpochSecond() * factor + PRECISE.getNano() / (1_000_000_000L / factor);
            Instant expected = Instant.ofEpochSecond(PRECISE.getEpochSecond(),
                    PRECISE.getNano() / (1_000_000_000L / factor) * (1_000_000_000L / factor));
            TapTable table = new TapTable("values");
            table.add(new TapField("stamp", "timestamp").tapType(new TapDateTime().fraction(fraction)));
            PdkRowSchema schema = PdkRowSchema.of(table);
            var cdc = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                    .after(Map.of("stamp", raw)), new TapCodecsRegistry(), schema);
            var snapshot = TapEventCodec.decodeSnapshotRow(TapInsertRecordEvent.create().table("values")
                    .after(Map.of("stamp", expected.atZone(java.time.ZoneOffset.UTC))), new TapCodecsRegistry(), schema);
            assertThat(ConvertedValue.unwrap(cdc.after().get("stamp"))).isEqualTo(expected);
            assertThat(ConvertedValue.unwrap(snapshot.after().get("stamp"))).isEqualTo(expected);
            TapCodecsRegistry target = new TapCodecsRegistry();
            target.registerFromTapValue(TapDateTimeValue.class, value -> {
                assertThat(value.getTapType().getFraction()).isEqualTo(fraction);
                return value.getValue().toInstant();
            });
            assertThat(((TapInsertRecordEvent) TapEventCodec.encode(cdc, target)).getAfter().get("stamp"))
                    .isEqualTo(expected);
        }
    }

    @Test
    void customSourceCodecsReceiveTheActualFieldTypeAndTakePrecedence() {
        TapDateTime type = new TapDateTime().fraction(6).withTimeZone(true);
        TapTable table = new TapTable("values");
        table.add(new TapField("stamp", "custom-time").tapType(type));
        TapCodecsRegistry source = new TapCodecsRegistry();
        source.registerToTapValue(Long.class, (raw, schema) -> {
            assertThat(schema).isSameAs(type);
            return new TapDateTimeValue(new DateTime(PRECISE));
        });
        var cdc = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("stamp", 7L)), source, PdkRowSchema.of(table));
        assertThat(ConvertedValue.unwrap(cdc.after().get("stamp"))).isEqualTo(PRECISE);
    }

    @Test
    void schemaConversionAppliesToBothUpdateImagesAndTheDeleteImage() {
        TapTable table = new TapTable("values");
        table.add(new TapField("stamp", "timestamp").tapType(new TapDateTime().fraction(6)));
        Map<String, Object> before = Map.of("stamp", 1637539683849711L, "label", "old");
        Map<String, Object> after = Map.of("stamp", 1637539683849711L, "label", "new");
        var update = TapEventCodec.decodeChange(TapUpdateRecordEvent.create().table("values")
                .before(before).after(after), new TapCodecsRegistry(), PdkRowSchema.of(table));
        var delete = TapEventCodec.decodeChange(TapDeleteRecordEvent.create().table("values")
                .before(before), new TapCodecsRegistry(), PdkRowSchema.of(table));
        Instant expected = Instant.parse("2021-11-22T00:08:03.849711Z");
        assertThat(ConvertedValue.unwrap(update.before().get("stamp"))).isEqualTo(expected);
        assertThat(ConvertedValue.unwrap(update.after().get("stamp"))).isEqualTo(expected);
        assertThat(ConvertedValue.unwrap(delete.before().get("stamp"))).isEqualTo(expected);
        assertThat(before.get("stamp")).isEqualTo(1637539683849711L);
    }

    private static Object roundTrip(TapValue<?, ?> value, TapCodecsRegistry target) {
        TapCodecsRegistry source = new TapCodecsRegistry();
        source.registerToTapValue(DriverCell.class, (raw, type) -> ((DriverCell) raw).converted());
        var event = TapInsertRecordEvent.create().table("values").after(Map.of("value", new DriverCell(value)));
        var table = new io.tapdata.entity.schema.TapTable("values");
        table.add(new io.tapdata.entity.schema.TapField("value", "declared-source-type").tapType(value.getTapType()));
        var envelope = TapEventCodec.decodeChange(event, source, PdkRowSchema.of(table));
        var encoded = (TapInsertRecordEvent) TapEventCodec.encode(envelope, target);
        return encoded.getAfter().get("value");
    }
}
