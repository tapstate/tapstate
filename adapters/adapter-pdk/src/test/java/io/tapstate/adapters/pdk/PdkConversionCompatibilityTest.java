package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class PdkConversionCompatibilityTest {
    private static final Instant PRECISE = Instant.parse("2021-11-22T00:08:03.849711123Z");

    @Test
    void aConnectorValueSubclassUsesItsCanonicalPdkSemantics() {
        assertThatCode(() -> {
            TapStringValue source = new ConnectorStringValue("key");
            source.setOriginType("native-key");
            TapCodecsRegistry target = nativeKeys();
            assertThat(roundTrip(Map.of("key", source), new TapCodecsRegistry(), target))
                    .containsEntry("key", new NativeKey("key"));
        }).doesNotThrowAnyException();
    }

    @Test
    void aSubclassNameCannotImpersonateAnUnrelatedBuiltinKind() {
        assertThatCode(() -> {
            TapStringValue source = new TapDateValue("key");
            source.setOriginType("native-key");
            assertThat(roundTrip(Map.of("key", source), new TapCodecsRegistry(), nativeKeys()))
                    .containsEntry("key", new NativeKey("key"));
        }).doesNotThrowAnyException();
    }

    @Test
    void alreadyConvertedArrayElementsRetainTheirOwnOriginType() {
        TapStringValue source = new TapStringValue("key");
        source.setOriginType("native-key");
        assertThat(roundTrip(Map.of("tags", List.of(source)), new TapCodecsRegistry(), nativeKeys()))
                .containsEntry("tags", List.of(new NativeKey("key")));
        assertThat(source.getOriginType()).isEqualTo("native-key");
    }

    @Test
    void aNullCodecPayloadRetainsTheOriginalValueIncludingInsideContainers() {
        UUID original = UUID.fromString("9cad877e-2c82-4392-9f0f-905e23aeeac1");
        TapCodecsRegistry source = new TapCodecsRegistry();
        source.registerToTapValue(UUID.class, (value, type) -> new TapStringValue(null));
        Map<String, Object> row = Map.of("key", original, "keys", List.of(original));
        assertThat(roundTrip(row, source, new TapCodecsRegistry())).isEqualTo(row);
    }

    @Test
    void nestedMapKeysKeepTheirTypeAndDoNotCollideWithTheirTextSpelling() {
        Map<Object, Object> values = new LinkedHashMap<>();
        values.put(7, "number-key");
        values.put("7", "text-key");
        Map<String, Object> row = Map.of("map", values, "array", List.of(values));
        assertThatCode(() -> assertThat(roundTrip(row, new TapCodecsRegistry(), new TapCodecsRegistry()))
                .isEqualTo(row)).doesNotThrowAnyException();
    }

    @Test
    void anUnknownStoredKindRetainsItsPortablePayload() {
        ConvertedValue future = new ConvertedValue(Map.of("key", "value"), "future", "FutureValue", null, null);
        assertThatCode(() -> assertThat(encoded(Envelope.read(0, "values", Map.of("future", future), null),
                new TapCodecsRegistry())).containsEntry("future", Map.of("key", "value")))
                .doesNotThrowAnyException();
    }

    @Test
    void anUnknownTemporalOriginRetainsNanosecondsAndFraction() {
        TapDateTimeValue source = new TapDateTimeValue(new DateTime(PRECISE) {
            @Override public int getOriginType() { return Integer.MAX_VALUE; }
            @Override public int getFraction() { return 9; }
        });
        source.setTapType(new TapDateTime().fraction(9));
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, value -> {
            assertThat(value.getValue().getFraction()).isEqualTo(9);
            return value.getValue().toInstant();
        });
        assertThatCode(() -> assertThat(roundTrip(Map.of("stamp", source), new TapCodecsRegistry(), target))
                .containsEntry("stamp", PRECISE)).doesNotThrowAnyException();
    }

    @Test
    void stableColumnMetadataReusesItsSerializedDescriptionAcrossRows() {
        TapTable table = new TapTable("values");
        table.add(new TapField("stamp", "timestamp(9)").tapType(new TapDateTime().fraction(9)));
        PdkRowSchema schema = PdkRowSchema.of(table);
        TapCodecsRegistry codecs = new TapCodecsRegistry();
        ConvertedValue first = timestamp(schema, codecs);
        ConvertedValue second = timestamp(schema, codecs);
        assertThat(second.typeDescription()).isSameAs(first.typeDescription());
    }

    @Test
    void unchangedContainerChildrenNeedNoCopy() {
        Map<String, Object> map = Map.of("key", "value");
        List<Object> list = List.of("value", 1L);
        Envelope decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("map", map, "list", list)), new TapCodecsRegistry(), Map.of());
        assertThat(((ConvertedValue) decoded.after().get("map")).value()).isSameAs(map);
        assertThat(((ConvertedValue) decoded.after().get("list")).value()).isSameAs(list);
    }

    @Test
    void ordinaryContainerTraversalDoesNotMutateTheInput() {
        Map<String, Object> nested = Map.of("stamp", 1637539683849711L);
        Map<String, Object> row = Map.of("nested", nested);
        TapTable table = new TapTable("values");
        table.add(new TapField("nested.stamp", "timestamp(6)").tapType(new TapDateTime().fraction(6)));
        Envelope decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(row), new TapCodecsRegistry(), PdkRowSchema.of(table));
        assertThat(ConvertedValue.unwrapRow(decoded.after())).containsEntry("nested",
                Map.of("stamp", Instant.parse("2021-11-22T00:08:03.849711Z")));
        assertThat(nested).containsEntry("stamp", 1637539683849711L);
        assertThat(row).containsEntry("nested", nested);
    }

    @Test
    void aValueSpecificTypeDoesNotBorrowTheCachedColumnDescription() {
        TapTable table = new TapTable("values");
        table.add(new TapField("stamp", "timestamp(9)").tapType(new TapDateTime().fraction(9)));
        PdkRowSchema schema = PdkRowSchema.of(table);
        TapDateTimeValue value = new TapDateTimeValue(new DateTime(PRECISE));
        value.setTapType(new TapDateTime().fraction(6));
        Envelope decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("stamp", value)), new TapCodecsRegistry(), schema);
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, restored -> {
            assertThat(restored.getTapType().getFraction()).isEqualTo(6);
            return restored.getValue().toInstant();
        });
        assertThat(encoded(decoded, target)).containsEntry("stamp", PRECISE);
    }

    @Test
    void customMapAndListCodecsReceiveTheirOriginalDriverClasses() {
        DriverMap map = new DriverMap();
        map.put("key", "value");
        DriverList list = new DriverList();
        list.add("value");
        TapCodecsRegistry source = new TapCodecsRegistry();
        source.registerToTapValue(DriverMap.class, (value, type) -> new TapStringValue(
                "map:" + ((DriverMap) value).get("key")));
        source.registerToTapValue(DriverList.class, (value, type) -> new TapStringValue(
                "list:" + ((DriverList) value).getFirst()));
        assertThat(roundTrip(Map.of("map", map, "list", list), source, new TapCodecsRegistry()))
                .containsEntry("map", "map:value").containsEntry("list", "list:value");
    }

    @Test
    void mutationsToTheSameSchemaTypeCannotReuseStaleMetadata() {
        TapDateTime type = new TapDateTime().fraction(9);
        TapTable table = new TapTable("values");
        table.add(new TapField("stamp", "timestamp").tapType(type));
        PdkRowSchema schema = PdkRowSchema.of(table);
        type.setFraction(6);
        TapDateTimeValue value = new TapDateTimeValue(new DateTime(PRECISE));
        value.setTapType(type);
        Envelope decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("stamp", value)), new TapCodecsRegistry(), schema);
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapDateTimeValue.class, restored -> {
            assertThat(restored.getTapType().getFraction()).isEqualTo(6);
            return restored.getValue().toInstant();
        });
        assertThat(encoded(decoded, target)).containsEntry("stamp", PRECISE);
    }

    @Test
    void customContainerTraversalKeepsTheOriginalElementForOriginInference() {
        DriverMap container = new DriverMap();
        container.put("tags", List.of(new NativeKey("array-key")));
        TapCodecsRegistry source = new TapCodecsRegistry();
        source.registerToTapValue(NativeKey.class, (value, type) -> new TapStringValue(((NativeKey) value).value()));
        source.registerToTapValue(DriverMap.class, (value, type) ->
                new io.tapdata.entity.schema.value.TapMapValue((DriverMap) value));
        TapTable table = new TapTable("values");
        table.add(new TapField("known", "native-key").tapType(new io.tapdata.entity.schema.type.TapString()));
        table.add(new TapField("container.tags", "outer-array").tapType(new io.tapdata.entity.schema.type.TapArray()));
        Envelope decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("known", new NativeKey("known"), "container", container)), source, PdkRowSchema.of(table));
        assertThat(encoded(decoded, nativeKeys())).containsEntry("container",
                Map.of("tags", List.of(new NativeKey("array-key"))));
    }

    @Test
    void aDriverContainerDoesNotEscapeInThePortableCarrier() {
        DriverMap map = new DriverMap();
        map.put("key", "value");
        TapCodecsRegistry source = new TapCodecsRegistry();
        source.registerToTapValue(DriverMap.class, (value, type) ->
                new io.tapdata.entity.schema.value.TapMapValue((DriverMap) value));
        Envelope decoded = TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("map", map)), source, Map.of());
        Object payload = ((ConvertedValue) decoded.after().get("map")).value();
        assertThat(payload).isEqualTo(Map.of("key", "value")).isNotInstanceOf(DriverMap.class);
    }

    @Test
    void publicDecodeEntryPointsKeepTheNamedColumnTypesGuard() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create().table("values").after(Map.of("id", 1L));
        assertThatNullPointerException().isThrownBy(() -> TapEventCodec.decodeChange(event,
                new TapCodecsRegistry(), (Map<String, String>) null)).withMessage("columnTypes");
        assertThatNullPointerException().isThrownBy(() -> TapEventCodec.decodeSnapshotRow(event,
                new TapCodecsRegistry(), (Map<String, String>) null)).withMessage("columnTypes");
    }

    private static ConvertedValue timestamp(PdkRowSchema schema, TapCodecsRegistry codecs) {
        return (ConvertedValue) TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values")
                .after(Map.of("stamp", PRECISE.getEpochSecond() * 1_000_000_000L + PRECISE.getNano())), codecs, schema)
                .after().get("stamp");
    }

    private static TapCodecsRegistry nativeKeys() {
        TapCodecsRegistry target = new TapCodecsRegistry();
        target.registerFromTapValue(TapStringValue.class, value -> "native-key".equals(value.getOriginType())
                ? new NativeKey(value.getValue()) : value.getValue());
        return target;
    }

    private static Map<String, Object> roundTrip(Map<String, Object> row, TapCodecsRegistry source,
            TapCodecsRegistry target) {
        return encoded(TapEventCodec.decodeChange(TapInsertRecordEvent.create().table("values").after(row),
                source, Map.of()), target);
    }

    private static Map<String, Object> encoded(Envelope envelope, TapCodecsRegistry target) {
        return ((TapInsertRecordEvent) TapEventCodec.encode(envelope, target)).getAfter();
    }

    private record NativeKey(String value) { }
    private static final class DriverMap extends LinkedHashMap<String, Object> {
        private static final long serialVersionUID = 1L;
    }
    private static final class DriverList extends java.util.ArrayList<Object> {
        private static final long serialVersionUID = 1L;
    }
    private static final class ConnectorStringValue extends TapStringValue {
        private ConnectorStringValue(String value) { super(value); }
    }
    private static final class TapDateValue extends TapStringValue {
        private TapDateValue(String value) { super(value); }
    }
}
