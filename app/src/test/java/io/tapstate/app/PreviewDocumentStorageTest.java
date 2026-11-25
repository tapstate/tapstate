package io.tapstate.app;

import io.tapdata.entity.schema.value.DateTime;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.RowValueError;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreviewDocumentStorageTest {

    @Test
    void preservesCompleteConversionMetadataAndNanoseconds() {
        ConvertedValue stamp = new ConvertedValue(Instant.parse("2021-11-22T00:08:03.849711123Z"),
                "timestamp(9)", "TapDateTimeValue", "{\"fraction\":9}", "{\"nano\":849711123}");
        Map<String, Object> document = Map.of("stamp", stamp);
        assertThat(PreviewDocumentStorage.decode(PreviewDocumentStorage.encode(document))).isEqualTo(document);
    }

    @Test
    void preservesLeafCodecsInsideConvertedContainers() {
        ConvertedValue key = new ConvertedValue("abcdef", "OBJECT_ID", "TapStringValue", "{\"type\":10}", null);
        ConvertedValue array = new ConvertedValue(List.of(Map.of("key", key)), "ARRAY", "TapArrayValue", "{\"type\":2}", null);
        Map<String, Object> document = Map.of("array", array);
        assertThat(PreviewDocumentStorage.decode(PreviewDocumentStorage.encode(document))).isEqualTo(document);
    }

    @Test
    void preservesTypeCarriersAcrossNestedDocumentsAndArrays() {
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("_id", new ConvertedValue("64f0c0de0011223344556677", "ObjectId"));
        nested.put("created_at", new ConvertedValue(
                new DateTime(Instant.parse("2026-10-02T00:00:00Z")), "timestamp"));
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("orders", List.of(nested));
        document.put("payload", new ConvertedValue(new Bytes((byte) 4, new byte[] {1, 2, 3}), null));
        document.put("nullable", null);

        Map<String, Object> restored = PreviewDocumentStorage.decode(PreviewDocumentStorage.encode(document));

        Map<?, ?> order = (Map<?, ?>) ((List<?>) restored.get("orders")).getFirst();
        assertThat(order.get("_id")).isEqualTo(new ConvertedValue("64f0c0de0011223344556677", "ObjectId"));
        assertThat(order.get("created_at")).isEqualTo(new ConvertedValue(
                new DateTime(Instant.parse("2026-10-02T00:00:00Z")), "timestamp"));
        assertThat(restored.get("payload")).isEqualTo(new ConvertedValue(
                new Bytes((byte) 4, new byte[] {1, 2, 3}), null));
        assertThat(restored).containsEntry("nullable", null);
    }

    @Test
    void escapesPointerTokensWhenRestoringConvertedFields() {
        Map<String, Object> document = Map.of("path/with~tokens",
                new ConvertedValue("value", "declared-type"));

        Map<String, Object> restored = PreviewDocumentStorage.decode(PreviewDocumentStorage.encode(document));

        assertThat(restored.get("path/with~tokens"))
                .isEqualTo(new ConvertedValue("value", "declared-type"));
    }

    @Test
    void traversesRawArraysAndRejectsNonStringDocumentFields() {
        Map<String, Object> document = Map.of("values", new int[] {3, 5, 8});

        Map<String, Object> restored = PreviewDocumentStorage.decode(PreviewDocumentStorage.encode(document));
        assertThat(restored).containsEntry("values", List.of(3L, 5L, 8L));

        Map<Object, Object> invalid = Map.of(7, "not-a-field-name");
        @SuppressWarnings("unchecked")
        Map<String, Object> invalidDocument = (Map<String, Object>) (Map<?, ?>) invalid;
        assertThatThrownBy(() -> PreviewDocumentStorage.encode(invalidDocument))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY);
                    assertThat(failure.args()).containsExactlyInAnyOrderEntriesOf(
                            Map.of("path", "$", "keyType", "java.lang.Integer"));
                });
    }

    @Test
    void rejectsNestedKeysBeforePreviewMaterialization() {
        Map<Object, Object> nested = new LinkedHashMap<>();
        nested.put(7, "number-key");
        nested.put("7", "text-key");
        Map<String, Object> document = Map.of("dict", new ConvertedValue(nested, "object"));
        assertThatThrownBy(() -> PreviewDocumentStorage.encode(document))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY);
                    assertThat(failure.args()).containsExactlyInAnyOrderEntriesOf(
                            Map.of("path", "$/dict", "keyType", "java.lang.Integer"));
                });
        assertThat(nested).hasSize(2);
    }

    @Test
    void restoresScalarAndBinaryCarrierTypesFromPrivateMetadata() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("double", new ConvertedValue(1.25d, "DOUBLE"));
        document.put("decimal", new ConvertedValue(new BigDecimal("1234567890.012300"), "DECIMAL"));
        document.put("integer", new ConvertedValue(new BigInteger("12345678901234567890"), "BIGINT"));
        document.put("int", new ConvertedValue(12, "INT"));
        document.put("long", new ConvertedValue(13L, "LONG"));
        document.put("short", new ConvertedValue((short) 14, "SHORT"));
        document.put("byte", new ConvertedValue((byte) 15, "BYTE"));
        document.put("float", new ConvertedValue(1.5f, "FLOAT"));
        document.put("date", new ConvertedValue(new Date(1_700_000_000_000L), "DATE"));
        document.put("byte_array", new ConvertedValue(new byte[] {1, 2}, "BINARY"));
        document.put("boolean", new ConvertedValue(true, "BOOLEAN"));
        document.put("character", new ConvertedValue('x', "CHAR"));

        Map<String, Object> restored = PreviewDocumentStorage.decode(PreviewDocumentStorage.encode(document));

        Map<String, Object> expected = new LinkedHashMap<>(document);
        expected.remove("byte_array");
        assertThat(restored).containsAllEntriesOf(expected);
        assertThat(((ConvertedValue) restored.get("byte_array")).value()).isEqualTo(new byte[] {1, 2});
    }

    @Test
    void roundTripsJsonCarrierValuesAndCountsEncodedUtf8Bytes() {
        Map<String, Object> value = Map.of("name", "customer", "active", true);
        Map<String, Object> document = Map.of("payload", new ConvertedValue(value, "JSON"));

        String encoded = PreviewDocumentStorage.encode(document);
        Map<String, Object> restored = PreviewDocumentStorage.decode(encoded);

        assertThat(restored).containsEntry("payload", new ConvertedValue(value, "JSON"));
        assertThat(PreviewDocumentStorage.encodedSize(encoded))
                .isEqualTo(encoded.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    }

    @Test
    void rejectsMalformedStoredEnvelopesAndTypeMetadata() {
        assertThatThrownBy(() -> PreviewDocumentStorage.decode("{}"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> PreviewDocumentStorage.decode("{\"document\":{},\"carriers\":[null]}"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> PreviewDocumentStorage.decode(
                "{\"document\":{\"x\":1},\"carriers\":[{\"path\":\"/x\",\"kind\":\"mystery\"}]}"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> PreviewDocumentStorage.decode(
                "{\"document\":{\"x\":1},\"carriers\":[{\"path\":\"/x\",\"kind\":\"integer\"}]}"))
                .isInstanceOf(IllegalStateException.class);
    }
}
