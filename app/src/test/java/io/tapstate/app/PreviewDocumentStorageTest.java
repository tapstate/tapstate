package io.tapstate.app;

import io.tapdata.entity.schema.value.DateTime;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
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
        assertThat(restored.get("values")).isEqualTo(List.of(3L, 5L, 8L));

        Map<Object, Object> invalid = Map.of(7, "not-a-field-name");
        @SuppressWarnings("unchecked")
        Map<String, Object> invalidDocument = (Map<String, Object>) (Map<?, ?>) invalid;
        assertThatThrownBy(() -> PreviewDocumentStorage.encode(invalidDocument))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field names must be strings");
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
