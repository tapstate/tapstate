package io.tapstate.app;

import io.tapdata.entity.schema.value.DateTime;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
}
