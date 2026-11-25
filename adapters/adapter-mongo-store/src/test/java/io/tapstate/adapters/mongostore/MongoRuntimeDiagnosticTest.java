package io.tapstate.adapters.mongostore;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoRuntimeDiagnosticTest {
    @Test void keepsTheCompleteVersionedProjectionThroughBson() {
        var projection = Map.<String, Object>of("code", "TS0060027", "codeSource", "tapstate-standard",
                "params", Map.of("connector", "fixture", "detail", "refused"),
                "cause", Map.of("message", "safe cause"));
        var stored = MongoRuntimeDiagnostic.document(projection);
        assertThat(stored).containsEntry("formatVersion", 2);
        assertThat(MongoRuntimeDiagnostic.read(Document.parse(stored.toJson()))).isEqualTo(projection);
    }

    @Test void enforcesActualBsonBytesIncludingUtf8AndEnvelope() {
        var params = new LinkedHashMap<String, Object>();
        for (int i = 0; i < 32; i++) params.put("parameter" + i, "\u4e00".repeat(4096));
        var oversized = new Document("formatVersion", 2)
                .append("projection", new Document("params", new Document(params)));
        assertThat(MongoConnectionDiagnostic.bsonSize(oversized)).isGreaterThan(MongoRuntimeDiagnostic.MAX_BYTES);
        assertThatThrownBy(() -> MongoRuntimeDiagnostic.read(oversized)).isInstanceOf(IllegalArgumentException.class);
        var bounded = MongoRuntimeDiagnostic.document(Map.of("params", params));
        assertThat(MongoConnectionDiagnostic.bsonSize(bounded)).isLessThanOrEqualTo(MongoRuntimeDiagnostic.MAX_BYTES);
        assertThat(MongoRuntimeDiagnostic.read(bounded)).containsExactlyEntriesOf(Map.of("truncated", true));
    }

    @Test void refusesUnknownVersionsFieldsAndInvalidStructure() {
        var valid = MongoRuntimeDiagnostic.document(Map.of("message", "safe"));
        assertThatThrownBy(() -> MongoRuntimeDiagnostic.read(new Document(valid).append("formatVersion", 3)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MongoRuntimeDiagnostic.read(new Document(valid).append("unexpected", true)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MongoRuntimeDiagnostic.read(new Document("formatVersion", 2)
                .append("projection", new Document("message", "safe").append("unexpected", true))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
