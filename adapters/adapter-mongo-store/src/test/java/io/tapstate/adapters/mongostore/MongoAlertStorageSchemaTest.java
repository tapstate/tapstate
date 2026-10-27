package io.tapstate.adapters.mongostore;

import java.nio.charset.StandardCharsets;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MongoAlertStorageSchemaTest {
    @Test void componentVersionFactMatchesItsAuthoredGoldenWithoutASeparateGlobalVersion() throws Exception {
        Document actual = MongoAlertStorageSchema.initialMarker("cluster-a",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "schema-op-a");
        try (var input = getClass().getResourceAsStream("/alert-schema-v1.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        }
    }
}
