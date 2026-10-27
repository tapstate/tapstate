package io.tapstate.adapters.mongostore;

import java.nio.charset.StandardCharsets;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class MongoAlertStorageSchemaTest {
    @Test void componentVersionFactMatchesItsAuthoredGoldenWithoutASeparateGlobalVersion() throws Exception {
        Document actual = MongoAlertStorageSchema.initialMarker("cluster-a",
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "schema-op-a");
        try (var input = getClass().getResourceAsStream("/alert-schema-v3.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        }
    }

    @Test void authorityVersionTwoGoldenRemainsReadableWithoutAQueryPreparationOrCounterReset() throws Exception {
        try (var input = getClass().getResourceAsStream("/alert-schema-v2.golden.json")) {
            Document marker = Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            Document counter = ConfigurationVersion.initial(); counter.put(ConfigurationVersion.ALERT_FENCE, 100L);
            assertThat(MongoAlertStorageSchema.verifyAuthorityState(new Document("installedVersion", 1).append("alerting", marker), counter,
                    "cluster-a", marker.getString("keyId"))).isEqualTo(marker).doesNotContainKey("queryPreparation");
            assertThat(counter.getLong(ConfigurationVersion.ALERT_FENCE)).isEqualTo(100L);
        }
    }

    @Test void legacyGoldenRemainsReadableWithoutEnrollmentOrRewritingItsInstallationOperation() throws Exception {
        try (var input = getClass().getResourceAsStream("/alert-schema-v1.golden.json")) {
            Document legacy = Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            Document schema = new Document("installedVersion", 1).append("alerting", legacy);
            Document counter = new Document("_id", "configuration").append("revision", 20L);
            assertThat(MongoAlertStorageSchema.verifyAuthorityState(schema, counter, "cluster-a", legacy.getString("keyId")))
                    .isEqualTo(legacy).doesNotContainKey("authorityPreparationOperationId");
            assertThat(counter).doesNotContainKey(ConfigurationVersion.ALERT_FENCE);
        }
    }

    @Test void singletonHasExactInt64GoldenAndAddsOnlyTwentyFiveBsonBytes() throws Exception {
        try (var input = getClass().getResourceAsStream("/alert-configuration-version-v2.golden.json")) {
            Document counter = ConfigurationVersion.initial();
            assertThat(counter).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
            Document legacy = new Document(counter); legacy.remove(ConfigurationVersion.ALERT_FENCE);
            assertThat(MongoAlertConfigurationCodec.size(counter) - MongoAlertConfigurationCodec.size(legacy)).isEqualTo(25);
            assertThat(MongoAlertConfigurationCodec.size(counter)).isLessThanOrEqualTo(256);
        }
    }
}
