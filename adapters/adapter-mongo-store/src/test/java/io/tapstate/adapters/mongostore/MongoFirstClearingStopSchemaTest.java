package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoFirstClearingStopSchemaTest {
    private static final String CLUSTER = "cluster";

    @Test
    void activationKeepsTheComponentVersionAndIsBoundToTheOriginalCluster() {
        var legacy = new Document("installedVersion", 2);
        var activated = Document.parse("""
                {"installedVersion": 4, "componentVersion": 2,
                 "firstClearingStop": {"formatVersion": 1, "clusterId": "cluster"}}
                """);
        assertThat(MongoFirstClearingStopSchema.enabled(legacy, CLUSTER)).isFalse();
        assertThat(MongoFirstClearingStopSchema.enablement(CLUSTER, 2)).isEqualTo(activated);
        assertThat(MongoFirstClearingStopSchema.enabled(activated, CLUSTER)).isTrue();
        assertThat(MongoFirstClearingStopSchema.componentVersion(activated)).isEqualTo(2);
        assertUnreadable(activated, "another-cluster");
    }

    @Test
    void partialOrUnsupportedActivationCannotBeOpenedAsLegacy() {
        var marker = new Document("formatVersion", 1).append("clusterId", CLUSTER);
        var activated = new Document("installedVersion", 4).append("componentVersion", 2)
                .append("firstClearingStop", marker);
        assertUnreadable(null, CLUSTER);
        for (var invalid : List.of(
                new Document(),
                new Document("installedVersion", 0),
                new Document("installedVersion", 5),
                new Document("installedVersion", 2).append("componentVersion", 2),
                new Document("installedVersion", 2).append("firstClearingStop", marker),
                new Document(activated).append("componentVersion", 0),
                new Document(activated).append("componentVersion", 4),
                new Document(activated).append("componentVersion", "2"),
                new Document(activated).append("firstClearingStop", null),
                new Document(activated).append("firstClearingStop", new Document("formatVersion", 1)),
                new Document(activated).append("firstClearingStop", new Document(marker).append("clusterId", "")),
                new Document(activated).append("firstClearingStop", new Document(marker).append("formatVersion", 2)))) {
            assertUnreadable(invalid, CLUSTER);
        }
    }

    private static void assertUnreadable(Document schema, String cluster) {
        assertThatThrownBy(() -> MongoFirstClearingStopSchema.enabled(schema, cluster))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
