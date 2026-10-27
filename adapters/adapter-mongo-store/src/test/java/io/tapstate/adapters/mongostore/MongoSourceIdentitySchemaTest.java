package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.SourceBindingIdentity;
import java.nio.charset.StandardCharsets;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoSourceIdentitySchemaTest {
    private static final String KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test void sourceHeaderMatchesItsAuthoredBsonGolden() throws Exception {
        var expected = new MongoSourceIdentity.Header(new SourceBindingIdentity("orders", "9a96a925-3365-4e32-936b-fec2054eb7be", 1), 0);
        Document golden = golden("source-identity-v1.golden.json");
        assertThat(MongoSourceIdentity.encode(expected)).isEqualTo(golden);
        assertThat(MongoSourceIdentity.read(new Document("_id", "orders").append("kind", "source")
                .append("sourceIdentity", golden))).isEqualTo(expected);
    }

    @Test void preparationMatchesItsAuthoredGoldenAndCannotGrowPastItsFixedBudget() throws Exception {
        var expected = new MongoSourceIdentitySchema.State(MongoSourceIdentitySchema.Phase.PREPARING,
                "source-upgrade-a", "cluster-a", KEY, "9a96a925-3365-4e32-936b-fec2054eb7be", 1);
        Document golden = golden("source-identity-preparation-v1.golden.json");
        assertThat(MongoSourceIdentitySchema.encode(expected)).isEqualTo(golden);
        assertThat(MongoSourceIdentitySchema.decode(golden, "cluster-a", KEY)).isEqualTo(expected);
        Document maximum = MongoSourceIdentitySchema.encode(new MongoSourceIdentitySchema.State(
                MongoSourceIdentitySchema.Phase.PREPARING, "o".repeat(256), "c".repeat(256), "k".repeat(128), "9a96a925-3365-4e32-936b-fec2054eb7be", Long.MAX_VALUE));
        assertThat(MongoSourceIdentityPreparation.size(new Document("sourceIdentity", maximum)))
                .isLessThanOrEqualTo(MongoSourceIdentitySchema.MAX_CONTROL_BYTES);
    }

    @Test void preparationRefusesUnknownFieldsVersionsNumericTypesAndAnotherOriginalIdentity() throws Exception {
        Document golden = golden("source-identity-preparation-v1.golden.json");
        for (Document bad : java.util.List.of(new Document(golden).append("extra", true),
                new Document(golden).append("installedVersion", 2), new Document(golden).append("preparedCount", 1),
                new Document(golden).append("phase", "UNKNOWN"), new Document(golden).append("lastSourceIncarnation", null))) {
            assertThatThrownBy(() -> MongoSourceIdentitySchema.decode(bad, "cluster-a", KEY)).isInstanceOf(TapstateException.class);
        }
        assertThatThrownBy(() -> MongoSourceIdentitySchema.decode(golden, "another-cluster", KEY)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> MongoSourceIdentitySchema.decode(golden, "cluster-a", "another-key")).isInstanceOf(TapstateException.class);
    }

    private static Document golden(String name) throws Exception {
        try (var input = MongoSourceIdentitySchemaTest.class.getResourceAsStream("/" + name)) {
            assertThat(input).isNotNull();
            return Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
}
