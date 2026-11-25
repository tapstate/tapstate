package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertTargetLimit;
import io.tapstate.core.alerting.AlertTestQuota;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoAlertNotificationCodecTest {
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org-a", "cluster-a");
    static final Instant MINUTE = Instant.parse("2026-10-27T00:00:00Z");
    static final AlertTargetLimit.Domain DOMAIN = new AlertTargetLimit.Domain(SCOPE, "webhook", "target-a", "auth-a");

    @Test void closedControlShapesMatchAuthoredGoldenAndRoundTripRealBson() throws Exception {
        var quota = new AlertTestQuota.Bucket(SCOPE, MINUTE, Map.of("actor-a", 1, "actor-b", 2));
        var limit = new AlertTargetLimit(DOMAIN, MINUTE.plusSeconds(1).plusNanos(1));
        var actual = new Document("quota", MongoAlertNotificationCodec.quota(quota, 7))
                .append("limit", MongoAlertNotificationCodec.limit(limit, 3));
        try (var input = getClass().getResourceAsStream("/alert-notification-control-v1.golden.json")) {
            assertThat(input).isNotNull();
            assertThat(actual).isEqualTo(Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8)));
        }
        assertThat(MongoAlertNotificationCodec.readQuota(SCOPE, MINUTE, bson(actual.get("quota", Document.class))))
                .isEqualTo(new MongoAlertNotificationCodec.Quota(7, quota));
        assertThat(MongoAlertNotificationCodec.readLimit(DOMAIN, bson(actual.get("limit", Document.class))))
                .isEqualTo(new MongoAlertNotificationCodec.Limit(3, limit));
        assertThat(actual.get("limit", Document.class).getDate("cleanupAt").toInstant())
                .isEqualTo(MINUTE.plusSeconds(3601).plusMillis(1));
    }

    @Test void missingFutureMalformedOrForeignQuotaFactsAreRefusedWithoutDefaulting() {
        List<Consumer<Document>> corruptions = List.of(row -> row.put("formatVersion", 2), row -> row.put("formatVersion", 1L),
                row -> row.put("recordRevision", 1), row -> row.put("recordRevision", 0L), row -> row.remove("body"),
                row -> row.get("_id", Document.class).put("authorityId", "foreign"),
                row -> row.get("body", Document.class).put("extra", 0),
                row -> row.get("body", Document.class).getList("actors", Document.class).getFirst().put("accepted", 1L),
                row -> row.get("body", Document.class).getList("actors", Document.class).getFirst().put("accepted", 4),
                row -> row.get("body", Document.class).getList("actors", Document.class).add(new Document("actor", "a").append("accepted", 1)),
                row -> row.put("cleanupAt", java.util.Date.from(MINUTE)));
        for (var corrupt : corruptions) {
            Document row = bson(MongoAlertNotificationCodec.quota(new AlertTestQuota.Bucket(SCOPE, MINUTE, Map.of("a", 1)), 1));
            corrupt.accept(row); assertUnreadable(() -> MongoAlertNotificationCodec.readQuota(SCOPE, MINUTE, row));
        }
    }

    @Test void targetCleanupNeverRoundsEarlierThanTheOriginalNanosecondRestriction() {
        for (Instant at : List.of(MINUTE, MINUTE.plusNanos(1), Instant.EPOCH.minusNanos(1))) {
            var value = new AlertTargetLimit(DOMAIN, at);
            Document row = bson(MongoAlertNotificationCodec.limit(value, 1));
            assertThat(row.getDate("cleanupAt").toInstant()).isAfterOrEqualTo(at.plusSeconds(3600));
            assertThat(MongoAlertNotificationCodec.readLimit(DOMAIN, row).value()).isEqualTo(value);
            row.get("body", Document.class).put("notBefore", at.toString() + "invalid");
            assertUnreadable(() -> MongoAlertNotificationCodec.readLimit(DOMAIN, row));
        }
    }

    @Test void maximumLegalUnicodeIdentitiesAndTenActorsFitTheFixedControlEnvelope() {
        String text = new String(Character.toChars(0x1f680)).repeat(256);
        var scope = new AlarmKey.Scope(text, text);
        var counts = new java.util.HashMap<String, Integer>();
        for (int i = 0; i < 10; i++) { counts.put(text.substring(0, text.length() - 2) + i, 1); }
        var value = new AlertTestQuota.Bucket(scope, MINUTE, counts);
        Document quota = MongoAlertNotificationCodec.quota(value, 1);
        assertThat(MongoAlertConfigurationCodec.size(quota)).isLessThanOrEqualTo(MongoAlertNotificationCodec.MAX_CONTROL_BYTES);
        assertThat(MongoAlertNotificationCodec.readQuota(scope, MINUTE, bson(quota)).value()).isEqualTo(value);
    }

    private static Document bson(Document document) {
        try (BasicOutputBuffer out = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(out)) {
            new DocumentCodec().encode(writer, document, EncoderContext.builder().build());
            try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(out.toByteArray()))) {
                return new DocumentCodec().decode(reader, DecoderContext.builder().build());
            }
        }
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
