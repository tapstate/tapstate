package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.AlertTargetLimit;
import io.tapstate.core.alerting.AlertTestQuota;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;

/** Closed notification control records, with nanosecond facts and rounded-up index-only cleanup dates. */
final class MongoAlertNotificationCodec {
    static final int VERSION = 1;
    static final int MAX_CONTROL_BYTES = 16 * 1024;
    private static final Set<String> FIELDS = Set.of("_id", "formatVersion", "recordRevision", "cleanupAt", "body");
    record Quota(long recordRevision, AlertTestQuota.Bucket value) { }
    record Limit(long recordRevision, AlertTargetLimit value) { }
    private MongoAlertNotificationCodec() { }

    static Document quotaKey(AlarmKey.Scope scope, Instant minute) {
        return scope(scope).append("minute", minute.toString());
    }
    static Document limitKey(AlertTargetLimit.Domain domain) {
        return scope(domain.scope()).append("channelType", domain.channelType()).append("semanticTargetId", domain.semanticTargetId())
                .append("authenticationIdentity", domain.authenticationIdentity());
    }
    static Document quota(AlertTestQuota.Bucket bucket, long revision) {
        var actors = bucket.actors().entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(entry -> new Document("actor", entry.getKey()).append("accepted", entry.getValue())).toList();
        return envelope(quotaKey(bucket.scope(), bucket.minute()), revision, cleanup(bucket.cleanupAfter()), new Document("actors", actors));
    }
    static Document limit(AlertTargetLimit limit, long revision) {
        return envelope(limitKey(limit.domain()), revision, cleanup(plusHour(limit.notBefore())),
                new Document("notBefore", limit.notBefore().toString()));
    }
    static Quota readQuota(AlarmKey.Scope scope, Instant minute, Document row) {
        try {
            long revision = header(quotaKey(scope, minute), row);
            Document body = document(row.get("body")); fields(body, Set.of("actors"));
            require(body.get("actors") instanceof List<?>);
            List<?> actors = (List<?>) body.get("actors"); require(actors.size() <= AlertTestQuota.SCOPE_LIMIT);
            Map<String, Integer> counts = new LinkedHashMap<>();
            for (Object value : actors) {
                Document actor = document(value); fields(actor, Set.of("actor", "accepted"));
                require(actor.get("actor") instanceof String && actor.get("accepted") instanceof Integer);
                require(counts.putIfAbsent(actor.getString("actor"), actor.getInteger("accepted")) == null);
            }
            var bucket = new AlertTestQuota.Bucket(scope, minute, counts);
            require(cleanup(bucket.cleanupAfter()).equals(row.get("cleanupAt")));
            return new Quota(revision, bucket);
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException | java.time.DateTimeException | TapstateException invalid) {
            throw unreadable("quota-format-or-identity");
        }
    }
    static Limit readLimit(AlertTargetLimit.Domain domain, Document row) {
        try {
            long revision = header(limitKey(domain), row);
            Document body = document(row.get("body")); fields(body, Set.of("notBefore"));
            Instant notBefore = time(body.get("notBefore"));
            require(cleanup(plusHour(notBefore)).equals(row.get("cleanupAt")));
            return new Limit(revision, new AlertTargetLimit(domain, notBefore));
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException | java.time.DateTimeException | TapstateException invalid) {
            throw unreadable("limit-format-or-identity");
        }
    }
    private static Document envelope(Document key, long revision, Date cleanup, Document body) {
        if (revision <= 0) { throw new IllegalArgumentException("stored revision must be positive"); }
        var row = new Document("_id", key).append("formatVersion", VERSION).append("recordRevision", revision)
                .append("cleanupAt", cleanup).append("body", body);
        if (MongoAlertConfigurationCodec.size(row) > MAX_CONTROL_BYTES) { throw invalid("controlBytes"); }
        return row;
    }
    private static long header(Document key, Document row) {
        fields(row, FIELDS);
        require(MongoAlertConfigurationCodec.size(row) <= MAX_CONTROL_BYTES);
        require(key.equals(row.get("_id")) && Integer.valueOf(VERSION).equals(row.get("formatVersion")));
        require(row.get("recordRevision") instanceof Long revision && revision > 0);
        require(row.get("cleanupAt") instanceof Date);
        return row.getLong("recordRevision");
    }
    private static Document scope(AlarmKey.Scope value) {
        return new Document("authorityId", value.authorityId()).append("clusterId", value.clusterId());
    }
    private static Instant plusHour(Instant value) {
        try { return value.plusSeconds(3600); }
        catch (ArithmeticException | java.time.DateTimeException overflow) { throw invalid("cleanupAt"); }
    }
    private static Date cleanup(Instant value) {
        try {
            long millis = value.toEpochMilli();
            return new Date(value.getNano() % 1_000_000 == 0 ? millis : Math.incrementExact(millis));
        } catch (ArithmeticException overflow) { throw invalid("cleanupAt"); }
    }
    private static Instant time(Object value) {
        require(value instanceof String text && text.length() <= 40);
        Instant parsed = Instant.parse((String) value); require(parsed.toString().equals(value)); return parsed;
    }
    private static Document document(Object value) { require(value instanceof Document); return (Document) value; }
    private static void fields(Document row, Set<String> expected) { require(row != null && row.keySet().equals(expected)); }
    private static void require(boolean value) { if (!value) { throw new IllegalArgumentException("invalid notification control record"); } }
    private static TapstateException invalid(String field) {
        return new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", field, "reason", "outside-notification-storage-bound"), null);
    }
    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-notification", "field", field), null);
    }
}
