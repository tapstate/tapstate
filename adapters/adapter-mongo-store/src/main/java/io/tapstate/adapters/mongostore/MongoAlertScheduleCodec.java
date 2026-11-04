package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertSchedule.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.bson.Document;
import static io.tapstate.adapters.mongostore.MongoAlertDeliveryCodec.*;

/** Explicit bounded scheduling progress; no delivery or history is inferred from an absent record. */
final class MongoAlertScheduleCodec {
    record Stored(long recordRevision, AlertSchedule.Record value) { }
    private static final Set<String> FIELDS = Set.of("_id", "formatVersion", "recordRevision", "route", "retention", "createdAt", "owner",
            "grouping", "clock", "batchSequence", "pending", "members", "batch", "disposition", "summaries", "protectedIntents", "retiredAt", "summariesHandedOff", "nextCheckAt", "cleanupAt");
    private MongoAlertScheduleCodec() { }
    static Document key(Key value) {
        return scope(value.scope()).append("subjectKind", value.kind().name()).append("subjectId", value.subjectId()).append("subjectIncarnation", value.subjectIncarnation())
                .append("routeId", value.routeId()).append("routeIncarnation", value.routeIncarnation()).append("channelType", value.channelType())
                .append("receiverIncarnation", value.receiverIncarnation()).append("targetId", value.targetId());
    }
    static Document write(AlertSchedule.Record value, long revision) {
        if (revision < 1) { throw new IllegalArgumentException("stored revision must be positive"); }
        var g = value.grouping(); var c = value.clock();
        Document row = new Document("_id", key(value.key())).append("formatVersion", AlertSchedule.FORMAT_VERSION).append("recordRevision", revision)
                .append("route", route(value.route())).append("retention", policy(value.retention())).append("createdAt", time(value.createdAt())).append("owner", owner(value.owner()))
                .append("grouping", new Document("initialNanos", g.initialWait().toNanos()).append("changeNanos", g.changeInterval().toNanos()).append("repeatNanos", g.repeatInterval().toNanos()))
                .append("clock", new Document("firstCandidateAt", time(c.firstCandidateAt())).append("lastPlannedAt", time(c.lastPlannedAt())).append("changed", c.changed())
                        .append("round", c.round()).append("reminderEligibleAt", time(c.reminderEligibleAt())))
                .append("batchSequence", value.batchSequence()).append("pending", members(value.pending())).append("members", members(value.members())).append("batch", batch(value.batch()))
                .append("disposition", disposition(value.disposition())).append("summaries", value.summaries().stream().map(MongoAlertScheduleCodec::summary).toList())
                .append("protectedIntents", value.protectedIntents().stream().map(MongoAlertDeliveryCodec::deliveryKey).toList())
                .append("retiredAt", time(value.retiredAt())).append("summariesHandedOff", value.summariesHandedOff())
                .append("nextCheckAt", MongoAlertQueryProjection.scheduleCheck(value)).append("cleanupAt", value.retiredCleanupAt() == null ? null : MongoAlertNotificationCodec.cleanup(value.retiredCleanupAt()));
        if (MongoAlertConfigurationCodec.size(row) > AlertSchedule.MAX_BYTES) { throw new TapstateException(AlertError.INVALID_CONFIGURATION,
                Map.of("field", "scheduleBytes", "reason", "encoded-schedule-too-large"), null); }
        return row;
    }
    static Stored read(Key expected, Document row) {
        try {
            fields(row, FIELDS); require(MongoAlertConfigurationCodec.size(row) <= AlertSchedule.MAX_BYTES);
            sameKey(key(expected), doc(row, "_id")); require(Integer.valueOf(AlertSchedule.FORMAT_VERSION).equals(row.get("formatVersion")));
            Document g = doc(row, "grouping"); fields(g, Set.of("initialNanos", "changeNanos", "repeatNanos"));
            Document c = doc(row, "clock"); fields(c, Set.of("firstCandidateAt", "lastPlannedAt", "changed", "round", "reminderEligibleAt"));
            List<Previous> summaries = new ArrayList<>();
            for (Object item : list(row, "summaries", 100)) { summaries.add(readSummary(document(item))); }
            List<DeliveryKey> refs = new ArrayList<>(); for (Object item : list(row, "protectedIntents", 100)) { refs.add(readDeliveryKey(document(item))); }
            var value = new AlertSchedule.Record(expected, readRoute(doc(row, "route")), readPolicy(doc(row, "retention")), instant(row, "createdAt", false), readOwner(doc(row, "owner")),
                    new AlertGroupingClock.Policy(Duration.ofNanos(number(g, "initialNanos", 0)), Duration.ofNanos(number(g, "changeNanos", 1)), Duration.ofNanos(number(g, "repeatNanos", 1))),
                    new AlertGroupingClock(instant(c, "firstCandidateAt", true), instant(c, "lastPlannedAt", true), bool(c, "changed"), number(c, "round", 0), instant(c, "reminderEligibleAt", true)),
                    number(row, "batchSequence", 0), readMembers(row, "pending"), row.get("batch") == null ? null : readBatch(doc(row, "batch")),
                    row.get("disposition") == null ? null : readDisposition(doc(row, "disposition")), summaries, refs, instant(row, "retiredAt", true), bool(row, "summariesHandedOff"), readMembers(row, "members"));
            require(Objects.equals(MongoAlertQueryProjection.scheduleCheck(value), row.get("nextCheckAt"))
                    && Objects.equals(value.retiredCleanupAt() == null ? null : MongoAlertNotificationCodec.cleanup(value.retiredCleanupAt()), row.get("cleanupAt")));
            return new Stored(number(row, "recordRevision", 1), value);
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException | java.time.DateTimeException | TapstateException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-schedule", "field", "format-or-identity"), null);
        }
    }
    private static List<Document> members(List<AlertIntent.Member> values) { return values.stream().map(MongoAlertDeliveryCodec::member).toList(); }
    private static List<AlertIntent.Member> readMembers(Document row, String name) {
        List<AlertIntent.Member> values = new ArrayList<>(); for (Object item : list(row, name, 100)) { values.add(readMember(document(item))); } return values;
    }
    private static Document batch(Batch value) {
        return value == null ? null : new Document("delivery", deliveryKey(value.delivery())).append("revision", value.revision()).append("members", members(value.members()));
    }
    private static Batch readBatch(Document value) {
        fields(value, Set.of("delivery", "revision", "members")); return new Batch(readDeliveryKey(doc(value, "delivery")), number(value, "revision", 1), readMembers(value, "members"));
    }
    private static Document disposition(Disposition value) {
        return value == null ? null : new Document("original", deliveryKey(value.original())).append("originalRevision", value.originalRevision()).append("action", value.action().name())
                .append("pending", members(value.pending())).append("successor", value.successor() == null ? null : deliveryKey(value.successor()))
                .append("recheckAt", time(value.recheckAt())).append("previousOutcome", value.previousOutcome().name());
    }
    private static Disposition readDisposition(Document value) {
        fields(value, Set.of("original", "originalRevision", "action", "pending", "successor", "recheckAt", "previousOutcome"));
        return new Disposition(readDeliveryKey(doc(value, "original")), number(value, "originalRevision", 1), enumeration(FrozenGroupPolicy.Action.values(), text(value, "action")),
                readMembers(value, "pending"), value.get("successor") == null ? null : readDeliveryKey(doc(value, "successor")), instant(value, "recheckAt", true),
                enumeration(DeliveryEvidence.Outcome.values(), text(value, "previousOutcome")));
    }
    private static Document summary(Previous value) {
        return new Document("alarm", MongoAlertStateCodec.key(value.alarm())).append("episode", value.episode()).append("outcome", value.outcome().name())
                .append("renderingFailed", value.renderingFailed());
    }
    private static Previous readSummary(Document value) {
        fields(value, Set.of("alarm", "episode", "outcome", "renderingFailed"));
        return new Previous(MongoAlertStateCodec.readKey(doc(value, "alarm")), number(value, "episode", 1), enumeration(DeliveryEvidence.Outcome.values(), text(value, "outcome")),
                bool(value, "renderingFailed"));
    }
}
