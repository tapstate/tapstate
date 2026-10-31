package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertStateRecord.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Instant;
import java.util.*;
import org.bson.Document;
import static io.tapstate.adapters.mongostore.MongoAlertDeliveryCodec.*;

/** Closed state metadata encoding. Legacy rows remain diagnostic-only and are never assigned missing policy facts. */
final class MongoAlertStateEnvelope {
    record Stored(long revision, Head value) { }
    private static final Set<String> HEAD_EXTRA = Set.of("deployment", "owner", "retiredAt", "summariesHandedOff", "cleanupAt",
            "severityRank", "recentSecondOrder", "recentNanoOrder", "binding");
    private static final Set<String> EPISODE_EXTRA = Set.of("originalBinding", "policy", "diagnostics", "summariesHandedOff",
            "historyUntil", "diagnosticUntil", "cleanupAt", "endedSecondOrder", "endedNanoOrder");
    private static final Set<String> TRANSITION_EXTRA = Set.of("endReason", "diagnosticStatus", "diagnosticReference", "policy", "cleanupAt");
    private MongoAlertStateEnvelope() { }
    static Document head(Head value, long revision) {
        Document row = MongoAlertStateCodec.head(value.snapshot(), revision);
        row.put("formatVersion", value.operationalEvidence() == null ? AlertStateRecord.FORMAT_VERSION : value.operationalEvidence().version()==1?3:4);
        row.append("deployment", value.deployment().name()).append("owner", owner(value.owner()))
                .append("retiredAt", time(value.retiredAt())).append("summariesHandedOff", value.summariesHandedOff())
                .append("cleanupAt", cleanup(value.cleanupAt())).append("binding", value.binding() == null ? null : member(value.binding()));
        if (value.operationalEvidence() != null) {
            var evidence = value.operationalEvidence();
            var encoded = new Document("version", evidence.version()).append("bindingDigest", evidence.bindingDigest())
                    .append("failedCapabilities", evidence.failedCapabilities());
            require(MongoAlertConfigurationCodec.size(new Document("operationalEvidence", encoded)) <= 192);
            row.append("operationalEvidence", encoded);
        }
        var snapshot = value.snapshot();
        Instant recent = snapshot.episode() == null ? snapshot.lastObservedAt() : snapshot.episode().firedAt();
        if (recent == null) { row.append("severityRank", null).append("recentSecondOrder", null).append("recentNanoOrder", null); }
        else { row.putAll(MongoAlertQueryProjection.recent(snapshot.episode() == null ? AlarmSeverity.INDETERMINATE : snapshot.episode().severity(), recent)); }
        require(MongoAlertConfigurationCodec.size(row) <= 16 * 1024); return row;
    }
    static Document episode(Episode value) {
        Document row = MongoAlertStateCodec.episode(value.originalBinding().key(), value.value());
        row.put("formatVersion", AlertStateRecord.FORMAT_VERSION);
        row.append("originalBinding", MongoAlertStateCodec.binding(value.originalBinding())).append("policy", policy(value.policy()))
                .append("diagnostics", diagnostics(value.diagnostics())).append("summariesHandedOff", value.summariesHandedOff())
                .append("historyUntil", time(value.historyUntil())).append("diagnosticUntil", time(value.diagnosticUntil()))
                .append("cleanupAt", cleanup(value.diagnostics().status() == DiagnosticStatus.CAPTURED && value.diagnosticUntil() != null
                        ? value.diagnosticUntil() : value.historyUntil()));
        row.putAll(MongoAlertQueryProjection.ended(value.value().endedAt()));
        require(MongoAlertConfigurationCodec.size(row) <= 48 * 1024); return row;
    }
    static Document transition(AlarmKey key, long revision, int ordinal, Transition value) {
        Document row = MongoAlertStateCodec.transition(key, revision, ordinal, value.value());
        row.put("formatVersion", AlertStateRecord.FORMAT_VERSION);
        return row.append("endReason", value.endReason() == null ? null : value.endReason().name())
                .append("diagnosticStatus", value.diagnosticStatus().name()).append("diagnosticReference", occurrence(value.diagnosticReference()))
                .append("policy", policy(value.policy())).append("cleanupAt", cleanup(value.cleanupAt()));
    }
    static Stored read(AlarmKey key, Document head, Document episode) {
        try {
            boolean operational = Integer.valueOf(3).equals(head.get("formatVersion")) || Integer.valueOf(4).equals(head.get("formatVersion"));
            Document compatibleHead = new Document(head);
            OperationalEvidence evidence = null;
            if (operational) {
                Document encoded = doc(head, "operationalEvidence");
                fields(encoded, Set.of("version", "bindingDigest", "failedCapabilities"));
                int expected=Integer.valueOf(4).equals(head.get("formatVersion"))?2:1;
                require(Integer.valueOf(expected).equals(encoded.get("version")) && encoded.get("failedCapabilities") instanceof Integer);
                evidence = new OperationalEvidence(text(encoded,"bindingDigest"), encoded.getInteger("failedCapabilities"),expected);
                compatibleHead.remove("operationalEvidence"); compatibleHead.put("formatVersion", AlertStateRecord.FORMAT_VERSION);
            }
            Document commonHead = legacy(compatibleHead, HEAD_EXTRA);
            Document commonEpisode = episode == null ? null : legacy(episode, EPISODE_EXTRA);
            var common = MongoAlertStateCodec.read(key, commonHead, commonEpisode);
            Episode detail = episode == null ? null : readEpisode(key, common.snapshot().episode().number(), episode);
            Head value = new Head(common.snapshot(), enumeration(AlertChannelPolicy.Deployment.values(), text(head, "deployment")),
                    readOwner(doc(head, "owner")), instant(head, "retiredAt", true), bool(head, "summariesHandedOff"), detail, head.get("binding") == null ? null : readMember(doc(head,"binding")), evidence);
            require(head.equals(head(value, common.recordRevision()))); return new Stored(common.recordRevision(), value);
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException | TapstateException invalid) { throw unreadable(); }
    }
    static Episode readEpisode(AlarmKey key, long number, Document row) {
        try {
            var common = MongoAlertStateCodec.readEpisode(key, number, legacy(row, EPISODE_EXTRA));
            Episode value = new Episode(MongoAlertStateCodec.readBinding(key, doc(row, "originalBinding")), common,
                    readPolicy(doc(row, "policy")), readDiagnostics(doc(row, "diagnostics")), bool(row, "summariesHandedOff"));
            require(row.equals(episode(value))); return value;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException | TapstateException invalid) { throw unreadable(); }
    }
    static Transition readTransition(AlarmKey key, long revision, int ordinal, Document row) {
        try {
            var common = MongoAlertStateCodec.readTransition(key, revision, ordinal, legacy(row, TRANSITION_EXTRA));
            Transition value = new Transition(common, row.get("endReason") == null ? null : enumeration(AlertSnapshot.EndReason.values(), text(row, "endReason")),
                    enumeration(DiagnosticStatus.values(), text(row, "diagnosticStatus")), row.get("diagnosticReference") == null ? null
                    : readOccurrence(doc(row, "diagnosticReference")), readPolicy(doc(row, "policy")));
            require(value.diagnosticReference() == null || value.diagnosticReference().bindingDigest().matches("[0-9a-f]{64}"));
            require(row.equals(transition(key, revision, ordinal, value))); return value;
        } catch (IllegalArgumentException | NullPointerException | java.time.DateTimeException | TapstateException invalid) { throw unreadable(); }
    }
    private static Document legacy(Document row, Set<String> extras) {
        require(row != null && Integer.valueOf(AlertStateRecord.FORMAT_VERSION).equals(row.get("formatVersion")) && row.keySet().containsAll(extras));
        Document copy = new Document(row); extras.forEach(copy::remove); copy.put("formatVersion", 1); return copy;
    }
    private static Document diagnostics(Diagnostics value) {
        Document row = new Document("status", value.status().name()).append("first", occurrence(value.first())).append("latest", occurrence(value.latest()))
                .append("firstTrace", value.history() == null ? null : trace(value.history().first().trace()))
                .append("latestTrace", value.history() == null || value.history().first().equals(value.history().latest()) ? null : trace(value.history().latest().trace()))
                .append("prunedAt", time(value.prunedAt()));
        require(MongoAlertConfigurationCodec.size(row) <= AlarmDiagnostics.MAX_ENCODED_PAIR_BYTES); return row;
    }
    private static Diagnostics readDiagnostics(Document row) {
        fields(row, Set.of("status", "first", "latest", "firstTrace", "latestTrace", "prunedAt"));
        var status = enumeration(DiagnosticStatus.values(), text(row, "status"));
        var first = row.get("first") == null ? null : readOccurrence(doc(row, "first"));
        var latest = row.get("latest") == null ? null : readOccurrence(doc(row, "latest"));
        AlarmDiagnostics.History history = null;
        if (status == DiagnosticStatus.CAPTURED) {
            var a = new AlarmDiagnostics.Capture(first, readTrace(doc(row, "firstTrace")));
            var b = row.get("latestTrace") == null ? a : new AlarmDiagnostics.Capture(latest, readTrace(doc(row, "latestTrace")));
            history = new AlarmDiagnostics.History(a, b);
        } else { require(row.get("firstTrace") == null && row.get("latestTrace") == null); }
        var value = new Diagnostics(status, history, first, latest, instant(row, "prunedAt", true));
        require(row.equals(diagnostics(value))); return value;
    }
    static Document occurrence(AlarmDiagnostics.Occurrence value) {
        return value == null ? null : new Document("bindingDigest", value.bindingDigest()).append("episode", value.episode())
                .append("sequence", value.sequence()).append("observedAt", time(value.observedAt()));
    }
    static AlarmDiagnostics.Occurrence readOccurrence(Document row) {
        fields(row, Set.of("bindingDigest", "episode", "sequence", "observedAt"));
        return new AlarmDiagnostics.Occurrence(text(row, "bindingDigest"), number(row, "episode", 1), number(row, "sequence", 0), instant(row, "observedAt", false));
    }
    private static Document trace(AlarmDiagnostics.SafeTrace value) {
        return new Document("text", value.text()).append("exceptionNodes", value.exceptionNodes()).append("redacted", value.redacted()).append("truncated", value.truncated());
    }
    private static AlarmDiagnostics.SafeTrace readTrace(Document row) {
        fields(row, Set.of("text", "exceptionNodes", "redacted", "truncated"));
        var value = new AlarmDiagnostics.SafeTrace(text(row, "text"), integer(row, "exceptionNodes", 1), bool(row, "redacted"), bool(row, "truncated"));
        require(row.equals(trace(value))); return value;
    }
    private static Date cleanup(Instant value) { return value == null ? null : MongoAlertNotificationCodec.cleanup(value); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-state", "field", "state-envelope"), null); }
}
