package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlertIntent;
import io.tapstate.core.alerting.AlarmSeverity;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import org.bson.Document;

/** Derived index candidates only; actual permission, pruning and reference checks remain mandatory. */
final class MongoAlertQueryProjection {
    private MongoAlertQueryProjection() { }
    static Document recent(AlarmSeverity severity, Instant at) {
        int rank = switch (severity) { case CRITICAL -> 0; case MAJOR -> 1; case MINOR -> 2; case WARNING -> 3; case INDETERMINATE -> 4; };
        return new Document("severityRank", rank).append("recentSecondOrder", -at.getEpochSecond()).append("recentNanoOrder", -at.getNano());
    }
    static Document ended(Instant at) {
        return new Document("endedSecondOrder", at == null ? null : -at.getEpochSecond()).append("endedNanoOrder", at == null ? null : -at.getNano());
    }
    static Date intentCheck(AlertIntent.Record value) {
        var s = value.state(); var frozen = value.frozen();
        if (s.retained().stoppedAt() != null) { return null; }
        var times = new ArrayList<Instant>();
        s.attempts().stream().filter(attempt -> attempt.endedAt() == null).forEach(attempt -> times.add(attempt.permit().deadline()));
        if (!s.permitsStopped() && s.nextAttemptAt() != null) {
            Instant next = s.nextAttemptAt();
            if (s.owner().generation() > 0 && s.owner().leaseUntil().isAfter(next)) { next = s.owner().leaseUntil(); }
            times.add(next);
        }
        times.add(frozen.deadline());
        return MongoAlertNotificationCodec.cleanup(times.stream().min(Comparator.naturalOrder()).orElseThrow());
    }
    static Date intentCleanup(AlertIntent.Record value) {
        var s = value.state(); var frozen = value.frozen(); var r = s.retained();
        List<Instant> candidates = new ArrayList<>();
        if (r.stoppedAt() == null) {
            Instant control = frozen.deadline();
            if (s.permitsStopped()) {
                control = s.permitsStoppedAt();
                for (var attempt : s.attempts()) {
                    Instant end = attempt.endedAt() == null ? attempt.permit().deadline() : attempt.endedAt();
                    if (end.isAfter(control)) { control = end; }
                }
            }
            candidates.add(control);
        }
        if (s.candidate() != null) { candidates.add(r.stoppedAt() == null ? frozen.deadline() : r.stoppedAt()); }
        if (s.payload() != null && r.stoppedAt() != null) { candidates.add(r.payloadUntil()); }
        candidates.add(r.resultUntil());
        return MongoAlertNotificationCodec.cleanup(candidates.stream().min(Comparator.naturalOrder()).orElseThrow());
    }
}
