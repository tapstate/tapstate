package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.Objects;

/** Fixed-minute arithmetic, never a local quota authority. Counts and the admitted test must commit in one conditional transaction. */
public final class AlertTestQuota {
    private AlertTestQuota() { }
    public static final int ACTOR_LIMIT = 3;
    public static final int SCOPE_LIMIT = 10;
    public enum Refusal { NONE, ACTOR_LIMIT, SCOPE_LIMIT, RECEIVER_BUSY }
    public record Window(AlarmKey.Scope scope, String actorPrincipal, Instant minute, int actorAccepted, int scopeAccepted) {
        public Window {
            Objects.requireNonNull(scope, "scope"); actorPrincipal = AlertValidation.text(actorPrincipal, "actorPrincipal", 256);
            AlertValidation.require(minute != null && minute.getNano() == 0 && Math.floorMod(minute.getEpochSecond(), 60) == 0,
                    "quotaMinute", "not-fixed-minute");
            AlertValidation.require(actorAccepted >= 0 && scopeAccepted >= actorAccepted, "quotaCounts", "invalid-counts");
        }
    }
    public record ReceiverBusy(AlarmKey.Scope scope, String receiverIncarnation, Instant originalDeadline) {
        public ReceiverBusy {
            Objects.requireNonNull(scope, "scope"); receiverIncarnation = AlertValidation.text(receiverIncarnation, "receiverIncarnation", 256);
            Objects.requireNonNull(originalDeadline, "originalDeadline");
        }
    }
    public record Decision(boolean allowed, Window candidate, Refusal refusal, Instant retryAt) { }
    public static Instant minute(Instant now) {
        return Instant.ofEpochSecond(Math.floorDiv(now.getEpochSecond(), 60) * 60);
    }
    public static Window empty(AlertTestCommand.Key key, Instant now) {
        return new Window(key.scope(), key.actorPrincipal(), minute(now), 0, 0);
    }
    /** A replay never consumes quota or waits for a receiver. A busy snapshot belongs to this original receiver, not its mutable name. */
    public static Decision evaluate(AlertTestCommand.Admission admission, Instant now, Window current, ReceiverBusy busy) {
        Objects.requireNonNull(admission, "admission"); Objects.requireNonNull(now, "now"); Objects.requireNonNull(current, "current");
        var key = admission.command().key();
        AlertValidation.require(current.scope().equals(key.scope()) && current.actorPrincipal().equals(key.actorPrincipal())
                && current.minute().equals(minute(now)), "quotaWindow", "foreign-or-stale-window");
        if (admission.replay()) { return new Decision(true, current, Refusal.NONE, null); }
        AlertValidation.require(admission.command().acceptedAt().equals(now), "quotaTime", "must-share-admission-time");
        if (busy != null) {
            AlertValidation.require(busy.scope().equals(key.scope()) && busy.receiverIncarnation().equals(admission.command().parameters().receiverIncarnation()),
                    "receiverQuota", "foreign-receiver");
        }
        Refusal refusal = current.scopeAccepted() >= SCOPE_LIMIT ? Refusal.SCOPE_LIMIT
                : current.actorAccepted() >= ACTOR_LIMIT ? Refusal.ACTOR_LIMIT : Refusal.NONE;
        Instant retryAt = refusal == Refusal.NONE ? null : current.minute().plusSeconds(60);
        if (busy != null) {
            retryAt = retryAt == null || busy.originalDeadline().isAfter(retryAt) ? busy.originalDeadline() : retryAt;
            if (refusal == Refusal.NONE) { refusal = Refusal.RECEIVER_BUSY; }
        }
        if (refusal != Refusal.NONE) { return new Decision(false, current, refusal, retryAt); }
        return new Decision(true, new Window(current.scope(), current.actorPrincipal(), current.minute(),
                current.actorAccepted() + 1, current.scopeAccepted() + 1), Refusal.NONE, null);
    }
}
