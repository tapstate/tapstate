package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.List;

/** Cleanup changes content only after the original permission and evidence windows have closed. */
public final class AlertMaintenancePolicy {
    public static final long CAPACITY_BYTES = 512L * 1024 * 1024;
    private AlertMaintenancePolicy() { }
    public enum Pressure { NORMAL, WARNING, PAUSED, FULL }
    public record Capacity(Pressure pressure, int reliefSamples) {
        public Capacity { if (reliefSamples < 0 || reliefSamples > 1) throw new IllegalArgumentException("invalid capacity hysteresis"); }
        public static Capacity initial() { return new Capacity(Pressure.NORMAL, 0); }
        public Capacity sample(long logicalBytes) {
            if (logicalBytes < 0) throw new IllegalArgumentException("negative logical bytes");
            Pressure measured = logicalBytes >= CAPACITY_BYTES ? Pressure.FULL
                    : logicalBytes >= CAPACITY_BYTES * 9 / 10 ? Pressure.PAUSED
                    : logicalBytes >= CAPACITY_BYTES * 8 / 10 ? Pressure.WARNING : Pressure.NORMAL;
            if (measured.ordinal() >= pressure.ordinal()) return new Capacity(measured, 0);
            if (measured != Pressure.NORMAL) return new Capacity(pressure, 0);
            return reliefSamples == 0 ? new Capacity(pressure, 1) : initial();
        }
        public boolean admitsTestOrReminder() { return pressure.ordinal() < Pressure.PAUSED.ordinal(); }
        public boolean admitsNewFact() { return pressure != Pressure.FULL; }
    }
    public static AlertIntent.Record prune(AlertIntent.Record original, Instant now) {
        var value = original;
        var s = value.state();
        if (s.retained().stoppedAt() == null && (s.permitsStopped() || !now.isBefore(value.frozen().deadline())))
            value = AlertIntentControl.stop(value, AlertIntent.StopReason.EXPIRED, now);
        s = value.state();
        boolean settled = s.retained().stoppedAt() != null && s.attempts().stream().noneMatch(a -> a.endedAt() == null);
        boolean candidate = s.candidate() != null && settled;
        boolean payload = s.payload() != null && settled && !now.isBefore(s.retained().payloadUntil());
        boolean details = s.detailsPrunedAt() == null && settled && !now.isBefore(s.retained().resultUntil());
        if (!candidate && !payload && !details) return value;
        return new AlertIntent.Record(value.frozen(), new AlertIntent.State(s.owner(), s.manualCycles(), s.permitsStopped(),
                s.stopReason(), s.nextAttemptAt(), s.attemptsConsumed(), details ? List.of() : s.attempts(), s.summary(), s.retained(),
                payload ? null : s.payload(), candidate ? null : s.candidate(), payload ? now : s.payloadPrunedAt(),
                candidate ? now : s.candidatePrunedAt(), details ? now : s.detailsPrunedAt(), s.permitsStoppedAt(),
                s.lastAttemptEnd(), s.cycleStarts(), s.acceptedReceipt()));
    }
}
