package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static io.tapstate.core.alerting.AlertSnapshot.*;
import static io.tapstate.core.alerting.AlertSignal.Condition;
import static io.tapstate.core.alerting.AlertUpdate.Kind;

/** Deterministic lifecycle calculations; storage CAS, authorization and dispatch belong to the host. */
public final class AlertLifecycle {
    private AlertLifecycle() { }

    public static AlertUpdate evaluate(AlertSnapshot old, AlertSignal signal, Instant now) {
        Objects.requireNonNull(now, "now");
        AlertRule rule = old.rule();
        if (!old.enabled() || !rule.key().equals(signal.key()) || rule.revision() != signal.revision()
                || rule.generation() != signal.generation()
                || !rule.producerIncarnation().equals(signal.producerIncarnation())
                || signal.sequence() <= old.lastSequence()
                || (old.lastObservedAt() != null && !signal.observedAt().isAfter(old.lastObservedAt()))) {
            return new AlertUpdate(old, List.of(), true);
        }
        Quality quality = signal.observedAt().isAfter(now) ? Quality.CLOCK_INVALID
                : signal.observedAt().plus(rule.maxAge()).isBefore(now) ? Quality.STALE
                : signal.condition() == Condition.UNKNOWN ? Quality.UNKNOWN : Quality.VALID;
        // Invalid timestamps cannot move the frontier and poison all subsequent legitimate samples.
        if (quality == Quality.CLOCK_INVALID || quality == Quality.STALE) {
            return uncertain(old, quality, now);
        }
        boolean gap = old.lastObservedAt() == null
                || old.lastSequence() == Long.MAX_VALUE || signal.sequence() != old.lastSequence() + 1
                || old.lastObservedAt().plus(rule.maxGap()).isBefore(signal.observedAt())
                || old.quality() != Quality.VALID;
        Status status = old.status();
        Instant pending = gap ? null : old.pendingSince();
        Instant recovery = gap ? null : old.recoverySince();
        long number = old.episodeSequence();
        Episode episode = old.episode();
        List<AlertUpdate.Transition> transitions = new ArrayList<>();
        if (quality != old.quality()) { add(transitions, Kind.QUALITY_CHANGED, now, number, signal.severity()); }
        if (quality != Quality.VALID) {
            pending = null;
            recovery = null;
            if (status == Status.PENDING) { status = Status.CLEAR; }
        } else if (signal.condition() == Condition.BAD) {
            recovery = null;
            if (status != Status.FIRING) {
                if (pending == null) { pending = signal.observedAt(); }
                if (!signal.observedAt().isBefore(pending.plus(rule.triggerFor()))) {
                    number = Math.incrementExact(number);
                    episode = new Episode(number, signal.observedAt(), signal.severity(), null, null, null);
                    status = Status.FIRING;
                    pending = null;
                    add(transitions, Kind.FIRING, signal.observedAt(), number, signal.severity());
                } else {
                    if (status != Status.PENDING) { add(transitions, Kind.PENDING, signal.observedAt(), number, signal.severity()); }
                    status = Status.PENDING;
                }
            } else if (episode.severity() != signal.severity()) {
                episode = new Episode(number, episode.firedAt(), signal.severity(), null, null, null);
                add(transitions, Kind.SEVERITY_CHANGED, signal.observedAt(), number, signal.severity());
            }
        } else {
            pending = null;
            if (status == Status.FIRING) {
                if (recovery == null) { recovery = signal.observedAt(); }
                if (!signal.observedAt().isBefore(recovery.plus(rule.resolveFor()))) {
                    episode = new Episode(number, episode.firedAt(), episode.severity(), signal.observedAt(),
                            signal.observedAt(), EndReason.RECOVERED);
                    status = Status.RESOLVED;
                    recovery = null;
                    add(transitions, Kind.RESOLVED, signal.observedAt(), number, episode.severity());
                }
            } else {
                recovery = null;
                if (status == Status.PENDING) { status = Status.CLEAR; }
            }
        }
        return new AlertUpdate(new AlertSnapshot(rule, true, status, quality, signal.sequence(),
                signal.observedAt(), pending, recovery, number, episode), transitions, false);
    }

    /** An independent tick detects missing callbacks; it never advances a trigger/recovery window. */
    public static AlertUpdate tick(AlertSnapshot old, Instant now) {
        Objects.requireNonNull(now, "now");
        if (!old.enabled()) { return new AlertUpdate(old, List.of(), true); }
        if (old.lastObservedAt() != null && old.lastObservedAt().isAfter(now)) {
            return uncertain(old, Quality.CLOCK_INVALID, now);
        }
        if (old.lastObservedAt() == null || old.lastObservedAt().plus(old.rule().maxAge()).isBefore(now)) {
            return uncertain(old, Quality.STALE, now);
        }
        return new AlertUpdate(old, List.of(), false);
    }

    /** Current-producer input was refused or lost. Never advance its accepted sample frontier. */
    public static AlertUpdate discontinuity(AlertSnapshot old, Instant now) {
        Objects.requireNonNull(old, "old"); Objects.requireNonNull(now, "now");
        return old.enabled() ? uncertain(old, Quality.UNKNOWN, now) : new AlertUpdate(old, List.of(), true);
    }

    public static AlertUpdate terminate(AlertSnapshot old, EndReason reason, Instant now) {
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(now, "now");
        if (reason == EndReason.RECOVERED) { throw new IllegalArgumentException("recovery requires current evidence"); }
        Episode episode = old.episode();
        List<AlertUpdate.Transition> transitions = List.of();
        if (episode != null && episode.endedAt() == null) {
            if (now.isBefore(episode.firedAt())) { throw stale("terminate"); }
            episode = new Episode(episode.number(), episode.firedAt(), episode.severity(), now, null, reason);
            transitions = List.of(new AlertUpdate.Transition(Kind.ADMINISTRATIVELY_ENDED, now, episode.number(), episode.severity()));
        }
        return new AlertUpdate(new AlertSnapshot(old.rule(), false, Status.CLEAR, Quality.DISABLED,
                old.lastSequence(), old.lastObservedAt(), null, null, old.episodeSequence(), episode), transitions, false);
    }

    public static AlertUpdate replace(AlertSnapshot old, AlertRule replacement, Instant now) {
        if (!old.rule().key().equals(replacement.key()) || replacement.generation() <= old.rule().generation()) {
            throw stale("replace");
        }
        AlertUpdate ended = terminate(old, EndReason.SUPERSEDED, now);
        return new AlertUpdate(new AlertSnapshot(replacement, true, Status.CLEAR, Quality.UNKNOWN,
                -1, null, null, null, old.episodeSequence(), ended.snapshot().episode()), ended.transitions(), false);
    }

    private static AlertUpdate uncertain(AlertSnapshot old, Quality quality, Instant now) {
        AlertSnapshot state = new AlertSnapshot(old.rule(), old.enabled(),
                old.status() == Status.PENDING ? Status.CLEAR : old.status(), quality, old.lastSequence(),
                old.lastObservedAt(), null, null, old.episodeSequence(), old.episode());
        List<AlertUpdate.Transition> transitions = quality == old.quality() ? List.of()
                : List.of(new AlertUpdate.Transition(Kind.QUALITY_CHANGED, now, old.episodeSequence(),
                        old.episode() == null ? AlarmSeverity.INDETERMINATE : old.episode().severity()));
        return new AlertUpdate(state, transitions, false);
    }
    private static void add(List<AlertUpdate.Transition> changes, Kind kind, Instant at, long episode, AlarmSeverity severity) {
        changes.add(new AlertUpdate.Transition(kind, at, episode, severity));
    }
    private static TapstateException stale(String action) {
        return new TapstateException(AlertError.STALE_ACTION, Map.of("action", action), null);
    }
}
