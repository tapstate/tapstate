package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Bounded diagnostic values only; source capture, redaction, authorization and atomic storage belong to the host. */
public final class AlarmDiagnostics {
    private AlarmDiagnostics() { }
    public static final int CONTRACT_VERSION = 1;
    public static final int MAX_TEXT_BYTES = 16 * 1024;
    public static final int MAX_LINES = 256;
    public static final int MAX_EXCEPTION_NODES = 8;
    public static final int MAX_ENCODED_PAIR_BYTES = 36 * 1024;

    /** Input is already safe formatted text. This bounds output, not the work of capturing an arbitrary Throwable. */
    public record SafeTrace(String text, int exceptionNodes, boolean redacted, boolean truncated) {
        public SafeTrace {
            AlertValidation.require(exceptionNodes > 0 && exceptionNodes <= MAX_EXCEPTION_NODES,
                    "diagnosticNodes", "outside-capture-budget");
            Limited limited = limit(text);
            text = limited.text();
            truncated |= limited.truncated();
        }
    }
    public record Occurrence(String bindingDigest, long episode, long sequence, Instant observedAt) {
        public Occurrence {
            AlertValidation.require(bindingDigest != null && bindingDigest.matches("[0-9a-f]{64}"),
                    "diagnosticBinding", "invalid-digest");
            AlertValidation.require(episode > 0 && sequence >= 0, "diagnosticOccurrence", "invalid-sequence");
            Objects.requireNonNull(observedAt, "observedAt");
        }
    }
    public record Capture(Occurrence occurrence, SafeTrace trace) {
        public Capture { Objects.requireNonNull(occurrence, "occurrence"); Objects.requireNonNull(trace, "trace"); }
    }
    /** At most two captures; the first captured occurrence never changes and intermediate captures are not retained. */
    public record History(Capture first, Capture latest) {
        public History {
            Objects.requireNonNull(first, "first"); Objects.requireNonNull(latest, "latest");
            Occurrence a = first.occurrence(), b = latest.occurrence();
            AlertValidation.require(sameEpisode(a, b) && a.sequence() <= b.sequence()
                    && (a.sequence() == b.sequence() ? first.equals(latest) : a.observedAt().isBefore(b.observedAt())), "diagnosticHistory", "mixed-or-reordered-occurrences");
        }
    }

    /** A binding digest identifies the complete original evaluation domain; it is not a credential or authorization proof. */
    public static String bindingDigest(AlertRule rule) {
        Objects.requireNonNull(rule, "rule");
        AlarmKey key = rule.key();
        return DeliveryKey.fingerprint(List.of("alarm-diagnostic-v1", key.scope().authorityId(), key.scope().clusterId(),
                key.resource().kind(), key.resource().id(), key.resource().incarnation(), key.typeId(), key.qualifier(),
                Long.toString(rule.revision()), Long.toString(rule.generation()), rule.producerIncarnation()));
    }

    /** Only the just-accepted BAD observation may supply occurrence evidence. Computing a history is not durable acceptance. */
    public static Optional<History> attach(AlertSnapshot accepted, AlertSignal signal, SafeTrace trace, History previous) {
        Objects.requireNonNull(accepted, "accepted"); Objects.requireNonNull(signal, "signal"); Objects.requireNonNull(trace, "trace");
        AlertRule rule = accepted.rule();
        var episode = accepted.episode();
        if (!accepted.enabled() || accepted.status() != AlertSnapshot.Status.FIRING || accepted.quality() != AlertSnapshot.Quality.VALID
                || episode == null || episode.endedAt() != null || signal.condition() != AlertSignal.Condition.BAD
                || !rule.key().equals(signal.key()) || rule.revision() != signal.revision() || rule.generation() != signal.generation()
                || !rule.producerIncarnation().equals(signal.producerIncarnation()) || accepted.lastSequence() != signal.sequence()
                || !signal.observedAt().equals(accepted.lastObservedAt()) || signal.observedAt().isBefore(episode.firedAt())) {
            return Optional.empty();
        }
        Occurrence occurrence = new Occurrence(bindingDigest(rule), episode.number(), signal.sequence(), signal.observedAt());
        if (previous != null) {
            Occurrence latest = previous.latest().occurrence();
            if (!sameEpisode(latest, occurrence)) { throw new IllegalStateException("diagnostic lookup returned a foreign episode"); }
            if (latest.equals(occurrence)) { return Optional.of(previous); }
            if (occurrence.sequence() <= latest.sequence() || !occurrence.observedAt().isAfter(latest.observedAt())) { return Optional.empty(); }
        }
        Capture capture = new Capture(occurrence, trace);
        return Optional.of(previous == null ? new History(capture, capture) : new History(previous.first(), capture));
    }

    /** Uses the episode's frozen policy. This is a cutoff candidate, not permission to delete evidence. */
    public static Optional<Instant> cutoff(AlertRetentionPolicy frozenPolicy, AlertSnapshot.Episode episode) {
        Objects.requireNonNull(frozenPolicy, "frozenPolicy"); Objects.requireNonNull(episode, "episode");
        if (episode.endedAt() == null) { return Optional.empty(); }
        Duration traceAge = frozenPolicy.deployment() == AlertChannelPolicy.Deployment.CLOUD_MANAGED
                ? Duration.ofDays(1) : Duration.ofDays(7);
        Duration historyAge = frozenPolicy.duration(AlertRetentionPolicy.Setting.ENDED_EPISODE);
        return Optional.of(DeliveryRetention.add(episode.endedAt(), traceAge.compareTo(historyAge) < 0 ? traceAge : historyAge));
    }

    private static boolean sameEpisode(Occurrence a, Occurrence b) {
        return a.bindingDigest().equals(b.bindingDigest()) && a.episode() == b.episode();
    }
    private record Limited(String text, boolean truncated) { }
    private static Limited limit(String source) {
        AlertValidation.require(source != null && !source.isEmpty(), "diagnosticText", "required");
        StringBuilder output = new StringBuilder(Math.min(source.length(), MAX_TEXT_BYTES));
        int offset = 0, bytes = 0, completedLines = 0;
        while (offset < source.length() && completedLines < MAX_LINES) {
            char first = source.charAt(offset);
            AlertValidation.require(!Character.isLowSurrogate(first), "diagnosticText", "invalid-unicode");
            if (Character.isHighSurrogate(first)) {
                AlertValidation.require(offset + 1 < source.length() && Character.isLowSurrogate(source.charAt(offset + 1)),
                        "diagnosticText", "invalid-unicode");
            }
            int cp = source.codePointAt(offset), chars = Character.charCount(cp);
            AlertValidation.require(!Character.isISOControl(cp) || cp == '\t' || cp == '\r' || cp == '\n',
                    "diagnosticText", "control-character");
            boolean crlf = cp == '\r' && offset + 1 < source.length() && source.charAt(offset + 1) == '\n';
            int encoded = crlf ? 2 : cp <= 0x7f ? 1 : cp <= 0x7ff ? 2 : cp <= 0xffff ? 3 : 4;
            if (bytes + encoded > MAX_TEXT_BYTES) { break; }
            if (crlf) { chars = 2; }
            output.append(source, offset, offset + chars);
            bytes += encoded; offset += chars;
            if (cp == '\r' || cp == '\n' || cp == 0x2028 || cp == 0x2029) { completedLines++; }
        }
        return new Limited(output.toString(), offset < source.length());
    }
}
