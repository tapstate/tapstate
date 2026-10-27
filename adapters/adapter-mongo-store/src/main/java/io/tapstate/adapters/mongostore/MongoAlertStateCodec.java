package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlarmSeverity;
import io.tapstate.core.alerting.AlertRule;
import io.tapstate.core.alerting.AlertSnapshot;
import io.tapstate.core.alerting.AlertUpdate;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.bson.Document;

/** Explicit CONFIG state codecs. The current head keeps its evaluator binding and references an episode. */
final class MongoAlertStateCodec {
    static final int VERSION = 1;
    private static final Set<String> KEY_FIELDS = Set.of("authorityId", "clusterId", "resourceKind",
            "resourceId", "resourceIncarnation", "typeId", "qualifier");
    private static final Set<String> BINDING_FIELDS = Set.of("ruleRevision",
            "generation", "producerIncarnation", "triggerNanos", "resolveNanos", "maxAgeNanos", "maxGapNanos");
    private static final Set<String> HEAD_FIELDS = Set.of("_id", "formatVersion", "recordRevision",
            "evaluationBinding", "enabled", "status", "quality", "lastSequence", "lastObservedAt",
            "pendingSince", "recoverySince", "episodeSequence", "episodeNumber");
    private static final Set<String> EPISODE_FIELDS = Set.of("_id", "formatVersion", "firedAt", "severity",
            "endedAt", "recoveredAt", "endReason");
    private static final Set<String> TRANSITION_FIELDS = Set.of("_id", "formatVersion", "kind", "at",
            "episodeNumber", "severity");

    record State(long recordRevision, AlertSnapshot snapshot) { }

    private MongoAlertStateCodec() { }

    static Document key(AlarmKey key) {
        Objects.requireNonNull(key, "key");
        return new Document("authorityId", key.scope().authorityId()).append("clusterId", key.scope().clusterId())
                .append("resourceKind", key.resource().kind()).append("resourceId", key.resource().id())
                .append("resourceIncarnation", key.resource().incarnation()).append("typeId", key.typeId())
                .append("qualifier", key.qualifier());
    }

    private static Document binding(AlertRule rule) {
        return new Document("ruleRevision", rule.revision()).append("generation", rule.generation())
                .append("producerIncarnation", rule.producerIncarnation()).append("triggerNanos", rule.triggerFor().toNanos())
                .append("resolveNanos", rule.resolveFor().toNanos()).append("maxAgeNanos", rule.maxAge().toNanos())
                .append("maxGapNanos", rule.maxGap().toNanos());
    }

    static Document head(AlertSnapshot snapshot, long recordRevision) {
        if (recordRevision < 1 || recordRevision == Long.MAX_VALUE) {
            throw new IllegalArgumentException("record revision must be positive and incrementable");
        }
        return header(key(snapshot.rule().key())).append("recordRevision", recordRevision)
                .append("evaluationBinding", binding(snapshot.rule()))
                .append("enabled", snapshot.enabled()).append("status", snapshot.status().name())
                .append("quality", snapshot.quality().name()).append("lastSequence", snapshot.lastSequence())
                .append("lastObservedAt", time(snapshot.lastObservedAt())).append("pendingSince", time(snapshot.pendingSince()))
                .append("recoverySince", time(snapshot.recoverySince())).append("episodeSequence", snapshot.episodeSequence())
                .append("episodeNumber", snapshot.episode() == null ? null : snapshot.episode().number());
    }

    static Document episode(AlarmKey key, AlertSnapshot.Episode episode) {
        return header(new Document("alarm", key(key)).append("number", episode.number()))
                .append("firedAt", time(episode.firedAt())).append("severity", episode.severity().name())
                .append("endedAt", time(episode.endedAt())).append("recoveredAt", time(episode.recoveredAt()))
                .append("endReason", episode.endReason() == null ? null : episode.endReason().name());
    }

    static Document transition(AlarmKey key, long recordRevision, int ordinal, AlertUpdate.Transition transition) {
        if (recordRevision < 1 || ordinal < 0) { throw new IllegalArgumentException("invalid transition identity"); }
        return header(new Document("alarm", key(key)).append("recordRevision", recordRevision).append("ordinal", ordinal))
                .append("kind", transition.kind().name()).append("at", time(transition.at()))
                .append("episodeNumber", transition.episode()).append("severity", transition.severity().name());
    }

    /** Both rows must come from the same CONFIG transaction snapshot. Missing history is never recreated. */
    static State read(AlarmKey expected, Document head, Document episode) {
        try {
            fields(head, HEAD_FIELDS);
            sameKey(expected, document(head, "_id"));
            Document rule = document(head, "evaluationBinding");
            require(rule.keySet().equals(BINDING_FIELDS));
            long revision = number(head, "recordRevision", 1);
            require(revision < Long.MAX_VALUE);
            AlertRule binding = new AlertRule(expected, number(rule, "ruleRevision", 1), number(rule, "generation", 1),
                    text(rule, "producerIncarnation", 256), Duration.ofNanos(number(rule, "triggerNanos", 0)),
                    Duration.ofNanos(number(rule, "resolveNanos", 0)), Duration.ofNanos(number(rule, "maxAgeNanos", 1)),
                    Duration.ofNanos(number(rule, "maxGapNanos", 1)));
            long sequence = number(head, "episodeSequence", 0);
            AlertSnapshot.Episode current = null;
            if (head.get("episodeNumber") != null) {
                long episodeNumber = number(head, "episodeNumber", 1);
                require(episodeNumber == sequence);
                current = readEpisode(expected, episodeNumber, episode);
            } else { require(episode == null); }
            AlertSnapshot snapshot = new AlertSnapshot(binding, bool(head, "enabled"), status(text(head, "status", 16)),
                    quality(text(head, "quality", 16)), number(head, "lastSequence", -1), instant(head, "lastObservedAt", true),
                    instant(head, "pendingSince", true), instant(head, "recoverySince", true), sequence, current);
            require(snapshot.status() != AlertSnapshot.Status.FIRING || current != null && current.endedAt() == null);
            require(snapshot.status() != AlertSnapshot.Status.RESOLVED || current == null || current.recoveredAt() != null);
            require(snapshot.status() != AlertSnapshot.Status.RESOLVED || sequence > 0 && snapshot.lastObservedAt() != null);
            require(snapshot.status() != AlertSnapshot.Status.RESOLVED || current == null
                    || !snapshot.lastObservedAt().isBefore(current.recoveredAt()));
            require(snapshot.status() == AlertSnapshot.Status.FIRING || current == null || current.endedAt() != null);
            if (current != null && current.endedAt() == null) {
                require(snapshot.lastObservedAt() != null && !snapshot.lastObservedAt().isBefore(current.firedAt()));
                require(snapshot.recoverySince() == null || !snapshot.recoverySince().isBefore(current.firedAt()));
            }
            require((snapshot.lastSequence() == -1) == (snapshot.lastObservedAt() == null));
            require(snapshot.enabled() || snapshot.status() == AlertSnapshot.Status.CLEAR
                    && snapshot.quality() == AlertSnapshot.Quality.DISABLED);
            require((snapshot.status() == AlertSnapshot.Status.PENDING) == (snapshot.pendingSince() != null));
            require(snapshot.recoverySince() == null || snapshot.status() == AlertSnapshot.Status.FIRING);
            require(snapshot.quality() == AlertSnapshot.Quality.VALID
                    || snapshot.pendingSince() == null && snapshot.recoverySince() == null);
            for (Instant window : new Instant[] {snapshot.pendingSince(), snapshot.recoverySince()}) {
                require(window == null || snapshot.lastObservedAt() != null && !window.isAfter(snapshot.lastObservedAt()));
            }
            return new State(revision, snapshot);
        } catch (TapstateException | IllegalArgumentException | NullPointerException invalid) {
            throw unreadable();
        }
    }

    static AlertSnapshot.Episode readEpisode(AlarmKey expected, long number, Document row) {
        try {
            fields(row, EPISODE_FIELDS);
            Document id = document(row, "_id"); require(List.copyOf(id.keySet()).equals(List.of("alarm", "number")));
            sameKey(expected, document(id, "alarm")); require(number(id, "number", 1) == number);
            var episode = new AlertSnapshot.Episode(number, instant(row, "firedAt", false), severity(text(row, "severity", 16)),
                    instant(row, "endedAt", true), instant(row, "recoveredAt", true),
                    row.get("endReason") == null ? null : reason(text(row, "endReason", 24)));
            require(episode.endedAt() == null || !episode.endedAt().isBefore(episode.firedAt()));
            require(episode.recoveredAt() == null || episode.recoveredAt().equals(episode.endedAt()));
            return episode;
        } catch (TapstateException | IllegalArgumentException | NullPointerException invalid) { throw unreadable(); }
    }

    static AlertUpdate.Transition readTransition(AlarmKey expected, long revision, int ordinal, Document row) {
        try {
            fields(row, TRANSITION_FIELDS);
            Document id = document(row, "_id");
            require(List.copyOf(id.keySet()).equals(List.of("alarm", "recordRevision", "ordinal")));
            sameKey(expected, document(id, "alarm")); require(number(id, "recordRevision", 1) == revision);
            require(id.get("ordinal") instanceof Integer index && index == ordinal && ordinal >= 0);
            return new AlertUpdate.Transition(kind(text(row, "kind", 32)), instant(row, "at", false),
                    number(row, "episodeNumber", 0), severity(text(row, "severity", 16)));
        } catch (TapstateException | IllegalArgumentException | NullPointerException invalid) { throw unreadable(); }
    }

    private static Document header(Document id) { return new Document("_id", id).append("formatVersion", VERSION); }
    private static String time(Instant value) { return value == null ? null : value.toString(); }
    private static void fields(Document row, Set<String> fields) {
        require(row != null && row.keySet().equals(fields));
        require(row.get("formatVersion") instanceof Integer version && version == VERSION);
    }
    private static void sameKey(AlarmKey expected, Document row) {
        Document canonical = key(expected);
        require(row.keySet().equals(KEY_FIELDS) && row.equals(canonical)
                && List.copyOf(row.keySet()).equals(List.copyOf(canonical.keySet())));
    }
    private static Document document(Document row, String name) {
        require(row.get(name) instanceof Document); return (Document) row.get(name);
    }
    private static long number(Document row, String name, long minimum) {
        require(row.get(name) instanceof Long value && value >= minimum); return (Long) row.get(name);
    }
    private static boolean bool(Document row, String name) {
        require(row.get(name) instanceof Boolean); return (Boolean) row.get(name);
    }
    private static String text(Document row, String name, int maximum) {
        require(row.get(name) instanceof String value && !value.isBlank()
                && value.length() <= maximum * 2 && value.codePointCount(0, value.length()) <= maximum);
        return (String) row.get(name);
    }
    private static Instant instant(Document row, String name, boolean nullable) {
        if (nullable && row.get(name) == null) { return null; }
        try { return Instant.parse(text(row, name, 40)); }
        catch (DateTimeParseException invalid) { throw unreadable(); }
    }
    private static AlertSnapshot.Status status(String value) {
        return switch (value) {
            case "CLEAR" -> AlertSnapshot.Status.CLEAR;
            case "PENDING" -> AlertSnapshot.Status.PENDING;
            case "FIRING" -> AlertSnapshot.Status.FIRING;
            case "RESOLVED" -> AlertSnapshot.Status.RESOLVED;
            default -> throw unreadable();
        };
    }
    private static AlertSnapshot.Quality quality(String value) {
        return switch (value) {
            case "VALID" -> AlertSnapshot.Quality.VALID;
            case "UNKNOWN" -> AlertSnapshot.Quality.UNKNOWN;
            case "STALE" -> AlertSnapshot.Quality.STALE;
            case "CLOCK_INVALID" -> AlertSnapshot.Quality.CLOCK_INVALID;
            case "DISABLED" -> AlertSnapshot.Quality.DISABLED;
            default -> throw unreadable();
        };
    }
    private static AlarmSeverity severity(String value) {
        return switch (value) {
            case "CRITICAL" -> AlarmSeverity.CRITICAL;
            case "MAJOR" -> AlarmSeverity.MAJOR;
            case "MINOR" -> AlarmSeverity.MINOR;
            case "WARNING" -> AlarmSeverity.WARNING;
            case "INDETERMINATE" -> AlarmSeverity.INDETERMINATE;
            default -> throw unreadable();
        };
    }
    private static AlertSnapshot.EndReason reason(String value) {
        return switch (value) {
            case "RECOVERED" -> AlertSnapshot.EndReason.RECOVERED;
            case "DISABLED" -> AlertSnapshot.EndReason.DISABLED;
            case "SUPERSEDED" -> AlertSnapshot.EndReason.SUPERSEDED;
            case "RESOURCE_REMOVED" -> AlertSnapshot.EndReason.RESOURCE_REMOVED;
            default -> throw unreadable();
        };
    }
    private static AlertUpdate.Kind kind(String value) {
        return switch (value) {
            case "PENDING" -> AlertUpdate.Kind.PENDING;
            case "FIRING" -> AlertUpdate.Kind.FIRING;
            case "RESOLVED" -> AlertUpdate.Kind.RESOLVED;
            case "SEVERITY_CHANGED" -> AlertUpdate.Kind.SEVERITY_CHANGED;
            case "ADMINISTRATIVELY_ENDED" -> AlertUpdate.Kind.ADMINISTRATIVELY_ENDED;
            case "QUALITY_CHANGED" -> AlertUpdate.Kind.QUALITY_CHANGED;
            default -> throw unreadable();
        };
    }
    private static void require(boolean condition) { if (!condition) { throw unreadable(); } }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-state", "field", "envelope"), null);
    }
}
