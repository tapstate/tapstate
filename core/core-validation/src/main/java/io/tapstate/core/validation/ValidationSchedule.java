package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Fixed schedule inputs and preview values; no scheduler, storage authority or automatic restart. */
public sealed interface ValidationSchedule permits ValidationSchedule.Interval, ValidationSchedule.Cron {
    String RULES_VERSION = "validation-unix-v1";
    String zoneId();

    enum Unit {
        MINUTES(60), HOURS(3600), DAYS(86400);
        private final long seconds;
        Unit(long seconds) { this.seconds = seconds; }
    }
    /** Days are elapsed 24-hour intervals on the original UTC timeline, not local calendar days. */
    record Interval(long every, Unit unit, Instant anchor, String zoneId) implements ValidationSchedule {
        public Interval {
            Objects.requireNonNull(unit, "unit"); Objects.requireNonNull(anchor, "anchor");
            if (every < 1) throw invalid("interval", 1, Long.toString(every), null);
            try { Math.multiplyExact(every, unit.seconds); }
            catch (ArithmeticException tooLarge) { throw invalid("interval", 1, Long.toString(every), tooLarge); }
            requireZone(zoneId);
        }
        public Duration duration() { return Duration.ofSeconds(Math.multiplyExact(every, unit.seconds)); }
    }
    record Cron(String expression, String zoneId) implements ValidationSchedule {
        public Cron {
            if (expression == null || expression.isBlank() || expression.length() > 512)
                throw invalid("expression", 1, expression, null);
            requireZone(zoneId);
        }
    }
    record SearchBudget(int steps) {
        public static final SearchBudget DEFAULT = new SearchBudget(200_000);
        public SearchBudget {
            if (steps < 1 || steps > 2_000_000) throw new TapstateException(ValidationError.INVALID_CONTRACT,
                    Map.of("field", "schedule.searchBudget"), null);
        }
    }
    record Occurrence(Instant at, LocalDateTime local, ZoneOffset offset, DayOfWeek weekday) {}
    record Preview(Instant base, long inputRevision, String zoneId, String rulesVersion, List<Occurrence> occurrences) {
        public Preview {
            Objects.requireNonNull(base, "base"); requireZone(zoneId);
            if (inputRevision < 1 || !RULES_VERSION.equals(rulesVersion) || occurrences == null || occurrences.size() != 5)
                throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "schedule.preview"), null);
            occurrences = List.copyOf(occurrences);
        }
    }
    interface Calculator {
        Preview preview(ValidationSchedule schedule, Instant base, long inputRevision, SearchBudget budget);
        Instant nextAfter(ValidationSchedule schedule, Instant after, SearchBudget budget);
    }

    static ZoneId requireZone(String id) {
        if (id == null || id.length() > 256 || !ZoneId.getAvailableZoneIds().contains(id))
            throw invalid("timeZone", 1, id, null);
        return ZoneId.of(id);
    }
    static TapstateException invalid(String field, int position, String value, Throwable cause) {
        String bounded = value == null ? "null" : value.substring(0, Math.min(128, value.length()));
        return new TapstateException(ValidationError.SCHEDULE_INVALID,
                Map.of("field", field, "position", position, "value", bounded), cause);
    }
}
