package io.tapstate.app;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.CronFieldName;
import com.cronutils.model.field.definition.DayOfWeekFieldDefinition;
import com.cronutils.model.field.expression.Always;
import com.cronutils.model.time.generator.FieldValueGeneratorFactory;
import com.cronutils.parser.CronParser;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationSchedule;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * The same bounded UNIX calendar calculation serves previews and due-time discovery. Parsing uses
 * the fixed library definition; calendar steps use explicit zone rules and never replay DST overlaps.
 * A returned time is only a planned occurrence. The controller still owns revisions, lateness, skips,
 * full-slot admission and original-run creation. No timer, queue or job is created here.
 */
public final class ValidationScheduleCalculator implements ValidationSchedule.Calculator {
    private static final Pattern TOKEN = Pattern.compile("\\S+");
    private static final Pattern COMPONENT = Pattern.compile("(?:\\*|(?:[0-9]+|[A-Za-z]+)(?:-(?:[0-9]+|[A-Za-z]+))?)(?:/[0-9]+)?");
    private static final CronFieldName[] FIELDS = { CronFieldName.MINUTE, CronFieldName.HOUR, CronFieldName.DAY_OF_MONTH,
            CronFieldName.MONTH, CronFieldName.DAY_OF_WEEK };
    private static final String[] NAMES = { "minute", "hour", "dayOfMonth", "month", "dayOfWeek" };

    @Override public ValidationSchedule.Preview preview(ValidationSchedule schedule, Instant base, long inputRevision,
            ValidationSchedule.SearchBudget budget) {
        if (inputRevision < 1) throw ValidationSchedule.invalid("inputRevision", 1, Long.toString(inputRevision), null);
        return new ValidationSchedule.Preview(base, inputRevision, schedule.zoneId(), ValidationSchedule.RULES_VERSION,
                occurrences(schedule, base, 5, budget));
    }
    @Override public Instant nextAfter(ValidationSchedule schedule, Instant after, ValidationSchedule.SearchBudget budget) {
        return occurrences(schedule, after, 1, budget).getFirst().at();
    }
    private List<ValidationSchedule.Occurrence> occurrences(ValidationSchedule schedule, Instant after, int count,
            ValidationSchedule.SearchBudget limits) {
        Objects.requireNonNull(schedule, "schedule"); Objects.requireNonNull(after, "after");
        var budget = new Counter(Objects.requireNonNull(limits, "budget").steps());
        var zone = ValidationSchedule.requireZone(schedule.zoneId());
        try {
            return switch (schedule) {
                case ValidationSchedule.Interval interval -> interval(interval, after, count, zone, budget);
                case ValidationSchedule.Cron cron -> calendar(parse(cron.expression()), after, count, zone, budget);
            };
        } catch (DateTimeException | ArithmeticException range) { throw exhausted("calendarRange", range); }
    }

    private static List<ValidationSchedule.Occurrence> interval(ValidationSchedule.Interval spec, Instant after, int count,
            ZoneId zone, Counter budget) {
        var step = spec.duration();
        long ordinal = after.isBefore(spec.anchor()) ? 1 : Math.addExact(Duration.between(spec.anchor(), after).dividedBy(step), 1);
        var result = new ArrayList<ValidationSchedule.Occurrence>(count);
        for (int i = 0; i < count; i++) {
            budget.spend();
            var next = spec.anchor().plus(step.multipliedBy(Math.addExact(ordinal, i)));
            result.add(occurrence(next, zone));
        }
        return List.copyOf(result);
    }

    private static List<ValidationSchedule.Occurrence> calendar(Fields fields, Instant after, int count, ZoneId zone, Counter budget) {
        var result = new ArrayList<ValidationSchedule.Occurrence>(count);
        var date = after.atZone(zone).toLocalDate();
        while (result.size() < count) {
            budget.spend();
            if (fields.matches(date)) {
                for (int hour : fields.hours) for (int minute : fields.minutes) {
                    budget.spend();
                    var local = date.atTime(hour, minute);
                    var offsets = zone.getRules().getValidOffsets(local);
                    if (offsets.isEmpty()) continue;
                    // The earlier occurrence is the only permitted instant, even if the later one
                    // would be after the preview base or a scheduler restart.
                    var candidate = local.toInstant(offsets.getFirst());
                    if (!candidate.isAfter(after)) continue;
                    result.add(occurrence(candidate, zone));
                    if (result.size() == count) return List.copyOf(result);
                }
            }
            date = date.plusDays(1);
        }
        throw new IllegalStateException("schedule calculation left without its result");
    }

    private static Fields parse(String expression) {
        var tokens = new ArrayList<String>(); var positions = new ArrayList<Integer>();
        var matcher = TOKEN.matcher(expression);
        while (matcher.find()) { tokens.add(matcher.group()); positions.add(matcher.start() + 1); }
        if (tokens.size() != 5) throw new TapstateException(ValidationError.SCHEDULE_FIELD_COUNT, Map.of("count", tokens.size()), null);
        var parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX));
        for (int i = 0; i < tokens.size(); i++) {
            var token = tokens.get(i);
            for (int c = 0; c < token.length(); c++) {
                char ch = token.charAt(c);
                if (!(ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || "*,-/".indexOf(ch) >= 0))
                    throw ValidationSchedule.invalid(NAMES[i], positions.get(i) + c, token, null);
            }
            int partStart = 0;
            for (String part : token.split(",", -1)) {
                // Validate the whole token shape before handing it to the library: malformed range
                // steps must neither lose a suffix nor escape as an internal parser exception.
                if (!COMPONENT.matcher(part).matches())
                    throw ValidationSchedule.invalid(NAMES[i], positions.get(i) + partStart, part, null);
                partStart += part.length() + 1;
            }
            tokens.set(i, token.toUpperCase(Locale.ROOT));
            var isolated = new String[] { "*", "*", "*", "*", "*" }; isolated[i] = tokens.get(i);
            try { parser.parse(String.join(" ", isolated)).validate(); }
            catch (IllegalArgumentException error) { throw ValidationSchedule.invalid(NAMES[i], positions.get(i), token, error); }
        }
        var cron = parser.parse(String.join(" ", tokens)).validate();
        int[][] values = new int[4][];
        int[] min = { 0, 0, 1, 1 }; int[] max = { 59, 23, 31, 12 };
        for (int field = 0; field < 4; field++) {
            var allowed = new ArrayList<Integer>();
            var generator = FieldValueGeneratorFactory.forCronField(cron.retrieve(FIELDS[field]));
            for (int value = min[field]; value <= max[field]; value++) if (generator.isMatch(value)) allowed.add(value);
            if (allowed.isEmpty()) throw ValidationSchedule.invalid(NAMES[field], positions.get(field), tokens.get(field), null);
            values[field] = allowed.stream().mapToInt(Integer::intValue).toArray();
        }
        var dow = cron.retrieve(CronFieldName.DAY_OF_WEEK);
        var definition = (DayOfWeekFieldDefinition)cron.getCronDefinition().getFieldDefinition(CronFieldName.DAY_OF_WEEK);
        var weekdays = new boolean[7];
        // One fixed month covers all weekdays. The library's weekday generator also handles a
        // numeric range ending in Sunday 7, which the UNIX definition maps to zero.
        var generator = FieldValueGeneratorFactory.createDayOfWeekValueGeneratorInstance(dow, 2000, 1, definition.getMondayDoWValue());
        for (int day : generator.generateCandidates(1, 31)) weekdays[LocalDate.of(2000, 1, day).getDayOfWeek().getValue() % 7] = true;
        var fields = new Fields(values[0], values[1], set(values[2], 32), set(values[3], 13), weekdays,
                cron.retrieve(CronFieldName.DAY_OF_MONTH).getExpression() instanceof Always, dow.getExpression() instanceof Always);
        if (!fields.possible()) throw new TapstateException(ValidationError.SCHEDULE_NO_DATE, Map.of(), null);
        return fields;
    }
    private record Fields(int[] minutes, int[] hours, boolean[] days, boolean[] months, boolean[] weekdays,
                          boolean anyDay, boolean anyWeekday) {
        private boolean matches(LocalDate date) {
            if (!months[date.getMonthValue()]) return false;
            boolean dom = days[date.getDayOfMonth()]; boolean dow = weekdays[date.getDayOfWeek().getValue() % 7];
            return anyDay ? dow : anyWeekday ? dom : dom || dow;
        }
        private boolean possible() {
            boolean hasWeekday = false; for (boolean allowed : weekdays) hasWeekday |= allowed;
            if (!hasWeekday) return false;
            if (!anyWeekday) return true;
            // Gregorian leap-year month lengths establish calendar feasibility without a short
            // future search that would incorrectly reject February 29 near a non-leap century.
            for (int month = 1; month <= 12; month++) if (months[month])
                for (int day = 1; day <= Month.of(month).length(true); day++) if (days[day]) return true;
            return false;
        }
    }
    private static boolean[] set(int[] values, int size) { var result = new boolean[size]; for (int value : values) result[value] = true; return result; }
    private static ValidationSchedule.Occurrence occurrence(Instant instant, ZoneId zone) {
        var local = instant.atZone(zone);
        return new ValidationSchedule.Occurrence(instant, local.toLocalDateTime(), local.getOffset(), local.getDayOfWeek());
    }
    private static final class Counter {
        private int remaining;
        private Counter(int steps) { remaining = steps; }
        private void spend() { if (remaining-- == 0) throw exhausted("searchSteps", null); }
    }
    private static TapstateException exhausted(String limit, Throwable cause) {
        return new TapstateException(ValidationError.SCHEDULE_SEARCH_EXHAUSTED, Map.of("limit", limit), cause);
    }
}
