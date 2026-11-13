package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One group of an aggregate step as a person names it, and as the step shows it: each name of the output
 * key with its value written as text.
 *
 * <p>The two directions agree. What {@link #shown} writes for a group, {@link #parse} reads back as the
 * same group, so a group listed by a check can be handed to the next request as it was printed. A name
 * with no value is the null of that column.
 */
public final class AggregateGroupKey {

    private AggregateGroupKey() {
    }

    /** {@code group}'s values under the names of the step's output key, each as text; a null stays null. */
    public static Map<String, String> shown(AggregateStepSpec spec, List<Object> group) {
        Map<String, String> shown = new LinkedHashMap<>();
        List<String> names = spec.outputKey();
        for (int i = 0; i < names.size(); i++) {
            Object value = group.get(i);
            shown.put(names.get(i), value == null ? null : String.valueOf(value));
        }
        return shown;
    }

    /**
     * The values of the group {@code named} names, in the order of the step's output key: the first of
     * {@link #readings}.
     *
     * @throws TapstateException with {@link AggregateError#GROUP_INVALID} when a name is missing, a name
     *                           is not part of the key, or a value does not read as its column's type
     */
    public static List<Object> parse(AggregateStepSpec spec, Map<String, String> named) {
        return readings(spec, named).getFirst();
    }

    /** How many readings of one named group are tried; past it only the first of each value is kept. */
    private static final int MOST_READINGS = 64;

    /**
     * Every group {@code named} can name, most literal first.
     *
     * <p>A date or a date and time has more than one reading because sources do not agree on how they
     * hand one over: one gives a calendar date, another the instant that date begins at in UTC, and the
     * step keys a group by the value as it arrived. A person writes {@code 2026-03-01} either way, so
     * the text is read every way such a value arrives and the caller keeps the reading the step holds.
     * Taking only one reading named a group the step does not have while a group of that very day sat
     * beside it.
     *
     * @throws TapstateException with {@link AggregateError#GROUP_INVALID} when a name is missing, a name
     *                           is not part of the key, or a value does not read as its column's type
     */
    public static List<List<Object>> readings(AggregateStepSpec spec, Map<String, String> named) {
        List<String> names = spec.outputKey();
        for (String given : named.keySet()) {
            if (!names.contains(given)) {
                throw invalid(spec, "`" + given + "` is not one of its group names " + names);
            }
        }
        List<List<Object>> readings = new ArrayList<>();
        readings.add(new ArrayList<>());
        for (String name : names) {
            if (!named.containsKey(name)) {
                throw invalid(spec, "no value is given for `" + name + "`; a group is named by all of " + names);
            }
            String text = named.get(name);
            List<Object> values;
            try {
                values = text == null ? java.util.Collections.singletonList(null) : values(spec.groupTypes().get(name), text);
            } catch (NumberFormatException | DateTimeParseException unreadable) {
                throw invalid(spec, "`" + text + "` does not read as the " + spec.groupTypes().get(name).spelled()
                        + " value of `" + name + "`");
            }
            if (readings.size() * values.size() > MOST_READINGS) {
                values = values.subList(0, 1);
            }
            List<List<Object>> extended = new ArrayList<>(readings.size() * values.size());
            for (List<Object> reading : readings) {
                for (Object value : values) {
                    List<Object> next = new ArrayList<>(reading);
                    next.add(value);
                    extended.add(next);
                }
            }
            readings = extended;
        }
        return readings;
    }

    private static List<Object> values(ValueType type, String text) {
        return switch (type.type()) {
            // A number is stored by its value, however it is held, so one reading serves every numeric type.
            case INT64, DECIMAL, DOUBLE -> List.of(new BigDecimal(text));
            case BOOLEAN -> {
                if (!text.equals("true") && !text.equals("false")) {
                    throw new NumberFormatException(text);
                }
                yield List.of(Boolean.parseBoolean(text));
            }
            case DATE -> {
                if (text.indexOf('T') >= 0) {
                    // A date shown as the instant or the local time it was keyed by, read back as shown.
                    Object at = pointOrLocal(text);
                    LocalDate day = at instanceof Instant instant
                            ? instant.atOffset(ZoneOffset.UTC).toLocalDate() : ((LocalDateTime) at).toLocalDate();
                    yield List.of(at, day);
                }
                LocalDate day = LocalDate.parse(text);
                yield List.of(day, day.atStartOfDay(ZoneOffset.UTC).toInstant(), day.atStartOfDay());
            }
            case TIME -> List.of(LocalTime.parse(text));
            case YEAR -> List.of(Year.parse(text));
            case DATETIME -> {
                Object at = pointOrLocal(text);
                // The same clock reading as the other kind, taking a local one as UTC.
                yield at instanceof Instant instant
                        ? List.of(at, LocalDateTime.ofInstant(instant, ZoneOffset.UTC))
                        : List.of(at, ((LocalDateTime) at).toInstant(ZoneOffset.UTC));
            }
            default -> List.of(text);
        };
    }

    /** A point in time carries its offset; a local date and time carries none. */
    private static Object pointOrLocal(String text) {
        return zoned(text) ? OffsetDateTime.parse(text).toInstant() : LocalDateTime.parse(text);
    }

    private static boolean zoned(String text) {
        int time = text.indexOf('T');
        if (time < 0) {
            return false;
        }
        String clock = text.substring(time);
        return clock.endsWith("Z") || clock.indexOf('+') >= 0 || clock.indexOf('-') >= 0;
    }

    private static TapstateException invalid(AggregateStepSpec spec, String reason) {
        return new TapstateException(AggregateError.GROUP_INVALID, Map.of("step", spec.step(), "reason", reason), null);
    }
}
