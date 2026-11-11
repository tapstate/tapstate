package io.tapstate.core.dsl.aggregate;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Year;
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
     * The values of the group {@code named} names, in the order of the step's output key.
     *
     * @throws TapstateException with {@link AggregateError#GROUP_INVALID} when a name is missing, a name
     *                           is not part of the key, or a value does not read as its column's type
     */
    public static List<Object> parse(AggregateStepSpec spec, Map<String, String> named) {
        List<String> names = spec.outputKey();
        for (String given : named.keySet()) {
            if (!names.contains(given)) {
                throw invalid(spec, "`" + given + "` is not one of its group names " + names);
            }
        }
        List<Object> values = new ArrayList<>();
        for (String name : names) {
            if (!named.containsKey(name)) {
                throw invalid(spec, "no value is given for `" + name + "`; a group is named by all of " + names);
            }
            String text = named.get(name);
            try {
                values.add(text == null ? null : value(spec.groupTypes().get(name), text));
            } catch (NumberFormatException | DateTimeParseException unreadable) {
                throw invalid(spec, "`" + text + "` does not read as the " + spec.groupTypes().get(name).spelled()
                        + " value of `" + name + "`");
            }
        }
        return values;
    }

    private static Object value(ValueType type, String text) {
        return switch (type.type()) {
            // A number is stored by its value, however it is held, so one reading serves every numeric type.
            case INT64, DECIMAL, DOUBLE -> new BigDecimal(text);
            case BOOLEAN -> {
                if (!text.equals("true") && !text.equals("false")) {
                    throw new NumberFormatException(text);
                }
                yield Boolean.parseBoolean(text);
            }
            case DATE -> LocalDate.parse(text);
            case TIME -> LocalTime.parse(text);
            case YEAR -> Year.parse(text);
            // A point in time carries its offset; a local date and time carries none.
            case DATETIME -> zoned(text) ? OffsetDateTime.parse(text).toInstant() : LocalDateTime.parse(text);
            default -> text;
        };
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
