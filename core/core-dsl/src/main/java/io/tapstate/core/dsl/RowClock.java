package io.tapstate.core.dsl;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * What the clock functions of a row expression compute, apart from the expression language that calls them.
 *
 * <p>{@code now()} is the moment the row is processed, read once per evaluation so that every clock call in one
 * expression sees the same instant. {@code today(zone)} is that moment's calendar day in {@code zone}. Neither is
 * recomputed later: a row that has passed a filter, or a field computed from them, keeps the answer it got when it
 * was processed until the row changes again. A value that has to follow the calendar on its own belongs to whoever
 * reads it.
 *
 * <p>The functions taking a zone take a region id such as {@code America/Los_Angeles}. An abbreviation such as
 * {@code PST} is refused rather than guessed at, because the same letters name different offsets in different
 * places.
 */
public final class RowClock {

    /** {@code today(zone)}: the calendar day of now in a zone. */
    public static final String TODAY = "today";
    public static final String TODAY_OVERLOAD = "tapstate_today_string";

    /** {@code date.addDays(n)}, {@code date.addMonths(n)}, {@code date.addYears(n)}: calendar arithmetic. */
    public static final String ADD_DAYS = "addDays";
    public static final String ADD_DAYS_OVERLOAD = "tapstate_date_add_days";
    public static final String ADD_MONTHS = "addMonths";
    public static final String ADD_MONTHS_OVERLOAD = "tapstate_date_add_months";
    public static final String ADD_YEARS = "addYears";
    public static final String ADD_YEARS_OVERLOAD = "tapstate_date_add_years";

    /** {@code datetime.toDate(zone)}: the calendar day a date and time falls on. */
    public static final String TO_DATE = "toDate";
    public static final String TO_DATE_OVERLOAD = "tapstate_datetime_to_date";

    /** The overload id {@code now()} is declared under. */
    public static final String NOW_OVERLOAD = "now_timestamp";

    private RowClock() {
    }

    /** The calendar day of {@code now} in {@code zone}. */
    public static LocalDate today(Instant now, String zone) {
        return now.atZone(zone(zone)).toLocalDate();
    }

    /** {@code date} moved by {@code days} calendar days. */
    public static LocalDate addDays(Object date, long days) {
        return date(date).plusDays(days);
    }

    /** {@code date} moved by {@code months} calendar months; a day past the month's end lands on its last day. */
    public static LocalDate addMonths(Object date, long months) {
        return date(date).plusMonths(months);
    }

    /** {@code date} moved by {@code years} calendar years; 29 February lands on 28 February in a common year. */
    public static LocalDate addYears(Object date, long years) {
        return date(date).plusYears(years);
    }

    /**
     * The calendar day {@code value} falls on in {@code zone}. A date and time that carries no offset is already
     * a wall-clock reading, so its own day is the answer and the zone is not applied to it a second time.
     */
    public static LocalDate toDate(Object value, String zone) {
        return switch (value) {
            case LocalDateTime local -> local.toLocalDate();
            case Instant instant -> instant.atZone(zone(zone)).toLocalDate();
            case OffsetDateTime offset -> offset.atZoneSameInstant(zone(zone)).toLocalDate();
            case ZonedDateTime zoned -> zoned.withZoneSameInstant(zone(zone)).toLocalDate();
            case Date legacy -> legacy.toInstant().atZone(zone(zone)).toLocalDate();
            case null -> throw new IllegalArgumentException("toDate() of null");
            default -> throw new IllegalArgumentException(
                    "toDate() takes a date and time, not a " + value.getClass().getSimpleName());
        };
    }

    /**
     * Why {@code zone} is not a time zone a clock function accepts, or {@code null} when it is. Only region ids
     * and fixed offsets are accepted.
     */
    public static String zoneError(String zone) {
        try {
            ZoneId.of(zone);
            return null;
        } catch (DateTimeException refused) {
            return "'" + zone + "' is not a time zone; use a region id such as America/Los_Angeles";
        }
    }

    private static ZoneId zone(String zone) {
        String error = zoneError(zone);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
        return ZoneId.of(zone);
    }

    private static LocalDate date(Object value) {
        if (value instanceof LocalDate date) {
            return date;
        }
        throw new IllegalArgumentException("date arithmetic takes a date, not a "
                + (value == null ? "null" : value.getClass().getSimpleName()));
    }
}
