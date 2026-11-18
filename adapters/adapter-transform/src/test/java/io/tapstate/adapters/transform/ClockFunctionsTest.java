package io.tapstate.adapters.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The clock functions read the moment a row is processed: {@code now()} is that instant, {@code today(zone)} its
 * day in a zone, and the calendar arithmetic moves a day by days, months or years. Each test holds the clock still
 * so the answer is the one the row would get at that moment.
 */
class ClockFunctionsTest {

    /** 23:30 on 17 November in Las Vegas, 07:30 on the 18th in UTC. */
    private static final Instant LATE_EVENING_IN_VEGAS = Instant.parse("2026-11-18T07:30:00Z");

    /** 00:30 on 18 November in Las Vegas. */
    private static final Instant JUST_AFTER_MIDNIGHT_IN_VEGAS = Instant.parse("2026-11-18T08:30:00Z");

    private static Clock at(Instant instant) {
        return Clock.fixed(instant, ZoneOffset.UTC);
    }

    private static Envelope row(Map<String, Object> after) {
        return Envelope.insert(1L, "t", after, null);
    }

    @Test
    @DisplayName("now() is the processing instant, and leaves an expression as an instant")
    void nowIsTheProcessingInstant() {
        Object value = RowExpressionProgram.value("now()", at(LATE_EVENING_IN_VEGAS)).eval(row(Map.of()));

        assertThat(value).isEqualTo(LATE_EVENING_IN_VEGAS);
    }

    @Test
    @DisplayName("every clock call in one evaluation reads the same moment")
    void oneEvaluationReadsOneMoment() {
        Object value = RowExpressionProgram.predicate("now() == now()").eval(row(Map.of()));

        assertThat(value).isEqualTo(true);
    }

    @Test
    @DisplayName("today(zone) is the day in that zone, on both sides of its midnight")
    void todayIsTheDayInTheZone() {
        String expr = "today('America/Los_Angeles')";

        assertThat(RowExpressionProgram.value(expr, at(LATE_EVENING_IN_VEGAS)).eval(row(Map.of())))
                .isEqualTo(LocalDate.of(2026, 11, 17));
        assertThat(RowExpressionProgram.value(expr, at(JUST_AFTER_MIDNIGHT_IN_VEGAS)).eval(row(Map.of())))
                .isEqualTo(LocalDate.of(2026, 11, 18));
        assertThat(RowExpressionProgram.value("today('UTC')", at(LATE_EVENING_IN_VEGAS)).eval(row(Map.of())))
                .isEqualTo(LocalDate.of(2026, 11, 18));
    }

    @Test
    @DisplayName("a five-year cutoff keeps a row issued after the day five years ago and drops one on or before it")
    void aFiveYearCutoffFiltersOnArrival() {
        RowExpressionProgram cutoff = RowExpressionProgram.predicate(
                "after.IssueDate > today('America/Los_Angeles').addYears(-5)", at(JUST_AFTER_MIDNIGHT_IN_VEGAS));

        assertThat(cutoff.eval(row(Map.of("IssueDate", LocalDate.of(2021, 11, 19))))).isEqualTo(true);
        assertThat(cutoff.eval(row(Map.of("IssueDate", LocalDate.of(2021, 11, 18))))).isEqualTo(false);
        assertThat(cutoff.eval(row(Map.of("IssueDate", LocalDate.of(2019, 3, 1))))).isEqualTo(false);
    }

    @Test
    @DisplayName("a date and time is cut off by the day it falls on")
    void aDateTimeIsCutOffByItsDay() {
        RowExpressionProgram cutoff = RowExpressionProgram.predicate(
                "after.IssuedAt.toDate('America/Los_Angeles') > today('America/Los_Angeles').addYears(-5)",
                at(JUST_AFTER_MIDNIGHT_IN_VEGAS));

        // A wall-clock reading keeps its own day; an instant takes the zone's.
        assertThat(cutoff.eval(row(Map.of("IssuedAt", LocalDateTime.of(2021, 11, 19, 0, 15))))).isEqualTo(true);
        assertThat(cutoff.eval(row(Map.of("IssuedAt", Instant.parse("2021-11-19T07:00:00Z"))))).isEqualTo(false);
        assertThat(cutoff.eval(row(Map.of("IssuedAt", Instant.parse("2021-11-19T09:00:00Z"))))).isEqualTo(true);
    }

    @Test
    @DisplayName("calendar arithmetic lands on the last day of a shorter month")
    void calendarArithmeticClampsToTheMonth() {
        Clock clock = at(LATE_EVENING_IN_VEGAS);

        assertThat(RowExpressionProgram.value("after.d.addMonths(1)", clock)
                .eval(row(Map.of("d", LocalDate.of(2026, 1, 31))))).isEqualTo(LocalDate.of(2026, 2, 28));
        assertThat(RowExpressionProgram.value("after.d.addYears(1)", clock)
                .eval(row(Map.of("d", LocalDate.of(2024, 2, 29))))).isEqualTo(LocalDate.of(2025, 2, 28));
        assertThat(RowExpressionProgram.value("after.d.addDays(-1)", clock)
                .eval(row(Map.of("d", LocalDate.of(2026, 3, 1))))).isEqualTo(LocalDate.of(2026, 2, 28));
    }

    @Test
    @DisplayName("date arithmetic on a value that is not a date fails the expression with a code")
    void arithmeticOnANonDateIsCoded() {
        RowExpressionProgram program = RowExpressionProgram.value("after.d.addDays(1)", at(LATE_EVENING_IN_VEGAS));

        assertThatThrownBy(() -> program.eval(row(Map.of("d", "2026-01-01"))))
                .isInstanceOfSatisfying(TapstateException.class,
                        e -> assertThat(e.code().code()).isEqualTo("transform.expression-failed"));
    }
}
