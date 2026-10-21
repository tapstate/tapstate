package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationSchedule;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationScheduleCalculatorTest {
    private final ValidationScheduleCalculator calculator = new ValidationScheduleCalculator();
    private static final ValidationSchedule.SearchBudget BUDGET = ValidationSchedule.SearchBudget.DEFAULT;

    @Test void intervalUsesItsOriginalAnchorAndSkipsPastOccurrencesWithoutDriftingFromCompletion() {
        var anchor = Instant.parse("2026-10-22T10:00:03.123Z");
        var spec = new ValidationSchedule.Interval(30, ValidationSchedule.Unit.MINUTES, anchor, "UTC");
        var preview = calculator.preview(spec, Instant.parse("2026-10-22T10:45:00Z"), 8, BUDGET);
        assertThat(preview.occurrences()).extracting(ValidationSchedule.Occurrence::at).containsExactly(
                Instant.parse("2026-10-22T11:00:03.123Z"), Instant.parse("2026-10-22T11:30:03.123Z"),
                Instant.parse("2026-10-22T12:00:03.123Z"), Instant.parse("2026-10-22T12:30:03.123Z"), Instant.parse("2026-10-22T13:00:03.123Z"));
        assertThat(preview.inputRevision()).isEqualTo(8); assertThat(preview.base()).isEqualTo(Instant.parse("2026-10-22T10:45:00Z"));
        assertThat(calculator.nextAfter(spec, preview.base(), BUDGET)).isEqualTo(preview.occurrences().getFirst().at());
        assertThat(calculator.nextAfter(spec, anchor.minusSeconds(3600), BUDGET)).isEqualTo(anchor.plusSeconds(1800));
        assertThat(calculator.nextAfter(spec, anchor.plusSeconds(1800), BUDGET)).isEqualTo(anchor.plusSeconds(3600));
    }

    @Test void intervalDaysAreElapsedTimeAcrossDstAndTheSavedZoneControlsPresentation() {
        var spec = new ValidationSchedule.Interval(1, ValidationSchedule.Unit.DAYS, Instant.parse("2026-03-07T13:00:00Z"), "America/New_York");
        var next = calculator.preview(spec, Instant.parse("2026-03-07T13:00:00Z"), 1, BUDGET).occurrences().getFirst();
        assertThat(next.at()).isEqualTo(Instant.parse("2026-03-08T13:00:00Z"));
        assertThat(next.local().getHour()).isEqualTo(9); assertThat(next.offset()).isEqualTo(ZoneOffset.ofHours(-4));
    }

    @Test void unixMonthDayAndWeekdayUseOrOnlyWhenBothAreRestricted() {
        assertThat(times("0 2 1 * MON", "UTC", "2026-09-30T23:00:00Z")).containsExactly(
                "2026-10-01T02:00:00Z", "2026-10-05T02:00:00Z", "2026-10-12T02:00:00Z", "2026-10-19T02:00:00Z", "2026-10-26T02:00:00Z");
        assertThat(times("0 2 * * MON", "UTC", "2026-09-30T23:00:00Z")).startsWith("2026-10-05T02:00:00Z", "2026-10-12T02:00:00Z");
        assertThat(times("0 2 1 * *", "UTC", "2026-09-30T23:00:00Z")).startsWith("2026-10-01T02:00:00Z", "2026-11-01T02:00:00Z");
    }

    @Test void namesSundayAliasesListsRangesAndStepsUseTheSameBackendCalculation() {
        var sunday = times("0 2 * * SUN", "UTC", "2026-10-22T00:00:00Z");
        assertThat(times("0 2 * * 0", "UTC", "2026-10-22T00:00:00Z")).isEqualTo(sunday);
        assertThat(times("0 2 * * 7", "UTC", "2026-10-22T00:00:00Z")).isEqualTo(sunday);
        assertThat(times("0 2 * * 5-7", "UTC", "2026-10-22T00:00:00Z")).startsWith(
                "2026-10-23T02:00:00Z", "2026-10-24T02:00:00Z", "2026-10-25T02:00:00Z");
        assertThat(times("*/30 8-9 * JAN,MAR MON-FRI", "UTC", "2026-01-05T08:01:00Z")).containsExactly(
                "2026-01-05T08:30:00Z", "2026-01-05T09:00:00Z", "2026-01-05T09:30:00Z", "2026-01-06T08:00:00Z", "2026-01-06T08:30:00Z");
    }

    @Test void nonexistentLocalTimesAreSkippedAndRepeatedTimesUseOnlyTheirFirstOccurrence() {
        assertThat(times("30 2 * * *", "America/New_York", "2026-03-07T08:00:00Z")).startsWith(
                "2026-03-09T06:30:00Z", "2026-03-10T06:30:00Z");
        assertThat(times("30 1 * * *", "America/New_York", "2026-11-01T04:00:00Z")).startsWith(
                "2026-11-01T05:30:00Z", "2026-11-02T06:30:00Z");
        assertThat(times("30 1 * * *", "America/New_York", "2026-11-01T05:45:00Z")).startsWith("2026-11-02T06:30:00Z");
        assertThat(times("30 1 * * *", "America/New_York", "2026-11-01T06:10:00Z")).startsWith("2026-11-02T06:30:00Z");
        assertThat(times("* * * * *", "America/New_York", "2026-11-01T05:58:00Z")).startsWith(
                "2026-11-01T05:59:00Z", "2026-11-01T07:00:00Z", "2026-11-01T07:01:00Z");
    }

    @Test void leapDaysRemainValidAcrossTheNonLeapCenturyAndImpossibleDatesAreDistinct() {
        assertThat(times("0 0 29 FEB *", "UTC", "2096-03-01T00:00:00Z")).containsExactly(
                "2104-02-29T00:00:00Z", "2108-02-29T00:00:00Z", "2112-02-29T00:00:00Z", "2116-02-29T00:00:00Z", "2120-02-29T00:00:00Z");
        assertThatThrownBy(() -> times("0 0 30 FEB *", "UTC", "2026-01-01T00:00:00Z"))
                .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ValidationError.SCHEDULE_NO_DATE));
        // The weekday branch still supplies valid dates when the month-day branch is impossible.
        assertThat(times("0 0 30 FEB MON", "UTC", "2026-01-01T00:00:00Z")).startsWith("2026-02-02T00:00:00Z");
    }

    @Test void unsupportedDialectAndInvalidFieldsHaveCodedLocationDiagnostics() {
        for (String expression : List.of("0 0 2 * * *", "0 2 * *", "@daily")) {
            assertThatThrownBy(() -> times(expression, "UTC", "2026-01-01T00:00:00Z"))
                    .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ValidationError.SCHEDULE_FIELD_COUNT));
        }
        for (String expression : List.of("60 2 * * *", "0 24 * * *", "0 2 L * *", "0 2 1W * *", "0 2 ? * MON",
                "0 2 * * MON#2", "*/0 * * * *", "0 2 * 13 *", "0 2 * * 8", "0 2 * * FUNDAY")) {
            assertThatThrownBy(() -> times(expression, "UTC", "2026-01-01T00:00:00Z"))
                    .as(expression).isInstanceOfSatisfying(TapstateException.class, e -> {
                        assertThat(e.code()).isEqualTo(ValidationError.SCHEDULE_INVALID);
                        assertThat(e.args()).containsKeys("field", "position", "value");
                    });
        }
        assertThatThrownBy(() -> times("0 2 * * *", "+08:00", "2026-01-01T00:00:00Z"))
                .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ValidationError.SCHEDULE_INVALID));
    }

    @Test void budgetExhaustionDoesNotMasqueradeAsAnInvalidLeapDayExpression() {
        var spec = new ValidationSchedule.Cron("0 0 29 FEB *", "UTC");
        assertThatThrownBy(() -> calculator.preview(spec, Instant.parse("2096-03-01T00:00:00Z"), 1, new ValidationSchedule.SearchBudget(3)))
                .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ValidationError.SCHEDULE_SEARCH_EXHAUSTED));
        assertThatThrownBy(() -> new ValidationSchedule.Interval(Long.MAX_VALUE, ValidationSchedule.Unit.DAYS, Instant.EPOCH, "UTC"))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new ValidationSchedule.Interval(0, ValidationSchedule.Unit.MINUTES, Instant.EPOCH, "UTC"))
                .isInstanceOf(TapstateException.class);
    }

    @Test void anExtraStepSuffixCannotBeSilentlyIgnored() {
        assertThatThrownBy(() -> times("1-5/2/3 * * * *", "UTC", "2026-01-01T00:00:00Z"))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.SCHEDULE_INVALID));
    }

    @Test void aMissingRangeStepCannotEscapeAsAnUncodedParserException() {
        assertThatThrownBy(() -> times("1-5/ * * * *", "UTC", "2026-01-01T00:00:00Z"))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ValidationError.SCHEDULE_INVALID));
        assertThat(times("1-5/2 * * * *", "UTC", "2026-01-01T00:00:00Z")).containsExactly(
                "2026-01-01T00:01:00Z", "2026-01-01T00:03:00Z", "2026-01-01T00:05:00Z", "2026-01-01T01:01:00Z", "2026-01-01T01:03:00Z");
    }

    private List<String> times(String expression, String zone, String base) {
        return calculator.preview(new ValidationSchedule.Cron(expression, zone), Instant.parse(base), 1, BUDGET)
                .occurrences().stream().map(time -> time.at().toString()).toList();
    }
}
