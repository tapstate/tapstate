package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.common.TapstateType;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The calendar functions are checked where every row expression is: a cutoff written against a DATE column
 * compiles, one that mixes a date and time with a date is refused until it says which day it means, and a
 * literal time zone that names no zone is refused before anything runs.
 */
class CalendarFunctionsCheckTest {

    private static final Map<String, TapstateType> COMPS = Map.of(
            "IssueDate", TapstateType.DATE,
            "IssuedAt", TapstateType.DATETIME);

    @Test
    @DisplayName("a five-year cutoff against a DATE column compiles")
    void aDateCutoffCompiles() {
        assertThat(RowExpressions.typedPredicateError(
                "after.IssueDate > today('America/Los_Angeles').addYears(-5)", COMPS)).isNull();
        assertThat(RowExpressions.typedPredicateError(
                "after.IssueDate >= today('UTC').addMonths(-60).addDays(1)", COMPS)).isNull();
    }

    @Test
    @DisplayName("a date and time is compared with a day only through the day it falls on")
    void aDateTimeNeedsToDate() {
        assertThat(RowExpressions.typedPredicateError(
                "after.IssuedAt > today('America/Los_Angeles').addYears(-5)", COMPS)).isNotNull();
        assertThat(RowExpressions.typedPredicateError(
                "after.IssuedAt.toDate('America/Los_Angeles') > today('America/Los_Angeles').addYears(-5)",
                COMPS)).isNull();
    }

    @Test
    @DisplayName("a literal zone that is not a region id is refused, naming the zone")
    void aLiteralZoneMustBeAZone() {
        assertThat(RowExpressions.typedPredicateError("after.IssueDate == today('PST')", COMPS))
                .contains("'PST' is not a time zone");
        assertThat(RowExpressions.predicateError("after.d.toDate('Mars/Olympus') == today('UTC')"))
                .contains("'Mars/Olympus' is not a time zone");
        assertThat(RowExpressions.typedValueError("today('Nowhere')", COMPS))
                .contains("'Nowhere' is not a time zone");
    }

    @Test
    @DisplayName("today() needs its zone; there is no implicit one")
    void todayHasNoDefaultZone() {
        assertThat(RowExpressions.predicateError("after.d == today()")).isNotNull();
    }

    @Test
    @DisplayName("date arithmetic is refused on a value that is not a date")
    void arithmeticNeedsADate() {
        assertThat(RowExpressions.typedValueError("after.IssuedAt.addDays(1)", COMPS)).isNotNull();
        assertThat(RowExpressions.typedValueType("today('UTC').addDays(1)", COMPS)).isEqualTo(TapstateType.DATE);
    }
}
