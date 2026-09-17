package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A rate is two readings apart in the pipeline's own time, and everything short of that is said to be
 * not known rather than rounded to a number.
 */
class MovementReadingTest {

    private static final Instant AT = Instant.parse("2026-09-17T10:00:00Z");

    private static MetricsOutcome.FactPoint records(String direction, String table, Instant at, long value) {
        return new MetricsOutcome.FactPoint(MetricsFacts.RECORDS_FACT,
                Map.of(MetricsFacts.DIRECTION_ATTRIBUTE, direction, MetricsFacts.TABLE_ATTRIBUTE, table), at, value);
    }

    private static MetricsOutcome.FactPoint lag(String table, Instant at, long seconds) {
        return new MetricsOutcome.FactPoint(MetricsFacts.LAG_FACT, Map.of(MetricsFacts.TABLE_ATTRIBUTE, table), at, seconds);
    }

    @Test
    void recordsAreSummedOverTablesPerDirectionAndLagIsKeptPerTable() {
        MovementReading reading = MetricsFacts.movementOf(List.of(
                records("out", "orders", AT, 10),
                records("out", "items", AT.plusMillis(5), 5),
                records("in", "orders", AT, 12),
                lag("orders", AT, 3),
                lag("items", AT, 0),
                // A point of some other fact is not movement, and a records point without a direction
                // is a point nothing can be summed under.
                new MetricsOutcome.FactPoint("tapstate.pipeline.errors", Map.of("code", "x"), AT, 1L),
                new MetricsOutcome.FactPoint(MetricsFacts.RECORDS_FACT, Map.of(), AT, 99L)));

        assertThat(reading.recordsByDirection()).containsExactly(Map.entry("in", 12L), Map.entry("out", 15L));
        assertThat(reading.lagSecondsByTable()).containsExactly(Map.entry("items", 0L), Map.entry("orders", 3L));
        // The latest of its points' times: they are one observation, and this is when it was taken.
        assertThat(reading.observedAt()).isEqualTo(AT.plusMillis(5));
    }

    @Test
    void factsThatCarryNoMovementReadAsNoReadingRatherThanAnEmptyOne() {
        assertThat(MetricsFacts.movementOf(List.of())).isNull();
        assertThat(MetricsFacts.movementOf(List.of(
                new MetricsOutcome.FactPoint("tapstate.pipeline.errors", Map.of("code", "x"), AT, 1L)))).isNull();
    }

    @Test
    void aRateIsTheDifferenceOfCountsOverTheReadingsOwnTime() {
        MovementReading earlier = new MovementReading(AT, Map.of("out", 100L, "in", 100L), Map.of());
        MovementReading later = new MovementReading(AT.plusSeconds(2), Map.of("out", 150L, "in", 160L), Map.of());

        MovementReading.Movement movement = later.since(earlier);

        assertThat(movement).isEqualTo(new MovementReading.Movement.Rate(
                Duration.ofSeconds(2), Map.of("out", 25.0, "in", 30.0)));
        assertThat(MovementReading.describe(movement))
                .isEqualTo("in 30.0 rows/s · out 25.0 rows/s (over 2.0s of the pipeline's own time)");
    }

    @Test
    void aDirectionTheEarlierReadingHadNotCountedYetIsRatedFromNought() {
        // The first rows a target confirms land between the two readings: before them there was no
        // point to count under, and a cumulative counter's value before it exists is nought.
        MovementReading earlier = new MovementReading(AT, Map.of("in", 4L), Map.of());
        MovementReading later = new MovementReading(AT.plusSeconds(4), Map.of("in", 204L, "out", 200L), Map.of());

        assertThat(later.since(earlier)).isEqualTo(new MovementReading.Movement.Rate(
                Duration.ofSeconds(4), Map.of("in", 50.0, "out", 50.0)));
    }

    @Test
    void oneReadingIsNotARate() {
        MovementReading only = new MovementReading(AT, Map.of("out", 100L), Map.of());

        assertThat(only.since(null)).isEqualTo(new MovementReading.Movement.NotKnown("one reading gives no rate"));
    }

    @Test
    void aReadingWhoseTimeHasNotMovedOnIsStillOneReadingHoweverLongTheWallSaysItHasBeen() {
        MovementReading earlier = new MovementReading(AT, Map.of("out", 100L), Map.of());
        MovementReading same = new MovementReading(AT, Map.of("out", 100L), Map.of());

        assertThat(same.since(earlier)).isInstanceOf(MovementReading.Movement.NotKnown.class);
        assertThat(MovementReading.describe(same.since(earlier))).startsWith("not known -- the reading has not advanced");
    }

    @Test
    void aReadingWithoutATimeGivesNoRate() {
        MovementReading earlier = new MovementReading(null, Map.of("out", 100L), Map.of());
        MovementReading later = new MovementReading(AT, Map.of("out", 150L), Map.of());

        assertThat(later.since(earlier)).isInstanceOf(MovementReading.Movement.NotKnown.class);
        assertThat(earlier.since(later)).isInstanceOf(MovementReading.Movement.NotKnown.class);
    }

    @Test
    void aCounterThatWentBackwardsIsARestartNotARate() {
        MovementReading earlier = new MovementReading(AT, Map.of("out", 100L), Map.of());
        MovementReading later = new MovementReading(AT.plusSeconds(1), Map.of("out", 40L), Map.of());

        assertThat(MovementReading.describe(later.since(earlier)))
                .isEqualTo("not known -- the counter went backwards between the readings, which is a run that restarted, not a rate");
    }

    @Test
    void lagReadsPerTableAtAGlanceOrSaysNothingWasPublished() {
        assertThat(new MovementReading(AT, Map.of(), Map.of("orders", 125L, "items", 0L)).describeLag())
                .isEqualTo("items 0s · orders 2m5s");
        assertThat(new MovementReading(AT, Map.of("out", 1L), Map.of()).describeLag()).isEqualTo("not published");
        assertThat(MovementReading.human(3_725)).isEqualTo("1h2m");
        assertThat(MovementReading.seconds(Duration.ofMillis(300))).isEqualTo("0.3s");
    }
}
