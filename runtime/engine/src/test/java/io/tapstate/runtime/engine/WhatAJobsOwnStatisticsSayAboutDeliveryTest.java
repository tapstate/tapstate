package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How a run's own statistics are read back into the three delivery readings, and which names each one
 * takes. A run's statistics are one flat namespace of bare numbers, so every reading here is telling its
 * own names apart from its neighbours' by prefix alone — and getting that wrong does not fail, it
 * answers with the neighbour's numbers.
 *
 * <p>Driven with statistics built by hand rather than through a running job. That is the point: the
 * half of each readback that finds the job has no decisions in it, and the half that reads the names is
 * all decisions — which names are taken, which are passed over, and how several sinks' figures combine.
 * Reachable only through a live job, those decisions had nothing checking them: a readback pointed at
 * the wrong family of names, or adding what it should keep at its widest, returns a plausible map either
 * way.
 *
 * <p>Two sinks over one table is the shape every case here uses, because it is the one that tells the
 * combining rules apart. With a single sink, adding and keeping the widest give the same answer.
 */
class WhatAJobsOwnStatisticsSayAboutDeliveryTest {

    private static final String JOB = "job-a";
    private static final String EXECUTION = "execution-a";

    private static JobMetrics statistics(Map<String, List<Long>> valuesByMetric) {
        Map<String, List<Measurement>> measurements = new LinkedHashMap<>();
        valuesByMetric.forEach((metric, values) -> measurements.put(metric,
                values.stream().map(value -> Measurement.of(metric, value, 0L, Map.of())).toList()));
        return JobMetrics.of(measurements);
    }

    @Test
    @DisplayName("the payload total is read from the payload names and from no others")
    void theByteReadbackTakesItsOwnFamilyOfNames() {
        JobMetrics collected = statistics(new LinkedHashMap<>(Map.of(
                "bytesOut.orders", List.of(4_000L),
                // The neighbours, present on purpose. Read by mistake they answer in the right shape and
                // the wrong units - an event time as a byte count is a number in the billions, and a row
                // count as one is a number that looks entirely reasonable.
                "outEventTime.orders", List.of(1_700_000_000_000L),
                "recordsOut.i.orders", List.of(7L))));

        assertThat(Engine.settledBytesIn(collected)).containsExactly(Map.entry("orders", 4_000L));
    }

    @Test
    @DisplayName("two sinks over one table have their payloads added, because both writes happened")
    void twoSinksPayloadsAreAddedRatherThanTheWidestKept() {
        JobMetrics collected = statistics(Map.of("bytesOut.orders", List.of(4_000L, 1_500L)));

        // Added, not maxed. A row written to two targets was written twice and its payload crossed
        // twice; keeping the widest would report the busier sink's work as the pipeline's whole output.
        assertThat(Engine.settledBytesIn(collected)).containsExactly(Map.entry("orders", 5_500L));
    }

    @Test
    @DisplayName("the row counts come out by table and by the operation the source performed")
    void theRowReadbackSplitsTheNameBackIntoItsDimensions() {
        JobMetrics collected = statistics(new LinkedHashMap<>(Map.of(
                "recordsOut.i.orders", List.of(7L, 3L),
                "recordsOut.d.orders", List.of(2L),
                "recordsOut.r.public.items", List.of(5_000L),
                "bytesOut.orders", List.of(4_000L))));

        // The table goes last and takes the whole remainder, dots and all: a schema-qualified name is
        // the ordinary case, and a split that guessed would put one table's rows under two names.
        assertThat(Engine.deliveredRowsIn(collected)).containsOnly(
                Map.entry("orders", Map.of("i", 10L, "d", 2L)),
                Map.entry("public.items", Map.of("r", 5_000L)));
    }

    @Test
    @DisplayName("a recency reading keeps the newer of two sinks, rather than adding them")
    void theRecencyReadbackKeepsTheWidestRatherThanAddingIt() {
        JobMetrics collected = statistics(Map.of("outEventTime.orders", List.of(1_000L, 9_000L)));

        // Added, these two would make a timestamp in the future of both of them, and the age worked out
        // from it would read as a pipeline that is ahead of its own source.
        assertThat(Engine.highestIn(collected, JetDeliveryGauge::reachedTableOf))
                .containsExactly(Map.entry("orders", 9_000L));
    }

    @Test
    @DisplayName("a statistic none of the readbacks claims is passed over by all of them")
    void anUnrelatedStatisticIsNotReadByAnyOfThem() {
        JobMetrics collected = statistics(Map.of("queuesCapacity", List.of(1_024L)));

        // A run's statistics hold whatever the engine itself publishes alongside what a sink leaves
        // there. A readback that claimed one of those would put an internal number on a user's chart.
        assertThat(Engine.settledBytesIn(collected)).isEmpty();
        assertThat(Engine.deliveredRowsIn(collected)).isEmpty();
        assertThat(Engine.highestIn(collected, JetDeliveryGauge::reachedTableOf)).isEmpty();
    }

    @Test
    @DisplayName("a table whose name contains a dot survives every readback that names it")
    void aDottedTableNameIsNotSplitByAnyOfThem() {
        JobMetrics collected = statistics(new LinkedHashMap<>(Map.of(
                "bytesOut.public.orders", List.of(4_000L),
                "outEventTime.public.orders", List.of(9_000L))));

        assertThat(Engine.settledBytesIn(collected))
                .containsExactly(Map.entry("public.orders", 4_000L));
        assertThat(Engine.highestIn(collected, JetDeliveryGauge::reachedTableOf))
                .containsExactly(Map.entry("public.orders", 9_000L));
    }

    @Test
    @DisplayName("statistics with nothing in them read back as nothing, not as tables at zero")
    void emptyStatisticsReadBackAsEmptyRatherThanAsZeroes() {
        JobMetrics collected = JobMetrics.empty();

        assertThat(Engine.settledBytesIn(collected)).isEmpty();
        assertThat(Engine.deliveredRowsIn(collected)).isEmpty();
        assertThat(Engine.highestIn(collected, JetDeliveryGauge::reachedTableOf)).isEmpty();
    }

    // ---- the delivery-duration distribution travels as its count, its sum and one number per bucket ----

    private static Map<String, List<Long>> aDistribution(String table, long count, long sumMillis, int inBucket,
            List<Long> perSink) {
        Map<String, List<Long>> statistics = new LinkedHashMap<>();
        statistics.put("outDeliveryCount." + table, perSink.stream().map(share -> count * share).toList());
        statistics.put("outDeliverySumMillis." + table, perSink.stream().map(share -> sumMillis * share).toList());
        for (int index = 0; index < 16; index++) {
            int bucket = index;
            statistics.put("outDeliveryBucket." + index + "." + table,
                    perSink.stream().map(share -> bucket == inBucket ? count * share : 0L).toList());
        }
        return statistics;
    }

    @Test
    @DisplayName("a table's distribution is read back whole: count, sum in seconds and every bucket")
    void theDurationReadbackReassemblesADistributionFromItsParts() {
        JobMetrics collected = statistics(aDistribution("orders", 7L, 2_100L, 5, List.of(1L)));

        Map<String, HistogramValue> read = Engine.settledDurationsIn(collected);

        assertThat(read).containsOnlyKeys("orders");
        HistogramValue orders = read.get("orders");
        assertThat(orders.count()).isEqualTo(7L);
        assertThat(orders.sum()).isEqualTo(2.1);
        assertThat(orders.bounds()).isEqualTo(HistogramBounds.RECORD_DELIVERY_DURATION.bounds());
        assertThat(orders.bucketCounts()).hasSize(16);
        assertThat(orders.bucketCounts().get(5)).isEqualTo(7L);
        assertThat(orders.bucketCounts().stream().mapToLong(Long::longValue).sum()).isEqualTo(7L);
    }

    @Test
    @DisplayName("two sinks over one table have their distributions added bucket by bucket")
    void twoSinksDistributionsAreAddedBucketByBucket() {
        // Each sink reports its own running totals; the second delivered twice as many rows as the first.
        JobMetrics collected = statistics(aDistribution("orders", 3L, 900L, 8, List.of(1L, 2L)));

        HistogramValue orders = Engine.settledDurationsIn(collected).get("orders");

        assertThat(orders.count()).isEqualTo(9L);
        assertThat(orders.sum()).isEqualTo(2.7);
        assertThat(orders.bucketCounts().get(8)).isEqualTo(9L);
    }

    @Test
    @DisplayName("a distribution caught with a bucket missing is left out rather than read as a shape")
    void aHalfReportedDistributionIsLeftOut() {
        Map<String, List<Long>> parts = aDistribution("orders", 7L, 2_100L, 5, List.of(1L));
        parts.remove("outDeliveryBucket.15.orders");
        JobMetrics collected = statistics(parts);

        // Fifteen buckets are not a distribution short one bucket: a consumer given them cannot tell an
        // overflow bucket that was dropped from one that never existed, and reads a tail that is not there.
        assertThat(Engine.settledDurationsIn(collected)).isEmpty();
    }

    @Test
    @DisplayName("a dotted table name is not split by the duration readback either")
    void aDottedTableNameSurvivesTheDurationReadback() {
        JobMetrics collected = statistics(aDistribution("shop.orders", 1L, 40L, 2, List.of(1L)));

        assertThat(Engine.settledDurationsIn(collected)).containsOnlyKeys("shop.orders");
    }

    @Test
    @DisplayName("the neighbouring delivery names are not read as parts of a distribution")
    void theDurationReadbackTakesItsOwnFamilyOfNames() {
        JobMetrics collected = statistics(new LinkedHashMap<>(Map.of(
                "bytesOut.orders", List.of(4_000L),
                "outEventTime.orders", List.of(1_700_000_000_000L),
                "recordsOut.i.orders", List.of(7L),
                "outCountingSince", List.of(1_700_000_000_000L))));

        assertThat(Engine.settledDurationsIn(collected)).isEmpty();
    }

    @Test
    void oneCollectionCarriesACompleteNativeDeliveryTupleFromOneExecution() {
        DeliveryReading reading = Engine.deliveryReadingIn(JobMetrics.of(deliveryFixture()), JOB);

        assertThat(reading.rowsByTableAndOp()).containsExactly(Map.entry("orders", Map.of("i", 9L)));
        assertThat(reading.bytesByTable()).containsExactly(Map.entry("orders", 90L));
        assertThat(reading.newestEventTimeByTable()).containsExactly(Map.entry("orders", 2_000L));
        assertThat(reading.start()).contains(Instant.ofEpochMilli(1_700_000_000_000L));
        assertThat(reading.deliveryDurationByTable().get("orders").count()).isEqualTo(9);
        assertThat(reading.deliveryDurationByTable().get("orders").sum()).isEqualTo(2.7);
    }

    @Test
    void aLaterSinkInTheSameExecutionDoesNotMoveThePhysicalCounterStart() {
        Map<String, List<Measurement>> firstSink = deliveryFixture();
        firstSink.replaceAll((name, readings) -> List.of(readings.getFirst()));
        Map<String, List<Measurement>> bothSinks = deliveryFixture();

        DeliveryReading first = Engine.deliveryReadingIn(JobMetrics.of(firstSink), JOB);
        DeliveryReading second = Engine.deliveryReadingIn(JobMetrics.of(bothSinks), JOB);

        assertThat(first.rowsByTableAndOp().get("orders").get("i")).isEqualTo(3);
        assertThat(second.rowsByTableAndOp().get("orders").get("i")).isEqualTo(9);
        assertThat(first.start()).contains(Instant.ofEpochMilli(1_700_000_000_000L));
        assertThat(second.start()).isEqualTo(first.start());
    }

    @Test
    void aMissingOrMixedExecutionOnAnyDeliveryPartInvalidatesTheWholeTuple() {
        for (String metric : List.of("recordsOut.i.orders", "bytesOut.orders", "outEventTime.orders",
                JetDeliveryGauge.SINCE_METRIC, "outDeliveryCount.orders", "outDeliveryBucket.8.orders")) {
            Map<String, List<Measurement>> missing = deliveryFixture();
            replaceFirst(missing, metric, Map.of(MetricTags.JOB, JOB,
                    MetricTags.MEMBER, "member-a"));
            assertThat(Engine.deliveryReadingIn(JobMetrics.of(missing), JOB)).as("missing execution on %s", metric)
                    .isEqualTo(DeliveryReading.NONE);

            Map<String, List<Measurement>> mixed = deliveryFixture();
            replaceFirst(mixed, metric, tags("execution-b"));
            assertThat(Engine.deliveryReadingIn(JobMetrics.of(mixed), JOB)).as("mixed execution on %s", metric)
                    .isEqualTo(DeliveryReading.NONE);
        }
        Map<String, List<Measurement>> foreignJob = deliveryFixture();
        replaceFirst(foreignJob, "bytesOut.orders", Map.of(MetricTags.JOB, "job-b",
                MetricTags.EXECUTION, EXECUTION, MetricTags.MEMBER, "member-a"));
        assertThat(Engine.deliveryReadingIn(JobMetrics.of(foreignJob), JOB)).isEqualTo(DeliveryReading.NONE);
    }

    @Test
    void aQuietJobOrHalfCollectedTupleRemainsAbsentWithoutInventingZeroes() {
        assertThat(Engine.deliveryReadingIn(JobMetrics.empty(), JOB)).isEqualTo(DeliveryReading.NONE);
        Map<String, List<Measurement>> onlyStart = Map.of(
                JetDeliveryGauge.SINCE_METRIC,
                List.of(Measurement.of(JetDeliveryGauge.SINCE_METRIC, 1_700_000_000_100L, 1L,
                        tags(EXECUTION))),
                MetricNames.EXECUTION_START_TIME, deliveryFixture().get(MetricNames.EXECUTION_START_TIME));
        assertThat(Engine.deliveryReadingIn(JobMetrics.of(onlyStart), JOB)).isEqualTo(DeliveryReading.NONE);

        Map<String, List<Measurement>> withoutStart = deliveryFixture();
        withoutStart.remove(JetDeliveryGauge.SINCE_METRIC);
        assertThat(Engine.deliveryReadingIn(JobMetrics.of(withoutStart), JOB)).isEqualTo(DeliveryReading.NONE);

        Map<String, List<Measurement>> withoutBucket = deliveryFixture();
        withoutBucket.remove("outDeliveryBucket.15.orders");
        assertThat(Engine.deliveryReadingIn(JobMetrics.of(withoutBucket), JOB)).isEqualTo(DeliveryReading.NONE);

        Map<String, List<Measurement>> tornCount = deliveryFixture();
        replaceFirst(tornCount, "recordsOut.i.orders", tags(EXECUTION), 4L);
        assertThat(Engine.deliveryReadingIn(JobMetrics.of(tornCount), JOB)).isEqualTo(DeliveryReading.NONE);

        assertThat(Engine.deliveryReadingIn(JobMetrics.of(deliveryFixture()), JOB,
                java.util.Set.of("member-a", "member-b")))
                .as("one member cannot stand in for the complete current roster")
                .isEqualTo(DeliveryReading.NONE);

        Map<String, List<Measurement>> mixedNative = deliveryFixture();
        replaceFirst(mixedNative, MetricNames.EXECUTION_START_TIME, tags("execution-b"));
        assertThat(Engine.deliveryReadingIn(JobMetrics.of(mixedNative), JOB))
                .as("the native start and delivery parts name different executions")
                .isEqualTo(DeliveryReading.NONE);
    }

    private static Map<String, List<Measurement>> deliveryFixture() {
        Map<String, List<Long>> values = aDistribution("orders", 3, 900, 8, List.of(1L, 2L));
        values.put("recordsOut.i.orders", List.of(3L, 6L));
        values.put("bytesOut.orders", List.of(30L, 60L));
        values.put("outEventTime.orders", List.of(1_000L, 2_000L));
        values.put(JetDeliveryGauge.SINCE_METRIC,
                List.of(1_700_000_000_000L, 1_700_000_000_100L));
        Map<String, List<Measurement>> result = new LinkedHashMap<>();
        values.forEach((name, readings) -> result.put(name, readings.stream()
                .map(value -> Measurement.of(name, value, 1L, tags(EXECUTION))).toList()));
        result.put(MetricNames.EXECUTION_START_TIME, List.of(Measurement.of(
                MetricNames.EXECUTION_START_TIME, 1_700_000_000_000L, 1L, tags(EXECUTION))));
        return result;
    }

    private static Map<String, String> tags(String execution) {
        return Map.of(MetricTags.JOB, JOB, MetricTags.EXECUTION, execution,
                MetricTags.MEMBER, "member-a");
    }

    private static void replaceFirst(Map<String, List<Measurement>> fixture, String name,
            Map<String, String> tags) {
        replaceFirst(fixture, name, tags, fixture.get(name).getFirst().value());
    }

    private static void replaceFirst(Map<String, List<Measurement>> fixture, String name,
            Map<String, String> tags, long value) {
        List<Measurement> readings = new ArrayList<>(fixture.get(name));
        readings.set(0, Measurement.of(name, value, 1L, tags));
        fixture.put(name, readings);
    }
}
