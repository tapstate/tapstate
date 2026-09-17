package io.tapstate.runtime.scheduler;

import io.tapstate.core.event.Op;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.FlatMetricProjection;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The two measurements a person actually asks a running pipeline for: how much it has moved, and how far
 * behind it is. They are the first that carry their dimensions as attributes rather than baked into a
 * name, so they are also the first that the flat face the command line reads cannot hold as they are —
 * and what that face does with them instead is pinned here rather than left to whoever looks next.
 *
 * <p>The honesty of the second one is the part worth the most care. What is published is the age of the
 * last row that reached a target, which is not the same as how far the source is ahead: a source with
 * nothing new to send drives it up while the pipeline is perfectly caught up. Working it out at the moment
 * somebody asks, rather than where the row settled, is what keeps a stalled pipeline from reading healthy
 * forever - and it is the reason a case here moves the clock without moving the data.
 */
class HowMuchMovedAndHowCurrentItIsReachTheReadFaceTest {

    private static final Instant STARTED = Instant.parse("2026-09-16T12:00:00Z");
    /** Strictly before the target side: the sources are opened before the job that writes to them. */
    private static final Instant READING_SINCE = Instant.parse("2026-09-16T11:59:58Z");
    private static final Instant AT = Instant.parse("2026-09-16T12:05:00Z");

    private final InMemoryStateStore state = new InMemoryStateStore();
    private final CapturingObservationStore observations = new CapturingObservationStore();

    /** What the capture side reports: rows taken from the sources, counted from when it opened. */
    private static CaptureReading readSoFar() {
        return new CaptureReading(
                Map.of("orders", Map.of("i", 1_400L, "u", 20L), "items", Map.of("d", 180L)),
                Map.of("orders", 260_000L, "items", 9_000L),
                READING_SINCE);
    }

    /** What a two-table run reports: rows confirmed by their targets, and how recent the newest are. */
    private static DeliveryReading twoTables() {
        return new DeliveryReading(
                Map.of("orders", Map.of("i", 1_180L, "u", 20L), "items", Map.of("d", 38L)),
                Map.of("orders", 218_000L, "items", 1_900L),
                Map.of("orders", AT.minusSeconds(2).toEpochMilli(),
                        "items", AT.minusSeconds(47).toEpochMilli()),
                STARTED,
                Map.of("orders", ORDERS_TOOK));
    }

    /** Twelve hundred orders rows, all of them between a quarter and half a second from stamp to confirmation. */
    private static final HistogramValue ORDERS_TOOK = HistogramBounds.RECORD_DELIVERY_DURATION.value(
            1_200L, 420.0, bucketsWith(5, 1_200L));

    private static List<Long> bucketsWith(int bucket, long count) {
        List<Long> counts = new ArrayList<>();
        for (int index = 0; index < HistogramBounds.RECORD_DELIVERY_DURATION.buckets(); index++) {
            counts.add(index == bucket ? count : 0L);
        }
        return counts;
    }

    @Test
    @DisplayName("how long rows took arrives as a distribution per table, over the registered bounds")
    void howLongRowsTookArrivesAsAHistogramPerTable() {
        MetricFact took = factNamed(facts(twoTables(), AT), "tapstate.pipeline.record.delivery.duration");

        assertThat(took.type()).isEqualTo(MetricType.HISTOGRAM);
        assertThat(took.unit()).isEqualTo("s");
        assertThat(took.points()).singleElement().satisfies(point -> {
            assertThat(point.attributes()).isEqualTo(
                    Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "orders"));
            // Accumulated from the same start as the counts: it is over the same rows.
            assertThat(point.startTime()).isEqualTo(STARTED);
            assertThat(point.observedAt()).isEqualTo(AT);
            assertThat(point.histogram()).isEqualTo(ORDERS_TOOK);
        });
    }

    @Test
    @DisplayName("rows confirmed by a target arrive as one counter carrying its dimensions")
    void theRowsThatLandedArriveAsACounterBrokenOutByTableAndOperation() {
        MetricFact records = factNamed(facts(twoTables(), AT), "tapstate.pipeline.records");

        assertThat(records.type()).isEqualTo(MetricType.COUNTER);
        assertThat(records.unit()).isEqualTo("{record}");
        assertThat(records.points()).extracting(MetricPoint::attributes)
                .containsExactlyInAnyOrder(
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "orders",
                                "direction", "out", "op", "insert"),
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "orders",
                                "direction", "out", "op", "update"),
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "items",
                                "direction", "out", "op", "delete"));
    }

    @Test
    @DisplayName("the counter says what it accumulates from, which is the run's own start")
    void theCounterCarriesTheStartTheRunReported() {
        MetricFact records = factNamed(facts(twoTables(), AT), "tapstate.pipeline.records");

        // Without this a consumer cannot tell a restart from a count going backwards, and every family
        // already on this face is a reading rather than a counter for exactly that want.
        assertThat(records.points()).isNotEmpty().allSatisfy(
                point -> assertThat(point.startTime()).isEqualTo(STARTED));
    }

    @Test
    @DisplayName("the payload that crossed arrives as a counter of bytes, per table and direction")
    void thePayloadThatCrossedArrivesAsACounterBrokenOutByTableAndDirection() {
        MetricFact bytes = factNamed(facts(twoTables(), AT), "tapstate.pipeline.bytes");

        assertThat(bytes.type()).isEqualTo(MetricType.COUNTER);
        assertThat(bytes.unit()).isEqualTo("By");
        // No operation among the attributes, unlike the counter beside it. What a reader does with bytes
        // is compare the ends or divide by the rows, and which operation produced a row says nothing
        // about how much of it there was -- a dimension nothing reads is one nothing would notice going
        // wrong. Asserted as the whole attribute map rather than as a missing key, so an operation added
        // later fails here instead of quietly multiplying the series.
        assertThat(bytes.points()).extracting(MetricPoint::attributes)
                .containsExactlyInAnyOrder(
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "orders",
                                "direction", "out"),
                        Map.of("tapstate.pipeline.id", "orders", "tapstate.table.id", "items",
                                "direction", "out"));
        assertThat(bytes.points()).extracting(
                        point -> point.attributes().get("tapstate.table.id"), MetricPoint::value)
                .containsExactlyInAnyOrder(tuple2("orders", 218_000L), tuple2("items", 1_900L));
    }

    @Test
    @DisplayName("both ends of the payload are one measurement told apart by an attribute")
    void thePayloadAtEachEndIsOneMeasurementRatherThanTwo() {
        MetricFact bytes = factNamed(facts(readSoFar(), twoTables(), AT), "tapstate.pipeline.bytes");

        // One name for both ends, for the reason the rows have one: what a reader wants is the comparison,
        // and two names could be defined or counted differently without anything noticing.
        assertThat(bytes.points()).extracting(point -> point.attributes().get("direction"))
                .containsExactlyInAnyOrder("in", "in", "out", "out");
    }

    @Test
    @DisplayName("each end's payload accumulates from the same moment as the rows counted there")
    void thePayloadSharesTheStartOfTheCountsTakenAtItsOwnEnd() {
        List<MetricFact> facts = facts(readSoFar(), twoTables(), AT);
        MetricFact bytes = factNamed(facts, "tapstate.pipeline.bytes");

        // The two ends do not share a start with each other -- the sources open before the job that
        // writes to them -- but each end's two totals must share one, or bytes per row works out over a
        // window that is not either total's window.
        assertThat(bytes.points()).isNotEmpty().allSatisfy(point -> assertThat(point.startTime()).isEqualTo(
                "in".equals(point.attributes().get("direction")) ? READING_SINCE : STARTED));
    }

    @Test
    @DisplayName("a table whose rows carried no payload is present at nought, not absent")
    void aTableThatMovedRowsOfNoPayloadSaysSoRatherThanGoingMissing() {
        DeliveryReading schemaOnly = new DeliveryReading(Map.of("orders", Map.of("ddl", 3L)),
                Map.of("orders", 0L), Map.of("orders", AT.toEpochMilli()), STARTED);

        // Nought here is measured, not absent, and the difference is the whole of what absence means on
        // this face: a table nothing arrived for is missing, and a table whose arrivals carried no row --
        // which is what a stream of schema changes is -- arrived and weighed nothing.
        assertThat(valueFor(factNamed(facts(schemaOnly, AT), "tapstate.pipeline.bytes"), "orders"))
                .isZero();
    }

    @Test
    @DisplayName("the flat view carries the payload per direction, collapsed over the tables")
    void theFlatFaceKeepsThePayloadsDirectionsApart() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherAt(readSoFar(), twoTables(), AT).publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        // 260000 + 9000 in, 218000 + 1900 out: the tables collapse the way they do for the rows, and the
        // direction does not, because these two numbers are meant to be subtracted.
        assertThat(published.metrics()).contains(
                Map.entry("bytes.in", 269_000L), Map.entry("bytes.out", 219_900L));
    }

    @Test
    @DisplayName("how far behind a table is, is worked out at the moment it is asked")
    void theAgeIsTakenAgainstTheClockOfWhoeverAsks() {
        MetricFact lag = factNamed(facts(twoTables(), AT), "tapstate.pipeline.lag");

        assertThat(lag.type()).isEqualTo(MetricType.GAUGE);
        assertThat(lag.unit()).isEqualTo("s");
        assertThat(lag.points()).extracting(point -> point.attributes().get("tapstate.table.id"),
                        MetricPoint::value)
                .containsExactlyInAnyOrder(tuple2("orders", 2L), tuple2("items", 47L));
    }

    @Test
    @DisplayName("a pipeline that stops moving goes on falling behind, with nothing new arriving")
    void theAgeKeepsGrowingWhileTheSameRowStaysTheNewest() {
        DeliveryReading stalled = twoTables();

        long soon = valueFor(factNamed(facts(stalled, AT), "tapstate.pipeline.lag"), "orders");
        long later = valueFor(
                factNamed(facts(stalled, AT.plusSeconds(600)), "tapstate.pipeline.lag"), "orders");

        // Identical readings from the run, ten minutes apart. An age worked out where the row settled
        // would be the same number twice, and a pipeline that died would read as healthy for as long as it
        // stayed dead.
        assertThat(soon).isEqualTo(2L);
        assertThat(later).isEqualTo(602L);
    }

    @Test
    @DisplayName("a source clock ahead of ours reads as caught up, never as a negative age")
    void anEventFromTheFutureIsNotANegativeAge() {
        DeliveryReading ahead = new DeliveryReading(Map.of("orders", Map.of("i", 1L)), Map.of(),
                Map.of("orders", AT.plusSeconds(90).toEpochMilli()), STARTED);

        assertThat(valueFor(factNamed(facts(ahead, AT), "tapstate.pipeline.lag"), "orders")).isZero();
    }

    @Test
    @DisplayName("a snapshot read keeps its own operation, because that is what the source did")
    void aSnapshotReadIsNotFoldedIntoTheInsertsBesideIt() {
        DeliveryReading loading = new DeliveryReading(Map.of("orders", Map.of("r", 5_000L)), Map.of(),
                Map.of("orders", AT.toEpochMilli()), STARTED);

        MetricFact records = factNamed(facts(loading, AT), "tapstate.pipeline.records");

        // This attribute carries what the source did, never what the target did with the row afterwards,
        // and at the source nothing was inserted - a row that was already there was read. Not "other"
        // either: that is for a kind nothing recognises, and a reader meeting it has been told only that
        // somebody gave up.
        assertThat(records.points()).singleElement()
                .satisfies(point -> assertThat(point.attributes().get("op")).isEqualTo("read"));
    }

    @Test
    @DisplayName("loading a table and tailing it are two series, so either can be read on its own")
    void theInitialLoadAndTheChangesAfterItStayApart() {
        DeliveryReading both = new DeliveryReading(Map.of("orders", Map.of("r", 5_000L, "i", 7L)),
                Map.of(), Map.of("orders", AT.toEpochMilli()), STARTED);

        MetricFact records = factNamed(facts(both, AT), "tapstate.pipeline.records");

        // Added together, an operator sizing a source could not tell five thousand rows that were already
        // there from seven that have just come into existence - and the second number is the one that says
        // what this pipeline will be doing for the rest of its life.
        assertThat(records.points())
                .extracting(point -> point.attributes().get("op"), MetricPoint::value)
                .containsExactlyInAnyOrder(tuple2("read", 5_000L), tuple2("insert", 7L));
    }

    @Test
    @DisplayName("two kinds nobody recognises share the one name kept for them, and are added up")
    void unrecognisedKindsLandOnOneNameAndTheirRowsAreSummed() {
        DeliveryReading strange = new DeliveryReading(Map.of("orders", Map.of("zzz", 3L, "qqq", 4L)),
                Map.of(), Map.of("orders", AT.toEpochMilli()), STARTED);

        MetricFact records = factNamed(facts(strange, AT), "tapstate.pipeline.records");

        // The name map is deliberately not injective, so this is the shape that produces two points with
        // identical attributes - two values of one series, which a fact refuses outright. Summing them is
        // what the counter means; without it this throws rather than publishing anything at all.
        assertThat(records.points()).singleElement().satisfies(point -> {
            assertThat(point.attributes().get("op")).isEqualTo("other");
            assertThat(point.value()).isEqualTo(7L);
        });
    }

    @Test
    @DisplayName("one kind nobody recognises is carried under the name kept for exactly that")
    void anUnknownChangeKindDoesNotLeakOntoTheFaceAsItsOwnName() {
        DeliveryReading strange = new DeliveryReading(Map.of("orders", Map.of("zzz", 3L)), Map.of(),
                Map.of("orders", AT.toEpochMilli()), STARTED);

        MetricFact records = factNamed(facts(strange, AT), "tapstate.pipeline.records");

        // The whole point of the operation being a closed set is that nothing arriving from the data can
        // add a value to it. A symbol passed through as its own name would put the number of series this
        // metric has in the hands of whatever produced the symbol.
        assertThat(records.points()).singleElement()
                .satisfies(point -> assertThat(point.attributes().get("op")).isEqualTo("other"));
    }

    @Test
    @DisplayName("a run that started but has settled nothing publishes no count at all")
    void aRunWithAStartAndNoRowsPublishesNoCount() {
        DeliveryReading startedOnly = new DeliveryReading(Map.of(), Map.of(), Map.of(), STARTED);

        // A start on its own is not a delivery. Publishing the counter here would put a pipeline that has
        // moved nothing on the face as one whose total happens to be zero, and the two differ by whether
        // anybody should be worried.
        assertThat(facts(startedOnly, AT)).extracting(MetricFact::name)
                .doesNotContain("tapstate.pipeline.records", "tapstate.pipeline.bytes",
                        "tapstate.pipeline.lag");
    }

    @Test
    @DisplayName("the stored flat view carries a total per direction and an age per table")
    void theFlatFaceShowsTheThroughputCollapsedAndTheAgePerTable() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherAt(twoTables(), AT).publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        // 1180 + 20 + 38: every table and operation added together, because a person at a command line
        // asking how fast a pipeline is going is asking about the pipeline.
        assertThat(published.metrics()).contains(
                Map.entry("records.out", 1_238L),
                Map.entry("lag.orders", 2L),
                Map.entry("lag.items", 47L));
        // The families that were already here are untouched by any of it. errorCount is not among them
        // any more: a state written as a number is not a count, and the failures it stood in for are
        // counted per code where one is witnessed.
        assertThat(published.metrics()).containsKeys("recordCount");
    }

    @Test
    @DisplayName("the flat view reports the collapse rather than performing it quietly")
    void theCollapseIsOnTheRecordAndIsNotADrop() {
        FlatMetricProjection projected = projectionOf(facts(twoTables(), AT));

        // The driven count is squeezed too: it names the pipeline it is about, as every canonical fact does,
        // and reaches the flat face under its bare key through the same reductions as the three movement
        // measurements. Listed here so that a fact arriving with an attribute and no flat spelling is a
        // change to this list, never a quiet drop.
        assertThat(projected.reduced()).containsExactlyInAnyOrder("tapstate.pipeline.records",
                "tapstate.pipeline.bytes", "tapstate.pipeline.lag", "tapstate.pipeline.records.driven");
        // The one measurement this face cannot carry: a distribution has no single number to be, and the
        // flat face refuses it a rule rather than squeezing it. It is read on the facts, which travel on
        // the same document, so the drop is a drop from this projection and not from what was measured.
        assertThat(projected.dropped()).containsExactly("tapstate.pipeline.record.delivery.duration");
    }

    @Test
    @DisplayName("a run reporting nothing publishes neither measurement, rather than either at zero")
    void aPipelineWithNoRunIsAbsentFromBothRatherThanPresentAtZero() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherAt(DeliveryReading.NONE, AT).publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        // Absent, not zero. A pipeline that has delivered nothing and one whose delivery counting is not
        // wired want opposite responses, and a published zero spells them the same way.
        assertThat(published.metrics()).doesNotContainKeys("records.out", "lag.orders");
        assertThat(facts(DeliveryReading.NONE, AT)).extracting(MetricFact::name)
                .doesNotContain("tapstate.pipeline.records", "tapstate.pipeline.lag");
    }

    @Test
    @DisplayName("what a source handed over and what a target confirmed are one counter, told apart by direction")
    void bothEndsOfTheCrossingArriveAsOneMeasurement() {
        MetricFact records = factNamed(facts(readSoFar(), twoTables(), AT), "tapstate.pipeline.records");

        // One metric with an attribute, not two metrics. What a reader wants from these is the difference
        // between them - everything read and not yet confirmed is in flight - and two separate names could
        // be defined, counted or published differently without anything noticing.
        assertThat(records.points())
                .extracting(point -> point.attributes().get("direction"),
                        point -> point.attributes().get("tapstate.table.id"),
                        point -> point.attributes().get("op"),
                        MetricPoint::value)
                .contains(org.assertj.core.groups.Tuple.tuple("in", "orders", "insert", 1_400L),
                        org.assertj.core.groups.Tuple.tuple("out", "orders", "insert", 1_180L));
    }

    @Test
    @DisplayName("each end counts from its own start, because they did not begin at the same moment")
    void theTwoDirectionsCarryTheirOwnStarts() {
        MetricFact records = factNamed(facts(readSoFar(), twoTables(), AT), "tapstate.pipeline.records");

        // The capture opens its account when the sources are opened; the targets open theirs when the job
        // that writes to them starts, which is strictly later. One start over both would misdate whichever
        // it was not taken from, and a rate is computed against exactly this.
        assertThat(records.points()).filteredOn(point -> "in".equals(point.attributes().get("direction")))
                .allSatisfy(point -> assertThat(point.startTime()).isEqualTo(READING_SINCE));
        assertThat(records.points()).filteredOn(point -> "out".equals(point.attributes().get("direction")))
                .allSatisfy(point -> assertThat(point.startTime()).isEqualTo(STARTED));
    }

    @Test
    @DisplayName("a pipeline reading steadily and confirming nothing says so on the face")
    void whatIsBeingReadIsVisibleWithNothingConfirmedYet() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherAt(readSoFar(), DeliveryReading.NONE, AT).publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        // The whole reason both ends are measured. On the target side alone this pipeline is
        // indistinguishable from one whose source has nothing to send; here it is plainly reading and
        // plainly landing none of it.
        assertThat(published.metrics()).contains(Map.entry("records.in", 1_600L));
        assertThat(published.metrics()).doesNotContainKey("records.out");
    }

    @Test
    @DisplayName("the flat view carries a total for each direction")
    void theFlatFaceKeepsTheDirectionsApartAfterCollapsingEverythingElse() {
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisherAt(readSoFar(), twoTables(), AT).publish("orders");

        Observation published = observations.read("orders").orElseThrow();
        // Table and operation collapse into the pipeline's own throughput; direction does not, because the
        // two numbers subtract and a single total would answer a question nobody asked.
        assertThat(published.metrics()).contains(
                Map.entry("records.in", 1_600L), Map.entry("records.out", 1_238L));
    }

    @Test
    @DisplayName("a capture that has opened its account and read nothing publishes no count")
    void aCaptureWithAStartAndNoRowsPublishesNoCount() {
        CaptureReading openedOnly = new CaptureReading(Map.of(), Map.of(), READING_SINCE);

        // Symmetrical with the target side, and for the same reason: a start on its own is not an arrival,
        // and a published zero spells "has read nothing" the same way as "is not being measured".
        assertThat(facts(openedOnly, DeliveryReading.NONE, AT)).extracting(MetricFact::name)
                .doesNotContain("tapstate.pipeline.records");
    }

    @Test
    @DisplayName("a load counts on the source side as the read it was there too")
    void theOperationIsTheSourcesOwnOnTheWayInAsWell() {
        CaptureReading loading =
                new CaptureReading(Map.of("orders", Map.of("r", 5_000L)), Map.of(), READING_SINCE);

        MetricFact records = factNamed(facts(loading, DeliveryReading.NONE, AT), "tapstate.pipeline.records");

        // One name map serves both ends. Were the two to spell an operation differently, the subtraction
        // the directions exist for would be between series that do not line up.
        assertThat(records.points()).singleElement().satisfies(point -> {
            assertThat(point.attributes().get("op")).isEqualTo("read");
            assertThat(point.attributes().get("direction")).isEqualTo("in");
        });
    }

    @Test
    @DisplayName("no change kind the engine has lands on the name kept for kinds nothing recognises")
    void everyOperationTheEngineProducesReachesTheFaceAsItself() {
        Set<String> names = new HashSet<>();
        int checked = 0;
        for (Op op : Op.values()) {
            CaptureReading one =
                    new CaptureReading(Map.of("orders", Map.of(op.symbol(), 1L)), Map.of(), READING_SINCE);

            MetricFact records =
                    factNamed(facts(one, DeliveryReading.NONE, AT), "tapstate.pipeline.records");

            String name = (String) records.points().get(0).attributes().get("op");
            // "other" is for a symbol arriving from outside the engine's own set. A kind the engine
            // itself produces reaching it would tell a reader only that somebody gave up - and it would
            // arrive the day somebody adds a kind, silently, with nothing to notice a list gone stale.
            assertThat(name).as("%s reaches the face as itself", op).isNotEqualTo("other");
            assertThat(name).as("%s is named at all", op).isNotBlank();
            names.add(name);
            checked++;
        }
        // The witness the loop above needs: an empty set of kinds would satisfy every assertion in it.
        assertThat(checked).isEqualTo(Op.values().length).isNotZero();
        // And distinct, which the assertions inside the loop cannot see one at a time. Two kinds sharing
        // a name would be two series added together, and the sum reads exactly like a healthy single one.
        assertThat(names).hasSize(checked);
    }

    private List<MetricFact> facts(DeliveryReading reading, Instant at) {
        return facts(CaptureReading.NONE, reading, at);
    }

    private List<MetricFact> facts(CaptureReading captured, DeliveryReading delivered, Instant at) {
        return publisherAt(captured, delivered, at).facts("orders", PipelineState.RUNNING, at,
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), SnapshotReading.NONE);
    }

    /**
     * The publisher's own reductions, not a copy of them: a copy kept here answered for three families
     * while the publisher had grown a fourth, and reported that fourth as dropped when it was squeezed.
     */
    private static FlatMetricProjection projectionOf(List<MetricFact> facts) {
        return FlatMetricProjection.of(facts, ObservationPublisher.FLAT_REDUCTIONS);
    }

    private ObservationPublisher publisherAt(DeliveryReading reading, Instant at) {
        return publisherAt(CaptureReading.NONE, reading, at);
    }

    private ObservationPublisher publisherAt(
            CaptureReading captured, DeliveryReading delivered, Instant at) {
        return new ObservationPublisher(state, observations,
                id -> OptionalLong.of(128_500L),
                id -> Map.of(), id -> SnapshotReading.NONE, id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(FrontierStallPressure.DEFAULT, FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of(),
                id -> captured,
                id -> delivered,
                Clock.fixed(at, ZoneOffset.UTC));
    }

    private static MetricFact factNamed(List<MetricFact> facts, String name) {
        return facts.stream().filter(fact -> fact.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("no fact named " + name + " among "
                        + facts.stream().map(MetricFact::name).toList()));
    }

    private static long valueFor(MetricFact fact, String table) {
        return fact.points().stream()
                .filter(point -> table.equals(point.attributes().get("tapstate.table.id")))
                .findFirst().orElseThrow().value();
    }

    private static org.assertj.core.groups.Tuple tuple2(String table, long value) {
        return org.assertj.core.groups.Tuple.tuple(table, value);
    }

    /** Keeps the latest observation written, which is what a read face would find. */
    private static final class CapturingObservationStore implements ObservationStore {

        private final Map<String, Observation> saved = new HashMap<>();

        @Override
        public void save(Observation observation) {
            saved.put(observation.pipelineId(), observation);
        }

        @Override
        public Optional<Observation> read(String pipelineId) {
            return Optional.ofNullable(saved.get(pipelineId));
        }

        @Override
        public void delete(String pipelineId) {
            saved.remove(pipelineId);
        }
    }
}
