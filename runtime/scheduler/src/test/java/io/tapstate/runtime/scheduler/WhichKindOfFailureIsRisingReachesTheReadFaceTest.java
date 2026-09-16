package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which kind of failure is rising, which is the question the shape this replaces could not be asked at all.
 *
 * <p>What stood here before was {@code errorCount}: the pipeline's own state written as a number, one while
 * it was FAILED and nought otherwise. Three things are wrong with that and only the third is obvious. It
 * cannot say how many times anything happened. It goes back down when the pipeline recovers, so anything
 * reading it as a count sees a counter run backwards. And it has no room for the one dimension that makes
 * the number actionable -- a total number of errors is not something an operator can do anything with,
 * while "the connector write failures are climbing and nothing else is" is.
 *
 * <p>One death is one count, and the arrangement that makes that true is worth stating: the converge side
 * hands over a cause only on the pass that witnessed the death, and every later pass over a pipeline still
 * sitting in FAILED hands over nothing. So the counter does not need to know whether it has already
 * counted this failure -- there is nothing to count twice.
 */
class WhichKindOfFailureIsRisingReachesTheReadFaceTest {

    private static final Instant AT = Instant.parse("2026-09-17T12:00:00Z");
    private static final String ERRORS = "tapstate.pipeline.errors";

    private static final ObservationFailure WRITE_FAILED =
            new ObservationFailure("connector.write-failed", Map.of());
    private static final ObservationFailure JOB_FAILED =
            new ObservationFailure("engine.job-failed", Map.of());

    private final InMemoryStateStore state = new InMemoryStateStore();
    private final SavedObservations observations = new SavedObservations();

    @Test
    @DisplayName("a witnessed failure arrives as a counter carrying the code that names it")
    void aFailureArrivesAsACounterBrokenOutByCode() {
        ObservationPublisher publisher = publisher();
        state.create("orders", PipelineState.FAILED.name(), AT);

        publisher.publish("orders", WRITE_FAILED);

        MetricFact errors = factNamed(publisher, ERRORS);
        assertThat(errors.type()).isEqualTo(MetricType.COUNTER);
        assertThat(errors.unit()).isEqualTo("{error}");
        assertThat(errors.points()).singleElement().satisfies(point -> {
            assertThat(point.value()).isEqualTo(1L);
            assertThat(point.attributes()).containsOnly(
                    Map.entry("tapstate.pipeline.id", "orders"),
                    Map.entry("code", "connector.write-failed"));
            assertThat(point.startTime()).isNotNull();
        });
    }

    @Test
    @DisplayName("a pipeline left sitting in FAILED is not counted again on every later pass")
    void oneDeathIsCountedOnceHoweverLongItStaysDead() {
        ObservationPublisher publisher = publisher();
        state.create("orders", PipelineState.FAILED.name(), AT);

        publisher.publish("orders", WRITE_FAILED);
        // Three more passes over a pipeline that is still dead. The converge side holds the cause only on
        // the pass that saw it, so these carry none -- which is exactly the arrangement being pinned: were
        // the count taken from the state instead of from the witness, it would climb once per tick forever.
        publisher.publish("orders", null);
        publisher.publish("orders", null);
        publisher.publish("orders", null);

        assertThat(factNamed(publisher, ERRORS).points()).singleElement()
                .satisfies(point -> assertThat(point.value()).isEqualTo(1L));
    }

    @Test
    @DisplayName("two kinds of failure are two series, not one total")
    void failuresOfDifferentKindsAccumulateApart() {
        ObservationPublisher publisher = publisher();
        state.create("orders", PipelineState.FAILED.name(), AT);

        publisher.publish("orders", WRITE_FAILED);
        publisher.publish("orders", JOB_FAILED);
        publisher.publish("orders", WRITE_FAILED);

        assertThat(factNamed(publisher, ERRORS).points())
                .extracting(point -> point.attributes().get("code"), point -> point.value())
                .containsExactlyInAnyOrder(
                        org.assertj.core.groups.Tuple.tuple("connector.write-failed", 2L),
                        org.assertj.core.groups.Tuple.tuple("engine.job-failed", 1L));
    }

    @Test
    @DisplayName("a pipeline that has failed nothing publishes no failure metric at all")
    void nothingFailedPublishesNothingRatherThanAZero() {
        ObservationPublisher publisher = publisher();
        state.create("orders", PipelineState.RUNNING.name(), AT);

        publisher.publish("orders", null);

        // Absent, not nought -- and the cell that used to be here unconditionally, errorCount, is gone
        // rather than renamed: it was the pipeline's state wearing a count's name.
        assertThat(observations.read("orders").orElseThrow().metrics()).doesNotContainKey("errorCount");
        assertThat(facts(publisher)).extracting(MetricFact::name).doesNotContain(ERRORS);
    }

    @Test
    @DisplayName("the flat face carries the failures collapsed by code, and says it collapsed them")
    void theFlatFaceKeepsACellPerCodeAndRecordsTheCollapse() {
        ObservationPublisher publisher = publisher();
        state.create("orders", PipelineState.FAILED.name(), AT);

        publisher.publish("orders", WRITE_FAILED);
        publisher.publish("orders", JOB_FAILED);

        Map<String, Long> published = observations.read("orders").orElseThrow().metrics();
        // Reduced onto this face rather than dropped from it. The load's two measurements are dropped
        // because the same observation carries them as its snapshot dataset; failures have no second face,
        // so dropping them would leave them measured and readable nowhere.
        assertThat(published)
                .containsEntry("errors.connector.write-failed", 1L)
                .containsEntry("errors.engine.job-failed", 1L);
    }

    @Test
    @DisplayName("a reconcile streak is published as a streak, under a name that says so")
    void theReconcileStreakIsNotSpelledAsACount() {
        ObservationPublisher publisher = publisher();

        publisher.publishReconcileFailure("orders", 4L);

        Map<String, Long> published = observations.read("orders").orElseThrow().metrics();
        assertThat(published).containsEntry("reconcileFailuresInARow", 4L);
        // It is not a count and no longer shares a key with one. One clean pass returns it to nothing, and
        // the command line reports it as "N passes in a row have thrown" -- a name saying count would have
        // that sentence start lying the day somebody believed the name.
        assertThat(published).doesNotContainKey("errorCount");
        assertThat(published).doesNotContainKey("tapstate.pipeline.errors");
    }

    @Test
    @DisplayName("a deleted pipeline's failures are forgotten; a stopped one's are kept")
    void onlyAPipelineThatNoLongerExistsIsForgotten() {
        ObservationPublisher publisher = publisher();
        state.create("kept", PipelineState.FAILED.name(), AT);
        state.create("deleted", PipelineState.FAILED.name(), AT);
        publisher.publish("kept", WRITE_FAILED);
        publisher.publish("deleted", WRITE_FAILED);

        // What crosses once a tick is the set of pipelines that still have a stored intent. A stopped
        // pipeline keeps its intent and so stays in it; only the reclaim of the pipeline itself removes
        // one. So this is "deleted was deleted", not "deleted was stopped".
        publisher.forgetPipelinesOutside(List.of("kept"));

        publisher.publish("kept", null);
        assertThat(factNamed(publisher, "kept", ERRORS).points()).singleElement()
                .satisfies(point -> assertThat(point.attributes().get("tapstate.pipeline.id"))
                        .isEqualTo("kept"));
        assertThat(facts(publisher, "deleted")).extracting(MetricFact::name).doesNotContain(ERRORS);
        publisher.publish("deleted", null);
        assertThat(observations.read("deleted").orElseThrow().metrics())
                .doesNotContainKey("errors.connector.write-failed");
    }

    private ObservationPublisher publisher() {
        return new ObservationPublisher(state, observations,
                id -> java.util.OptionalLong.empty(), id -> Map.of(),
                id -> io.tapstate.core.lifecycle.SnapshotReading.NONE, id -> Map.of(), id -> Map.of(),
                new NestColdLayerWatch(io.tapstate.core.lifecycle.NestColdLayerPressure.DEFAULT,
                        NestColdLayerAlert.NONE),
                id -> Map.of(),
                new FrontierStallWatch(io.tapstate.core.lifecycle.FrontierStallPressure.DEFAULT,
                        FrontierStallAlert.NONE),
                id -> Map.of(), id -> Map.of(), id -> Map.of(),
                Clock.fixed(AT, ZoneOffset.UTC));
    }

    private List<MetricFact> facts(ObservationPublisher publisher) {
        return facts(publisher, "orders");
    }

    private List<MetricFact> facts(ObservationPublisher publisher, String pipelineId) {
        return publisher.facts(pipelineId, PipelineState.FAILED, AT,
                Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
                io.tapstate.core.lifecycle.SnapshotReading.NONE);
    }

    private MetricFact factNamed(ObservationPublisher publisher, String name) {
        return factNamed(publisher, "orders", name);
    }

    private MetricFact factNamed(ObservationPublisher publisher, String pipelineId, String name) {
        List<MetricFact> measured = facts(publisher, pipelineId);
        return measured.stream().filter(fact -> fact.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("no fact named " + name + " among "
                        + measured.stream().map(MetricFact::name).toList()));
    }

    /** An observation store that keeps the latest projection per pipeline, so a case can read it back. */
    private static final class SavedObservations implements ObservationStore {

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
