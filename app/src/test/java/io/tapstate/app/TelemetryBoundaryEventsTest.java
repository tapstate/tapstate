package io.tapstate.app;

import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryBoundaryEventsTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-28T01:00:00Z"), ZoneOffset.UTC);
    private static final ObservationStore.Scope OLD = new ObservationStore.Scope("inc-a", 41);
    private static final ObservationStore.Scope NEXT = new ObservationStore.Scope("inc-b", 42);

    @Test
    void repeatedFailuresAndHealthyWritesProduceOnlyBoundaryPairs() {
        List<PipelineEvent> events = new ArrayList<>();
        TelemetryBoundaryEvents boundaries = new TelemetryBoundaryEvents(2, CLOCK,
                (id, scope) -> OLD.equals(scope), events::add, event -> { throw new AssertionError("full"); });
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.LATEST);
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.LATEST);
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.LATEST, System.nanoTime());
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.LATEST, System.nanoTime());
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.LATEST);
        assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED,
                PipelineEvent.Kind.TELEMETRY_DEGRADED);
        assertThat(events).extracting(PipelineEvent::id).doesNotHaveDuplicates();
        assertThat(events).allSatisfy(event -> {
            assertThat(event.pipelineId()).isEqualTo("orders");
            assertThat(event.pipelineIncarnationId()).isEqualTo("inc-a");
            assertThat(event.executionGeneration()).isEqualTo(41);
            assertThat(event.reason()).isEqualTo("latest observation write");
        });
    }

    @Test
    void oldCallbacksCannotRestoreANewResourcesDegradedEpisode() {
        List<PipelineEvent> events = new ArrayList<>();
        AtomicReference<ObservationStore.Scope> current = new AtomicReference<>(OLD);
        TelemetryBoundaryEvents boundaries = new TelemetryBoundaryEvents(1, CLOCK,
                (id, scope) -> current.get().equals(scope), events::add,
                event -> { throw new AssertionError("obsolete owner should free capacity"); });
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.HISTORY);
        current.set(NEXT);
        boundaries.failed("orders", NEXT, TelemetryDispatcher.Sink.HISTORY);
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.HISTORY, System.nanoTime());
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.HISTORY);
        boundaries.succeeded("orders", NEXT, TelemetryDispatcher.Sink.HISTORY, System.nanoTime());
        assertThat(events).extracting(PipelineEvent::executionGeneration).containsExactly(41L, 42L, 42L);
        assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_DEGRADED,
                PipelineEvent.Kind.TELEMETRY_RESTORED);
    }

    @Test
    void aFullBoundaryBudgetReportsLossAndDeletionFreesItsSlot() {
        List<PipelineEvent> events = new ArrayList<>();
        List<PipelineEvent> lost = new ArrayList<>();
        TelemetryBoundaryEvents boundaries = new TelemetryBoundaryEvents(1, CLOCK,
                (id, scope) -> true, events::add, lost::add);
        boundaries.failed("one", OLD, TelemetryDispatcher.Sink.LATEST);
        boundaries.failed("two", OLD, TelemetryDispatcher.Sink.LATEST);
        assertThat(events).extracting(PipelineEvent::pipelineId).containsExactly("one");
        assertThat(lost).extracting(PipelineEvent::pipelineId).containsExactly("two");
        boundaries.retain(List.of("two"));
        boundaries.failed("two", OLD, TelemetryDispatcher.Sink.LATEST);
        assertThat(events).extracting(PipelineEvent::pipelineId).containsExactly("one", "two");
    }

    @Test
    void eachSinkRecoversSeparatelyAndLegacyFramesInventNoIdentity() {
        List<PipelineEvent> events = new ArrayList<>();
        TelemetryBoundaryEvents boundaries = new TelemetryBoundaryEvents(1, CLOCK,
                (id, scope) -> true, events::add, event -> { throw new AssertionError("full"); });
        boundaries.failed("legacy", null, TelemetryDispatcher.Sink.LATEST);
        boundaries.succeeded("legacy", null, TelemetryDispatcher.Sink.LATEST, System.nanoTime());
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.LATEST);
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.HISTORY);
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.EXPORT);
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.HISTORY, System.nanoTime());
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.LATEST, System.nanoTime());
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.EXPORT, System.nanoTime());
        assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_DEGRADED,
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED,
                PipelineEvent.Kind.TELEMETRY_RESTORED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        assertThat(events).extracting(PipelineEvent::reason).containsExactly(
                "latest observation write", "history sample", "local metrics export offer",
                "history sample", "latest observation write", "local metrics export offer");
    }

    @Test
    void anOlderOfferCannotRestoreLossObservedAfterItStarted() {
        List<PipelineEvent> events = new ArrayList<>();
        TelemetryBoundaryEvents boundaries = new TelemetryBoundaryEvents(1, CLOCK,
                (id, scope) -> true, events::add, event -> { throw new AssertionError("full"); });
        long startedBeforeLoss = System.nanoTime();
        boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.EXPORT);
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.EXPORT, startedBeforeLoss);
        assertThat(events).extracting(PipelineEvent::kind).containsExactly(PipelineEvent.Kind.TELEMETRY_DEGRADED);
        boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.EXPORT, System.nanoTime());
        assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
    }

    @Test
    void sameMillisecondQueryKeysKeepTheActualTransitionOrderWithoutRetimingEvents() {
        List<PipelineEvent> events = new ArrayList<>();
        TelemetryBoundaryEvents boundaries = new TelemetryBoundaryEvents(1, CLOCK,
                (id, scope) -> true, events::add, event -> { throw new AssertionError("full"); });
        for (int episode = 0; episode < 20; episode++) {
            boundaries.failed("orders", OLD, TelemetryDispatcher.Sink.LATEST);
            boundaries.succeeded("orders", OLD, TelemetryDispatcher.Sink.LATEST, System.nanoTime());
        }
        assertThat(events).hasSize(40).allSatisfy(event ->
                assertThat(event.occurredAt()).isEqualTo(CLOCK.instant()));
        assertThat(events.stream().sorted(Comparator.comparing(PipelineEvent::occurredAt)
                .thenComparing(PipelineEvent::id)).toList()).containsExactlyElementsOf(events);
    }
}
