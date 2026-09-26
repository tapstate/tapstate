package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.PipelineEventStore;
import io.tapstate.spi.store.RateHistoryStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class PipelineEventsQueryServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-20T11:00:00Z");
    private static final Instant FROM = Instant.parse("2026-09-20T10:00:00Z");
    private static final byte[] SECRET = "events-test-secret".getBytes(StandardCharsets.UTF_8);

    @Test
    void sameTimestampPagesStayScopedAndOnlyTheReturnedMarkerProjectsAKnownGap() {
        RecordingEvents store = new RecordingEvents();
        store.append(event("ev-b", "inc-a", FROM.plusSeconds(10), PipelineEvent.Kind.STATE_CHANGED));
        store.append(event("ev-a", "inc-a", FROM.plusSeconds(10), PipelineEvent.Kind.STATE_CHANGED));
        store.append(event("ev-old", "inc-old", FROM.plusSeconds(10), PipelineEvent.Kind.STATE_CHANGED));
        PipelineEvent.Gap gap = new PipelineEvent.Gap(FROM.minusSeconds(120), FROM.plusSeconds(20),
                List.of(PipelineEvent.GapReason.WRITE_FAILURE, PipelineEvent.GapReason.QUEUE_FULL));
        store.append(new PipelineEvent("ev-g", "orders", "inc-a", 42,
                PipelineEvent.Kind.TELEMETRY_GAP, FROM.plusSeconds(30), null, null, null, null, gap));
        store.append(new PipelineEvent("ev-outside", "orders", "inc-a", 42,
                PipelineEvent.Kind.TELEMETRY_GAP, FROM.minusSeconds(1), null, null, null, null, gap));
        PipelineEventsQueryService service = service(store, Clock.fixed(NOW, ZoneOffset.UTC), "inc-a");

        PipelineEventsQuery firstQuery = new PipelineEventsQuery("orders", FROM, NOW, 1, null);
        PipelineEvents first = service.query(firstQuery);
        PipelineEvents second = service.query(new PipelineEventsQuery("orders", FROM, NOW, 1,
                first.nextCursor()));
        PipelineEvents third = service.query(new PipelineEventsQuery("orders", FROM, NOW, 1,
                second.nextCursor()));

        assertThat(first.events()).extracting(PipelineEvents.Event::id).containsExactly("ev-a");
        assertThat(second.events()).extracting(PipelineEvents.Event::id).containsExactly("ev-b");
        assertThat(third.events()).extracting(PipelineEvents.Event::id).containsExactly("ev-g");
        assertThat(first.knownGaps()).isEmpty();
        assertThat(second.knownGaps()).isEmpty();
        assertThat(third.knownGaps()).singleElement().satisfies(known -> {
            assertThat(known.eventId()).isEqualTo("ev-g");
            assertThat(known.from()).isEqualTo(gap.from());
            assertThat(known.reasons()).containsExactly(PipelineEvent.GapReason.QUEUE_FULL,
                    PipelineEvent.GapReason.WRITE_FAILURE);
        });
        assertThat(third.nextCursor()).isNull();
        assertThat(first.completeness()).isEqualTo(PipelineEvents.Completeness.BEST_EFFORT);
        assertThat(store.reads).isEqualTo(3);
        assertThat(store.limits).containsExactly(1, 1, 1);
    }

    @Test
    void failureSurvivesARecoveryAndNeverCarriesInternalOwnerIntoTheProjection() {
        RecordingEvents store = new RecordingEvents();
        store.append(new PipelineEvent("ev-a7", "orders", "inc-a", 41, PipelineEvent.Kind.FAILURE,
                FROM.plusSeconds(600), PipelineState.RUNNING, PipelineState.FAILED,
                new ObservationFailure("engine.job-failed",
                        Map.of("pipeline", "orders", "cause", "sink refused the batch")), null, null));
        store.append(new PipelineEvent("ev-b2", "orders", "inc-a", 42,
                PipelineEvent.Kind.EXECUTION_RECOVERED, FROM.plusSeconds(720),
                PipelineState.FAILED, PipelineState.RUNNING, null, "worker.internal.local", null));
        PipelineEvents events = service(store, Clock.fixed(NOW, ZoneOffset.UTC), "inc-a")
                .query(new PipelineEventsQuery("orders", FROM, NOW));

        assertThat(events.events()).extracting(PipelineEvents.Event::kind)
                .containsExactly(PipelineEvent.Kind.FAILURE, PipelineEvent.Kind.EXECUTION_RECOVERED);
        assertThat(events.events().getFirst().failure().params())
                .containsEntry("cause", "sink refused the batch");
        assertThat(events.events().getFirst().message()).isEqualTo(events.events().getFirst().failure().message());
        assertThat(events.events().getLast().message()).isEqualTo("Pipeline execution recovered.");
        assertThat(events.events().getLast().reason()).isNull();
        assertThat(events.knownGaps()).isEmpty();
        assertThat(events.retentionCutoff()).isEqualTo(Instant.parse("2026-09-05T11:00:00Z"));
    }

    @Test
    void cursorRejectsChangedQueryTamperingAndExpiryWithExistingCodedShape() {
        RecordingEvents store = new RecordingEvents();
        store.append(event("ev-a", "inc-a", FROM.plusSeconds(1), PipelineEvent.Kind.STATE_CHANGED));
        store.append(event("ev-b", "inc-a", FROM.plusSeconds(2), PipelineEvent.Kind.STATE_CHANGED));
        PipelineEvents first = service(store, Clock.fixed(NOW, ZoneOffset.UTC), "inc-a")
                .query(new PipelineEventsQuery("orders", FROM, NOW, 1, null));

        checkCursorFailure(() -> service(store, Clock.fixed(NOW, ZoneOffset.UTC), "inc-a")
                .query(new PipelineEventsQuery("orders", FROM, NOW, 2, first.nextCursor())),
                MonitorError.INVALID_CURSOR, "QUERY_MISMATCH");
        checkCursorFailure(() -> service(store, Clock.fixed(NOW, ZoneOffset.UTC), "inc-b")
                .query(new PipelineEventsQuery("orders", FROM, NOW, 1, first.nextCursor())),
                MonitorError.INVALID_CURSOR, "QUERY_MISMATCH");
        String tampered = (first.nextCursor().charAt(0) == 'A' ? "B" : "A")
                + first.nextCursor().substring(1);
        checkCursorFailure(() -> service(store, Clock.fixed(NOW, ZoneOffset.UTC), "inc-a")
                .query(new PipelineEventsQuery("orders", FROM, NOW, 1, tampered)),
                MonitorError.INVALID_CURSOR, "TAMPERED");
        checkCursorFailure(() -> service(store, Clock.fixed(NOW.plus(Duration.ofMinutes(10)), ZoneOffset.UTC), "inc-a")
                .query(new PipelineEventsQuery("orders", FROM, NOW, 1, first.nextCursor())),
                MonitorError.CURSOR_EXPIRED, null);
    }

    @Test
    void clippedOrUnincarnatedWindowIsEmptyButNeverCompleteAndInvalidInputsAreCoded() {
        RecordingEvents store = new RecordingEvents();
        PipelineEventsQueryService service = service(store, Clock.fixed(NOW, ZoneOffset.UTC), null);
        PipelineEvents empty = service.query(new PipelineEventsQuery("orders", FROM, NOW));
        assertThat(empty.events()).isEmpty();
        assertThat(empty.knownGaps()).isEmpty();
        assertThat(empty.completeness()).isEqualTo(PipelineEvents.Completeness.BEST_EFFORT);
        assertThat(store.reads).isZero();

        PipelineEvents clipped = service(store, Clock.fixed(NOW, ZoneOffset.UTC), "inc-a")
                .query(new PipelineEventsQuery("orders", NOW.minus(Duration.ofDays(20)),
                        NOW.minus(Duration.ofDays(19))));
        assertThat(clipped.effectiveFrom()).isEqualTo(clipped.effectiveTo());
        assertThat(store.reads).isZero();
        for (PipelineEventsQuery invalid : List.of(
                new PipelineEventsQuery("orders", NOW, FROM),
                new PipelineEventsQuery("orders", FROM.minus(Duration.ofDays(15)).minusSeconds(1), FROM),
                new PipelineEventsQuery("orders", FROM, NOW, 0, null),
                new PipelineEventsQuery("orders", FROM, NOW, 501, null))) {
            TapstateException refusal = catchThrowableOfType(() -> service.query(invalid), TapstateException.class);
            assertThat(refusal.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
        }
        TapstateException unknown = catchThrowableOfType(() -> service.query(
                new PipelineEventsQuery("ghost", FROM, NOW)), TapstateException.class);
        assertThat(unknown.code().code()).isEqualTo("lifecycle.unknown-pipeline");
        ArtifactStore wrongKind = new ArtifactStore() {
            private final Resource source = new SourceResource("wrong", null, "mysql", Map.of(),
                    null, null, null, null);
            @Override public void saveAll(List<Resource> values) { throw new UnsupportedOperationException(); }
            @Override public Optional<Resource> get(String id) {
                return id.equals("wrong") ? Optional.of(source) : Optional.empty();
            }
            @Override public List<Resource> list() { return List.of(source); }
        };
        PipelineEventsQueryService wrongKindService = new PipelineEventsQueryService(
                new ArtifactQueryService(wrongKind), store, new EventsCursorCodec(SECRET,
                Clock.fixed(NOW, ZoneOffset.UTC)), Clock.fixed(NOW, ZoneOffset.UTC),
                (key, args) -> key);
        TapstateException invalidKind = catchThrowableOfType(() -> wrongKindService.query(
                new PipelineEventsQuery("wrong", FROM, NOW)), TapstateException.class);
        assertThat(invalidKind.code().code()).isEqualTo("lifecycle.unknown-pipeline");
    }

    private static void checkCursorFailure(Runnable read, MonitorError expected, String reason) {
        TapstateException refusal = catchThrowableOfType(read::run, TapstateException.class);
        assertThat(refusal.code()).isEqualTo(expected);
        assertThat(refusal.args()).containsEntry("operation", "pipeline.events");
        if (reason != null) {
            assertThat(refusal.args()).containsEntry("reason", reason);
        }
    }

    private static PipelineEventsQueryService service(RecordingEvents store, Clock clock, String incarnation) {
        Resource pipeline = new DslParser().parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders
                source: source
                serve:
                  from: /.*/
                  sync:
                    - id: sink
                      source: target
                      write_mode: upsert
                      ddl: apply
                """);
        ArtifactStore artifacts = new ArtifactStore() {
            @Override public void saveAll(List<Resource> values) { throw new UnsupportedOperationException(); }
            @Override public Optional<Resource> get(String id) {
                return id.equals("orders") ? Optional.of(pipeline) : Optional.empty();
            }
            @Override public List<Resource> list() { return List.of(pipeline); }
            @Override public Optional<HistoryOwner> pipelineHistoryOwner(String id) {
                return id.equals("orders") ? Optional.of(new HistoryOwner(
                        new RateHistoryStore.Visibility(incarnation, incarnation == null))) : Optional.empty();
            }
        };
        return new PipelineEventsQueryService(new ArtifactQueryService(artifacts), store,
                new EventsCursorCodec(SECRET, clock), clock,
                (key, args) -> key.equals("engine.job-failed")
                        ? "Pipeline orders stopped because its job failed: sink refused the batch."
                        : key.equals("event.execution-recovered") ? "Pipeline execution recovered." : key);
    }

    private static PipelineEvent event(String id, String incarnation, Instant at, PipelineEvent.Kind kind) {
        return new PipelineEvent(id, "orders", incarnation, 41, kind, at,
                null, null, null, null, null);
    }

    private static final class RecordingEvents implements PipelineEventStore {
        private final List<PipelineEvent> rows = new ArrayList<>();
        private final List<Integer> limits = new ArrayList<>();
        private int reads;

        @Override public void append(PipelineEvent event) { rows.add(event); }
        @Override public void deleteIncarnation(String pipelineId, String incarnationId) { }
        @Override public Duration retention() { return Duration.ofDays(15); }

        @Override
        public Page readPage(String pipelineId, String incarnationId, Instant from, Instant to, Key after, int limit) {
            reads++;
            limits.add(limit);
            List<PipelineEvent> matches = rows.stream()
                    .filter(event -> event.pipelineId().equals(pipelineId)
                            && event.pipelineIncarnationId().equals(incarnationId)
                            && !event.occurredAt().isBefore(from) && event.occurredAt().isBefore(to)
                            && (after == null || event.occurredAt().isAfter(after.occurredAt())
                            || event.occurredAt().equals(after.occurredAt()) && event.id().compareTo(after.id()) > 0))
                    .sorted(Comparator.comparing(PipelineEvent::occurredAt).thenComparing(PipelineEvent::id))
                    .toList();
            return new Page(matches.subList(0, Math.min(limit, matches.size())), matches.size() > limit);
        }
    }
}
