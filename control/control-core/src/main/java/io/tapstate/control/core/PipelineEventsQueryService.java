package io.tapstate.control.core;

import io.tapstate.control.core.EventsCursorCodec.QueryBinding;
import io.tapstate.control.core.EventsCursorCodec.State;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.spi.store.PipelineEventStore;
import io.tapstate.spi.store.PipelineEventStore.Page;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** One-incarnation, one-page event query with a frozen eventual continuation window. */
public final class PipelineEventsQueryService {

    public static final Duration MAX_RANGE = Duration.ofDays(15);

    private final ArtifactQueryService artifacts;
    private final PipelineEventStore store;
    private final EventsCursorCodec cursors;
    private final Clock clock;
    private final EventMessages messages;

    public PipelineEventsQueryService(ArtifactQueryService artifacts, PipelineEventStore store,
            EventsCursorCodec cursors, Clock clock, EventMessages messages) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.store = Objects.requireNonNull(store, "store");
        this.cursors = Objects.requireNonNull(cursors, "cursors");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.messages = Objects.requireNonNull(messages, "messages");
    }

    public PipelineEvents query(PipelineEventsQuery request) {
        validate(request);
        String incarnation = requireIncarnation(request.pipelineId());
        QueryBinding binding = new QueryBinding(request.pipelineId(), incarnation,
                request.from(), request.to(), request.limit());
        State continuation = request.cursor() == null ? null : cursors.read(request.cursor(), binding);
        Instant now = clock.instant();
        Instant cutoff = continuation == null ? now.minus(store.retention()) : continuation.retentionCutoff();
        Instant effectiveFrom = continuation == null ? max(request.from(), cutoff) : continuation.effectiveFrom();
        Instant effectiveTo = continuation == null ? min(request.to(), now) : continuation.effectiveTo();
        if (effectiveFrom.isAfter(effectiveTo)) {
            effectiveFrom = effectiveTo;
        }
        if (!effectiveFrom.isBefore(effectiveTo) || incarnation.isEmpty()) {
            return response(request, effectiveFrom, effectiveTo, cutoff, List.of(), null);
        }
        Page page = store.readPage(request.pipelineId(), incarnation, effectiveFrom, effectiveTo,
                continuation == null ? null : continuation.after(), request.limit());
        String next = page.hasMore()
                ? cursors.issue(binding, effectiveFrom, effectiveTo, cutoff, page.lastKey().orElseThrow())
                : null;
        return response(request, effectiveFrom, effectiveTo, cutoff, page.events(), next);
    }

    private PipelineEvents response(PipelineEventsQuery request, Instant effectiveFrom, Instant effectiveTo,
            Instant cutoff, List<PipelineEvent> raw, String next) {
        List<PipelineEvents.Event> events = raw.stream().map(this::event).toList();
        List<PipelineEvents.KnownGap> gaps = raw.stream()
                .filter(event -> event.kind() == PipelineEvent.Kind.TELEMETRY_GAP)
                .map(event -> new PipelineEvents.KnownGap(event.id(), event.gap().from(),
                        event.gap().to(), event.gap().reasons()))
                .toList();
        return new PipelineEvents(request.pipelineId(), request.from(), request.to(),
                effectiveFrom, effectiveTo, cutoff, PipelineEvents.Completeness.BEST_EFFORT,
                events, gaps, next);
    }

    private PipelineEvents.Event event(PipelineEvent source) {
        ObservationFailure failure = source.failure();
        PipelineEvents.Failure publicFailure = failure == null ? null : new PipelineEvents.Failure(
                failure.code(), failure.params(),
                messages.render(failure.code(), Map.copyOf(failure.params())));
        String message = publicFailure == null
                ? messages.render("event." + source.kind().name().toLowerCase(java.util.Locale.ROOT)
                        .replace('_', '-'), Map.of())
                : publicFailure.message();
        // Stored free text has no public reason vocabulary; the catalog message is the display text.
        return new PipelineEvents.Event(source.id(), source.occurredAt(), source.kind(), message,
                source.beforeState(), source.afterState(), publicFailure, null);
    }

    private String requireIncarnation(String pipelineId) {
        boolean pipeline = artifacts.get(pipelineId)
                .map(artifact -> "pipeline".equals(artifact.kind()))
                .orElse(false);
        if (!pipeline) {
            throw new TapstateException(LifecycleError.UNKNOWN_PIPELINE, Map.of("pipeline", pipelineId), null);
        }
        String incarnation = artifacts.historyVisibilityOf(pipelineId)
                .orElseThrow(() -> new TapstateException(LifecycleError.UNKNOWN_PIPELINE,
                        Map.of("pipeline", pipelineId), null))
                .incarnationId();
        return incarnation == null ? "" : incarnation;
    }

    private static void validate(PipelineEventsQuery request) {
        Objects.requireNonNull(request, "request");
        if (request.pipelineId().isBlank()) {
            throw malformed("pipelineId is required");
        }
        if (!request.from().isBefore(request.to())) {
            throw malformed("from must be before to");
        }
        if (Duration.between(request.from(), request.to()).compareTo(MAX_RANGE) > 0) {
            throw malformed("the events range is at most 15 days");
        }
        if (request.limit() < 1 || request.limit() > PipelineEventsQuery.MAX_LIMIT) {
            throw malformed("limit is between 1 and " + PipelineEventsQuery.MAX_LIMIT);
        }
    }

    private static Instant min(Instant a, Instant b) {
        return a.isBefore(b) ? a : b;
    }

    private static Instant max(Instant a, Instant b) {
        return a.isAfter(b) ? a : b;
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }
}
