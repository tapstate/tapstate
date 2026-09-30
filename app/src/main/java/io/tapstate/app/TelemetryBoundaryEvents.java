package io.tapstate.app;

import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.spi.store.ObservationStore;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

/** Bounded transition evidence for the current owner, without an event-store feedback loop. */
final class TelemetryBoundaryEvents {

    private record Episode(ObservationStore.Scope scope, EnumMap<TelemetryDispatcher.Sink, Long> failures) { }

    private final int capacity;
    private final Clock clock;
    private final BiPredicate<String, ObservationStore.Scope> current;
    private final Consumer<PipelineEvent> offer;
    private final Consumer<PipelineEvent> lost;
    private final Map<String, Episode> episodes = new LinkedHashMap<>();
    private final String streamId = UUID.randomUUID().toString();
    private long sequence;

    TelemetryBoundaryEvents(int capacity, Clock clock, BiPredicate<String, ObservationStore.Scope> current,
            Consumer<PipelineEvent> offer, Consumer<PipelineEvent> lost) {
        if (capacity < 1) {
            throw new IllegalArgumentException("telemetry boundary capacity must be positive");
        }
        this.capacity = capacity;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.current = Objects.requireNonNull(current, "current");
        this.offer = Objects.requireNonNull(offer, "offer");
        this.lost = Objects.requireNonNull(lost, "lost");
    }

    synchronized void failed(String id, ObservationStore.Scope scope, TelemetryDispatcher.Sink sink) {
        failed(id, scope, sink, clock.instant(), System.nanoTime());
    }

    /** A qualified cold recovery can replay the original failure time after its scope becomes current. */
    synchronized void failed(String id, ObservationStore.Scope scope, TelemetryDispatcher.Sink sink,
            Instant occurredAt, long failedNanos) {
        requireDataSink(sink);
        if (scope == null || !current.test(id, scope)) {
            return;
        }
        Episode episode = episodes.get(id);
        if (episode != null && !episode.scope().equals(scope)) {
            episodes.remove(id);
            episode = null;
        }
        if (episode == null) {
            if (episodes.size() >= capacity) {
                episodes.entrySet().removeIf(entry -> !current.test(entry.getKey(), entry.getValue().scope()));
            }
            if (episodes.size() >= capacity) {
                lost.accept(event(id, scope, sink, PipelineEvent.Kind.TELEMETRY_DEGRADED, occurredAt));
                return;
            }
            episode = new Episode(scope, new EnumMap<>(TelemetryDispatcher.Sink.class));
            episodes.put(id, episode);
        }
        Long before = episode.failures().get(sink);
        if (before == null || failedNanos - before > 0) {
            episode.failures().put(sink, failedNanos);
        }
        if (before == null) {
            offer.accept(event(id, scope, sink, PipelineEvent.Kind.TELEMETRY_DEGRADED, occurredAt));
        }
    }

    synchronized void succeeded(String id, ObservationStore.Scope scope, TelemetryDispatcher.Sink sink,
            long startedNanos) {
        requireDataSink(sink);
        if (scope == null || !current.test(id, scope)) {
            return;
        }
        Episode episode = episodes.get(id);
        if (episode == null) {
            return;
        }
        if (!episode.scope().equals(scope)) {
            episodes.remove(id);
            return;
        }
        Long failedAt = episode.failures().get(sink);
        if (failedAt != null && startedNanos - failedAt >= 0) {
            episode.failures().remove(sink);
            offer.accept(event(id, scope, sink, PipelineEvent.Kind.TELEMETRY_RESTORED));
        }
        if (episode.failures().isEmpty()) {
            episodes.remove(id);
        }
    }

    synchronized void retain(Collection<String> ids) {
        episodes.entrySet().removeIf(entry -> !ids.contains(entry.getKey())
                || !current.test(entry.getKey(), entry.getValue().scope()));
    }

    private PipelineEvent event(String id, ObservationStore.Scope scope,
            TelemetryDispatcher.Sink sink, PipelineEvent.Kind kind) {
        return event(id, scope, sink, kind, clock.instant());
    }

    private PipelineEvent event(String id, ObservationStore.Scope scope,
            TelemetryDispatcher.Sink sink, PipelineEvent.Kind kind, Instant occurredAt) {
        String reason = switch (sink) {
            case LATEST -> "latest observation write";
            case HISTORY -> "history sample";
            case EXPORT -> "local metrics export offer";
            case EVENT -> throw new IllegalArgumentException("event recovery is owned by gap markers");
        };
        sequence = Math.incrementExact(sequence);
        // Preserve transition order in the public (millisecond time, opaque id) query key.
        String eventId = "telemetry-" + streamId + "-" + String.format(Locale.ROOT, "%019d", sequence);
        return new PipelineEvent(eventId, id, scope.pipelineIncarnationId(),
                scope.executionGeneration(), kind, occurredAt, null, null, null, reason, null);
    }

    private static void requireDataSink(TelemetryDispatcher.Sink sink) {
        Objects.requireNonNull(sink, "sink");
        if (sink == TelemetryDispatcher.Sink.EVENT) {
            throw new IllegalArgumentException("event recovery is owned by gap markers");
        }
    }
}
