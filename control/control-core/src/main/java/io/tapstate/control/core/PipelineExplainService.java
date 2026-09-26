package io.tapstate.control.core;

import io.tapstate.control.core.PipelineExplanation.Evidence;
import io.tapstate.control.core.PipelineExplanation.Failure;
import io.tapstate.control.core.PipelineExplanation.Freshness;
import io.tapstate.control.core.PipelineExplanation.Kind;
import io.tapstate.control.core.PipelineExplanation.Next;
import io.tapstate.control.core.PipelineExplanation.NextAction;
import io.tapstate.control.core.PipelineExplanation.Pending;
import io.tapstate.control.core.PipelineExplanation.Source;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Optional;
import java.util.function.Function;

/** The fixed five-rule explain projection over one current observation. */
public final class PipelineExplainService {

    public static final Duration PUBLISHER_SILENCE = Duration.ofSeconds(30);

    private static final String PIPELINE_KIND = "pipeline";
    private static final String RECONCILE_STREAK = "reconcileFailuresInARow";
    private static final String RECORD_COUNT = "recordCount";
    private static final String STALLED_PREFIX = "frontierStalledMillis.";

    private final ArtifactQueryService artifacts;
    private final Function<String, Optional<Observation>> observations;
    private final Clock clock;
    private final ExplanationMessages messages;
    private final Function<String, Optional<Pending>> pending;

    public PipelineExplainService(ArtifactQueryService artifacts, ObservationStore observations,
            Clock clock, ExplanationMessages messages) {
        this(artifacts, observations::read, clock, messages, id -> Optional.empty());
    }

    public PipelineExplainService(ArtifactQueryService artifacts, ObservationStore observations,
            Clock clock, ExplanationMessages messages, Function<String, Optional<Pending>> pending) {
        this(artifacts, observations::read, clock, messages, pending);
    }

    public PipelineExplainService(ArtifactQueryService artifacts, CurrentObservationReader observations,
            Clock clock, ExplanationMessages messages) {
        this(artifacts, observations::read, clock, messages, id -> Optional.empty());
    }

    public PipelineExplainService(ArtifactQueryService artifacts, CurrentObservationReader observations,
            Clock clock, ExplanationMessages messages, Function<String, Optional<Pending>> pending) {
        this(artifacts, observations::read, clock, messages, pending);
    }

    private PipelineExplainService(ArtifactQueryService artifacts,
            Function<String, Optional<Observation>> observations, Clock clock, ExplanationMessages messages,
            Function<String, Optional<Pending>> pending) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.messages = Objects.requireNonNull(messages, "messages");
        this.pending = Objects.requireNonNull(pending, "pending");
    }

    /** Reads one observation once and applies the fixed first-match checklist without writing it back. */
    public PipelineExplanation explain(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Observation observation = observations.apply(pipelineId).orElseThrow(() -> unobserved(pipelineId));
        Pending lifecyclePending = pending.apply(pipelineId).orElse(null);
        Time time = time(observation.observedAt());
        Facts facts = facts(observation);

        if (time.freshness() == Freshness.STALE) {
            return answer(observation, time, Kind.OBSERVATION_STALE,
                    text("explain.observation-stale", Map.of("age", human(time.ageMillis()))),
                    List.of(evidence(Source.STATUS, "observedAgeMillis", time.ageMillis())),
                    List.of(text("explain.cannot-current-now", Map.of())),
                    next(NextAction.CHECK_SERVER, "explain.next-check-server", Map.of()), lifecyclePending);
        }
        if (observation.failure() != null) {
            ObservationFailure found = observation.failure();
            Map<String, Object> params = new LinkedHashMap<>(found.params());
            Failure failure = new Failure(found.code(), found.params(), messages.render(found.code(), params));
            return answer(observation, time, Kind.CODED_FAILURE,
                    text("explain.coded-failure", Map.of("code", found.code())),
                    List.of(evidence(Source.STATUS, "failure", failure)), List.of(),
                    next(NextAction.OPEN_PIPELINE_LOGS, "explain.next-open-logs",
                            Map.of("pipeline", pipelineId)), lifecyclePending);
        }
        if (facts.reconcileFailures() != null && facts.reconcileFailures() > 0
                && converging(observation.state())) {
            return answer(observation, time, Kind.RECONCILE_FAILURES,
                    text("explain.reconcile-failures", Map.of("count", facts.reconcileFailures())),
                    List.of(
                            evidence(Source.METRICS, RECONCILE_STREAK, facts.reconcileFailures()),
                            evidence(Source.STATUS, "state", observation.state())),
                    List.of(text("explain.cannot-job-alive", Map.of("state", lower(observation.state())))),
                    next(NextAction.CHECK_SERVER, "explain.next-check-server", Map.of()), lifecyclePending);
        }
        if (converging(observation.state())
                && (facts.recordCount() == null || facts.recordCount() == 0)
                && facts.snapshotRowsDone() == 0) {
            return answer(observation, time, Kind.NO_MOVEMENT,
                    text("explain.no-movement", Map.of()),
                    List.of(
                            evidence(Source.METRICS, RECORD_COUNT, facts.recordCount()),
                            evidence(Source.SNAPSHOT, "rowsDone", facts.snapshotRowsDone())),
                    List.of(text("explain.cannot-source-head", Map.of())),
                    next(NextAction.OPEN_PIPELINE_LOGS, "explain.next-open-logs",
                            Map.of("pipeline", pipelineId)), lifecyclePending);
        }
        if (!facts.stalledChains().isEmpty()) {
            List<Evidence> evidence = facts.stalledChains().entrySet().stream()
                    .map(entry -> evidence(Source.METRICS,
                            STALLED_PREFIX + entry.getKey(), entry.getValue()))
                    .toList();
            return answer(observation, time, Kind.FRONTIER_STALLED,
                    text("explain.frontier-stalled",
                            Map.of("chains", String.join(", ", facts.stalledChains().keySet()))),
                    evidence, List.of(),
                    next(NextAction.CHECK_TARGET, "explain.next-check-target", Map.of()), lifecyclePending);
        }

        List<Evidence> evidence = List.of(
                evidence(Source.STATUS, "observedAgeMillis", time.ageMillis()),
                evidence(Source.STATUS, "failure", null),
                evidence(Source.METRICS, RECONCILE_STREAK, facts.reconcileFailures()),
                evidence(Source.METRICS, RECORD_COUNT, facts.recordCount()),
                evidence(Source.METRICS, "frontierStalledMillis", facts.frontierStalledMillis()),
                evidence(Source.SNAPSHOT, "rowsDone", facts.snapshotRowsDone()));
        List<String> cannotSay = new ArrayList<>();
        if (time.freshness() == Freshness.UNKNOWN) {
            cannotSay.add(text("explain.cannot-age", Map.of()));
        }
        cannotSay.add(text("explain.cannot-source-head", Map.of()));
        if (observation.state() == PipelineState.PAUSED) {
            cannotSay.add(text("explain.cannot-paused-job", Map.of()));
        }
        cannotSay.add(text("explain.cannot-snapshot-phase", Map.of()));
        return answer(observation, time, Kind.NO_MATCH, text("explain.no-match", Map.of()),
                evidence, cannotSay, null, lifecyclePending);
    }

    private PipelineExplanation answer(Observation observation, Time time, Kind kind, String message,
            List<Evidence> evidence, List<String> cannotSay, Next next, Pending lifecyclePending) {
        return new PipelineExplanation(observation.pipelineId(), observation.state(), kind, message,
                observation.observedAt(), time.ageMillis(), time.freshness(), evidence, cannotSay, next,
                lifecyclePending);
    }

    private Facts facts(Observation observation) {
        Map<String, Long> frontierStalledMillis = new TreeMap<>();
        Map<String, Long> stalled = new TreeMap<>();
        observation.metrics().forEach((name, value) -> {
            if (name.startsWith(STALLED_PREFIX) && value != null) {
                String chain = name.substring(STALLED_PREFIX.length());
                frontierStalledMillis.put(chain, value);
                if (FrontierStallPressure.EXPLAIN.isOver(value)) {
                    stalled.put(chain, value);
                }
            }
        });
        long rowsDone = observation.snapshot().values().stream().mapToLong(snapshot -> snapshot.rowsDone()).sum();
        return new Facts(observation.metrics().get(RECONCILE_STREAK),
                observation.metrics().get(RECORD_COUNT),
                Collections.unmodifiableMap(new LinkedHashMap<>(frontierStalledMillis)),
                Collections.unmodifiableMap(new LinkedHashMap<>(stalled)), rowsDone);
    }

    private Time time(Instant observedAt) {
        if (observedAt == null) {
            return new Time(null, Freshness.UNKNOWN);
        }
        long age = Math.max(0, Duration.between(observedAt, clock.instant()).toMillis());
        return new Time(age, age >= PUBLISHER_SILENCE.toMillis() ? Freshness.STALE : Freshness.FRESH);
    }

    private TapstateException unobserved(String pipelineId) {
        boolean isPipeline = artifacts.get(pipelineId)
                .map(artifact -> PIPELINE_KIND.equals(artifact.kind()))
                .orElse(false);
        return new TapstateException(isPipeline ? MonitorError.NO_OBSERVATION : LifecycleError.UNKNOWN_PIPELINE,
                Map.of("pipeline", pipelineId), null);
    }

    private String text(String key, Map<String, Object> args) {
        return messages.render(key, args);
    }

    private Next next(NextAction action, String key, Map<String, Object> args) {
        return new Next(action, text(key, args));
    }

    private static Evidence evidence(Source source, String field, Object value) {
        return new Evidence(source, field, value);
    }

    private static boolean converging(PipelineState state) {
        return state == PipelineState.NEW || state == PipelineState.RUNNING;
    }

    private static String lower(PipelineState state) {
        return state.name().toLowerCase(java.util.Locale.ROOT);
    }

    private static String human(long millis) {
        if (millis < 1_000) {
            return millis + "ms";
        }
        long seconds = millis / 1_000;
        if (seconds < 60) {
            return seconds + "s";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + "m" + (seconds % 60) + "s";
        }
        long hours = minutes / 60;
        return hours + "h" + (minutes % 60) + "m";
    }

    private record Time(Long ageMillis, Freshness freshness) {
    }

    private record Facts(Long reconcileFailures, Long recordCount,
            Map<String, Long> frontierStalledMillis, Map<String, Long> stalledChains,
            long snapshotRowsDone) {
    }
}
