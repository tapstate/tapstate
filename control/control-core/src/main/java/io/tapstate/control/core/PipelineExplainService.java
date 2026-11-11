package io.tapstate.control.core;

import io.tapstate.control.core.PipelineExplanation.Evidence;
import io.tapstate.control.core.PipelineExplanation.Failure;
import io.tapstate.control.core.PipelineExplanation.Freshness;
import io.tapstate.control.core.PipelineExplanation.Kind;
import io.tapstate.control.core.PipelineExplanation.Next;
import io.tapstate.control.core.PipelineExplanation.NextAction;
import io.tapstate.control.core.PipelineExplanation.Source;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.RollupExpressions;
import io.tapstate.core.dsl.AggregateSizingRules;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RollupPick;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ExecutionPlans;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.ObservationStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * The fixed five-rule explain projection over one current observation, answered beside the plan the pipeline's
 * current run was submitted on where one is recorded.
 */
public final class PipelineExplainService {

    public static final Duration PUBLISHER_SILENCE = Duration.ofSeconds(30);

    private static final String PIPELINE_KIND = "pipeline";
    private static final String RECONCILE_STREAK = "reconcileFailuresInARow";
    private static final String RECORD_COUNT = "recordCount";
    private static final String STALLED_PREFIX = "frontierStalledMillis.";

    private final ArtifactQueryService artifacts;
    private final ObservationStore observations;
    private final Clock clock;
    private final ExplanationMessages messages;
    private final ExecutionPlans plans;
    private final Supplier<List<String>> members;

    public PipelineExplainService(ArtifactQueryService artifacts, ObservationStore observations,
            Clock clock, ExplanationMessages messages) {
        this(artifacts, observations, clock, messages, ExecutionPlans.NONE);
    }

    /** As above, answering beside each explanation the plan its pipeline's current run was submitted on. */
    public PipelineExplainService(ArtifactQueryService artifacts, ObservationStore observations,
            Clock clock, ExplanationMessages messages, ExecutionPlans plans) {
        this(artifacts, observations, clock, messages, plans, List::of);
    }

    /**
     * As above, also naming the members of the cluster - {@code members}, by stable id - the plan was not worked
     * out for.
     */
    public PipelineExplainService(ArtifactQueryService artifacts, ObservationStore observations,
            Clock clock, ExplanationMessages messages, ExecutionPlans plans, Supplier<List<String>> members) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.observations = Objects.requireNonNull(observations, "observations");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.messages = Objects.requireNonNull(messages, "messages");
        this.plans = Objects.requireNonNull(plans, "plans");
        this.members = Objects.requireNonNull(members, "members");
    }

    /**
     * Reads one observation once and applies the fixed first-match checklist without writing it back, answering
     * beside it the plan the pipeline's current run was submitted on where one is recorded.
     */
    public PipelineExplanation explain(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Observation observation = observations.read(pipelineId).orElseThrow(() -> unobserved(pipelineId));
        ExecutionPlan plan = plans.current(List.of(pipelineId)).get(pipelineId);
        return diagnose(pipelineId, observation)
                .withPlan(plan, plan == null ? List.of() : plan.notPlannedFor(members.get()))
                .withRollups(rollupsOf(pipelineId))
                .withAggregates(aggregatesOf(pipelineId, observation));
    }

    /** What an aggregate step's count of its own state travels under on the flat face, before the step. */
    private static final String AGGREGATE_STATE_ENTRIES = "aggregateStateEntries.";

    /**
     * What each aggregate step the pipeline's stored definition declares keeps in state: the entries per
     * input row, which the definition alone decides, the total that comes to when the last load said how
     * many rows the inputs hold, and beside it what the running step last counted.
     */
    private List<PipelineExplanation.Aggregate> aggregatesOf(String pipelineId, Observation observation) {
        List<PipelineExplanation.Aggregate> steps = new ArrayList<>();
        artifacts.getResource(pipelineId).map(StoredResource::resource).ifPresent(resource -> {
            if (!(resource instanceof PipelineResource pipeline) || pipeline.transforms() == null) {
                return;
            }
            Map<String, Step.Inline> inline = new LinkedHashMap<>();
            for (Step step : pipeline.transforms()) {
                if (step instanceof Step.Inline written) {
                    inline.put(written.id(), written);
                }
            }
            inline.values().forEach(step -> {
                if (!(step.body() instanceof TransformBody.Aggregate aggregate)) {
                    return;
                }
                List<String> kept = AggregateSizingRules.keptInputFields(aggregate);
                int perRow = AggregateSizingRules.entriesPerRow(aggregate);
                Long rows = inputRows(step, inline, observation, new LinkedHashSet<>());
                steps.add(new PipelineExplanation.Aggregate(step.id(), perRow, kept, rows,
                        rows == null ? null : Math.multiplyExact(rows, (long) perRow),
                        observation.metrics().get(AGGREGATE_STATE_ENTRIES + step.id())));
            });
        });
        return steps;
    }

    /**
     * The rows the tables under {@code step} hold by the last load's estimate, added up; null when any of
     * them has none, since a total with a table missing would read as the whole.
     */
    private static Long inputRows(Step.Inline step, Map<String, Step.Inline> inline, Observation observation,
            Set<String> walked) {
        if (!(step.from() instanceof FromClause.Flow flow) || !walked.add(step.id())) {
            return null;
        }
        long rows = 0;
        for (FromRef ref : flow.refs()) {
            if (!(ref instanceof FromRef.Literal named)) {
                return null;
            }
            Step.Inline above = inline.get(named.ref());
            Long counted;
            if (above != null) {
                counted = inputRows(above, inline, observation, walked);
            } else {
                String table = named.ref().substring(named.ref().lastIndexOf('.') + 1);
                TableSnapshot loaded = observation.snapshot().containsKey(named.ref())
                        ? observation.snapshot().get(named.ref()) : observation.snapshot().get(table);
                counted = loaded == null ? null : loaded.rowsTotal();
            }
            if (counted == null) {
                return null;
            }
            rows += counted;
        }
        return rows;
    }

    /**
     * The rollup fields the pipeline's stored definition declares, in tree order. Read from the definition
     * rather than from the run, since what a field is computed from is a property of what was applied.
     */
    private List<PipelineExplanation.Rollup> rollupsOf(String pipelineId) {
        List<PipelineExplanation.Rollup> rollups = new ArrayList<>();
        artifacts.getResource(pipelineId).map(StoredResource::resource).ifPresent(resource -> {
            if (resource instanceof PipelineResource pipeline && pipeline.transforms() != null) {
                for (Step step : pipeline.transforms()) {
                    if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest
                            && nest.root() != null) {
                        collectRollups(step.id(), nest.root().embed(), List.of(), rollups);
                    }
                }
            }
        });
        return rollups;
    }

    private static void collectRollups(String step, List<Embed> embeds, List<String> parent,
            List<PipelineExplanation.Rollup> into) {
        if (embeds == null) {
            return;
        }
        String level = parent.isEmpty() ? "$root" : String.join(".", parent);
        for (Embed embed : embeds) {
            List<String> path = new ArrayList<>(parent);
            path.add(embed.treeSegment());
            Set<String> referencePaths = new LinkedHashSet<>();
            if (embed.embed() != null) {
                for (Embed child : embed.embed()) {
                    if (child.path() != null) {
                        referencePaths.add(child.path());
                    }
                }
            }
            embed.rollupFields().forEach((name, field) -> {
                Object argument = field.argument();
                List<String> written = new ArrayList<>();
                String shown;
                if (argument instanceof RollupPick pick) {
                    written.add(pick.value());
                    written.addAll(pick.by());
                    shown = pick.value() + " by " + String.join(", ", pick.by());
                } else if (argument instanceof String expression) {
                    written.add(expression);
                    shown = expression;
                } else {
                    shown = String.valueOf(argument);
                }
                if (field.filter() != null) {
                    written.add(field.filter());
                }
                // Which of the embed's children the expressions read. Only a child the element points at can
                // render absent while the element lives, and only those make the field null; reading a
                // gathered child is listed too, since the author asked what the field depends on.
                Set<String> read = new LinkedHashSet<>();
                for (String expression : written) {
                    for (String fieldRead : RollupExpressions.fieldsRead(expression)) {
                        if (referencePaths.contains(fieldRead)) {
                            read.add(fieldRead);
                        }
                    }
                }
                into.add(new PipelineExplanation.Rollup(step, level, String.join(".", path), name,
                        field.function().yaml(), shown, field.filter(), List.copyOf(read)));
            });
            collectRollups(step, embed.embed(), path, into);
        }
    }

    private PipelineExplanation diagnose(String pipelineId, Observation observation) {
        Time time = time(observation.observedAt());
        Facts facts = facts(observation);

        if (time.freshness() == Freshness.STALE) {
            return answer(observation, time, Kind.OBSERVATION_STALE,
                    text("explain.observation-stale", Map.of("age", human(time.ageMillis()))),
                    List.of(evidence(Source.STATUS, "observedAgeMillis", time.ageMillis())),
                    List.of(text("explain.cannot-current-now", Map.of())),
                    next(NextAction.CHECK_SERVER, "explain.next-check-server", Map.of()));
        }
        if (observation.failure() != null) {
            ObservationFailure found = observation.failure();
            Map<String, Object> params = new LinkedHashMap<>(found.params());
            Failure failure = new Failure(found.code(), found.params(), messages.render(found.code(), params));
            return answer(observation, time, Kind.CODED_FAILURE,
                    text("explain.coded-failure", Map.of("code", found.code())),
                    List.of(evidence(Source.STATUS, "failure", failure)), List.of(),
                    next(NextAction.OPEN_PIPELINE_LOGS, "explain.next-open-logs",
                            Map.of("pipeline", pipelineId)));
        }
        if (facts.reconcileFailures() != null && facts.reconcileFailures() > 0
                && converging(observation.state())) {
            return answer(observation, time, Kind.RECONCILE_FAILURES,
                    text("explain.reconcile-failures", Map.of("count", facts.reconcileFailures())),
                    List.of(
                            evidence(Source.METRICS, RECONCILE_STREAK, facts.reconcileFailures()),
                            evidence(Source.STATUS, "state", observation.state())),
                    List.of(text("explain.cannot-job-alive", Map.of("state", lower(observation.state())))),
                    next(NextAction.CHECK_SERVER, "explain.next-check-server", Map.of()));
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
                            Map.of("pipeline", pipelineId)));
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
                    next(NextAction.CHECK_TARGET, "explain.next-check-target", Map.of()));
        }

        List<Evidence> evidence = List.of(
                evidence(Source.STATUS, "observedAgeMillis", time.ageMillis()),
                evidence(Source.STATUS, "failure", null),
                evidence(Source.METRICS, RECONCILE_STREAK, facts.reconcileFailures()),
                evidence(Source.METRICS, RECORD_COUNT, facts.recordCount()),
                evidence(Source.METRICS, "frontierStalledMillis", facts.frontierStalledMillis()),
                evidence(Source.SNAPSHOT, "rowsDone", facts.snapshotRowsDone()));
        if (facts.initialLoadTables() != null) {
            evidence = new ArrayList<>(evidence);
            evidence.add(evidence(Source.SNAPSHOT, "initialLoad", facts.initialLoadTables()));
        }
        List<String> cannotSay = new ArrayList<>();
        if (time.freshness() == Freshness.UNKNOWN) {
            cannotSay.add(text("explain.cannot-age", Map.of()));
        }
        cannotSay.add(text("explain.cannot-source-head", Map.of()));
        if (observation.state() == PipelineState.PAUSED) {
            cannotSay.add(text("explain.cannot-paused-job", Map.of()));
        }
        if (facts.initialLoadTables() == null) {
            cannotSay.add(text("explain.cannot-snapshot-phase", Map.of()));
        }
        return answer(observation, time, Kind.NO_MATCH, text("explain.no-match", Map.of()),
                evidence, cannotSay, null);
    }

    private PipelineExplanation answer(Observation observation, Time time, Kind kind, String message,
            List<Evidence> evidence, List<String> cannotSay, Next next) {
        return new PipelineExplanation(observation.pipelineId(), observation.state(), kind, message,
                observation.observedAt(), time.ageMillis(), time.freshness(), evidence, cannotSay, next, null);
    }

    private Facts facts(Observation observation) {
        Map<String, InitialLoad.Table> initialLoadTables = initialLoadTables(observation.initialLoad());
        Map<String, Long> frontierStalledMillis = new TreeMap<>();
        Map<String, Long> stalled = new TreeMap<>();
        observation.metrics().forEach((name, value) -> {
            if (name.startsWith(STALLED_PREFIX) && value != null) {
                String chain = name.substring(STALLED_PREFIX.length());
                frontierStalledMillis.put(chain, value);
                // A table still in its initial load has no durable position to advance yet, so its pinned
                // time grows with the load and says nothing about the target.
                if (FrontierStallPressure.EXPLAIN.isOver(value)
                        && !stillLoading(initialLoadTables, observation, chain)) {
                    stalled.put(chain, value);
                }
            }
        });
        long rowsDone = observation.snapshot().values().stream().mapToLong(snapshot -> snapshot.rowsDone()).sum();
        return new Facts(observation.metrics().get(RECONCILE_STREAK),
                observation.metrics().get(RECORD_COUNT),
                Collections.unmodifiableMap(new LinkedHashMap<>(frontierStalledMillis)),
                Collections.unmodifiableMap(new LinkedHashMap<>(stalled)), rowsDone, initialLoadTables);
    }

    /** Each table's initial-load reading by name, or null when the observation carries no initial load. */
    private static Map<String, InitialLoad.Table> initialLoadTables(InitialLoad initialLoad) {
        if (initialLoad == null || initialLoad.tables().isEmpty()) {
            return null;
        }
        return Collections.unmodifiableMap(new TreeMap<>(initialLoad.tables()));
    }

    /**
     * Whether the chain's table is still in its initial load. Without an initial-load reading, as a copy publishes,
     * a table the snapshot reports read through but not yet landed is still loading.
     */
    private static boolean stillLoading(Map<String, InitialLoad.Table> initialLoadTables,
            Observation observation, String chain) {
        if (initialLoadTables == null) {
            TableSnapshot table = observation.snapshot().get(chain);
            return table != null && !table.landed();
        }
        InitialLoad.Table table = initialLoadTables.get(chain);
        return table != null && table.stage() != InitialLoad.Stage.LANDED;
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
            long snapshotRowsDone, Map<String, InitialLoad.Table> initialLoadTables) {
    }
}
