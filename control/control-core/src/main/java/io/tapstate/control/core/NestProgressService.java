package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Answers "how far has this nest's load got, and what is it waiting for" from what the pipeline already
 * published: the per-table load counters and the count of documents each nest's root level sent. It reads
 * the pipeline's definition to know which tables each nest depends on and reads nothing else; it never asks
 * the running job, so it answers the same whether the job is healthy or not.
 */
public final class NestProgressService {

    /** The root level of a nest, as the namespace mapping spells its path. */
    private static final String ROOT_PATH = "[]";

    /** The map a level keeps its assemblies in, as the namespace mapping spells it. */
    private static final String STATE_MAP = "state";

    private static final String NAMESPACE_INFO = CardinalityBudget.NEST_NAMESPACE_INFO.instrument();
    private static final String ROOTS_RENDERED = CardinalityBudget.NEST_ROOTS_RENDERED.instrument();

    private final ArtifactQueryService artifacts;
    private final PipelineObservationQueryService observations;

    public NestProgressService(ArtifactQueryService artifacts, PipelineObservationQueryService observations) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.observations = Objects.requireNonNull(observations, "observations");
    }

    /**
     * The progress of every nest step of {@code pipelineId}, or of {@code step} alone when it is not null.
     * Refuses a pipeline with no nest step, and a step that is not one of its nest steps, naming the ones
     * there are.
     */
    public NestProgress progress(String pipelineId, String step) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        PipelineStatus status = observations.status(pipelineId);
        PipelineResource pipeline = pipeline(pipelineId);
        Map<String, TransformBody.Nest> nests = nests(pipeline);
        if (nests.isEmpty()) {
            throw new TapstateException(MonitorError.NO_NEST_STEP, Map.of("pipeline", pipelineId), null);
        }
        if (step != null && !nests.containsKey(step)) {
            throw new TapstateException(MonitorError.UNKNOWN_NEST_STEP, Map.of("pipeline", pipelineId,
                    "step", step, "steps", String.join(", ", nests.keySet())), null);
        }
        PipelineSnapshot snapshot = observations.snapshot(pipelineId);
        PipelineMetrics metrics = observations.metrics(pipelineId);
        Map<String, Long> rendered = rootsRenderedByStep(metrics.facts());
        List<NestProgress.Step> steps = new ArrayList<>();
        nests.forEach((id, nest) -> {
            if (step == null || step.equals(id)) {
                steps.add(stepProgress(pipeline, id, nest, status.state(), snapshot, rendered.get(id)));
            }
        });
        return new NestProgress(pipelineId, status.state(), snapshot.phase(), status.observedAt(), steps);
    }

    private PipelineResource pipeline(String pipelineId) {
        return artifacts.getResource(pipelineId)
                .map(StoredResource::resource)
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast)
                .orElseThrow(() -> new TapstateException(io.tapstate.core.lifecycle.LifecycleError.UNKNOWN_PIPELINE,
                        Map.of("pipeline", pipelineId), null));
    }

    /** The definition's steps; a pipeline that declares no transforms has none. */
    static List<Step> steps(PipelineResource pipeline) {
        return pipeline.transforms() == null ? List.of() : pipeline.transforms();
    }

    /** Each nest step by id, in the order the definition declares them. */
    static Map<String, TransformBody.Nest> nests(PipelineResource pipeline) {
        Map<String, TransformBody.Nest> nests = new LinkedHashMap<>();
        for (Step step : steps(pipeline)) {
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest) {
                nests.put(step.id(), nest);
            }
        }
        return nests;
    }

    private static NestProgress.Step stepProgress(PipelineResource pipeline, String id, TransformBody.Nest nest,
            PipelineState state, PipelineSnapshot snapshot, Long rendered) {
        Map<String, FromRef> aliases = ((FromClause.Aliases) stepNamed(pipeline, id).from()).aliases();
        Map<String, InitialLoad.Table> loads = snapshot.initialLoad() == null ? Map.of()
                : snapshot.initialLoad().tables();
        Set<String> known = new LinkedHashSet<>(loads.keySet());
        known.addAll(snapshot.snapshot().keySet());

        List<NestProgress.Table> tables = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        NestRoot root = nest.root();
        List<String> rootTables = tablesOf(pipeline, aliases.get(root.from()), root.from(), known);
        for (String table : rootTables) {
            if (seen.add(table)) {
                tables.add(table(table, "", loads, snapshot.snapshot()));
            }
        }
        collect(pipeline, aliases, root.embed(), "", known, loads, snapshot.snapshot(), tables, seen);

        String rootTable = rootTables.isEmpty() ? root.from() : rootTables.getFirst();
        Long estimate = tables.isEmpty() ? null : tables.getFirst().rowsEstimate();
        NestProgress.Activity activity = activity(state, tables);
        return new NestProgress.Step(id, rootTable, rendered, estimate, activity, waitingFor(activity, tables),
                tables);
    }

    private static Step stepNamed(PipelineResource pipeline, String id) {
        return steps(pipeline).stream().filter(step -> step.id().equals(id)).findFirst().orElseThrow();
    }

    private static void collect(PipelineResource pipeline, Map<String, FromRef> aliases, List<Embed> embeds,
            String parentPath, Set<String> known, Map<String, InitialLoad.Table> loads,
            Map<String, TableSnapshot> snapshots, List<NestProgress.Table> tables, Set<String> seen) {
        if (embeds == null) {
            return;
        }
        for (Embed embed : embeds) {
            String path = embed.as() == EmbedAs.FLAT || embed.path() == null ? parentPath
                    : parentPath.isEmpty() ? embed.path() : parentPath + "." + embed.path();
            for (String table : tablesOf(pipeline, aliases.get(embed.from()), embed.from(), known)) {
                if (seen.add(table)) {
                    tables.add(table(table, path, loads, snapshots));
                }
            }
            collect(pipeline, aliases, embed.embed(), path, known, loads, snapshots, tables, seen);
        }
    }

    /**
     * The tables one alias of a nest reads, by the names the load reports them under. An alias naming a
     * table resolves to that table; one naming an upstream step resolves to whatever that step reads, all
     * the way to the sources. Where the load reports nothing that matches, the definition's own name is
     * kept so the table is still listed, just without numbers.
     */
    private static List<String> tablesOf(PipelineResource pipeline, FromRef ref, String alias, Set<String> known) {
        if (ref == null) {
            return List.of(alias);
        }
        Set<String> found = new LinkedHashSet<>();
        resolve(pipeline, ref, known, found, new HashSet<>());
        if (found.isEmpty() && ref instanceof FromRef.Literal literal) {
            found.add(literal.ref());
        }
        return List.copyOf(found);
    }

    private static void resolve(PipelineResource pipeline, FromRef ref, Set<String> known, Set<String> found,
            Set<String> visited) {
        if (ref instanceof FromRef.Regex regex) {
            Pattern pattern;
            try {
                pattern = Pattern.compile(regex.pattern());
            } catch (PatternSyntaxException invalid) {
                return;
            }
            known.stream().filter(label -> pattern.matcher(label).matches()
                    || pattern.matcher(unqualified(label)).matches()).forEach(found::add);
            return;
        }
        String name = ((FromRef.Literal) ref).ref();
        Step upstream = steps(pipeline).stream().filter(step -> step.id().equals(name)).findFirst()
                .orElse(null);
        if (upstream != null) {
            if (visited.add(name)) {
                for (FromRef inner : refsOf(upstream.from())) {
                    resolve(pipeline, inner, known, found, visited);
                }
            }
            return;
        }
        for (String label : known) {
            if (label.equals(name) || label.equals(unqualified(name)) || unqualified(label).equals(name)
                    && !name.contains(".")) {
                found.add(label);
            }
        }
    }

    private static List<FromRef> refsOf(FromClause from) {
        return switch (from) {
            case FromClause.Flow flow -> flow.refs();
            case FromClause.Aliases named -> List.copyOf(named.aliases().values());
        };
    }

    /** {@code source.table} without its source, or the name unchanged when it carries none. */
    private static String unqualified(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(dot + 1);
    }

    private static NestProgress.Table table(String table, String path, Map<String, InitialLoad.Table> loads,
            Map<String, TableSnapshot> snapshots) {
        InitialLoad.Table load = loads.get(table);
        if (load != null) {
            return new NestProgress.Table(table, path, load.stage(), load.rowsRead(), load.rowsTotalEstimate(),
                    null);
        }
        TableSnapshot read = snapshots.get(table);
        if (read != null) {
            return new NestProgress.Table(table, path,
                    read.landed() ? InitialLoad.Stage.LANDED : InitialLoad.Stage.LANDING,
                    read.rowsDone(), read.rowsTotal(), null);
        }
        return new NestProgress.Table(table, path, null, null, null, null);
    }

    private static NestProgress.Activity activity(PipelineState state, List<NestProgress.Table> tables) {
        if (state != PipelineState.RUNNING && state != PipelineState.COMPLETED) {
            return NestProgress.Activity.NOT_RUNNING;
        }
        if (tables.stream().allMatch(table -> table.stage() == null)) {
            return NestProgress.Activity.UNKNOWN;
        }
        if (tables.stream().anyMatch(table -> table.stage() == InitialLoad.Stage.PENDING
                || table.stage() == InitialLoad.Stage.READING)) {
            return NestProgress.Activity.LOADING;
        }
        if (tables.stream().anyMatch(table -> table.stage() == InitialLoad.Stage.LANDING)) {
            return NestProgress.Activity.LANDING;
        }
        // A table with no reported load may still be unread: the load is complete only when every one says so.
        if (tables.stream().anyMatch(table -> table.stage() == null)) {
            return NestProgress.Activity.UNKNOWN;
        }
        return state == PipelineState.COMPLETED ? NestProgress.Activity.COMPLETED : NestProgress.Activity.MAINTAINING;
    }

    /**
     * The tables to look at. While loading, those not read through yet: a document built before them is
     * missing their rows, and the root table is listed last among them because the documents wait on it
     * only for their existence. While landing, those the target has not confirmed. Otherwise none.
     */
    private static List<String> waitingFor(NestProgress.Activity activity, List<NestProgress.Table> tables) {
        List<String> waiting = new ArrayList<>();
        switch (activity) {
            case LOADING -> {
                for (NestProgress.Table table : tables.subList(Math.min(1, tables.size()), tables.size())) {
                    if (table.stage() == InitialLoad.Stage.PENDING || table.stage() == InitialLoad.Stage.READING) {
                        waiting.add(table.table());
                    }
                }
                if (!tables.isEmpty() && (tables.getFirst().stage() == InitialLoad.Stage.PENDING
                        || tables.getFirst().stage() == InitialLoad.Stage.READING)) {
                    waiting.add(tables.getFirst().table());
                }
            }
            case LANDING -> tables.stream().filter(table -> table.stage() == InitialLoad.Stage.LANDING)
                    .map(NestProgress.Table::table).forEach(waiting::add);
            default -> {
            }
        }
        return waiting;
    }

    /**
     * Documents sent per nest step, read off the published facts: the namespace mapping says which namespace
     * is a step's root level, and the roots-rendered count is filed under that namespace.
     */
    static Map<String, Long> rootsRenderedByStep(List<MetricFact> facts) {
        Map<String, String> stepByRootNamespace = new LinkedHashMap<>();
        Map<String, Long> renderedByNamespace = new LinkedHashMap<>();
        for (MetricFact fact : facts) {
            if (fact.name().equals(NAMESPACE_INFO)) {
                for (MetricPoint point : fact.points()) {
                    Map<String, String> attributes = point.attributes();
                    if (ROOT_PATH.equals(attributes.get(MetricAttributes.NEST_PATH))
                            && STATE_MAP.equals(attributes.get(MetricAttributes.NEST_MAP))
                            && attributes.get(MetricAttributes.NEST_NAMESPACE) != null
                            && attributes.get(MetricAttributes.NEST_STEP) != null) {
                        stepByRootNamespace.put(attributes.get(MetricAttributes.NEST_NAMESPACE),
                                attributes.get(MetricAttributes.NEST_STEP));
                    }
                }
            } else if (fact.name().equals(ROOTS_RENDERED)) {
                for (MetricPoint point : fact.points()) {
                    String namespace = point.attributes().get(MetricAttributes.NEST_NAMESPACE);
                    if (namespace != null && point.value() != null) {
                        renderedByNamespace.merge(namespace, point.value(), Long::sum);
                    }
                }
            }
        }
        Map<String, Long> byStep = new LinkedHashMap<>();
        renderedByNamespace.forEach((namespace, rendered) -> {
            String step = stepByRootNamespace.get(namespace);
            if (step != null) {
                byStep.merge(step, rendered, Long::sum);
            }
        });
        return byStep;
    }
}
