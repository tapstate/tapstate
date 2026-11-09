package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.NestTree;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Explains a nest step before it runs: the tree it compiles to, and what a small sample of its sources says
 * about the shapes it assumes. Reads only - a few rows and a row count per table, through the same read-only
 * probes the data browser uses - and refuses nothing on what it finds: a finding is a warning naming the
 * embed, the count it was estimated from and the change that avoids the failure.
 */
public final class NestExplainService {

    /** Rows read from each table. Small on purpose: this runs on a person's command, against live sources. */
    static final int SAMPLE_ROWS = 200;

    /** A count at or above this share of its limit is reported; a run fails only at the limit itself. */
    static final double NEAR = 0.8;

    static final String BUILD = "not available: this version has no build phase, so the whole load runs "
            + "through change assembly, one row at a time";

    private final ArtifactQueryService artifacts;
    private final NestTreeReader trees;
    private final NestSampler sampler;

    public NestExplainService(ArtifactQueryService artifacts, NestTreeReader trees, NestSampler sampler) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.trees = Objects.requireNonNull(trees, "trees");
        this.sampler = Objects.requireNonNull(sampler, "sampler");
    }

    /** The explanation of nest step {@code step} of {@code pipelineId}. */
    public NestExplanation explain(String pipelineId, String step) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(step, "step");
        PipelineResource pipeline = artifacts.getResource(pipelineId)
                .map(StoredResource::resource)
                .filter(PipelineResource.class::isInstance)
                .map(PipelineResource.class::cast)
                .orElseThrow(() -> new TapstateException(LifecycleError.UNKNOWN_PIPELINE,
                        Map.of("pipeline", pipelineId), null));
        Map<String, TransformBody.Nest> nests = NestProgressService.nests(pipeline);
        if (nests.isEmpty()) {
            throw new TapstateException(MonitorError.NO_NEST_STEP, Map.of("pipeline", pipelineId), null);
        }
        if (!nests.containsKey(step)) {
            throw new TapstateException(MonitorError.UNKNOWN_NEST_STEP, Map.of("pipeline", pipelineId,
                    "step", step, "steps", String.join(", ", nests.keySet())), null);
        }
        NestTree tree = trees.tree(pipelineId, step);
        Step declared = NestProgressService.steps(pipeline).stream().filter(s -> s.id().equals(step)).findFirst().orElseThrow();
        Map<String, FromRef> aliases = declared.from() instanceof FromClause.Aliases named ? named.aliases()
                : Map.of();
        return new Run(pipeline, aliases, tree).explain();
    }

    /** One explanation's working state: each alias is located and sampled at most once. */
    private final class Run {
        private final PipelineResource pipeline;
        private final Map<String, FromRef> aliases;
        private final NestTree tree;
        private final Map<String, Located> located = new HashMap<>();
        private final List<NestExplanation.Sampled> samples = new ArrayList<>();
        private final List<NestExplanation.Warning> warnings = new ArrayList<>();

        Run(PipelineResource pipeline, Map<String, FromRef> aliases, NestTree tree) {
            this.pipeline = pipeline;
            this.aliases = aliases;
            this.tree = tree;
        }

        NestExplanation explain() {
            Located root = locate(tree.rootAlias());
            for (NestTree.Level level : tree.embeds()) {
                walk(level, root, root);
            }
            return new NestExplanation(tree, BUILD, samples, warnings);
        }

        private void walk(NestTree.Level level, Located parent, Located root) {
            Located child = locate(level.alias());
            check(level, parent, root, child);
            for (NestTree.Level below : level.children()) {
                walk(below, child, root);
            }
        }

        private void check(NestTree.Level level, Located parent, Located root, Located child) {
            String unavailable = unavailable(level, parent, child);
            if (unavailable != null) {
                samples.add(new NestExplanation.Sampled(level.path(), level.alias(), child.source(), child.table(),
                        child.sample() == null ? null : child.sample().tableRows(), 0, null, null, unavailable));
            } else if (level.owned()) {
                checkOwned(level, root, child);
            } else {
                checkShared(level, parent, root, child);
            }
        }

        /** Why nothing can be counted for {@code level}, or null when it can. */
        private static String unavailable(NestTree.Level level, Located parent, Located child) {
            if (child.why() != null) {
                return child.why();
            }
            // A shared row is counted from its parents' rows, so it needs the parent read as well.
            if (!level.owned() && parent.why() != null) {
                return "parent not sampled: " + parent.why();
            }
            return null;
        }

        /** Rows of an owned embed under one parent, by the child side of the join. */
        private void checkOwned(NestTree.Level level, Located root, Located child) {
            NestSampler.Sample rows = child.sample();
            long largest = largestGroup(rows.rows(), List.copyOf(level.on().keySet()));
            Double average = ratio(rows.tableRows(), rowsOf(root));
            samples.add(new NestExplanation.Sampled(level.path(), level.alias(), child.source(), child.table(),
                    rows.tableRows(), rows.rows().size(), largest, average, null));
            if (!"array".equals(level.as())) {
                if (largest > 1) {
                    warnings.add(warning(MonitorError.NEST_OBJECT_KEY_NOT_UNIQUE, level, child, largest,
                            "sampled", rows.rows().size()));
                }
                return;
            }
            long estimate = estimate(largest, average);
            if (tree.elementLimit() > 0 && estimate >= NEAR * tree.elementLimit()) {
                warnings.add(warning(MonitorError.NEST_NEAR_ELEMENT_LIMIT, level, child, estimate,
                        LIMIT, tree.elementLimit()));
            }
        }

        /** Parents pointing at one shared row, by the parent side of the join. */
        private void checkShared(NestTree.Level level, Located parent, Located root, Located child) {
            NestSampler.Sample rows = child.sample();
            long largest = largestGroup(parent.sample().rows(), List.copyOf(level.on().values()));
            Double average = ratio(rowsOf(root), rows.tableRows());
            samples.add(new NestExplanation.Sampled(level.path(), level.alias(), child.source(), child.table(),
                    rows.tableRows(), parent.sample().rows().size(), largest, average, null));
            long estimate = estimate(largest, average);
            if (level.referrerLimit() > 0 && estimate >= NEAR * level.referrerLimit()) {
                warnings.add(warning(MonitorError.NEST_REFERENCE_WIDELY_SHARED, level, child, estimate,
                        LIMIT, level.referrerLimit()));
            }
        }

        private Located locate(String alias) {
            return located.computeIfAbsent(alias, this::find);
        }

        private Located find(String alias) {
            FromRef ref = aliases.get(alias);
            if (!(ref instanceof FromRef.Literal literal)) {
                return Located.none(ref == null ? "the alias names nothing in from:"
                        : "the alias names a pattern, not one table");
            }
            String name = literal.ref();
            if (NestProgressService.steps(pipeline).stream().anyMatch(s -> s.id().equals(name))) {
                return Located.none("the alias reads step " + name + ", not a source table");
            }
            List<String> sources = pipeline.sources().stream().map(SourceRef::id).toList();
            String source = null;
            String table = name;
            int dot = name.indexOf('.');
            if (dot > 0 && sources.contains(name.substring(0, dot))) {
                source = name.substring(0, dot);
                table = name.substring(dot + 1);
            } else {
                List<String> naming = new ArrayList<>();
                for (String id : sources) {
                    if (declares(id, name)) {
                        naming.add(id);
                    }
                }
                if (naming.size() == 1) {
                    source = naming.getFirst();
                } else if (sources.size() == 1) {
                    source = sources.getFirst();
                }
            }
            if (source == null) {
                return Located.none("more than one source could hold " + name + "; qualify it as source.table");
            }
            NestSampler.Sample sample = sampler.sample(source, table, SAMPLE_ROWS);
            return new Located(source, table, sample, sample.unavailable());
        }

        private boolean declares(String sourceId, String table) {
            return artifacts.getResource(sourceId)
                    .map(StoredResource::resource)
                    .filter(SourceResource.class::isInstance)
                    .map(SourceResource.class::cast)
                    .map(SourceResource::tables)
                    .map(tables -> tables.stream().anyMatch(ref -> switch (ref) {
                        case TableRef.Literal literal -> literal.name().equals(table);
                        case TableRef.Spec spec -> spec.name().equals(table);
                        case TableRef.Regex ignored -> false;
                    }))
                    .orElse(false);
        }
    }

    private static final String LIMIT = "limit";

    private static NestExplanation.Warning warning(MonitorError code, NestTree.Level level, Located child,
            long count, String boundName, Object bound) {
        return new NestExplanation.Warning(code, level.path(), Map.of("embed", level.path(), "table", child.table(),
                "count", count, boundName, bound));
    }

    /** The larger of the most found under one parent and the average from the two tables' row counts. */
    private static long estimate(long largest, Double average) {
        return Math.max(largest, average == null ? 0 : Math.round(average));
    }

    private static Long rowsOf(Located located) {
        return located.sample() == null ? null : located.sample().tableRows();
    }

    /** Where an alias was read from, and what was read; {@code why} says why nothing was. */
    private record Located(String source, String table, NestSampler.Sample sample, String why) {
        static Located none(String why) {
            return new Located(null, null, null, why);
        }
    }

    /** The most rows sharing one value of {@code columns}; rows missing any of them are not counted. */
    static long largestGroup(List<Map<String, Object>> rows, List<String> columns) {
        if (columns.isEmpty()) {
            return 0;
        }
        Map<List<Object>, Long> groups = new LinkedHashMap<>();
        long largest = 0;
        for (Map<String, Object> row : rows) {
            List<Object> key = new ArrayList<>(columns.size());
            boolean complete = true;
            for (String column : columns) {
                Object value = row.get(column);
                if (value == null) {
                    complete = false;
                    break;
                }
                key.add(value);
            }
            if (complete) {
                largest = Math.max(largest, groups.merge(key, 1L, Long::sum));
            }
        }
        return largest;
    }

    private static Double ratio(Long over, Long under) {
        return over == null || under == null || under <= 0 ? null : (double) over / under;
    }
}
