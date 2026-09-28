package io.tapstate.core.dsl;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sizes each level of every nest in a batch against the in-memory budget it would run on, and reports
 * the levels that would spend their life reading state back from storage.
 *
 * <p>What makes this worth saying at all is that nothing downstream of it will. A level whose working
 * set does not fit keeps serving reads — from the layer behind memory, one round trip at a time — so
 * the events still flow, the queues stay short and the throughput stays whatever the source can supply.
 * The cost is per-event latency, which no queue depth and no backpressure signal carries.
 *
 * <p><b>Being over budget is the design, so the line is a multiple.</b> Everything past the budget is
 * kept on the layer behind it by intention; a level merely larger than its budget is a correctly
 * configured deployment. Reporting those would put a warning on nearly every pipeline, which teaches
 * the reader to stop looking at warnings — so what is reported is a level that will miss memory nearly
 * every time, not one that will miss it sometimes.
 *
 * <p><b>A level is named by the path its author wrote.</b> The map behind a level is named for the
 * pipeline and node it belongs to, and the author has never seen that name, cannot search for it, and
 * cannot change anything by knowing it. The path is what they typed and what they would edit.
 *
 * <p><b>A count that was never taken is not a count of zero.</b> Summing what is known and calling it
 * the answer would let an unmeasured table pass as an empty one — the single most expensive way to be
 * wrong here, since it silences the warning exactly where nobody has measured anything. An estimate
 * that could not see all of its inputs is reported as such, and stands as a floor.
 */
public final class NestSizingRules {

    /**
     * How many times over its budget a level has to be before it is worth telling the author about.
     *
     * <p><b>Provisional.</b> The number that belongs here is the one at which the storage round trips
     * stop being absorbed by the headroom between what a pipeline must keep up with and what it can
     * do — which needs a measured cost per round trip and a target event rate to compare it against,
     * and neither exists yet. Until then this errs high on purpose: too high loses warnings on
     * pipelines that would have been worth warning about, too low puts a warning on deployments that
     * are working as intended, and only the second kind teaches the reader to ignore the channel.
     */
    static final long FAR_EXCEEDS_MULTIPLE = 10L;

    /** How a level with no path of its own is named — the document the whole tree assembles into. */
    private static final String ROOT_LEVEL = "$root";

    private static final String PATH_SEPARATOR = ".";

    private NestSizingRules() {
    }

    /**
     * The findings over {@code batch}, in the order the levels are written. {@code tablesBySource} is
     * what each source was discovered to hold, keyed by the source's id; a source it omits has not been
     * discovered, and its tables count as uncounted rather than as empty.
     *
     * <p>{@code entriesHeldInMemoryByDefault} is the budget a nest that writes none of its own runs on
     * — what the deployment was started with. It is supplied rather than read here because the number
     * belongs to the process that will run the pipeline, not to the layer that judges it, and a copy
     * kept here would go on agreeing with a deployment that had changed underneath it.
     */
    public static List<Advisory> review(
            Collection<Resource> batch,
            Map<String, List<DiscoveredTable>> tablesBySource,
            long entriesHeldInMemoryByDefault) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource resource : batch) {
            byId.putIfAbsent(resource.id(), resource);
        }
        List<Advisory> findings = new ArrayList<>();
        for (Resource resource : batch) {
            if (resource instanceof PipelineResource pipeline) {
                reviewPipeline(pipeline, byId, tablesBySource, entriesHeldInMemoryByDefault, findings);
            }
        }
        return List.copyOf(findings);
    }

    private static void reviewPipeline(PipelineResource pipeline, Map<String, Resource> byId,
            Map<String, List<DiscoveredTable>> tablesBySource, long defaultBudget,
            List<Advisory> findings) {
        List<Step> steps = pipeline.transforms();
        if (steps == null) {
            return;
        }
        Wiring wiring = null;
        for (Step step : steps) {
            if (!(bodyOf(step, byId) instanceof TransformBody.Nest nest)
                    || !(step.from() instanceof FromClause.Aliases aliases)) {
                continue;
            }
            if (wiring == null) {
                wiring = new Wiring(pipeline, byId);
            }
            long budget = nest.entriesInMemory() == null ? defaultBudget : nest.entriesInMemory();
            Sizing sizing = new Sizing(pipeline.id(), aliases.aliases(), wiring, tablesBySource, budget,
                    findings);
            sizing.level(ROOT_LEVEL, nest.root().from());
            sizing.descend(nest.root().embed(), List.of());
        }
    }

    /**
     * The body a step runs. A step that names a reusable definition holds its own {@code from:}, so the
     * tree comes from the definition while the aliases it joins come from the step using it.
     */
    private static TransformBody bodyOf(Step step, Map<String, Resource> byId) {
        return switch (step) {
            case Step.Inline inline -> inline.body();
            case Step.Use use -> byId.get(use.use()) instanceof TransformResource definition
                    ? definition.body()
                    : null;
        };
    }

    /** One nest step being sized: everything the levels of a single tree are judged against. */
    private record Sizing(String pipelineId, Map<String, FromRef> aliases, Wiring wiring,
            Map<String, List<DiscoveredTable>> tablesBySource, long budget, List<Advisory> findings) {

        /**
         * Walks the embeds under {@code parentPath}, sizing the ones that hold state of their own.
         *
         * <p>A leaf embed is deliberately not a level. Its rows are elements inside the entry of the
         * level above it rather than entries of their own, so what bounds them is how many elements one
         * document may hold — a different knob, answering a different question. Counting them here
         * would report a level that does not exist, and would do it on the widest tables in a tree,
         * which are usually the leaves.
         */
        void descend(List<Embed> embeds, List<String> parentPath) {
            if (embeds == null) {
                return;
            }
            for (Embed embed : embeds) {
                if (embed.embed() == null || embed.embed().isEmpty()) {
                    continue;
                }
                List<String> path = new ArrayList<>(parentPath);
                path.addAll(List.of(embed.treeSegment().split("\\" + PATH_SEPARATOR, -1)));
                level(String.join(PATH_SEPARATOR, path), embed.from());
                descend(embed.embed(), path);
            }
        }

        /** Sizes the level fed by {@code alias} and reports whatever it has to say about it. */
        void level(String embedPath, String alias) {
            FromRef ref = aliases.get(alias);
            if (ref == null) {
                // An alias the from-map does not carry is a broken reference, and the gate that refuses
                // those has already run. Sizing it would replace that diagnosis with advice.
                return;
            }
            Set<Upstream> upstream = wiring.reaching(ref);
            long estimate = 0L;
            long counted = 0L;
            for (Upstream up : upstream) {
                Long rows = rowsOf(up);
                if (rows == null) {
                    continue;
                }
                estimate += rows;
                counted++;
            }
            // A level nothing could be counted for cannot reach the line: an estimate only ever grows by
            // a count that was taken, so one that saw nothing stays at zero and reports the gap below
            // instead. That is why there is no separate guard for it here — a guard no input can flip
            // would read as a case being handled when there is no such case.
            if (estimate > budget * FAR_EXCEEDS_MULTIPLE) {
                findings.add(new Advisory(NestSizingError.STATE_FAR_EXCEEDS_MEMORY_BUDGET, Map.of(
                        "pipeline", pipelineId,
                        "embedPath", embedPath,
                        "estimatedEntries", estimate,
                        "budget", budget,
                        "multiple", estimate / budget)));
            }
            if (counted < upstream.size()) {
                findings.add(new Advisory(NestSizingError.CAPACITY_ESTIMATE_INCOMPLETE, Map.of(
                        "pipeline", pipelineId,
                        "embedPath", embedPath,
                        "counted", counted,
                        "total", (long) upstream.size())));
            }
        }

        /**
         * How many rows {@code up} holds, or null where nobody counted them. An upstream the wiring
         * could not narrow to one table is null for the same reason a missing count is: which tables it
         * would read is not known, so neither is how much they hold.
         */
        private Long rowsOf(Upstream up) {
            if (up.table() == null) {
                return null;
            }
            List<DiscoveredTable> discovered = tablesBySource.get(up.source());
            if (discovered == null) {
                return null;
            }
            for (DiscoveredTable table : discovered) {
                if (table.name().equals(up.table())) {
                    return table.approximateRowCount();
                }
            }
            return null;
        }
    }
}
