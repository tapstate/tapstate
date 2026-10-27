package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.validation.ValidationError;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One pipeline's wiring, read backwards: what a given node reads. A reference is a step or view id
 * (follow it), a source-qualified table (that table of that source), or a bare table name (that
 * table, of whichever sources can supply it).
 *
 * <p>Every offline rule that has to reach past a node to the tables behind it resolves through this
 * one implementation. A second, narrower resolver written for one rule would answer the same
 * question differently the first time a wiring shape changed, and the rules would disagree about
 * what a pipeline reads without either of them being obviously wrong.
 */
final class Wiring {

    private final PipelineResource pipeline;
    private final Map<String, Resource> byId;
    private final Set<String> allSources;
    private final Map<String, FromClause> nodeFrom = new LinkedHashMap<>();
    private final Map<String, Step> stepsById = new LinkedHashMap<>();
    private final Map<String, Set<String>> tablesBySelector = new LinkedHashMap<>();
    /** Sources whose table set cannot be enumerated offline, so any table name may come from them. */
    private final Set<String> openSources = new LinkedHashSet<>();
    private final Set<String> indexedSources = new HashSet<>();
    private final InspectionBudget inspection;

    Wiring(PipelineResource pipeline, Map<String, Resource> byId) { this(pipeline, byId, null); }

    private Wiring(PipelineResource pipeline, Map<String, Resource> byId, InspectionBudget inspection) {
        this.inspection = inspection;
        this.pipeline = pipeline;
        this.byId = byId;
        if (inspection != null) {
            int steps = pipeline.transforms() == null ? 0 : pipeline.transforms().size();
            if (pipeline.sources().size() + (long)steps + (pipeline.view() == null ? 0 : 1) > inspection.limits.nodes()) throw inspection.limit("nodes");
        }
        if (inspection != null) {
            for (var source : pipeline.sources()) inspection.charge(128L + 2L * source.id().length());
        }
        this.allSources = new LinkedHashSet<>(pipeline.sourceIds());
        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                if (inspection != null) inspection.nodeDeclaration(step.id(), nodeFrom.containsKey(step.id()) || allSources.contains(step.id()));
                nodeFrom.put(step.id(), step.from());
                stepsById.put(step.id(), step);
            }
        }
        indexView(pipeline.view());
        if (inspection == null) for (String sourceId : allSources) indexSource(sourceId);
    }

    private void indexView(ViewBlock view) {
        switch (view) {
            case null -> {
            }
            case ViewBlock.Inline inline -> {
                if (inspection != null) inspection.nodeDeclaration(inline.id(), nodeFrom.containsKey(inline.id()) || allSources.contains(inline.id()));
                nodeFrom.put(inline.id(), new FromClause.Flow(List.of(inline.from())));
            }
            case ViewBlock.Use use -> {
                if (inspection != null) inspection.nodeDeclaration(use.id(), nodeFrom.containsKey(use.id()) || allSources.contains(use.id()));
                nodeFrom.put(use.id(), new FromClause.Flow(List.of(use.from())));
            }
        }
    }

    private void indexSource(String sourceId) {
        if (!indexedSources.add(sourceId)) return;
        if (inspection != null) {
            inspection.sourceDeclarations.add(sourceId); inspection.charge(128L + 2L * sourceId.length());
            if (!(byId.get(sourceId) instanceof SourceResource source) || !sourceId.equals(source.id())) throw missing("source", sourceId);
        }
        Set<String> tables = new LinkedHashSet<>();
        if (!(byId.get(sourceId) instanceof SourceResource source) || source.tables() == null) {
            openSources.add(sourceId);      // no table selector: the whole source is in play
            tablesBySelector.put(sourceId, tables);
            return;
        }
        for (TableRef table : source.tables()) {
            if (inspection != null) {
                inspection.touch();
                String selector = switch (table) {
                    case TableRef.Literal literal -> literal.name();
                    case TableRef.Spec spec -> spec.name();
                    case TableRef.Regex regex -> regex.pattern();
                };
                inspection.charge(64L + 2L * selector.length());
            }
            switch (table) {
                case TableRef.Literal literal -> tables.add(literal.name());
                case TableRef.Spec spec -> tables.add(spec.name());
                case TableRef.Regex regex -> openSources.add(sourceId);
            }
        }
        tablesBySelector.put(sourceId, tables);
    }

    Set<Upstream> reaching(FromClause from) {
        Set<Upstream> reached = new LinkedHashSet<>();
        collect(from, reached, new HashSet<>(), null, List.of());
        return attributed(reached);
    }

    /** One source-table path, with its transforms in the order rows encounter them. */
    record Route(Upstream upstream, List<Step> steps) {
    }

    /**
     * Keeps each path distinct so a map on one branch cannot change the types of another branch's
     * rows, even when both branches read the same source table. Uses the shared wiring traversal.
     */
    List<Route> routesReaching(FromClause from) {
        List<Route> routes = new ArrayList<>();
        collect(from, new LinkedHashSet<>(), new HashSet<>(), routes, new ArrayList<>());
        if (routes.isEmpty() && inspection == null) {
            for (Upstream upstream : whole(allSources)) {
                routes.add(new Route(upstream, List.of()));
            }
        }
        return List.copyOf(routes);
    }

    List<Route> routesReaching(FromRef ref) {
        return routesReaching(new FromClause.Flow(List.of(ref)));
    }

    record InspectionLimits(int nodes, int references, int routes, int depth, long bytes) {
        InspectionLimits {
            if (nodes < 1 || nodes > 4096 || references < 1 || references > 65536 || routes < 1 || routes > 4096
                    || depth < 1 || depth > 256 || bytes < 1 || bytes > 16L * 1024 * 1024)
                throw new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "validation.wiringLimits"), null);
        }
    }
    record InspectedRoute(Upstream upstream, FromRef selector, List<Step> steps) {}
    record PendingReference(FromRef reference, List<Step> downstreamSteps) {}
    record Inspection(List<InspectedRoute> routes, Set<String> nodes, Set<String> sourceDeclarations,
                      List<PendingReference> pending, long accountedBytes) {
        Inspection {
            routes = List.copyOf(routes); nodes = Set.copyOf(nodes); sourceDeclarations = Set.copyOf(sourceDeclarations);
            pending = List.copyOf(pending);
        }
        boolean declarationComplete() { return pending.isEmpty() && !routes.isEmpty(); }
    }

    /**
     * Bounded declaration inspection for validation. Dynamic links and ambiguous open table sets
     * remain pending; they never become an inferred all-source execution range. A complete declaration
     * still requires separately pinned discovery/models, reusable bodies and physical table binding.
     * Input resources and their materialization are admitted by the caller. Legacy discovery rules
     * retain their conservative over-approximation through the ordinary constructor above.
     */
    static Inspection inspect(PipelineResource pipeline, Map<String, Resource> byId, FromClause selected,
            InspectionLimits limits) {
        Objects.requireNonNull(pipeline, "pipeline"); Objects.requireNonNull(byId, "byId");
        Objects.requireNonNull(selected, "selected");
        var budget = new InspectionBudget(Objects.requireNonNull(limits, "limits"));
        new Wiring(pipeline, byId, budget).routesReaching(selected);
        return new Inspection(budget.routes, budget.nodes, budget.sourceDeclarations, budget.pending, budget.bytes);
    }

    /**
     * The pipeline's own nodes a reference reads through - the steps and views standing between it
     * and the sources, not the sources themselves. A rule about what a step does to the rows on
     * their way out needs this rather than {@link #reaching}, which deliberately walks past the
     * steps to the tables behind them.
     *
     * <p>The traversal is the same one, and the set it already keeps to stop a cycle from looping
     * forever is exactly the set of nodes it followed - so this reads that out rather than walking
     * the wiring a second way and eventually disagreeing about what a pipeline reads.
     */
    Set<String> nodesReaching(FromClause from) {
        Set<String> followed = new LinkedHashSet<>();
        collect(from, new LinkedHashSet<>(), followed, null, List.of());
        return followed;
    }

    /** A serve or view block is wired by a single reference rather than a list of them. */
    Set<Upstream> reaching(FromRef ref) {
        Set<Upstream> reached = new LinkedHashSet<>();
        collect(ref, reached, new HashSet<>(), null, List.of());
        return attributed(reached);
    }

    /**
     * Reaching nothing is not the same as reading nothing. Wiring that closes on itself — a step
     * whose chain of references leads back to a step already being followed — runs out of nodes
     * before it reaches a source, and every node in that chain still reads whatever the pipeline
     * reads. Answering "nothing" there would drop the discovery obligation and leave every column
     * untyped, so the expression would pass unexamined; the whole source set is the same answer an
     * unattributable name gets.
     */
    private Set<Upstream> attributed(Set<Upstream> reached) {
        return reached.isEmpty() ? whole(allSources) : reached;
    }

    private void collect(FromClause from, Set<Upstream> reached, Set<String> visiting,
            List<Route> routes, List<Step> steps) {
        switch (from) {
            case null -> {
            }
            case FromClause.Flow flow ->
                    flow.refs().forEach(ref -> collect(ref, reached, visiting, routes, steps));
            // nest / join: the alias map's values are what the node reads
            case FromClause.Aliases aliases ->
                    aliases.aliases().values().forEach(ref -> collect(ref, reached, visiting, routes, steps));
        }
    }

    private void collect(FromRef ref, Set<Upstream> reached, Set<String> visiting,
            List<Route> routes, List<Step> steps) {
        if (inspection != null) {
            inspection.touch();
            String value = ref instanceof FromRef.Literal literal ? literal.ref() : ((FromRef.Regex)ref).pattern();
            inspection.charge(64L + 2L * value.length());
        }
        if (ref instanceof FromRef.Regex regex) {
            if (inspection != null) { inspection.pending(ref, steps); return; }
            collectRegex(regex, reached, visiting, routes, steps);
            return;
        }
        String token = ((FromRef.Literal) ref).ref();
        int dot = token.indexOf('.');
        if (dot >= 0) {
            String prefix = token.substring(0, dot);
            String table = token.substring(dot + 1);
            if (allSources.contains(prefix)) {
                if (inspection != null) {
                    indexSource(prefix);
                    if (table.isEmpty() || !openSources.contains(prefix) && !tablesBySelector.get(prefix).contains(table)) throw missing("validation.from", token);
                }
                arrive(Set.of(new Upstream(prefix, table)), reached, routes, steps, ref);
            } else {
                if (inspection != null) throw missing("validation.from", token);
                arrive(whole(allSources), reached, routes, steps, ref);
            }
            return;
        }
        if (nodeFrom.containsKey(token)) {
            followNode(token, reached, visiting, routes, steps);
            return;
        }
        if (inspection != null) {
            if (allSources.contains(token)) {
                indexSource(token);
                arrive(Set.of(new Upstream(token, null)), reached, routes, steps, ref);
                return;
            }
            for (String sourceId : allSources) indexSource(sourceId);
        }
        // A bare token is a table name, so the table is known even where the source is not: it is
        // read from whichever sources can supply that name.
        Set<String> supplying = new LinkedHashSet<>(openSources);
        int declaredSuppliers = 0;
        for (var entry : tablesBySelector.entrySet()) {
            if (inspection != null) inspection.touch();
            if (entry.getValue().contains(token)) {
                supplying.add(entry.getKey());
                declaredSuppliers++;
            }
        }
        if (inspection != null) {
            if (declaredSuppliers > 1) throw new DslException(DslError.AMBIGUOUS_REFERENCE, "validation.from", 0, 0, null, Map.of("ref", token));
            if (!openSources.isEmpty()) { inspection.pending(ref, steps); return; }
            if (supplying.isEmpty()) throw missing("validation.from", token);
        }
        // A name nothing claims cannot be attributed, so no source may be ruled out.
        for (String sourceId : supplying.isEmpty() ? allSources : supplying) {
            arrive(Set.of(new Upstream(sourceId, token)), reached, routes, steps, ref);
        }
    }

    private void collectRegex(FromRef.Regex regex, Set<Upstream> reached, Set<String> visiting,
            List<Route> routes, List<Step> steps) {
        // Table discovery stays conservative, but declared transform IDs are already known.
        arrive(whole(allSources), reached, routes, steps, regex);
        java.util.regex.Pattern pattern;
        try {
            pattern = java.util.regex.Pattern.compile(regex.pattern());
        } catch (java.util.regex.PatternSyntaxException invalid) {
            // Runtime owns the syntax diagnostic; an unresolvable selector cannot hide a step.
            pattern = null;
        }
        for (String stepId : stepsById.keySet()) {
            if (pattern == null || pattern.matcher(stepId).matches()) {
                followNode(stepId, reached, visiting, routes, steps);
            }
        }
    }

    private void followNode(String token, Set<Upstream> reached, Set<String> visiting,
            List<Route> routes, List<Step> steps) {
        if (inspection != null && visiting.size() >= inspection.limits.depth()) throw inspection.limit("depth");
        if (!visiting.add(token)) {
            if (inspection != null) throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                    Map.of("node", token, "field", "from", "rule", "cyclic-reference"), null);
            return;
        }
        if (inspection != null) inspection.nodes.add(token);
        Step step = stepsById.get(token);
        boolean trackStep = routes != null && step != null;
        if (trackStep) {
            steps.add(step);
        }
        collect(nodeFrom.get(token), reached, visiting, routes, steps);
        if (routes != null) {
            if (trackStep) {
                steps.removeLast();
            }
            // Route collection follows shared nodes once per path; source-only collection
            // keeps its global visited set, which also backs nodesReaching.
            visiting.remove(token);
        }
    }

    private void arrive(Set<Upstream> upstream, Set<Upstream> reached,
            List<Route> routes, List<Step> steps, FromRef selector) {
        reached.addAll(upstream);
        if (routes != null) {
            List<Step> ordered = List.copyOf(steps.reversed());
            for (Upstream up : upstream) {
                if (inspection != null) inspection.route(up, selector, ordered);
                routes.add(new Route(up, ordered));
            }
        }
    }

    /** Every table of each named source — the answer where the wiring names no single one. */
    private Set<Upstream> whole(Set<String> sources) {
        Set<Upstream> all = new LinkedHashSet<>();
        for (String sourceId : sources) {
            all.add(new Upstream(sourceId, null));
        }
        return all;
    }

    private static DslException missing(String path, String reference) {
        return new DslException(DslError.MISSING_REFERENCE, path, 0, 0, null, Map.of("ref", reference));
    }
    private static final class InspectionBudget {
        private final InspectionLimits limits;
        private final List<InspectedRoute> routes = new ArrayList<>();
        private final List<PendingReference> pending = new ArrayList<>();
        private final Set<String> nodes = new LinkedHashSet<>();
        private final Set<String> sourceDeclarations = new LinkedHashSet<>();
        private int references;
        private long bytes;
        private InspectionBudget(InspectionLimits limits) { this.limits = limits; charge(512); }
        private void touch() { if (references++ == limits.references()) throw limit("references"); }
        private void nodeDeclaration(String id, boolean duplicate) {
            if (duplicate) throw new DslException(DslError.DUPLICATE_ID, "validation.nodes", 0, 0, null, Map.of("id", id));
            charge(128L + 2L * id.length());
        }
        private void route(Upstream upstream, FromRef selector, List<Step> steps) {
            if (routes.size() + pending.size() == limits.routes()) throw limit("routes");
            charge(192L + 2L * upstream.source().length() + (upstream.table() == null ? 0 : 2L * upstream.table().length()) + 8L * steps.size());
            routes.add(new InspectedRoute(upstream, selector, steps));
        }
        private void pending(FromRef reference, List<Step> steps) {
            if (routes.size() + pending.size() == limits.routes()) throw limit("routes");
            charge(128L + 8L * steps.size());
            pending.add(new PendingReference(reference, List.copyOf(steps.reversed())));
        }
        private void charge(long amount) { if (amount > limits.bytes() - bytes) throw limit("bytes"); bytes += amount; }
        private TapstateException limit(String dimension) {
            return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "validation.wiring." + dimension), null);
        }
    }

    @Override
    public String toString() {
        return "wiring of " + pipeline.id();
    }
}
