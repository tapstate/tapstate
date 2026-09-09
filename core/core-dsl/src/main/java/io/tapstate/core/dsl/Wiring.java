package io.tapstate.core.dsl;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
    private final Map<String, Set<String>> tablesBySelector = new LinkedHashMap<>();
    /** Sources whose table set cannot be enumerated offline, so any table name may come from them. */
    private final Set<String> openSources = new LinkedHashSet<>();

    Wiring(PipelineResource pipeline, Map<String, Resource> byId) {
        this.pipeline = pipeline;
        this.byId = byId;
        this.allSources = new LinkedHashSet<>(pipeline.sourceIds());
        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                nodeFrom.put(step.id(), step.from());
            }
        }
        indexView(pipeline.view());
        for (String sourceId : allSources) {
            indexSource(sourceId);
        }
    }

    private void indexView(ViewBlock view) {
        switch (view) {
            case null -> {
            }
            case ViewBlock.Inline inline -> nodeFrom.put(inline.id(), new FromClause.Flow(List.of(inline.from())));
            case ViewBlock.Use use -> nodeFrom.put(use.id(), new FromClause.Flow(List.of(use.from())));
        }
    }

    private void indexSource(String sourceId) {
        Set<String> tables = new LinkedHashSet<>();
        if (!(byId.get(sourceId) instanceof SourceResource source) || source.tables() == null) {
            openSources.add(sourceId);      // no table selector: the whole source is in play
            tablesBySelector.put(sourceId, tables);
            return;
        }
        for (TableRef table : source.tables()) {
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
        collect(from, reached, new HashSet<>());
        return attributed(reached);
    }

    /** A serve or view block is wired by a single reference rather than a list of them. */
    Set<Upstream> reaching(FromRef ref) {
        Set<Upstream> reached = new LinkedHashSet<>();
        collect(ref, reached, new HashSet<>());
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

    private void collect(FromClause from, Set<Upstream> reached, Set<String> visiting) {
        switch (from) {
            case null -> {
            }
            case FromClause.Flow flow -> flow.refs().forEach(ref -> collect(ref, reached, visiting));
            // nest / join: the alias map's values are what the node reads
            case FromClause.Aliases aliases ->
                    aliases.aliases().values().forEach(ref -> collect(ref, reached, visiting));
        }
    }

    private void collect(FromRef ref, Set<Upstream> reached, Set<String> visiting) {
        if (ref instanceof FromRef.Regex) {
            // which tables it selects needs a connection to answer, so every table is in play
            reached.addAll(whole(allSources));
            return;
        }
        String token = ((FromRef.Literal) ref).ref();
        int dot = token.indexOf('.');
        if (dot >= 0) {
            String prefix = token.substring(0, dot);
            String table = token.substring(dot + 1);
            if (allSources.contains(prefix)) {
                reached.add(new Upstream(prefix, table));
            } else {
                reached.addAll(whole(allSources));
            }
            return;
        }
        if (nodeFrom.containsKey(token)) {
            if (visiting.add(token)) {
                collect(nodeFrom.get(token), reached, visiting);
            }
            return;
        }
        // A bare token is a table name, so the table is known even where the source is not: it is
        // read from whichever sources can supply that name.
        Set<String> supplying = new LinkedHashSet<>(openSources);
        tablesBySelector.forEach((sourceId, tables) -> {
            if (tables.contains(token)) {
                supplying.add(sourceId);
            }
        });
        // A name nothing claims cannot be attributed, so no source may be ruled out.
        for (String sourceId : supplying.isEmpty() ? allSources : supplying) {
            reached.add(new Upstream(sourceId, token));
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

    @Override
    public String toString() {
        return "wiring of " + pipeline.id();
    }
}
