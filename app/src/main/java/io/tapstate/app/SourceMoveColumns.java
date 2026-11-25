package io.tapstate.app;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.runtime.engine.nest.NestTopology;
import io.tapstate.spi.sink.TargetTable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Input columns whose movement changes an operator's state owner before its effects reach the sink. */
final class SourceMoveColumns {
    private SourceMoveColumns() {}

    /** Source-known unique values can move independently of the row's primary identity. */
    static Set<String> uniqueColumns(TargetTable source) {
        var columns = new LinkedHashSet<String>();
        source.indexes().stream().filter(io.tapstate.spi.sink.TargetIndex::unique)
                .forEach(index -> columns.addAll(index.fields()));
        return Set.copyOf(columns);
    }

    static Map<String, Set<String>> forDirectInputs(Map<String, Step.Inline> steps,
            Map<String, StoreBackedDagSource.CompiledJoin> joins, Map<String, OperatorCoordinatePlan.NestStep> nests,
            Function<FromRef, List<String>> producers, Set<String> sources) {
        var fields = new HashMap<String, Set<String>>();
        steps.forEach((id, step) -> {
            if (!(step.from() instanceof FromClause.Aliases aliases)) return;
            BiConsumer<String, Collection<String>> add = (alias, columns) -> {
                var ref = aliases.aliases().get(alias);
                if (ref == null) return;
                for (var producer : producers.apply(ref)) {
                    if (sources.contains(producer)) {
                        fields.computeIfAbsent(producer, key -> new LinkedHashSet<>()).addAll(columns);
                    }
                }
            };
            var nest = nests.get(id);
            if (nest != null) addNest(nest.topology(), add);
            var join = joins.get(id);
            if (join != null) addJoin(join.plan().from(), add);
        });
        return fields;
    }

    static void addNest(NestTopology tree, BiConsumer<String, Collection<String>> add) {
        for (var vertex : tree.vertices()) {
            for (var edge : vertex.inbound()) {
                if (edge.tracksKeyChanges() && !edge.carriesDepartures()) {
                    add.accept(edge.alias(), vertex.trackedColumns(edge));
                }
            }
        }
        for (var lookup : tree.lookups()) {
            add.accept(lookup.alias(), lookup.partitionKey());
            add.accept(lookup.alias(), lookup.rowIdentity());
            add.accept(lookup.referrerAlias(), lookup.referenceFields());
            add.accept(lookup.referrerAlias(), lookup.referrerIdentity());
        }
    }

    static void addJoin(JoinTree tree, BiConsumer<String, Collection<String>> add) {
        var aliases = new HashMap<String, String>();
        for (var source : tree.sources()) aliases.put(source.name(), source.declared());
        addPredicates(tree, aliases, add);
    }

    private static void addPredicates(JoinTree tree, Map<String, String> aliases,
            BiConsumer<String, Collection<String>> add) {
        if (!(tree instanceof JoinTree.Join join)) return;
        for (var pair : join.on()) {
            add.accept(aliases.get(pair.left().source()), List.of(pair.left().column()));
            add.accept(aliases.get(pair.right().source()), List.of(pair.right().column()));
        }
        addPredicates(join.left(), aliases, add);
        addPredicates(join.right(), aliases, add);
    }
}
