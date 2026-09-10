package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRename;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorePort;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolves a sink's write-side target model from the source model discovery persisted for a connection. The
 * table structure a sink creates and the key an upsert matches on come from the upstream source's discovered
 * model, not from the events flowing through - so a target table is built by reading the persisted model for
 * the source the sink reads and mapping the discovered {@link SourceTable} onto a {@link TargetTable}.
 *
 * <p>A source may select several tables. When a table's schema has never been discovered, it is absent from
 * the resolved map. View materialization tolerates that absence; a sync start refuses it before binding.
 */
final class TargetModelResolver {

    private final StorePort storePort;

    TargetModelResolver(StorePort storePort) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
    }

    /**
     * Requires every source model that reaches a sync to have been discovered. Literal table selectors can
     * still resolve without discovery for legacy view/nest paths, but a sync target needs the discovered
     * fields and primary key before capture or a sink can safely start.
     */
    void requireAllDiscovered(Iterable<String> sourceIds) {
        for (String sourceId : sourceIds) {
            SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
            SourceModel discovered = SourceDiscovery.model(storePort, source);
            if (discovered == null) {
                throw new TapstateException(
                        ActuationError.SOURCE_SCHEMA_NOT_DISCOVERED, Map.of("source", source.id()), null);
            }
            // Preserve existing selector diagnostics for a discovered source whose literal/regex no longer
            // names a table, instead of converting that distinct error into a missing-schema refusal.
            SourceTableSelection.resolve(source, discovered);
        }
    }

    /** Resolves one target model per selected source table across the pipeline, in source and discovery order. */
    Optional<TargetTable> resolve(PipelineResource pipeline) {
        return resolveAll(pipeline).values().stream().findFirst();
    }

    /** Resolves one target model per selected source table, preserving source and discovery order. */
    Map<String, TargetTable> resolveAll(PipelineResource pipeline) {
        Map<String, TargetTable> targets = new LinkedHashMap<>();
        for (String sourceId : pipeline.sourceIds()) {
            SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
            resolveAll(source, SourceDiscovery.model(storePort, source)).forEach(targets::putIfAbsent);
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(targets));
    }

    /** Resolves one target model per selected table of the source that feeds a sink. */
    Map<String, TargetTable> resolveAll(String sourceId) {
        SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
        return resolveAll(source, SourceDiscovery.model(storePort, source));
    }

    /** Resolves the first selected table for callers that still require a single target. */
    ResolvedTarget resolve(String sourceId) {
        SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
        SourceModel discovered = SourceDiscovery.model(storePort, source);
        String table = SourceCaptureResolution.of(source, discovered).table();
        return new ResolvedTarget(table, resolveAll(source, discovered).get(table));
    }

    /**
     * One source's selected tables resolved against a model already in hand.
     *
     * <p><b>The discovery is read once, by the caller, and handed in.</b> It is stored per connection
     * and holds every table of that connection, so reading it again for each selected table asks the
     * store the same question once per table for one answer - and then walks the whole model looking
     * for a single name, which is that cost a second time. Neither shows in an answer: the resolution
     * is identical either way, and the difference only appears on a connection with many tables, which
     * is exactly where it is paid. The model is indexed by name here for the second half of it.
     */
    private Map<String, TargetTable> resolveAll(SourceResource source, SourceModel discovered) {
        Map<String, SourceTable> byName = tablesByName(discovered);
        Map<String, TargetTable> targets = new LinkedHashMap<>();
        for (String table : SourceCaptureResolution.of(source, discovered).tables()) {
            SourceTable found = byName.get(table);
            if (found != null) {
                targets.put(table, toTargetTable(found));
            }
        }
        return Collections.unmodifiableMap(targets);
    }

    /**
     * A discovered model's tables by name, first of a repeated name winning - which is the one the
     * per-table search this replaces would have found.
     */
    private static Map<String, SourceTable> tablesByName(SourceModel discovered) {
        Map<String, SourceTable> byName = new LinkedHashMap<>();
        if (discovered != null) {
            discovered.tables().forEach(table -> byName.putIfAbsent(table.name(), table));
        }
        return byName;
    }

    /** One source's table paired with the target model discovered for it, or a null model when none was. */
    record ResolvedTarget(String sourceTable, TargetTable target) {
    }

    /**
     * Maps one discovered source table onto the write-side target table a sink writes: each field carries over
     * with its source-declared type, and a field named in the table's primary key is flagged so the sink keys
     * an upsert on it. The sink keys the upsert in target-field order, so the key columns lead in the source's
     * key order and the remaining fields follow in source order.
     */
    static TargetTable toTargetTable(SourceTable source) {
        List<String> primaryKey = source.primaryKey();
        List<TargetField> fields = new ArrayList<>(source.fields().size());
        for (String keyColumn : primaryKey) {
            SourceField field = field(source, keyColumn);
            fields.add(new TargetField(field.name(), field.dataType(), true, field.type()));
        }
        for (SourceField field : source.fields()) {
            if (!primaryKey.contains(field.name())) {
                fields.add(new TargetField(field.name(), field.dataType(), false, field.type()));
            }
        }
        List<TargetIndex> indexes = new ArrayList<>();
        if (!primaryKey.isEmpty()) {
            indexes.add(new TargetIndex(primaryKey, true));
        }
        source.indexes().stream().filter(index -> !index.fields().isEmpty())
                .map(index -> new TargetIndex(index.fields(), index.unique()))
                .filter(index -> !indexes.contains(index)).forEach(indexes::add);
        return new TargetTable(source.name(), fields, indexes);
    }

    /**
     * The target table one sync element writes: the resolved model under the name its rename gives the source
     * table. The name a rename is applied to is the model's own rather than the stream key: a nest emits under
     * the id of the step that assembled it, while the table its documents land in is the root's, and renaming
     * the key there would name the target after the step.
     */
    static TargetTable rename(TargetTable target, RenameSpec rename) {
        Objects.requireNonNull(target, "target");
        if (rename == null) {
            return target;
        }
        return new TargetTable(TableRename.apply(target.name(), rename), target.fields(), target.indexes());
    }

    /**
     * The same model keyed on {@code key} instead of on the table's own primary key, key columns leading in
     * the order given. What a nest's documents are matched on when they are upserted is the nest root's key,
     * which is the author's choice and need not be the root table's primary key; the rest of the model - the
     * table the documents land in and the columns they carry - is the root table's unchanged.
     *
     * <p>A key naming a column the model does not carry is left out rather than invented. It reaches the sink
     * as a key one column short, which the sink reports against the table it is writing; conjuring a column
     * would instead produce a descriptor the connector cannot create.
     */
    static TargetTable keyedOn(TargetTable model, List<String> key) {
        if (key == null || key.isEmpty()) {
            return model;
        }
        List<TargetField> fields = new ArrayList<>(model.fields().size());
        for (String column : key) {
            for (TargetField field : model.fields()) {
                if (field.name().equals(column)) {
                    fields.add(new TargetField(field.name(), field.type(), true, field.inferredType()));
                    break;
                }
            }
        }
        for (TargetField field : model.fields()) {
            if (!key.contains(field.name())) {
                fields.add(new TargetField(field.name(), field.type(), false, field.inferredType()));
            }
        }
        List<TargetIndex> indexes = new ArrayList<>(model.indexes());
        List<String> resolvedKey = fields.stream().filter(TargetField::primaryKey).map(TargetField::name).toList();
        if (!resolvedKey.isEmpty()) {
            TargetIndex index = new TargetIndex(resolvedKey, true);
            if (!indexes.contains(index)) {
                indexes.add(index);
            }
        }
        return new TargetTable(model.name(), fields, indexes);
    }

    /** Applies one sync element's rename rules to every source table that can reach that sink. */
    static Map<String, TargetTable> renameAll(
            Map<String, TargetTable> targets, Iterable<String> sourceTables, RenameSpec rename) {
        Map<String, TargetTable> renamed = new LinkedHashMap<>();
        for (String sourceTable : sourceTables) {
            TargetTable target = Objects.requireNonNull(
                    targets.get(sourceTable), "no discovered target model for served stream '" + sourceTable + "'");
            renamed.put(sourceTable, rename(target, rename));
        }
        return Collections.unmodifiableMap(renamed);
    }

    /** The discovered field a key column names; a key naming no discovered field is a broken source model. */
    private static SourceField field(SourceTable source, String name) {
        for (SourceField field : source.fields()) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        throw new IllegalStateException(
                "primary key column '" + name + "' is not among the fields of discovered table '" + source.name() + "'");
    }
}
