package io.tapstate.app;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
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
 * the pipeline's source and mapping the discovered {@link SourceTable} onto a {@link TargetTable}.
 *
 * <p>A pipeline may select several source tables. When the source's schema has never been discovered, each
 * selected table is absent from the resolved map and the sink falls back to a bare table id for that stream.
 */
final class TargetModelResolver {

    private final StorePort storePort;

    TargetModelResolver(StorePort storePort) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
    }

    /**
     * Resolves the write-side target table for a pipeline's sink from the discovered model of the source it
     * reads: each selected source table looked up in its persisted model and mapped to a target table.
     * Empty when the source's schema was never discovered, or when the discovered model does not carry that
     * table.
     */
    Optional<TargetTable> resolve(PipelineResource pipeline) {
        return resolveAll(pipeline).values().stream().findFirst();
    }

    /** Resolves one target model per selected source table, preserving source and discovery order. */
    Map<String, TargetTable> resolveAll(PipelineResource pipeline) {
        Map<String, TargetTable> targets = new LinkedHashMap<>();
        for (String sourceId : pipeline.sources()) {
            SourceResource source = StoredArtifacts.requireSource(storePort.artifacts(), sourceId);
            SourceCaptureResolution resolution = SourceCaptureResolution.of(source, discoveredModel(sourceId));
            for (String table : resolution.tables()) {
                discoveredTable(sourceId, table).map(TargetModelResolver::toTargetTable)
                        .ifPresent(target -> targets.putIfAbsent(table, target));
            }
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(targets));
    }

    /** The named table in the source's persisted discovery model, or empty when neither is present. */
    private Optional<SourceTable> discoveredTable(String connectionId, String table) {
        return storePort.schemas().get(connectionId)
                .map(DiscoveredSourceModel::model)
                .flatMap(model -> model.tables().stream().filter(t -> t.name().equals(table)).findFirst());
    }

    private io.tapstate.spi.store.SourceModel discoveredModel(String sourceId) {
        return storePort.schemas().get(sourceId).map(DiscoveredSourceModel::model).orElse(null);
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
            fields.add(new TargetField(field.name(), field.dataType(), true));
        }
        for (SourceField field : source.fields()) {
            if (!primaryKey.contains(field.name())) {
                fields.add(new TargetField(field.name(), field.dataType(), false));
            }
        }
        return new TargetTable(source.name(), fields);
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
