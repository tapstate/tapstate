package io.tapstate.app;

import io.tapstate.control.core.AuditContext;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.DerivedSchemas;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.sql.OutputField;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorePort;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Reports and accepts what a pipeline's join steps work their own columns out to be.
 *
 * <p>The comparison is deliberately assembled from the same compile a start runs, borrowed rather than
 * repeated. A second implementation of the derivation would be two answers with nothing comparing
 * them, and the shape that takes here is a report saying the columns are fine while the start refuses
 * them - which is the worst of the three, because it sends the operator looking for the wrong thing.
 *
 * <p>The target side is read from the discovery kept for the connection the pipeline writes through,
 * which exists only if somebody discovered it. A pipeline whose target has never been discovered gets
 * a report with that side blank and {@code targetKnown} false rather than a report that quietly reads
 * as agreement - an unknown reported as agreement is the one answer here that sends someone to start a
 * pipeline that then truncates.
 */
final class StoreBackedDerivedSchemas implements DerivedSchemas {

    private final StorePort storePort;
    private final StoreBackedDagSource joins;
    private final AuditGate auditGate;

    StoreBackedDerivedSchemas(StorePort storePort, AuditGate auditGate) {
        this.storePort = Objects.requireNonNull(storePort, "storePort");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
        this.joins = new StoreBackedDagSource(storePort);
    }

    @Override
    public List<StepReport> compare(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Map<String, Map<String, String>> targetColumns = targetColumns(pipelineId);
        List<StepReport> reports = new ArrayList<>();
        joins.compiledJoinsOf(pipelineId).forEach((stepId, compiled) -> {
            Map<String, String> derived = columnsOf(compiled);
            Map<String, String> recorded = storePort.derivedSchemas().latest(pipelineId, stepId)
                    .map(DerivedSchema::schema)
                    .orElse(Map.of());
            String table = compiled.factTable();
            Map<String, String> target = targetColumns.get(table);
            Set<String> names = new LinkedHashSet<>(recorded.keySet());
            names.addAll(derived.keySet());
            List<ColumnReport> columns = new ArrayList<>();
            for (String name : names) {
                columns.add(new ColumnReport(name, recorded.get(name), derived.get(name),
                        target == null ? null : target.get(name)));
            }
            reports.add(new StepReport(stepId, table, target != null, columns));
        });
        return List.copyOf(reports);
    }

    @Override
    public void accept(String principal, String pipelineId) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(pipelineId, "pipelineId");
        auditGate.dispatch(ControlOperations.PIPELINE_ACCEPT_DERIVED_SCHEMA,
                new AuditContext(principal, pipelineId), () -> {
                    // Recorded through the same class the gate records through, so accepting cannot
                    // write a shape a start would then refuse - which is what a second write path here
                    // would eventually do.
                    JoinSchemaDrift accepting = new JoinSchemaDrift(storePort.derivedSchemas());
                    joins.compiledJoinsOf(pipelineId).forEach((stepId, compiled) -> accepting.record(
                            pipelineId, stepId, compiled.sql(), compiled.plan(), compiled.tables()));
                    return null;
                });
    }

    /** The columns a join publishes: output name to declared type, in the order it publishes them. */
    private static Map<String, String> columnsOf(StoreBackedDagSource.CompiledJoin compiled) {
        Map<String, String> columns = new LinkedHashMap<>();
        for (OutputField field : compiled.plan().outputFields()) {
            columns.put(field.name(), JoinSchemaDrift.declaredType(field));
        }
        return columns;
    }

    /**
     * What the tables this pipeline writes into actually hold, by table name; empty for a target whose
     * connection has never been discovered.
     */
    private Map<String, Map<String, String>> targetColumns(String pipelineId) {
        PipelineResource pipeline = PipelineInlining.inline(
                StoredArtifacts.requirePipeline(storePort.artifacts(), pipelineId), storePort.artifacts());
        if (!(pipeline.serve() instanceof ServeBlock.Inline serve) || serve.sync() == null) {
            return Map.of();
        }
        Map<String, Map<String, String>> byTable = new LinkedHashMap<>();
        Set<String> ambiguous = new LinkedHashSet<>();
        for (SyncElement element : serve.sync()) {
            Optional<SourceResource> target = storePort.artifacts().get(element.source())
                    .filter(SourceResource.class::isInstance)
                    .map(SourceResource.class::cast);
            if (target.isEmpty()) {
                continue;
            }
            SourceModel discovered = SourceDiscovery.model(storePort, target.get());
            if (discovered == null || discovered.tables() == null) {
                continue;
            }
            for (SourceTable table : discovered.tables()) {
                Map<String, String> columns = new LinkedHashMap<>();
                for (SourceField field : table.fields()) {
                    // The target's own declaration, in its own words - varchar(50), not STRING. That is
                    // the whole value of this column: whether the new values fit is decided by the
                    // width the target declares, and the shared vocabulary has thrown that away by the
                    // time it says STRING. Discovery records no nullability, so this side carries none
                    // and a reader is not invited to compare it with the derived side's.
                    columns.put(field.name(),
                            field.dataType() == null ? String.valueOf(field.type()) : field.dataType());
                }
                // A serve block may sync into several targets, and a table of one name in two of them is
                // two different tables. Reported as unknown rather than as whichever was read last: the
                // point of this column is to say whether the values will fit, and an answer taken from
                // the wrong database reads exactly like one taken from the right one.
                if (ambiguous.contains(table.name())) {
                    continue;
                }
                if (byTable.put(table.name(), columns) != null) {
                    ambiguous.add(table.name());
                    byTable.remove(table.name());
                }
            }
        }
        return byTable;
    }
}
