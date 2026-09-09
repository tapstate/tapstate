package io.tapstate.app;

import io.tapstate.control.core.AuditContext;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.DerivedSchemas;
import io.tapstate.control.core.SchemaDerivation;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
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
 * Reports and accepts what a pipeline's steps work their own columns out to be, and re-takes the
 * pipeline's copy of what its sources hold.
 *
 * <p>Three entry points, and the difference between them is only what they are allowed to refuse: the
 * report reads, the accept re-copies and records, and the derivation an apply runs re-copies without
 * holding anything to what it was recorded producing. The copy itself is the assembly's, borrowed
 * rather than written a second time here - two ways of taking one copy would drift apart, and the shape
 * that takes is a record a start then refuses.
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
final class StoreBackedDerivedSchemas implements DerivedSchemas, SchemaDerivation {

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
        // While a job is carrying the pipeline, the recorded side is the version that job was assembled
        // from rather than the newest one on file. The two are the same answer until somebody records a
        // shape while the pipeline runs, and that is exactly when this report is read: answering with
        // the newest would describe a pipeline that is not the one going on.
        boolean live = aRunExists(pipelineId);
        // Every node with a model of its own, not only the joins. A pipeline without a join used to
        // answer with an empty list, which is also what an unknown id answers - so the one face anyone
        // can ask said nothing at all about a source table that grew a column between two runs.
        Map<String, StoreBackedDagSource.CompiledJoin> compiledJoins =
                joins.compiledJoinsOf(pipelineId);
        List<StepReport> reports = new ArrayList<>();
        joins.derivedNodesOf(pipelineId).forEach((nodeId, node) -> {
            // A node nobody can describe - a script, or anything under one - is left out rather than
            // reported with no columns: an empty row reads as a node that produces nothing, and the
            // two want opposite reactions.
            if (!node.known()) {
                return;
            }
            Map<String, String> derived = node.columns();
            Map<String, String> recorded = heldBy(live, pipelineId, nodeId)
                    .map(DerivedSchema::schema)
                    .orElse(Map.of());
            // Only a join writes into a table of its own here; a source node reads one and a step
            // passes rows on, so naming a table for either would report what it reads as what it
            // writes.
            StoreBackedDagSource.CompiledJoin compiled = compiledJoins.get(nodeId);
            String table = compiled == null ? null : compiled.factTable();
            Map<String, String> target = table == null ? null : targetColumns.get(table);
            Set<String> names = new LinkedHashSet<>(recorded.keySet());
            names.addAll(derived.keySet());
            List<ColumnReport> columns = new ArrayList<>();
            for (String name : names) {
                columns.add(new ColumnReport(name, recorded.get(name), derived.get(name),
                        target == null ? null : target.get(name)));
            }
            reports.add(new StepReport(nodeId, table, target != null, columns));
        });
        return List.copyOf(reports);
    }

    @Override
    public void accept(String principal, String pipelineId) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(pipelineId, "pipelineId");
        refuseWhileAJobIsProducing(pipelineId);
        auditGate.dispatch(ControlOperations.PIPELINE_ACCEPT_DERIVED_SCHEMA,
                new AuditContext(principal, pipelineId), () -> {
                    // The physical model first, then everything worked out from it. This order is what
                    // makes the act one thing rather than two: a re-derivation over source copies that
                    // were not refreshed records today's answer to yesterday's question, and the report
                    // afterwards agrees with itself while agreeing with nothing outside.
                    joins.copySourceSchemas(pipelineId);
                    // Recorded through the same class the gate records through, so accepting cannot
                    // write a shape a start would then refuse - which is what a second write path here
                    // would eventually do.
                    JoinSchemaDrift accepting = new JoinSchemaDrift(storePort.derivedSchemas());
                    joins.compiledJoinsOf(pipelineId).forEach((stepId, compiled) -> accepting.record(
                            pipelineId, stepId, compiled.body(), compiled.plan(), compiled.tables()));
                    return null;
                });
    }

    @Override
    public void derive(String pipelineId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        // Refused while a job is carrying the pipeline, exactly as accepting is - and it was not, until
        // the copy gained a reader below it. The table a sink creates is now built from what the
        // pipeline publishes, worked forward from this copy, so moving the copy under a running job
        // moves what that job was assembled from: the record would say the pipeline produces one shape
        // while the job produces another, and every reader of the record afterwards describes a
        // pipeline that is not the one running.
        //
        // Nothing else is gated. What holds a join to the columns it was recorded producing is the
        // start's business: refusing that here would refuse a whole batch of unrelated resources over
        // one pipeline whose source widened a column, which is not something the author applying can
        // act on.
        refuseWhileAJobIsProducing(pipelineId);
        joins.copySourceSchemas(pipelineId);
    }

    /**
     * Refuses a re-copy while a job is carrying the pipeline, or while one has been asked for.
     *
     * <p>The run is never the thing at risk - it holds the versions it was assembled from and re-reads
     * none of them. What is refused is the disagreement a re-copy would leave behind: the record would
     * say the pipeline produces one shape while the job going on produces another, and everything read
     * off the record afterwards would describe a pipeline that is not running.
     *
     * <p><b>Paused is allowed and being resumed is not, and that pair is the whole judgement.</b> A
     * paused pipeline has no job producing anything, and both ways out of paused re-read the definition
     * - so a re-copy taken there is picked up rather than bypassed. One already asked to resume is a job
     * about to carry on under the assembly it was paused with, which is the running case arriving a
     * moment later. Reading only the checkpoint would let that one through.
     */
    private void refuseWhileAJobIsProducing(String pipelineId) {
        PipelineState actual = actualStateOf(pipelineId);
        PipelineState desired = desiredStateOf(pipelineId);
        if (actual == PipelineState.RUNNING || desired == PipelineState.RUNNING) {
            throw new TapstateException(ActuationError.SCHEMA_SYNC_WHILE_RUNNING,
                    Map.of("pipeline", pipelineId, "state", actual.name(), "desired", desired.name()),
                    null);
        }
    }

    /** Whether a run of this pipeline exists to be holding anything - one going on, or one paused. */
    private boolean aRunExists(String pipelineId) {
        PipelineState actual = actualStateOf(pipelineId);
        return actual == PipelineState.RUNNING || actual == PipelineState.PAUSED;
    }

    /**
     * The derivation a reader should be shown: what the run holds while one exists, and what is on file
     * otherwise. A run whose step was never pinned - one started before this was written down - falls
     * back to the latest rather than reporting nothing, which is the answer that side used to give.
     */
    private Optional<DerivedSchema> heldBy(boolean live, String pipelineId, String stepId) {
        if (live) {
            Optional<DerivedSchema> pinned = storePort.derivedSchemas().pinned(pipelineId, stepId);
            if (pinned.isPresent()) {
                return pinned;
            }
        }
        return storePort.derivedSchemas().latest(pipelineId, stepId);
    }

    private PipelineState actualStateOf(String pipelineId) {
        return storePort.state().read(pipelineId)
                .map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
                .orElse(PipelineState.NEW);
    }

    private PipelineState desiredStateOf(String pipelineId) {
        return storePort.desired().read(pipelineId)
                .map(DesiredState::targetState)
                .orElse(PipelineState.NEW);
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
        Set<String> alreadyRead = new LinkedHashSet<>();
        for (SyncElement element : serve.sync()) {
            // One connection is read once, however many sync elements name it. Two elements writing
            // through the same target is ordinary - a different write mode, a different rename, an id
            // for a query backend - and reading its tables twice would make every one of them collide
            // with itself below, marking it ambiguous and blanking the whole column this report exists
            // for.
            if (!alreadyRead.add(element.source())) {
                continue;
            }
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
