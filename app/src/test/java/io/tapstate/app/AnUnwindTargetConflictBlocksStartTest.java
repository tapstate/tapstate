package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.IdentityConnectorFixture;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.control.core.PipelineStartService;
import io.tapstate.control.core.StartCheckEvaluator;
import io.tapstate.control.core.StartCheckReport;
import io.tapstate.control.core.StartDefinitionWriter;
import io.tapstate.control.core.StartIntent;
import io.tapstate.control.core.StartPlanner;
import io.tapstate.control.core.StartRefusal;
import io.tapstate.control.core.TargetProbe;
import io.tapstate.control.core.UnwindTargetIdentityCheck;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.DiscoveredSourceModel;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class AnUnwindTargetConflictBlocksStartTest {
    @TempDir static Path directory;
    private static ConnectorRef connector;
    private final InMemoryStorePort store = new InMemoryStorePort();
    private final List<AuditRecord> audit = new ArrayList<>();
    private final DslParser parser = new DslParser();

    @BeforeAll
    static void connector() { connector = IdentityConnectorFixture.ref(directory); }

    @Test
    void twoStreamsInOneSinkReturnARefusalBeforeAnyDesiredOrAuditWrite() {
        var pipeline = pipeline("orders", "mongodb", "original_b");
        withStart(pipeline, service -> {
            var checks = service.checks(pipeline.id(), StartIntent.START);
            assertThat(checks.outcome()).isEqualTo(StartCheckReport.Outcome.BLOCKED);
            var refusal = catchThrowableOfType(StartRefusal.class,
                    () -> service.start("alice", pipeline.id(), List.of(), null));
            assertThat(refusal.report().outcome()).isEqualTo(StartCheckReport.Outcome.BLOCKED);
            assertThat(refusal.report().findings()).hasSize(2).allSatisfy(finding -> {
                assertThat(finding.code()).isEqualTo("start-check.target-identity-refused");
                assertThat(finding.params()).containsEntry("reason", "target-key-conflict");
            });
            assertThat(refusal.report().findings()).extracting(StartCheckReport.Finding::key).doesNotHaveDuplicates();
            assertThat(refusal.report().findings()).extracting(finding -> finding.params().get("stream"))
                    .containsExactlyInAnyOrder("a.orders", "b.orders");
            assertThat(refusal.report().findings()).allSatisfy(finding ->
                    assertThat(finding.subject().label()).contains(finding.params().get("stream").toString()));
            assertThat(store.desired().pipelineIds()).isEmpty();
            assertThat(audit).isEmpty();
        });
    }

    @Test
    void separatorsInSourceAndSinkIdsCannotCollapseDistinctFindings() {
        var pipeline = pipeline("orders", "mongodb", "original_b", "y/z", "z",
                "[{id: out, source: dest}, {id: out/y, source: dest}]");
        withStart(pipeline, service -> {
            var refusal = catchThrowableOfType(StartRefusal.class,
                    () -> service.start("alice", pipeline.id(), List.of(), null));
            assertThat(refusal.report().outcome()).isEqualTo(StartCheckReport.Outcome.BLOCKED);
            assertThat(refusal.report().findings()).hasSize(4).allSatisfy(finding -> {
                assertThat(finding.code()).isEqualTo("start-check.target-identity-refused");
                assertThat(finding.params()).containsEntry("reason", "target-key-conflict");
            });
            assertThat(refusal.report().findings()).extracting(StartCheckReport.Finding::key).doesNotHaveDuplicates();
            assertThat(store.desired().pipelineIds()).isEmpty();
            assertThat(audit).isEmpty();
        });
    }

    @Test
    void twoStreamsWithTheSameAutomaticMappingCanStart() {
        accepts(pipeline("orders", "mongodb", "original_a"), "automatic");
    }

    @Test
    void differentTargetTablesKeepTheirIndependentMappings() {
        accepts(pipeline("other_orders", "mongodb", "original_b"), "automatic");
    }

    @Test
    void compatibleTargetsDoNotRejectUnusedPreservedFieldDifferences() {
        accepts(pipeline("orders", "postgresql", "original_b"), "not-required");
    }

    private void accepts(PipelineResource pipeline, String status) {
        withStart(pipeline, service -> {
            var outcome = service.start("alice", pipeline.id(), List.of(), null);
            assertThat(outcome.report().outcome()).isEqualTo(StartCheckReport.Outcome.READY);
            assertThat(outcome.report().findings()).hasSize(2).allSatisfy(finding ->
                    assertThat(finding.params()).containsEntry("identityStatus", status));
            assertThat(outcome.report().findings()).extracting(StartCheckReport.Finding::key).doesNotHaveDuplicates();
            assertThat(store.desired().read(pipeline.id())).isPresent();
            assertThat(audit).extracting(AuditRecord::operationId).containsExactly("pipeline.start");
        });
    }

    private void withStart(PipelineResource pipeline, java.util.function.Consumer<PipelineStartService> assertion) {
        var identities = new StoreBackedTargetIdentities(store, id -> connector);
        var dag = new StoreBackedDagSource(store);
        var clock = Clock.systemUTC();
        var lifecycle = new PipelineLifecycleService(new ArtifactQueryService(store.artifacts()), store.desired(),
                new AuditGate(audit::add, clock), store.state()::read);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var evaluator = new StartCheckEvaluator(List.of(new UnwindTargetIdentityCheck(identities)),
                    new StartPlanner(id -> List.of(), store.meta(), dag::writeTargets),
                    (connection, table) -> TargetProbe.TargetRows.EMPTY, (code, params) -> code.code(),
                    executor, clock, Duration.ofSeconds(10), Duration.ofSeconds(2));
            StartDefinitionWriter definitions = (principal, changed, hash, changes) -> {
                throw new AssertionError("identity findings must not rewrite the definition");
            };
            assertion.accept(new PipelineStartService(lifecycle, evaluator, definitions));
        }
        assertThat(store.artifacts().get(pipeline.id())).contains(pipeline);
    }

    private PipelineResource pipeline(String secondTable, String targetConnector, String secondField) {
        return pipeline(secondTable, targetConnector, secondField, "a", "b", "[{id: out, source: dest}]");
    }

    private PipelineResource pipeline(String secondTable, String targetConnector, String secondField,
            String firstSource, String secondSource, String sinks) {
        source(firstSource, "orders");
        source(secondSource, secondTable);
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: dest
                connector: %s
                config: {exists: false}
                """.formatted(targetConnector)));
        var pipeline = (PipelineResource) parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: p
                source: [%s, %s]
                transforms:
                  - {id: ua, type: unwind, from: %s.orders, path: items, include_array_index: position, source_id_field: original_a}
                  - {id: ub, type: unwind, from: %s.%s, path: items, include_array_index: position, source_id_field: %s}
                  - {id: combined, type: union, from: [ua, ub]}
                serve: {from: combined, sync: %s}
                """.formatted(firstSource, secondSource, firstSource, secondSource, secondTable, secondField, sinks));
        store.artifacts().save(pipeline);
        return pipeline;
    }

    private void source(String id, String table) {
        store.artifacts().save(parser.parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                mode: cdc
                tables: [%s]
                """.formatted(id, table)));
        store.schemas().save(new DiscoveredSourceModel(id, "mongodb", 1L, new SourceModel(List.of(
                new SourceTable(table, List.of(new SourceField("_id", "string", TapstateType.STRING),
                        new SourceField("items", "array", TapstateType.JSON)), List.of("_id"), List.of())))));
    }
}
