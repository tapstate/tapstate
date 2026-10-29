package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.sink.*;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.schema.*;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.WriteListResult;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkTargetIdentityPlannerTest {
    private static ConnectorRef ref;
    @BeforeAll static void connector(@TempDir Path directory) { ref = IdentityConnectorFixture.ref(directory); }

    @Test
    void ordinaryAndEmptyTargetsDoNotEvenResolveAConnector() {
        var planner = new PdkTargetIdentityPlanner(id -> { throw new AssertionError("ordinary targets need no identity metadata"); });
        var expanded = target("_id");
        var ordinary = new TargetTable(expanded.name(), expanded.fields(), expanded.indexes());
        assertThat(planner.assess(config("mongodb", Map.of()), Map.of())).isEmpty();
        assertThat(planner.assess(config("mongodb", Map.of()), Map.of("orders", ordinary)).get("orders").status())
                .isEqualTo(TargetIdentityPlan.NOT_REQUIRED);
    }

    @Test
    void publicAssessmentReadsExactMetadataForAnExistingRelationalTarget() {
        var config = config("postgres", Map.of("rows", List.of(IdentityConnectorFixture.postgresRow())));
        var plan = new PdkTargetIdentityPlanner(id -> ref).assess(config, Map.of("orders", target("id"))).get("orders");
        assertThat(plan.status()).isEqualTo(TargetIdentityPlan.AUTOMATIC);
        assertThat(plan.writeKey()).containsExactly("_unwind_source_id", "position");
        assertThat(plan.requireSignedInt64Keys()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"names", "indexes", "indexes-clear"})
    void losingAuthorityDuringMetadataStopsTheNextExternalCall(String lastAllowed) throws Throwable {
        var active = new java.util.concurrent.atomic.AtomicBoolean(true);
        List<String> calls = new ArrayList<>();
        var lost = new TapstateException(io.tapstate.spi.store.IoError.NOTE_SCOPE_UNAVAILABLE, Map.of(), null);
        try (var connector = IdentityConnectorFixture.guarded("mongodb", ref, Map.of(), () -> {
            if (!active.get()) throw lost;
        })) {
            connector.functions().supportGetTableNamesFunction((context, size, out) -> {
                calls.add("names");
                out.accept(List.of("orders"));
                if (lastAllowed.equals("names")) active.set(false);
            });
            connector.functions().supportQueryIndexes((context, table, out) -> {
                calls.add("indexes");
                out.accept(List.of(new TapIndex().name("_id_").unique(true)
                        .indexField(new TapIndexField().name("_id"))));
                if (lastAllowed.startsWith("indexes")) active.set(false);
            });
            connector.functions().supportCountByPartitionFilterFunction((context, table, filter) -> {
                calls.add("count"); return 0L;
            });
            var assessment = lastAllowed.equals("indexes-clear")
                    ? new SinkConfig("mongodb", Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null, null, OnFullLoad.CLEAR, true)
                    : config("mongodb", Map.of());
            assertThatThrownBy(() -> PdkTargetIdentityPlanner.assess(connector, assessment,
                    Map.of("orders", target("_id")))).isSameAs(lost);
            assertThat(calls).containsExactlyElementsOf(lastAllowed.equals("names")
                    ? List.of("names") : List.of("names", "indexes"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"initFails", "discoverFails"})
    void connectorMetadataFailuresHaveAStableCodedDiagnosis(String failure) {
        var config = config("postgres", Map.of(failure, true));
        assertThatThrownBy(() -> new PdkTargetIdentityPlanner(id -> ref).assess(config, Map.of("orders", target("id"))))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CHECK_FAILED));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-collection", "empty-collection", "full-load-clear", "populated", "no-count", "no-create-index"})
    void onlyNewEmptyOrExplicitlyClearedTargetsMayPrepareTheMatchingIndex(String kind) throws Throwable {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("count", kind.equals("empty-collection") ? 0L : 5L);
        settings.put("exists", !kind.equals("missing-collection"));
        settings.put("rows", List.of(IdentityConnectorFixture.mongoIndex("_id_", Map.of("_id", 1))));
        var config = new SinkConfig("mongodb", settings, WriteMode.UPSERT, DdlPolicy.FAIL, null, null,
                kind.equals("full-load-clear") ? OnFullLoad.CLEAR : OnFullLoad.APPEND, true);
        try (var connector = PdkConnector.open("mongodb", ref, settings)) {
            if (kind.equals("no-count")) connector.functions().supportCountByPartitionFilterFunction(null);
            if (kind.equals("no-create-index")) connector.functions().supportCreateIndex(null);
            var plan = PdkTargetIdentityPlanner.assess(connector, config, Map.of("orders", target("_id"))).get("orders");
            boolean canPrepare = List.of("missing-collection", "empty-collection", "full-load-clear").contains(kind);
            assertThat(plan.status()).isEqualTo(canPrepare ? TargetIdentityPlan.AUTOMATIC : TargetIdentityPlan.ACTION_REQUIRED);
            if (!canPrepare) assertThat(plan.reason()).isEqualTo("logical-key-index-missing");
        }
    }

    @Test
    void aNewTargetWithoutIndexCreationCannotPromiseReplaySafeMatching() throws Throwable {
        try (var connector = PdkConnector.open("mongodb", ref, Map.of("exists", false))) {
            connector.functions().supportCreateIndex(null);
            var plan = PdkTargetIdentityPlanner.assess(connector, config("mongodb", Map.of()),
                    Map.of("orders", target("_id"))).get("orders");
            assertThat(plan.reason()).isEqualTo("logical-key-index-missing");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-table-names", "missing-index-query", "malformed-generic-index"})
    void missingOrMalformedCapabilitiesCannotPassStartup(String kind) throws Throwable {
        String id = kind.equals("malformed-generic-index") ? "postgres" : "mongodb";
        try (var connector = PdkConnector.open(id, ref, Map.of("rows", IdentityConnectorFixture.mongoIndexes()))) {
            switch (kind) {
                case "missing-table-names" -> connector.functions().supportGetTableNamesFunction(null);
                case "missing-index-query" -> connector.functions().supportQueryIndexes(null);
                default -> connector.functions().supportQueryIndexes((context, table, out) -> out.accept(List.of(new TapIndex().unique(true))));
            }
            assertThatThrownBy(() -> PdkTargetIdentityPlanner.assess(connector, config(id, Map.of()), Map.of("orders", target(id.equals("mongodb") ? "_id" : "id"))))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CHECK_FAILED));
        }
    }

    @Test
    void genericIndexesAreReadButCannotReplaceExactConstraintEvidence() throws Throwable {
        try (var connector = PdkConnector.open("other", ref, Map.of())) {
            connector.functions().supportQueryIndexes((context, table, out) -> out.accept(List.of(
                    new TapIndex().name("secondary").unique(false),
                    new TapIndex().name("logical").unique(true).indexField(new TapIndexField().name("position")))));
            var plan = PdkTargetIdentityPlanner.assess(connector, config("other", Map.of()), Map.of("orders", target("id"))).get("orders");
            assertThat(plan.status()).isEqualTo(TargetIdentityPlan.ACTION_REQUIRED);
        }
    }

    @Test
    void preparationMustLeaveAFullLogicalIndexBeforeWritesCanBegin() throws Throwable {
        Map<String, Object> settings = Map.of("rows", IdentityConnectorFixture.mongoIndexes());
        var config = config("mongodb", settings);
        var target = target("_id");
        try (var connector = PdkConnector.open("mongodb", ref, settings)) {
            var expected = PdkTargetIdentityPlanner.assess(connector, config, Map.of("orders", target));
            PdkTargetIdentityPlanner.verifyPrepared(connector, config, Map.of("orders", target), expected);
            connector.functions().supportQueryIndexes((context, table, out) -> out.accept(List.of(
                    new TapIndex().name("_id_").unique(true).indexField(new TapIndexField().name("_id")))));
            assertThatThrownBy(() -> PdkTargetIdentityPlanner.verifyPrepared(connector, config, Map.of("orders", target), expected))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.args()).containsEntry("reason", "logical-key-index-missing"));
        }
    }

    @Test
    void writerProjectsBothImagesAndItsModelBeforeInvokingTheConnector() throws Throwable {
        Map<String, Object> settings = Map.of("rows", IdentityConnectorFixture.mongoIndexes());
        var connector = PdkConnector.open("mongodb", ref, settings);
        var received = new ArrayList<TapRecordEvent>();
        var tables = new ArrayList<TapTable>();
        var target = target("_id");
        try (var writer = new PdkSinkWriter(connector, (context, rows, table, out) -> {
            received.addAll(rows); tables.add(table);
            out.accept(new WriteListResult<TapRecordEvent>().insertedCount(rows.size()));
        }, config("mongodb", settings), Map.of("orders", target), null)) {
            writer.prepareTargets();
            var before = Map.<String, Object>of("_id", "parent", "position", 1L, "value", "old");
            var after = Map.<String, Object>of("_id", "parent", "position", 1L, "value", "new");
            writer.write(List.of(Envelope.update(1L, "orders", before, after, null),
                    Envelope.delete(2L, "orders", before, null))).toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(((TapUpdateRecordEvent) received.getFirst()).getBefore()).containsEntry("_unwind_source_id", "parent").doesNotContainKey("_id");
            assertThat(((TapUpdateRecordEvent) received.getFirst()).getAfter()).containsEntry("_unwind_source_id", "parent");
            assertThat(((TapDeleteRecordEvent) received.getLast()).getBefore()).containsEntry("_unwind_source_id", "parent");
            assertThat(tables.getFirst().primaryKeys()).containsExactly("_unwind_source_id", "position");
            assertThat(tables.getFirst().getNameFieldMap()).doesNotContainKey("_id");
            var collision = Map.<String, Object>of("_id", "parent", "_unwind_source_id", "user", "position", 1L);
            assertThatThrownBy(() -> writer.write(List.of(Envelope.insert(3L, "orders", after, null),
                    Envelope.insert(3L, "orders", collision, null))).toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(TapstateException.class);
            assertThat(received).hasSize(2);
        }
    }

    @Test
    void changedFrozenProjectionRefusesPreparationAndWriterBeforeSideEffects() throws Throwable {
        var settings=Map.<String,Object>of("rows",IdentityConnectorFixture.mongoIndexes());
        var config=config("mongodb",settings);
        var targets=Map.of("orders",target("_id"));
        var port=new PdkSinkPort(id -> ref);
        var frozen=port.assessTargets(config,targets);
        var plan=frozen.get("orders");
        var changed=new TargetIdentityPlan(plan.status(),plan.reason(),plan.target(),plan.sourceField(),
                "another_preserved_id",plan.writeKey(),plan.requireSignedInt64Keys(),plan.requireDocumentKeys());
        var stale=Map.of("orders",changed);
        assertThatThrownBy(() -> port.withTargetIdentities(stale).prepare(config,targets))
                .isInstanceOfSatisfying(TapstateException.class,error ->
                        assertThat(error.code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CHECK_FAILED));
        try(var connector=PdkConnector.open("mongodb",ref,settings)) {
            connector.functions().supportCreateIndex((context,table,event) -> {
                throw new AssertionError("changed frozen identity reached target preparation");
            });
            assertThatThrownBy(() -> new PdkSinkWriter(connector,(context,rows,table,out) -> {
                throw new AssertionError("changed frozen identity reached a write");
            },config,targets,null,true,null,io.tapstate.core.model.PipelineNode.Purpose.SYNC,null,stale))
                    .isInstanceOfSatisfying(TapstateException.class,error ->
                            assertThat(error.code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CHECK_FAILED));
        }
    }

    private static SinkConfig config(String connector, Map<String, Object> settings) {
        return new SinkConfig(connector, settings, WriteMode.UPSERT, DdlPolicy.FAIL);
    }

    private static TargetTable target(String id) {
        return new TargetTable("orders", List.of(new TargetField(id, "bigint", true, TapstateType.INT64),
                new TargetField("position", "bigint", true, TapstateType.INT64)),
                List.of(new TargetIndex(List.of(id, "position"), true)), new UnwindIdentity(List.of(id), "_unwind_source_id"));
    }
}
