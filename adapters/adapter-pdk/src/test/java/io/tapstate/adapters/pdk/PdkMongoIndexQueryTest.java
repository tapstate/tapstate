package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.sink.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkMongoIndexQueryTest {
    private static ConnectorRef ref;
    @BeforeAll static void connector(@TempDir Path directory) { ref = IdentityConnectorFixture.ref(directory); }

    @ParameterizedTest
    @ValueSource(strings = {"mongodb", "mongodb-atlas", "aliyun-db-mongodb", "tencent-db-mongodb"})
    void existingTargetsUseTheConnectorsIndexQueryWithoutIndexStatistics(String connectorId) throws Throwable {
        try (var connector = PdkConnector.open(connectorId, ref, Map.of("count", 5L))) {
            connector.functions().supportQueryIndexes((context, table, out) -> {
                assertThat(table.getId()).isEqualTo("orders");
                out.accept(List.of(index("_id_", false, "_id")));
                out.accept(List.of(index("logical", true, "_unwind_source_id", "position"),
                        index("secondary", false, "value")));
            });
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                throw new AssertionError("Index inspection must use QueryIndexesFunction");
            });
            var config = config(connectorId);
            var plan = PdkTargetIdentityPlanner.assess(connector, config, Map.of("orders", target())).get("orders");
            assertThat(plan.status()).isEqualTo(TargetIdentityPlan.AUTOMATIC);
            assertThat(plan.writeKey()).containsExactly("_unwind_source_id", "position");
        }
    }

    @Test
    void atlasCanInspectTheUnwindIndexItJustCreatedWhenIndexStatsOmitsSpec() throws Throwable {
        var indexes = new ArrayList<TapIndex>();
        try (var connector = PdkConnector.open("mongodb-atlas", ref, Map.of())) {
            connector.functions().supportGetTableNamesFunction((context, size, out) ->
                    out.accept(indexes.isEmpty() ? List.of() : List.of("orders")));
            connector.functions().supportQueryIndexes((context, table, out) -> out.accept(List.copyOf(indexes)));
            connector.functions().supportCreateIndex((context, model, event) -> {
                indexes.add(index("_id_", false, "_id"));
                indexes.addAll(event.getIndexList());
            });
            connector.functions().supportExecuteCommandFunction((context, command, out) -> out.accept(
                    new ExecuteResult<>().result(List.of(Map.of("name", "logical", "key",
                            Map.of("_unwind_source_id", 1, "position", 1), "accesses", Map.of("ops", 0L))))));
            var config = config("mongodb-atlas");
            var targets = Map.of("orders", target());
            var expected = PdkTargetIdentityPlanner.assess(connector, config, targets);
            var projected = expected.get("orders").target();
            new PdkTargetPreparation("mongodb-atlas", connector.context(), connector.functions(),
                    OnFullLoad.APPEND, true, null, null).prepare(projected, TargetTapTable.build(projected));
            assertThat(indexes).hasSize(2);
            assertThat(indexes.get(1).getUnique()).isTrue();
            assertThat(indexes.get(1).getIndexFields()).extracting(TapIndexField::getName)
                    .containsExactly("_unwind_source_id", "position");
            PdkTargetIdentityPlanner.verifyPrepared(connector, config, targets, expected);
            indexes.removeLast();
            assertThatThrownBy(() -> PdkTargetIdentityPlanner.verifyPrepared(connector, config, targets, expected))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.args()).containsEntry("reason", "logical-key-index-missing"));
        }
    }

    @Test
    void anOldIndexDoesNotProveTheProjectedKeyButMayCoexistWithIt() throws Throwable {
        try (var connector = PdkConnector.open("mongodb-atlas", ref, Map.of("count", 5L))) {
            var indexes = new ArrayList<>(List.of(index("_id_", false, "_id"),
                    index("_id_1_position_1", true, "_id", "position")));
            connector.functions().supportQueryIndexes((context, table, out) -> out.accept(indexes));
            var config = config("mongodb-atlas");
            var targets = Map.of("orders", target());
            var missing = PdkTargetIdentityPlanner.assess(connector, config, targets).get("orders");
            assertThat(missing.reason()).isEqualTo("logical-key-index-missing");
            indexes.add(index("logical", true, "_unwind_source_id", "position"));
            assertThat(PdkTargetIdentityPlanner.assess(connector, config, targets).get("orders").status())
                    .isEqualTo(TargetIdentityPlan.AUTOMATIC);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-function", "no-answer", "null-batch", "empty", "null-index", "missing-fields", "null-field", "missing-name", "query-failed"})
    void missingMalformedOrFailedIndexQueriesRemainUnavailable(String kind) throws Throwable {
        try (var connector = PdkConnector.open("mongodb-atlas", ref, Map.of())) {
            connector.functions().supportQueryIndexes((context, table, out) -> {
                switch (kind) {
                    case "no-answer" -> { }
                    case "null-batch" -> out.accept(null);
                    case "empty" -> out.accept(List.of());
                    case "null-index" -> out.accept(Arrays.asList((TapIndex) null));
                    case "missing-fields" -> out.accept(List.of(new TapIndex().unique(true)));
                    case "null-field" -> {
                        var index = new TapIndex().unique(true);
                        index.setIndexFields(Arrays.asList((TapIndexField) null));
                        out.accept(List.of(index));
                    }
                    case "missing-name" -> out.accept(List.of(index("bad", true, (String) null)));
                    default -> throw new IllegalStateException("index query failed");
                }
            });
            if (kind.equals("missing-function")) connector.functions().supportQueryIndexes(null);
            assertThatThrownBy(() -> PdkTargetIdentityPlanner.assess(connector, config("mongodb-atlas"),
                    Map.of("orders", target())))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(ConnectorError.TARGET_IDENTITY_CHECK_FAILED));
        }
    }

    private static TapIndex index(String name, boolean unique, String... fields) {
        var index = new TapIndex().name(name).unique(unique);
        for (String field : fields) index.indexField(new TapIndexField().name(field).fieldAsc(true));
        return index;
    }

    private static TargetTable target() {
        return new TargetTable("orders", List.of(
                new TargetField("_id", "string", true, TapstateType.STRING),
                new TargetField("position", "long", true, TapstateType.INT64)),
                List.of(new TargetIndex(List.of("_id", "position"), true)),
                new UnwindIdentity(List.of("_id"), "_unwind_source_id"));
    }

    private static SinkConfig config(String id) {
        return new SinkConfig(id, Map.of(), WriteMode.UPSERT, DdlPolicy.FAIL, null, null, OnFullLoad.APPEND, true);
    }
}
