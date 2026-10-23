package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkIdentityIndexesTest {
    private static ConnectorRef ref;
    @BeforeAll static void connector(@TempDir Path directory) { ref = IdentityConnectorFixture.ref(directory); }

    @Test
    void mongoReadsFullSpecsAndKeepsPrimaryIdentityEvenWithoutTheUniqueFlag() throws Throwable {
        try (var connector = PdkConnector.open("mongodb", ref, Map.of())) {
            var rows = new ArrayList<>(IdentityConnectorFixture.mongoIndexes());
            rows.set(0, Map.of("spec", Map.of("name", "_id_", "key", Map.of("_id", 1))));
            rows.add(Map.of("spec", Map.of("name", "nonunique", "key", Map.of("value", "text"))));
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                assertThat(command.getCommand()).isEqualTo("aggregate");
                assertThat(command.getParams()).containsEntry("collection", "orders")
                        .containsEntry("pipeline", List.of(Map.of("$indexStats", Map.of())));
                out.accept(new ExecuteResult<>().result(rows));
            });
            var result = PdkIdentityIndexes.mongo(connector, "orders");
            assertThat(result.keys()).containsExactly(List.of("_id"), List.of("_unwind_source_id", "position"));
            assertThat(result.opaqueUniqueConstraints()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sparse", "partialFilterExpression", "collation", "empty", "hashed", "nonstring"})
    void mongoNeverTreatsAnOpaqueUniqueIndexAsACompleteKey(String kind) throws Throwable {
        var spec = new LinkedHashMap<String, Object>(Map.of("name", "unsafe", "unique", true, "key", Map.of("value", 1)));
        switch (kind) {
            case "empty" -> spec.put("key", Map.of());
            case "hashed" -> spec.put("key", Map.of("value", "hashed"));
            case "nonstring" -> spec.put("key", Map.of(1, 1));
            default -> spec.put(kind, true);
        }
        try (var connector = PdkConnector.open("mongodb", ref, Map.of("rows", List.of(Map.of("spec", spec))))) {
            var result = PdkIdentityIndexes.mongo(connector, "orders");
            assertThat(result.keys()).isEmpty();
            assertThat(result.opaqueUniqueConstraints()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"empty", "missing-spec", "missing-key", "no-result", "non-row", "error-after-rows", "missing-function"})
    void incompleteOrFailedMongoInspectionNeverBecomesEmptySafeEvidence(String kind) throws Throwable {
        try (var connector = PdkConnector.open("mongodb", ref, Map.of())) {
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                switch (kind) {
                    case "empty" -> out.accept(new ExecuteResult<>().result(List.of()));
                    case "missing-spec" -> out.accept(new ExecuteResult<>().result(List.of(Map.of())));
                    case "missing-key" -> out.accept(new ExecuteResult<>().result(List.of(Map.of("spec", Map.of()))));
                    case "non-row" -> out.accept(new ExecuteResult<>().result(List.of("wrong")));
                    case "error-after-rows" -> {
                        out.accept(new ExecuteResult<>().result(IdentityConnectorFixture.mongoIndexes()));
                        out.accept(new ExecuteResult<>().error(new IllegalStateException("permission denied")));
                    }
                    default -> { out.accept(null); out.accept(new ExecuteResult<>().result("unknown")); }
                }
            });
            if (kind.equals("missing-function")) connector.functions().supportExecuteCommandFunction(null);
            assertThatThrownBy(() -> PdkIdentityIndexes.mongo(connector, "orders")).isInstanceOf(IllegalStateException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"custom-schema", "default-schema"})
    void postgresUsesExactFullKeysAndEscapesIdentifiersAsData(String variant) throws Throwable {
        Map<String, Object> config = variant.equals("custom-schema") ? Map.of("schema", "a'bc") : Map.of();
        try (var connector = PdkConnector.open("postgres", ref, config)) {
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                assertThat(command.getCommand()).isEqualTo("executeQuery");
                assertThat(command.getParams().get("sql").toString()).doesNotContain("o'rders", "a'bc")
                        .contains(variant.equals("custom-schema") ? "61276263" : "current_schema()");
                out.accept(new ExecuteResult<>().result(List.of(IdentityConnectorFixture.postgresRow())));
            });
            var result = PdkIdentityIndexes.read(connector, "o'rders");
            assertThat(result.keys()).containsExactly(List.of("_unwind_source_id", "position"));
            assertThat(result.signedInt64Columns()).containsExactlyInAnyOrder("_unwind_source_id", "position");
            assertThat(result.opaqueUniqueConstraints()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"is_valid", "is_ready", "deterministic", "predicate", "expressions", "columns_json", "empty-columns", "nonstring-columns"})
    void postgresRefusesPartialExpressionUnavailableAndUnrepresentableKeys(String condition) throws Throwable {
        var row = IdentityConnectorFixture.postgresRow();
        switch (condition) {
            case "empty-columns" -> row.put("columns_json", "[]");
            case "nonstring-columns" -> row.put("columns_json", "[42]");
            default -> row.put(condition, false);
        }
        try (var connector = PdkConnector.open("postgres", ref, Map.of("rows", List.of(row)))) {
            var result = PdkIdentityIndexes.read(connector, "orders");
            assertThat(result.keys()).isEmpty();
            assertThat(result.opaqueUniqueConstraints()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mysql", "aws-rds-mysql", "aliyun-rds-mysql", "aliyun-rds-mariadb"})
    void mysqlRequiresEveryIndexPartAndOnlyClaimsWritableSignedIntegers(String connectorId) throws Throwable {
        var parent = IdentityConnectorFixture.mysqlRow("_unwind_source_id");
        var position = IdentityConnectorFixture.mysqlRow("position");
        position.put("column_type", "BIGINT");
        try (var connector = PdkConnector.open(connectorId, ref, Map.of("rows", List.of(parent, position)))) {
            var result = PdkIdentityIndexes.read(connector, "o'rders");
            assertThat(result.keys()).containsExactly(List.of("_unwind_source_id", "position"));
            assertThat(result.signedInt64Columns()).containsExactlyInAnyOrder("_unwind_source_id", "position");
            assertThat(result.opaqueUniqueConstraints()).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"prefix", "collation", "expression", "unsigned", "generated", "missing-type"})
    void mysqlDoesNotDiscardAnUnsafePartOrInventAnIntegerProof(String condition) throws Throwable {
        var parent = IdentityConnectorFixture.mysqlRow("_unwind_source_id");
        var position = IdentityConnectorFixture.mysqlRow("position");
        switch (condition) {
            case "prefix" -> position.put("prefix_length", 4);
            case "collation" -> position.put("collation_name", "utf8mb4_bin");
            case "expression" -> position.put("column_name", null);
            case "unsigned" -> position.put("column_type", "bigint unsigned");
            case "generated" -> position.put("generated_expression", "id + 1");
            default -> position.remove("column_type");
        }
        try (var connector = PdkConnector.open("mysql", ref, Map.of("rows", List.of(parent, position)))) {
            var result = PdkIdentityIndexes.read(connector, "orders");
            if (List.of("prefix", "collation", "expression").contains(condition)) {
                assertThat(result.keys()).isEmpty();
                assertThat(result.opaqueUniqueConstraints()).isTrue();
            } else {
                assertThat(result.keys()).containsExactly(List.of("_unwind_source_id", "position"));
                assertThat(result.signedInt64Columns()).doesNotContain("position");
            }
        }
    }
}
