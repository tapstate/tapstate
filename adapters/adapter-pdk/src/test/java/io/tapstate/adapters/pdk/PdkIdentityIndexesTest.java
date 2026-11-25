package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;

class PdkIdentityIndexesTest {
    private static ConnectorRef ref;
    @BeforeAll static void connector(@TempDir Path directory) { ref = IdentityConnectorFixture.ref(directory); }

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
