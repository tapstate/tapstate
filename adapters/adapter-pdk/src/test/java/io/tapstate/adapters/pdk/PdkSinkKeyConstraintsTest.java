package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;

class PdkSinkKeyConstraintsTest {
    private static final TargetTable TARGET = new TargetTable("orders",
            List.of(new TargetField("id", "bigint", true, TapstateType.INT64), new TargetField("owner", "varchar", false, TapstateType.STRING)),
            List.of(new TargetIndex(List.of("owner"), true)));

    @Test
    void requestsTheCompleteCollectionCatalogThroughTheExistingCommand(@TempDir Path directory) throws Throwable {
        var rows = catalog(Map.of(), List.of(index("_id_", Map.of("_id", 1), Map.of())));
        try (var connector = PdkConnector.open("mongodb", IdentityConnectorFixture.ref(directory), Map.of())) {
            connector.functions().supportExecuteCommandFunction((context, command, out) -> {
                assertThat(command.getCommand()).isEqualTo("aggregate");
                assertThat(command.getParams().get("collection")).isEqualTo("orders");
                assertThat(command.getParams().get("pipeline")).isEqualTo(List.of(Map.of("$listCatalog", Map.of()),
                        Map.of("$project", Map.of("_id", 0, "metadata", "$md"))));
                out.accept(new ExecuteResult<>().result(rows));
            });
            assertThat(PdkSinkKeyConstraints.read(connector, TARGET).exactScalarEquality()).isTrue();
        }
    }

    @Test
    void includesEveryActualUniqueKeyAndIdentifiesOnlyGeneratedDocumentIdentity() {
        var result = PdkSinkKeyConstraints.mongo(catalog(Map.of(), List.of(
                index("_id_", Map.of("_id", 1), Map.of()),
                index("owner_1", Map.of("owner", 1), Map.of("unique", true)),
                index("id_1", Map.of("id", 1), Map.of("unique", true)),
                index("other", Map.of("value", 1), Map.of()))), TARGET);
        assertThat(result.exactScalarEquality()).isTrue();
        assertThat(result.generatedDocumentIdentity()).isTrue();
        assertThat(result.uniqueKeys()).containsExactly(List.of("_id"), List.of("owner"), List.of("id"));
        var explicit = new TargetTable("orders", List.of(new TargetField("_id", "varchar", true, TapstateType.STRING)));
        assertThat(PdkSinkKeyConstraints.mongo(catalog(Map.of(), List.of(
                index("_id_", Map.of("_id", 1), Map.of()))), explicit).generatedDocumentIdentity()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"folded-index", "folded-collection", "sparse", "partial", "building",
            "expression", "dotted", "missing-identity", "duplicate-index", "missing-options", "two-catalogs",
            "prepare-unique", "malformed-unique", "external-unique"})
    void refusesAnIncompleteCatalogOrUnsupportedEquality(String condition) {
        var options = new LinkedHashMap<String, Object>();
        var specOptions = new LinkedHashMap<String, Object>(Map.of("unique", true));
        Map<String, Object> fields = Map.of("owner", 1);
        switch (condition) {
            case "folded-index" -> specOptions.put("collation", Map.of("locale", "en", "strength", 2));
            case "folded-collection" -> options.put("collation", Map.of("locale", "en", "strength", 2));
            case "sparse" -> specOptions.put("sparse", true);
            case "partial" -> specOptions.put("partialFilterExpression", Map.of("owner", Map.of("$exists", true)));
            case "expression" -> fields = Map.of("owner", "hashed");
            case "dotted" -> fields = Map.of("owner.name", 1);
            case "external-unique" -> fields = Map.of("external", 1);
            case "prepare-unique" -> {
                specOptions.put("unique", false);
                specOptions.put("prepareUnique", true);
            }
            case "malformed-unique" -> specOptions.put("unique", "true");
            default -> { }
        }
        var unique = index("owner_1", fields, specOptions);
        if (condition.equals("building")) unique.put("ready", false);
        var indexes = new ArrayList<Map<String, Object>>();
        if (!condition.equals("missing-identity")) indexes.add(index("_id_", Map.of("_id", 1), Map.of()));
        indexes.add(unique);
        if (condition.equals("duplicate-index")) indexes.add(unique);
        var rows = catalog(options, indexes);
        if (condition.equals("missing-options")) rows = List.of(Map.of("metadata", Map.of("indexes", indexes)));
        if (condition.equals("two-catalogs")) rows = List.of(rows.getFirst(), rows.getFirst());
        var result = PdkSinkKeyConstraints.mongo(rows, TARGET);
        assertThat(result.exactScalarEquality()).isFalse();
        assertThat(result.uniqueKeys()).isEmpty();
    }

    private static Map<String, Object> index(String name, Map<String, Object> fields, Map<String, Object> extra) {
        var spec = new LinkedHashMap<String, Object>(extra);
        spec.put("name", name);
        spec.put("key", fields);
        return new LinkedHashMap<>(Map.of("ready", true, "spec", spec));
    }

    private static List<Map<String, Object>> catalog(Map<String, Object> options, List<Map<String, Object>> indexes) {
        return List.of(Map.of("metadata", Map.of("options", options, "indexes", indexes)));
    }
}
