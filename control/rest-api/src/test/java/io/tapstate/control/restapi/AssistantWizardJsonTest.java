package io.tapstate.control.restapi;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class AssistantWizardJsonTest {
    @Test void viewRoundTripPreservesDomainFieldsAndNestedRelationKeys() {
        Map<String, Object> wizard = Map.of("root", Map.of("id", "root", "sourceId", "orders", "table", "orders", "key", List.of("id"), "preTransforms", List.of()),
                "related", List.of(Map.of("id", "items", "parentId", "root", "sourceId", "orders", "table", "items", "preTransforms", List.of(),
                        "relation", Map.of("shape", "array", "path", "items", "key", List.of("id"), "arrayKey", List.of("id"), "on", List.of(Map.of("childField", "order_id", "parentField", "id"))))),
                "transforms", List.of(Map.of("id", "filter", "type", "filter", "fields", Map.of("expr", "id > 0"))),
                "output", Map.of("kind", "atlas", "config", Map.of("sourceId", "target", "table", "view", "inputTable", "orders", "writeMode", "upsert")));
        assertThat(AssistantWizardJson.wire(AssistantWizardJson.view(wizard), wizard)).isEqualTo(wizard);
    }
    @Test void relationAndArrayKeysRoundTripIndependently() {
        for (List<String> relationKey : List.of(List.of("relation_id"), List.<String>of())) {
            Map<String, Object> wizard = Map.of("root", Map.of("id", "root", "sourceId", "orders", "table", "orders", "key", List.of("id"), "preTransforms", List.of()),
                    "related", List.of(Map.of("id", "items", "parentId", "root", "sourceId", "orders", "table", "items", "preTransforms", List.of(),
                            "relation", Map.of("shape", "array", "path", "items", "key", relationKey, "arrayKey", List.of("element_id"), "on", List.of(Map.of("childField", "order_id", "parentField", "id"))))),
                    "transforms", List.of(), "output", Map.of("kind", "atlas", "config", Map.of("sourceId", "target", "table", "view", "inputTable", "orders", "writeMode", "upsert")));
            assertThat(AssistantWizardJson.wire(AssistantWizardJson.view(wizard), wizard)).isEqualTo(wizard);
        }
    }

    @Test void defaultsUnknownRelationsToObjectsAndBuildsTheLegacyJoinCondition() {
        Map<String, Object> view = Map.of("root", Map.of("source", "src", "table", "orders",
                        "keys", List.of("id"), "transforms", List.of()),
                "relations", List.of(Map.ofEntries(Map.entry("id", "items"), Map.entry("type", "sub_doc"),
                        Map.entry("source", "src"), Map.entry("table", "items"), Map.entry("targetField", "items"),
                        Map.entry("primaryKey", List.of("order_id")), Map.entry("arrayKey", List.of()),
                        Map.entry("key", "order_id"), Map.entry("parentKey", "id"),
                        Map.entry("transforms", List.of()), Map.entry("children", List.of()))),
                "transforms", List.of(), "destinationSourceId", "target", "destinationTable", "view",
                "destinationWriteMode", "upsert");
        Map<String, Object> original = Map.of("output", Map.of("kind", "atlas", "config", Map.of()));

        Map<String, Object> relation = AssistantWizardJson.object(
                AssistantWizardJson.list(AssistantWizardJson.wire(view, original).get("related")).getFirst());
        Map<String, Object> relationContract = AssistantWizardJson.object(relation.get("relation"));

        assertThat(relationContract).containsEntry("shape", "object")
                .containsEntry("on", List.of(Map.of("childField", "order_id", "parentField", "id")));
    }

    @Test void roundTripsJavascriptAndUnwindTransforms() {
        List<Map<String, Object>> transforms = List.of(
                Map.of("id", "script", "type", "js", "fields", Map.of("script", "return record;")),
                Map.of("id", "expand", "type", "unwind", "fields", Map.of("path", "items")));

        assertThat(AssistantWizardJson.writeTransforms(AssistantWizardJson.readTransforms(transforms)))
                .containsExactly(Map.of("id", "script", "type", "js", "fields", Map.of("script", "return record;")),
                        Map.of("id", "expand", "type", "unwind", "fields", Map.of("path", "items")));
    }
    @Test void roundTripsUnionInputReferencesForWizardCandidates() {
        List<Map<String, Object>> transforms = List.of(Map.of("id", "union-orders", "type", "union",
                "fields", Map.of("from", List.of("src.archive_orders"))));

        List<Map<String, Object>> candidate = AssistantWizardJson.readTransforms(transforms);
        assertThat(AssistantWizardJson.writeTransforms(candidate)).containsExactly(Map.of(
                "id", "union-orders", "type", "union", "fields", Map.of("from", List.of("src.archive_orders"))));
    }
    @Test void mapTransformsRoundTripLiteralRenameComputeAndDeleteValues() {
        Map<String, Object> wizard = Map.of("root", Map.of("id", "root", "sourceId", "orders", "table", "orders", "key", List.of("id"), "preTransforms", List.of()),
                "related", List.of(),
                "transforms", List.of(Map.of("id", "map", "type", "map", "fields", Map.of(
                        "literalNumber", 42L, "literalText", "hello", "renamed", "$old_name", "computed", "=amount + 1", "removed", false))),
                "output", Map.of("kind", "atlas", "config", Map.of("sourceId", "target", "table", "view", "inputTable", "orders", "writeMode", "upsert")));

        assertThat(AssistantWizardJson.wire(AssistantWizardJson.view(wizard), wizard)).isEqualTo(wizard);
    }
    @Test void mapTransformRejectsUnknownRuleTypeAndKeepsInvalidLiteralAsText() {
        List<Map<String, Object>> unknownRule = List.of(Map.of("id", "map", "type", "map",
                "config", Map.of("map", Map.of("fields", List.of(Map.of("outputField", "value", "ruleType", "unknown", "value", "x"))))));
        assertThatThrownBy(() -> AssistantWizardJson.writeTransforms(unknownRule))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(AssistantWizardJson.writeTransforms(List.of(Map.of("id", "map", "type", "map",
                "config", Map.of("map", Map.of("fields", List.of(Map.of("outputField", "value", "ruleType", "literal", "value", "not-json"))))))))
                .containsExactly(Map.of("id", "map", "type", "map", "fields", Map.of("value", "not-json")));
    }
    @Test void unsupportedTransformAndExcessiveNestingCannotBecomeCandidate() {
        List<Map<String, Object>> unsupported = List.of(Map.of("id", "bad", "type", "sql", "config", Map.of()));
        assertThatThrownBy(() -> AssistantWizardJson.writeTransforms(unsupported))
                .isInstanceOf(IllegalArgumentException.class);
    }
    @Test void matchesTheCompilerRelationDepthAndCountLimits() {
        Map<String, Object> deepest = relation("relation-128", List.of());
        for (int index = 127; index >= 0; index--) deepest = relation("relation-" + index, List.of(deepest));
        Map<String, Object> maxDepth = deepest;
        Map<String, Object> view = view(List.of(maxDepth));

        assertThatCode(() -> AssistantWizardJson.validateViewShape(view)).doesNotThrowAnyException();
        Map<String, Object> wire = AssistantWizardJson.wire(view, Map.of("output", Map.of("kind", "atlas", "config", Map.of())));
        assertThat(AssistantWizardJson.list(AssistantWizardJson.view(wire).get("relations"))).hasSize(1);
        assertThat(AssistantWizardJson.list(wire.get("related"))).hasSize(129);
        assertThatThrownBy(() -> AssistantWizardJson.validateViewShape(view(List.of(
                relation("too-deep", List.of(maxDepth))))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> AssistantWizardJson.wire(
                view(List.of(relation("too-deep", List.of(maxDepth)))),
                Map.of("output", Map.of("kind", "atlas", "config", Map.of()))))
                .isInstanceOf(IllegalArgumentException.class);

        List<Map<String, Object>> tooMany = new java.util.ArrayList<>();
        for (int index = 0; index < 512; index++) tooMany.add(relation("flat-" + index, List.of()));
        Map<String, Object> flatWire = AssistantWizardJson.wire(view(tooMany),
                Map.of("output", Map.of("kind", "atlas", "config", Map.of())));
        assertThat(AssistantWizardJson.list(flatWire.get("related"))).hasSize(512);
        tooMany.add(relation("flat-512", List.of()));
        assertThatThrownBy(() -> AssistantWizardJson.wire(view(tooMany),
                Map.of("output", Map.of("kind", "atlas", "config", Map.of()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Map<String, Object> view(List<Map<String, Object>> relations) {
        return Map.of("root", Map.of("source", "src", "table", "orders", "keys", List.of("id"),
                        "columns", List.of(), "transforms", List.of()),
                "relations", relations, "transforms", List.of(), "destinationSourceId", "",
                "destinationTable", "", "destinationWriteMode", "upsert");
    }

    private static Map<String, Object> relation(String id, List<Map<String, Object>> children) {
        return Map.ofEntries(Map.entry("id", id), Map.entry("source", "src"), Map.entry("table", "items"),
                Map.entry("type", "sub_doc"), Map.entry("primaryKey", List.of("id")),
                Map.entry("arrayKey", List.of()), Map.entry("targetField", id), Map.entry("key", "id"),
                Map.entry("parentKey", "id"), Map.entry("parentTable", "orders"),
                Map.entry("conditions", List.of(Map.of("childKey", "id", "parentKey", "id"))),
                Map.entry("transforms", List.of()), Map.entry("children", children));
    }
}
