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
}
