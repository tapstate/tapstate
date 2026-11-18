package io.tapstate.control.restapi;

import io.tapstate.spi.store.PipelineDraft;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineDraftJsonTest {

    private final ObjectMapper json = new ObjectMapper();

    @Test
    void readsAndWritesRollupFieldsOnALowerCaseRollupRelation() {
        Map<String, Object> rollup = Map.of("item_count", Map.of("count", true));
        Map<String, Object> body = Map.of(
                "pipelineId", "orders",
                "mode", "wizard",
                "name", "Orders",
                "wizard", Map.of(
                        "root", Map.of("id", "orders", "sourceId", "mysql", "table", "orders",
                                "key", List.of("id"), "preTransforms", List.of()),
                        "related", List.of(Map.of(
                                "id", "items", "parentId", "orders", "sourceId", "mysql", "table", "order_items",
                                "relation", Map.of(
                                        "on", List.of(Map.of("childField", "order_id", "parentField", "id")),
                                        "shape", "rollup", "key", List.of(), "arrayKey", List.of("item_id"),
                                        "rollup", rollup, "trackKeyChanges", false),
                                "preTransforms", List.of())),
                        "transforms", List.of()));

        PipelineDraft draft = PipelineDraftJson.read(json, body, "orders", 1, "author");
        PipelineDraft.Relation relation = draft.wizard().related().getFirst().relation();

        assertThat(relation.shape()).isEqualTo(PipelineDraft.Shape.ROLLUP);
        assertThat(relation.path()).isNull();
        assertThat(relation.rollup()).isEqualTo(rollup);
        assertThat(relation.trackKeyChanges()).isFalse();

        Map<?, ?> wire = (Map<?, ?>) PipelineDraftJson.write(json, draft).get("wizard");
        Map<?, ?> wireRelation = (Map<?, ?>) ((Map<?, ?>) ((List<?>) wire.get("related")).getFirst()).get("relation");
        assertThat(wireRelation.get("shape")).isEqualTo("rollup");
        assertThat(wireRelation.get("rollup")).isEqualTo(rollup);
        assertThat(wireRelation.get("trackKeyChanges")).isEqualTo(false);
    }
}
