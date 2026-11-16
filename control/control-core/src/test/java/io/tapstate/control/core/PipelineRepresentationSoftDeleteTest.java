package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The editor face of a map's soft-delete column: the structured payload reads it, writes it back, and
 * refuses the same shapes the document parser refuses.
 */
class PipelineRepresentationSoftDeleteTest {

    private final PipelineRepresentation representation = new PipelineRepresentation();

    private PipelineResource model(Map<String, Object> step) {
        return representation.toModel(new PipelineInput(
                "p", null, List.of("src_a"), List.of(step), null,
                Map.of("from", "live", "sync", List.of(Map.of("source", "src_a"))),
                null, null), null);
    }

    private static Map<String, Object> mapStep(Map<String, Object> payload) {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("id", "live");
        step.put("from", "orders");
        step.putAll(payload);
        return step;
    }

    private static TransformBody.MapProjection mapIn(PipelineResource pipeline) {
        return (TransformBody.MapProjection) ((Step.Inline) pipeline.transforms().get(0)).body();
    }

    @Test
    @DisplayName("a body holding only soft_delete is read as a map with no fields")
    void softDeleteAloneIsAMap() {
        TransformBody.MapProjection body = mapIn(model(mapStep(Map.of("body", Map.of("soft_delete", "is_deleted")))));

        assertThat(body.softDelete()).isEqualTo("is_deleted");
        assertThat(body.fields()).isEmpty();
    }

    @Test
    @DisplayName("the camelCase spelling and a body object are read the same way")
    void camelCaseInABody() {
        TransformBody.MapProjection body = mapIn(model(mapStep(Map.of(
                "body", Map.of("softDelete", "is_deleted", "fields", Map.of("stage", "prod"))))));

        assertThat(body.softDelete()).isEqualTo("is_deleted");
        assertThat(body.fields()).containsEntry("stage", FieldRule.literal("prod"));
    }

    @Test
    @DisplayName("the view writes soft_delete back beside the fields, and reads back to the same model")
    void viewRoundTrips() {
        PipelineResource pipeline = model(mapStep(Map.of(
                "type", "map", "soft_delete", "is_deleted", "fields", Map.of("stage", "prod"))));

        PipelineView view = representation.toView(
                pipeline, "e".repeat(64), List.of(new PipelineSourceSummary("src_a", null, "mysql")));
        Map<String, Object> step = view.transforms().get(0);

        assertThat(step).containsEntry("type", "map");
        assertThat(step.toString()).contains("soft_delete=is_deleted");
        Map<String, Object> echoed = new LinkedHashMap<>(step);
        echoed.keySet().retainAll(List.of("id", "type", "from", "body", "fields", "soft_delete"));
        assertThat(mapIn(model(echoed))).isEqualTo(mapIn(pipeline));
    }

    @Test
    @DisplayName("a map without soft_delete writes no soft_delete key")
    void plainMapWritesNoMarker() {
        PipelineResource pipeline = model(mapStep(Map.of("type", "map", "fields", Map.of("stage", "prod"))));

        PipelineView view = representation.toView(
                pipeline, "e".repeat(64), List.of(new PipelineSourceSummary("src_a", null, "mysql")));

        assertThat(view.transforms().get(0).toString()).doesNotContain("soft_delete");
    }

    @Test
    @DisplayName("a blank soft_delete is refused")
    void blankIsRefused() {
        Map<String, Object> step = mapStep(Map.of("type", "map", "soft_delete", " "));

        assertThatThrownBy(() -> model(step))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("soft_delete must name a column");
    }

    @Test
    @DisplayName("a marker also declared under fields is refused")
    void markerAlsoAFieldIsRefused() {
        Map<String, Object> step = mapStep(Map.of(
                "type", "map", "soft_delete", "is_deleted", "fields", Map.of("is_deleted", false)));

        assertThatThrownBy(() -> model(step))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("also declared under fields");
    }

    @Test
    @DisplayName("a map with neither fields nor soft_delete is still refused")
    void neitherIsRefused() {
        Map<String, Object> step = mapStep(Map.of("type", "map"));

        assertThatThrownBy(() -> model(step))
                .isInstanceOf(TapstateException.class);
    }
}
