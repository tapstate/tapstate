package io.tapstate.control.core;

import io.tapstate.core.model.canonical.CanonicalWriter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OnFullLoadRepresentationTest {
    @ParameterizedTest
    @ValueSource(strings = {"on_full_load", "onFullLoad"})
    void editorPayloadAndReturnedViewPreserveFullLoadPolicy(String property) {
        PipelineRepresentation representation = new PipelineRepresentation();
        PipelineInput input = new PipelineInput("copy", null, List.of("origin"), null, null,
                Map.of("from", "orders", "sync", List.of(Map.of("source", "target", property, "clear"))), null, null);
        var model = representation.toModel(input, null);
        assertThat(new CanonicalWriter().write(model)).contains("on_full_load: clear");
        var view = representation.toView(model, "hash", List.of(PipelineSourceSummary.unresolved("origin")));
        var sync = (Map<?, ?>) ((List<?>) view.serve().get("sync")).getFirst();
        assertThat(sync.get("onFullLoad")).isEqualTo("CLEAR");
    }

    @ParameterizedTest
    @ValueSource(strings = {"snake", "camel"})
    void aViewsWriteSettingsSurviveTheStructuredRoundTrip(String spelling) {
        PipelineRepresentation representation = new PipelineRepresentation();
        Map<String, Object> view = "snake".equals(spelling)
                ? Map.of("id", "lead", "from", "orders", "primary_key", "id",
                        "write_mode", "append", "on_full_load", "clear")
                : Map.of("id", "lead", "from", "orders", "primaryKey", "id",
                        "writeMode", "append", "onFullLoad", "clear");
        var model = representation.toModel(
                new PipelineInput("copy", null, List.of("origin"), null, view, null, null, null), null);
        assertThat(new CanonicalWriter().write(model)).contains("write_mode: append\n  on_full_load: clear\n");

        var returned = representation.toView(model, "hash", List.of(PipelineSourceSummary.unresolved("origin")));
        assertThat(returned.view()).containsEntry("writeMode", "APPEND").containsEntry("onFullLoad", "CLEAR");

        var again = representation.toModel(new PipelineInput("copy", null, List.of("origin"), null,
                returned.view(), null, null, null), null);
        assertThat(new CanonicalWriter().write(again)).isEqualTo(new CanonicalWriter().write(model));
    }
}
