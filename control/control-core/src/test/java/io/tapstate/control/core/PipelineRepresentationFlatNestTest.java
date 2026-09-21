package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PipelineRepresentationFlatNestTest {

    private final PipelineRepresentation representation = new PipelineRepresentation();

    @Test
    void structuredPipelineInputCarriesAFlatEmbedWithoutAPath() {
        PipelineResource pipeline = representation.toModel(input(Map.of(
                "from", "profile",
                "on", Map.of("customer_id", "id"),
                "as", "flat")), null);

        Step.Inline step = (Step.Inline) pipeline.transforms().getFirst();
        Embed embed = ((TransformBody.Nest) step.body()).root().embed().getFirst();
        assertThat(embed.as()).isEqualTo(EmbedAs.FLAT);
        assertThat(embed.path()).isNull();

        PipelineView view = representation.toView(
                pipeline,
                "a".repeat(64),
                List.of(new PipelineSourceSummary("src", null, "mysql")));
        @SuppressWarnings("unchecked")
        Map<String, Object> root = (Map<String, Object>) view.transforms().getFirst().get("root");
        @SuppressWarnings("unchecked")
        Map<String, Object> represented = ((List<Map<String, Object>>) root.get("embed")).getFirst();
        assertThat(represented).containsEntry("as", "FLAT").doesNotContainKey("path");
    }

    @Test
    void structuredPipelineInputRefusesFlatPathAndArrayKey() {
        for (Map<String, Object> forbidden : List.of(
                Map.<String, Object>of("path", "profile"),
                Map.<String, Object>of("array_key", List.of("id")))) {
            Map<String, Object> embed = new java.util.LinkedHashMap<>(Map.of(
                    "from", "profile",
                    "on", Map.of("customer_id", "id"),
                    "as", "flat"));
            embed.putAll(forbidden);

            assertThatThrownBy(() -> representation.toModel(input(embed), null))
                    .isInstanceOf(TapstateException.class)
                    .hasMessageContaining("forbidden when as is flat");
        }
    }

    private static PipelineInput input(Map<String, Object> embed) {
        return new PipelineInput(
                "p",
                null,
                List.of("src"),
                List.of(Map.of(
                        "id", "document",
                        "type", "nest",
                        "from", Map.of("customer", "customers", "profile", "profiles"),
                        "root", Map.of(
                                "from", "customer",
                                "key", List.of("id"),
                                "embed", List.of(embed)))),
                null,
                null,
                null,
                null);
    }
}
