package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineRepresentationMalformedInputTest {

    private final PipelineRepresentation representation = new PipelineRepresentation();

    @Test
    void missingNestEmbedAsIsReportedAsCodedMalformedRequest() {
        PipelineInput input = new PipelineInput(
                "invalid",
                null,
                List.of("src"),
                List.of(Map.of(
                        "id", "nested",
                        "type", "nest",
                        "from", Map.of("orders", "src"),
                        "root", Map.of(
                                "from", "orders",
                                "embed", List.of(Map.of(
                                        "from", "orders",
                                        "on", Map.of("a", "b"),
                                        "path", "items"))))),
                null,
                null,
                null,
                null);

        assertThatThrownBy(() -> representation.toModel(input, null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(error.args()).containsKey("reason");
                });
    }
}
