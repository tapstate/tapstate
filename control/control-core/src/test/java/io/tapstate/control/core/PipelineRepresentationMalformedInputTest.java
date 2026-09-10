package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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

    @Test
    void refusesAStepCarryingOptions() {
        // Options are the engine's own configuration and its vocabulary is empty today, so the model
        // has nowhere to put one. Until now this face took the key and dropped it: the request read
        // as accepted and configured nothing, while the source face and the grammar both refused it.
        assertThatThrownBy(() -> representation.toModel(pipeline(
                List.of(Map.of(
                        "id", "active",
                        "type", "filter",
                        "from", "src",
                        "expr", "status == 'active'",
                        "options", Map.of("batch_size", 500))),
                null), null))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(error.args()).containsKey("reason");
                });
    }

    @Test
    void refusesASyncElementCarryingOptions() {
        assertThatThrownBy(() -> representation.toModel(pipeline(null, Map.of(
                "from", "src",
                "sync", List.of(Map.of(
                        "source", "target",
                        "write_mode", "append",
                        "options", Map.of("ordered", true))))), null))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
    }

    @Test
    void refusesAPushElementCarryingOptions() {
        assertThatThrownBy(() -> representation.toModel(pipeline(null, Map.of(
                "from", "src",
                "push", List.of(Map.of(
                        "source", "kafka_target",
                        "topic", "orders",
                        "options", Map.of("acks", "all"))))), null))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
    }

    /**
     * The discriminator for the three above: the same documents without the option are accepted, so
     * a refusal there can only be the option and never a malformed seed.
     */
    @Test
    void acceptsTheSameDocumentsWithoutOptions() {
        assertThat(representation.toModel(pipeline(
                List.of(Map.of(
                        "id", "active", "type", "filter", "from", "src", "expr", "status == 'active'")),
                null), null)).isNotNull();
        assertThat(representation.toModel(pipeline(null, Map.of(
                "from", "src",
                "sync", List.of(Map.of("source", "target", "write_mode", "append")))), null)).isNotNull();
        assertThat(representation.toModel(pipeline(null, Map.of(
                "from", "src",
                "push", List.of(Map.of("source", "kafka_target", "topic", "orders")))), null)).isNotNull();
    }

    private static PipelineInput pipeline(
            List<Map<String, Object>> transforms, Map<String, Object> serve) {
        return new PipelineInput(
                "probe", null, new ArrayList<Object>(List.of("src")), transforms, null, serve, null, null);
    }
}
