package io.tapstate.core.common;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DiagnosticProjectionTest {

    @Test
    void snapshotsNamedAndPositionalParametersWithoutCoercingTheirValues() {
        Map<String, Object> parameters = new LinkedHashMap<>();
        parameters.put("unknown", null);
        parameters.put("delay", 1.5d);
        parameters.put("backoff", 0.25f);
        parameters.put("retryable", true);
        parameters.put("attempts", 3);
        parameters.put("source", "orders");
        var positions = new ArrayList<>(List.of("orders", "inventory"));
        var input = Map.of("code", "connection.refused", "definitionStatus", "unverified",
                "params", parameters, "cause", Map.of("arguments", positions, "rawMessage", "Driver refusal"),
                "references", List.of(Map.of("type", "pipeline-node", "id", "read", "pipelineId", "sync"),
                        Map.of("type", "connection", "id", "orders", "internal", true)),
                "internalDetails", "ignored");

        var expectedParameters = new LinkedHashMap<>(parameters);
        var snapshot = DiagnosticProjection.read(input);
        parameters.clear();
        positions.set(0, "edited");
        assertThat(snapshot).containsEntry("definitionStatus", "unverified").doesNotContainKey("internalDetails");
        assertThat(snapshot.get("params")).isEqualTo(expectedParameters);
        assertThat(snapshot.get("cause")).isEqualTo(
                Map.of("arguments", List.of("orders", "inventory"), "rawMessage", "Driver refusal"));
        assertThat(snapshot.get("references")).isEqualTo(List.of(
                Map.of("type", "pipeline-node", "id", "read", "pipelineId", "sync"),
                Map.of("type", "connection", "id", "orders")));
        assertThatThrownBy(() -> snapshot.put("code", "changed")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void refusesMalformedParametersAndMetadataRatherThanRetainingAnAmbiguousExtension() {
        for (var invalid : List.of(
                Map.of("message", " "), Map.of("message", 4), Map.of("truncated", false),
                Map.of("definitionStatus", "unknown"), Map.of("params", List.of()),
                Map.of("params", Map.of(), "arguments", List.of()), Map.of("params", Map.of(2, "position")),
                Map.of("params", Map.of("delay", Double.NaN)), Map.of("params", Map.of("delay", Float.POSITIVE_INFINITY)),
                Map.of("params", Map.of("object", Map.of())), Map.of("arguments", Map.of()),
                Map.of("arguments", List.of(1)))) {
            var diagnostic = new LinkedHashMap<String, Object>(invalid);
            diagnostic.put("code", "fixture.refused");
            assertThat(DiagnosticProjection.read(diagnostic)).as("invalid extension %s", invalid).isEmpty();
        }
    }

    @Test
    void refusesMalformedCauseTreesAndLinkedReferences() {
        var leaf = Map.of("code", "driver");
        for (var invalid : List.of(
                Map.of("cause", "driver"), Map.of("cause", leaf, "causes", List.of(leaf, leaf)),
                Map.of("causes", "driver"), Map.of("causes", List.of(leaf)),
                Map.of("causes", List.of(leaf, Map.of("unknown", "ignored"))),
                Map.of("references", "orders"), Map.of("references", List.of("orders")),
                Map.of("references", List.of(Map.of("id", "orders"))),
                Map.of("references", List.of(Map.of("type", "", "id", "orders"))),
                Map.of("references", List.of(Map.of("type", "connection"))),
                Map.of("references", List.of(Map.of("type", "connection", "id", ""))),
                Map.of("references", List.of(Map.of("type", "pipeline-node", "id", "source"))),
                Map.of("references", List.of(Map.of("type", "pipeline-node", "id", "source", "pipelineId", 1))),
                Map.of("references", List.of(Map.of("type", "pipeline-node", "id", "source", "pipelineId", ""))))) {
            var diagnostic = new LinkedHashMap<String, Object>(invalid);
            diagnostic.put("code", "fixture.refused");
            assertThat(DiagnosticProjection.read(diagnostic)).as("invalid extension %s", invalid).isEmpty();
        }
    }

    @Test
    void retainsAnExplicitlyTruncatedBranchAndStopsAtTheDiagnosticDepthLimit() {
        var leaf = Map.of("code", "driver");
        assertThat(DiagnosticProjection.read(Map.of("truncated", true, "causes", List.of(leaf), "references", List.of())))
                .containsEntry("truncated", true).containsEntry("causes", List.of(leaf)).doesNotContainKey("references");
        Map<String, Object> nested = new LinkedHashMap<>(leaf);
        for (int level = 1; level < DiagnosticProjection.MAX_DEPTH; level++) nested = Map.of("cause", nested);
        assertThat(DiagnosticProjection.read(nested)).isNotEmpty();
        assertThat(DiagnosticProjection.read(Map.of("cause", nested))).isEmpty();
        Map<String, Object> cyclic = new LinkedHashMap<>(leaf);
        cyclic.put("cause", cyclic);
        assertThat(DiagnosticProjection.read(cyclic)).isEmpty();
    }
}
