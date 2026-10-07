package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeQueueProbeTest {
    private static Map<String, Object> vertex(Object index) {
        return Map.of("name", "sink", "executionId", "exec", "processors",
                List.of(Map.of("memberUuid", "member", "index", index)));
    }

    @Test void completeTopologyUsesExactProcessorAndExecutionIdentities() {
        var roster = BenchmarkNativeQueueProbe.topologyRoster(Map.of("vertices", List.of(vertex(BigDecimal.ZERO))), "member");
        assertThat(roster.identities()).containsExactly("member/sink/0");
        assertThat(roster.executions()).containsExactly("exec");
    }

    @Test void malformedVertexCannotDisappearFromAQualifiedSubset() {
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(0), Map.of("name", "missing"))), "member"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("incomplete execution identity");
    }

    @Test void fractionalAndDuplicateProcessorsCannotBecomeACompleteRoster() {
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(new BigDecimal("0.5")))), "member"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("exact integer");
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(0), vertex(0))), "member"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicate");
        assertThatThrownBy(() -> BenchmarkNativeQueueProbe.topologyRoster(
                Map.of("vertices", List.of(vertex(0))), "other"))
                .isInstanceOf(AssertionError.class).hasMessageContaining("different processor");
    }
}
