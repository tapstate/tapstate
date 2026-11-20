package io.tapstate.core.common;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class RuntimeDiagnosticProjectionTest {
    @Test void brokenAttachmentAccessCannotReplaceThePrimaryFailure() {
        var broken = new java.util.AbstractMap<String, Object>() {
            @Override public java.util.Set<Entry<String, Object>> entrySet() {
                throw new IllegalStateException("attachment unavailable");
            }
        };
        assertThat(RuntimeDiagnosticProjection.fit(broken)).containsEntry("truncated", true);
    }

    @Test void preservesTheBoundaryAndRejectsAdditionalNodes() {
        var children = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < 63; i++) children.add(Map.of("message", "child-" + i));
        Map<String, Object> accepted = Map.of("causes", List.copyOf(children));
        assertThat(RuntimeDiagnosticProjection.read(accepted)).isEqualTo(accepted);
        children.add(Map.of("message", "overflow"));
        assertThat(RuntimeDiagnosticProjection.read(Map.of("causes", children))).isEmpty();
    }

    @Test void depthCyclesAndUnknownFieldsNeverBecomeStoredFacts() {
        Map<String, Object> chain = Map.of("message", "leaf");
        for (int i = 1; i < 32; i++) chain = Map.of("cause", chain);
        assertThat(RuntimeDiagnosticProjection.read(chain)).isEqualTo(chain);
        assertThat(RuntimeDiagnosticProjection.read(Map.of("cause", chain))).isEmpty();
        var cycle = new LinkedHashMap<String, Object>();
        cycle.put("cause", cycle);
        assertThat(RuntimeDiagnosticProjection.fit(cycle)).containsExactlyEntriesOf(Map.of("truncated", true));
        assertThat(RuntimeDiagnosticProjection.read(Map.of("message", "known", "unexpected", "value"))).isEmpty();
    }

    @Test void parametersTextStackAndReferencesHaveIndependentLimits() {
        var params = new LinkedHashMap<String, Object>();
        for (int i = 0; i < 32; i++) params.put("p" + i, "x".repeat(4096));
        assertThat(RuntimeDiagnosticProjection.read(Map.of("params", params))).isNotEmpty();
        params.put("overflow", "value");
        assertThat(RuntimeDiagnosticProjection.read(Map.of("params", params))).isEmpty();
        assertThat(RuntimeDiagnosticProjection.read(Map.of("message", "x".repeat(4097)))).isEmpty();
        assertThat(RuntimeDiagnosticProjection.read(Map.of("stackTrace", "frame\n".repeat(32)))).isNotEmpty();
        assertThat(RuntimeDiagnosticProjection.read(Map.of("stackTrace", "frame\n".repeat(33)))).isEmpty();
        var refs = new ArrayList<Map<String, String>>();
        for (int i = 0; i < 128; i++) refs.add(Map.of("type", "source", "id", "s" + i));
        assertThat(RuntimeDiagnosticProjection.read(Map.of("references", refs))).isNotEmpty();
        refs.add(Map.of("type", "source", "id", "overflow"));
        assertThat(RuntimeDiagnosticProjection.read(Map.of("references", refs))).isEmpty();
    }

    @Test void keepsAnImmutableSnapshotAndDoesNotRepairStoredInvalidInput() {
        var params = new LinkedHashMap<String, Object>();
        params.put("reason", "original");
        var snapshot = RuntimeDiagnosticProjection.fit(Map.of("params", params));
        params.put("reason", "later");
        assertThat(snapshot).containsEntry("params", Map.of("reason", "original"));
        assertThat(RuntimeDiagnosticProjection.read(Map.of("message", ""))).isEmpty();
        assertThat(RuntimeDiagnosticProjection.fit(Map.of("message", ""))).containsEntry("truncated", true);
    }
}
