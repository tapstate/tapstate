package io.tapstate.mcp;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EnvironmentExpanderTest {

    @Test
    void recursivelyExpandsMapsListsDefaultsAndLeavesOtherValuesUntouched() {
        Object expanded = EnvironmentExpander.expand(
                Map.of("password", "${PASSWORD}", "values", List.of(
                        "${var:MISSING:fallback}", "literal", 42)),
                Map.of("PASSWORD", "secret"));

        assertThat(expanded).isEqualTo(Map.of(
                "password", "secret", "values", List.of("fallback", "literal", 42)));
    }

    @Test
    void restoresReferencesAcrossNestedValuesWithoutChangingLiteralOutput() {
        Map<String, Object> original = Map.of(
                "nested", List.of(Map.of("host", "${MYSQL_HOST}"), "literal"),
                "port", 3306);
        Object expanded = EnvironmentExpander.expand(original, Map.of("MYSQL_HOST", "expanded-host"));

        assertThat(EnvironmentExpander.containsReference(original)).isTrue();
        assertThat(EnvironmentExpander.containsReference(List.of("literal", 42))).isFalse();
        assertThat(EnvironmentExpander.restoreReferences(expanded, original)).isEqualTo(original);
        assertThat(EnvironmentExpander.restoreReferences("expanded", "literal"))
                .isEqualTo("expanded");
        assertThat(EnvironmentExpander.restoreReferences(42, null)).isEqualTo(42);
    }

    @Test
    void missingEnvironmentReferenceIsAStableCodedFailure() {
        assertThatThrownBy(() -> EnvironmentExpander.expand("${MISSING}", Map.of()))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("TS0260002");
    }
}
