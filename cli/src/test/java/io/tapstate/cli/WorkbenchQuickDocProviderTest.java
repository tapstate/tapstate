package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchQuickDocProviderTest {

    private final WorkbenchQuickDocProvider provider = WorkbenchQuickDocProvider.BUNDLED;

    @Test
    void mapsTheSelectedYamlFieldToBundledSchemaDocumentation() {
        assertThat(provider.document("kind: source\nid: orders\n", 1))
                .hasValueSatisfying(doc -> {
                    assertThat(doc.title()).startsWith("source.id");
                    assertThat(doc.validationError()).isEmpty();
                });
    }

    @Test
    void leavesAnUnknownFreeformConfigPathUndocumented() {
        assertThat(provider.document("kind: source\nconfig:\n  custom: value\n", 2)).isEmpty();
    }

    @Test
    void givesTheCurrentLineValidationFailurePriorityOverSchemaDocumentation() {
        assertThat(provider.document("kind: source\nunknown: value\n", 1))
                .hasValueSatisfying(doc -> {
                    assertThat(doc.validationError()).contains("dsl.unknown-field");
                    assertThat(doc.entries()).contains("Validation error: dsl.unknown-field");
                });
    }
}
