package io.tapstate.cli;

import dev.tamboui.style.Color;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchThemeTest {

    @Test
    void resolvesDarkAndLightSemanticTokensFromPackagedStylesheets() {
        WorkbenchTheme dark = WorkbenchTheme.load(WorkbenchTheme.Mode.DARK);
        WorkbenchTheme light = WorkbenchTheme.load(WorkbenchTheme.Mode.LIGHT);

        assertThat(dark.fallbackUsed()).isFalse();
        assertThat(light.fallbackUsed()).isFalse();
        assertThat(dark.accent().fg()).contains(Color.hex("#4FACBC"));
        assertThat(light.accent().fg()).contains(Color.hex("#2A7481"));
        assertThat(dark.base().bg()).contains(Color.hex("#1E1E1E"));
        assertThat(light.base().bg()).contains(Color.hex("#F7FAFA"));
        assertThat(dark.success().fg()).isNotEqualTo(dark.accent().fg());
        assertThat(light.error().fg()).isNotEqualTo(light.accent().fg());
    }

    @Test
    void missingOrMalformedStylesheetFallsBackWithoutLosingSemanticStyles() {
        WorkbenchTheme missing = WorkbenchTheme.load(
                WorkbenchTheme.Mode.DARK,
                path -> {
                    throw new IOException("missing");
                });
        WorkbenchTheme malformed = WorkbenchTheme.load(
                WorkbenchTheme.Mode.LIGHT,
                path -> "#accent { definitely-not-css }");

        assertThat(missing.fallbackUsed()).isTrue();
        assertThat(malformed.fallbackUsed()).isTrue();
        assertThat(missing.accent().fg()).contains(Color.hex("#4FACBC"));
        assertThat(malformed.accent().fg()).contains(Color.hex("#2A7481"));
        assertThat(missing.selection().effectiveModifiers()).isNotEmpty();
        assertThat(malformed.muted().fg()).isPresent();
    }
}
