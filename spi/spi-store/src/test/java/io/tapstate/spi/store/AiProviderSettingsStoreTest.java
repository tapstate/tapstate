package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AiProviderSettingsStoreTest {
    @Test void settingsDiagnosticStringNeverContainsProviderCredentials() {
        var settings = new AiProviderSettingsStore.Settings(
                "openai-compatible", "https://relay.example.com/v1", "model", "sk-private-sentinel");

        assertThat(settings.toString()).isEqualTo("AiProviderSettings[redacted]")
                .doesNotContain("sk-private-sentinel", "relay.example.com");
    }
}
