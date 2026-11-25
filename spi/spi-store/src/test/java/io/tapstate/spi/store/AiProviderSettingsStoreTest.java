package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AiProviderSettingsStoreTest {
    @Test void providerVocabularyIsSharedAndRejectsUnknownIds() {
        assertThat(AiProviderSettingsStore.supportsProvider("openai-compatible")).isTrue();
        assertThat(AiProviderSettingsStore.supportsProvider("openai-responses")).isTrue();
        assertThat(AiProviderSettingsStore.supportsProvider("bedrock")).isFalse();
        assertThat(AiProviderSettingsStore.supportsProvider(null)).isFalse();
    }
    @Test void settingsDiagnosticStringNeverContainsProviderCredentials() {
        var settings = new AiProviderSettingsStore.Settings(
                "openai-compatible", "https://relay.example.com/v1", "model", "sk-private-sentinel");

        assertThat(settings.toString()).isEqualTo("AiProviderSettings[redacted]")
                .doesNotContain("sk-private-sentinel", "relay.example.com");
    }
}
