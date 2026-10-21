package io.tapstate.control.core;

import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.spi.store.AiProviderSettingsStore.Settings;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class AiProviderServiceTest {
    @Test void acceptsTheConfiguredOpenAiCompatibleEndpoint() {
        for (String endpoint : List.of("https://api.super-nb.me", "https://api.super-nb.me/v1")) {
            assertThat(AiProviderService.validated(
                    new AiProviderSettings("openai-compatible", endpoint, "model", "sk-test")).baseUrl())
                    .isEqualTo(endpoint);
        }
    }

    @Test void endpointAllowlistRejectsSsrfAndHeaderInjectionWithoutEchoingSecrets() {
        for (String endpoint : List.of("http://localhost/v1", "https://127.0.0.1/v1", "https://169.254.169.254/v1",
                "https://api.openai.com.evil.example/v1", "https://api.openai.com@evil.example/v1", "https://api.openai.com/v1?secret=sk-test")) {
            assertThatThrownBy(() -> AiProviderService.validated(new AiProviderSettings("openai-compatible", endpoint, "model", "sk-test")))
                    .isInstanceOf(TapstateException.class).hasMessageNotContaining("sk-test");
        }
        assertThatThrownBy(() -> AiProviderService.validated(new AiProviderSettings("openai-compatible", null, "model", "key\r\nx: y")))
                .isInstanceOf(TapstateException.class);
        assertThat(AiProviderService.validated(new AiProviderSettings("openai-compatible", null, "model", "sk-test")).baseUrl())
                .isEqualTo("https://api.openai.com/v1");
    }
    @Test void storageIdentityIsSeparateFromAuditActorAndHasNoCredentialDiagnostics() {
        Map<String, Settings> values = new HashMap<>();
        AiProviderSettingsStore store = new AiProviderSettingsStore() {
            public Optional<Settings> get(String owner) { return Optional.ofNullable(values.get(owner)); }
            public void put(String owner, Settings settings) { values.put(owner, settings); }
            public void delete(String owner) { values.remove(owner); }
        };
        List<AuditRecord> events = new ArrayList<>();
        AuditStore audit = new AuditStore() {
            public void record(AuditRecord record) { events.add(record); }
            public List<AuditRecord> query(Instant since, int limit) { return events; }
        };
        var service = new AiProviderService(store, new AuditGate(audit, Clock.systemUTC()));
        AiProviderSettings secret = new AiProviderSettings("openai-compatible", null, "model", "sk-secret");
        service.put("org-a/cluster-a", "user-a", secret);
        assertThat(service.get("org-b/cluster-b")).isEmpty();
        assertThat(service.require("org-a/cluster-a").apiKey()).isEqualTo("sk-secret");
        assertThat(secret.toString()).doesNotContain("sk-secret");
        assertThat(events.toString()).contains("user-a").doesNotContain("sk-secret");
        service.put("org-a/cluster-a", "user-b", new AiProviderSettings("openai-compatible", null, "updated-model", null));
        assertThat(service.require("org-a/cluster-a").model()).isEqualTo("updated-model");
        assertThat(service.resolve("org-a/cluster-a", new AiProviderSettings("openai-compatible", null, "test-model", null)).apiKey()).isEqualTo("sk-secret");
        assertThatThrownBy(() -> service.resolve("org-a/cluster-a",
                new AiProviderSettings("openai-compatible", "https://api.deepseek.com/v1", "test-model", null)))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(ControlError.MALFORMED_REQUEST);
                    assertThat(failure).hasMessageNotContaining("sk-secret");
                });
        service.delete("org-a/cluster-a", "user-b");
        assertThat(service.get("org-a/cluster-a")).isEmpty();
    }
}
