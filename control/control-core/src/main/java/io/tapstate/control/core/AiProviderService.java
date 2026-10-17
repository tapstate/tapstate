package io.tapstate.control.core;

import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.core.common.TapstateException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Settings use a trusted workspace scope supplied by the authenticated application boundary. */
public final class AiProviderService {
    private static final Set<String> ENDPOINTS = Set.of(
            "https://api.openai.com/v1", "https://api.deepseek.com/v1", "https://api.deepseek.com",
            "https://api.groq.com/openai/v1", "https://api.mistral.ai/v1", "https://api.x.ai/v1",
            "https://generativelanguage.googleapis.com/v1beta/openai",
            "https://api.moonshot.ai/v1", "https://dashscope.aliyuncs.com/compatible-mode/v1");
    private final AiProviderSettingsStore store;
    private final AuditGate audit;
    public AiProviderService(AiProviderSettingsStore store, AuditGate audit) {
        this.store = java.util.Objects.requireNonNull(store);
        this.audit = java.util.Objects.requireNonNull(audit);
    }
    public Optional<AiProviderSettings> get(String caller) { return store.get(caller).map(value -> new AiProviderSettings(value.providerId(), value.baseUrl(), value.model(), value.apiKey())); }
    public AiProviderSettings require(String caller) {
        return validated(get(caller).orElseThrow(() -> error(ControlError.AI_NOT_CONFIGURED)));
    }
    public AiProviderSettings resolve(String scope, AiProviderSettings settings) {
        if (settings.apiKey() == null || settings.apiKey().isBlank()) {
            AiProviderSettings saved = require(scope);
            if (!java.util.Objects.equals(settings.providerId(), saved.providerId())
                    || !normalizedBaseUrl(settings.baseUrl()).equals(saved.baseUrl())) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "a new API key is required when changing the provider endpoint"), null);
            }
            settings = new AiProviderSettings(settings.providerId(), settings.baseUrl(), settings.model(), saved.apiKey());
        }
        return validated(settings);
    }
    public void put(String caller, String actor, AiProviderSettings settings) {
        AiProviderSettings checked = resolve(caller, settings);
        audit.dispatch(ControlOperations.AI_PROVIDER_REPLACE, new AuditContext(actor, "ai-provider-settings"),
                () -> { store.put(caller, new AiProviderSettingsStore.Settings(checked.providerId(), checked.baseUrl(), checked.model(), checked.apiKey())); return null; });
    }
    public void delete(String caller, String actor) {
        audit.dispatch(ControlOperations.AI_PROVIDER_DELETE, new AuditContext(actor, "ai-provider-settings"),
                () -> { store.delete(caller); return null; });
    }
    public static AiProviderSettings validated(AiProviderSettings settings) {
        String base = normalizedBaseUrl(settings.baseUrl());
        if (!"openai-compatible".equals(settings.providerId()) || !ENDPOINTS.contains(base)
                || settings.model() == null || settings.model().isBlank() || settings.model().length() > 200
                || settings.apiKey() == null || settings.apiKey().isBlank() || settings.apiKey().length() > 8192
                || settings.apiKey().chars().anyMatch(c -> c < 32 || c == 127)) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "provider settings require a supported HTTPS API endpoint, model and API key"), null);
        }
        URI.create(base);
        return new AiProviderSettings("openai-compatible", base, settings.model(), settings.apiKey());
    }
    private static String normalizedBaseUrl(String baseUrl) {
        return baseUrl == null || baseUrl.isBlank()
                ? "https://api.openai.com/v1" : baseUrl.replaceAll("/+$", "");
    }
    public static TapstateException error(ControlError error) { return new TapstateException(error, Map.of(), null); }
}
