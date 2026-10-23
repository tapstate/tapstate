package io.tapstate.control.core;

import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.spi.store.AiProviderSettingsStore.Settings;
import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Optional;

/** Settings use a trusted workspace scope supplied by the authenticated application boundary. */
public final class AiProviderService {
    private final AiProviderSettingsStore store;
    private final AuditGate audit;
    public AiProviderService(AiProviderSettingsStore store, AuditGate audit) {
        this.store = java.util.Objects.requireNonNull(store);
        this.audit = java.util.Objects.requireNonNull(audit);
    }
    public Optional<Settings> get(String caller) { return store.get(caller); }
    public Settings require(String caller) {
        return validated(get(caller).orElseThrow(() -> error(ControlError.AI_NOT_CONFIGURED)));
    }
    public Settings resolve(String scope, Settings settings) {
        if (settings.apiKey() == null || settings.apiKey().isBlank()) {
            Settings saved = require(scope);
            if (!java.util.Objects.equals(settings.providerId(), saved.providerId())
                    || !OpenAiCompatibleEndpointPolicy.normalizedBaseUrl(settings.baseUrl()).equals(saved.baseUrl())) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "a new API key is required when changing the provider endpoint"), null);
            }
            settings = new Settings(settings.providerId(), settings.baseUrl(), settings.model(), saved.apiKey());
        }
        return validated(settings);
    }
    public void put(String caller, String actor, Settings settings) {
        Settings checked = resolve(caller, settings);
        audit.dispatch(ControlOperations.AI_PROVIDER_REPLACE, new AuditContext(actor, "ai-provider-settings"),
                () -> { store.put(caller, checked); return null; });
    }
    public void delete(String caller, String actor) {
        audit.dispatch(ControlOperations.AI_PROVIDER_DELETE, new AuditContext(actor, "ai-provider-settings"),
                () -> { store.delete(caller); return null; });
    }
    public static Settings validated(Settings settings) {
        String base = OpenAiCompatibleEndpointPolicy.normalizedBaseUrl(settings.baseUrl());
        if (!"openai-compatible".equals(settings.providerId())
                || settings.model() == null || settings.model().isBlank() || settings.model().length() > 200
                || settings.apiKey() == null || settings.apiKey().isBlank() || settings.apiKey().length() > 8192
                || settings.apiKey().chars().anyMatch(c -> c < 32 || c == 127)) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "provider settings require a supported HTTPS API endpoint, model and API key"), null);
        }
        return new Settings("openai-compatible", base, settings.model(), settings.apiKey());
    }
    public static TapstateException error(ControlError error) { return new TapstateException(error, Map.of(), null); }
}
