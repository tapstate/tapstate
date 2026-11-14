package io.tapstate.control.core;

import io.tapstate.spi.store.AiProviderSettingsStore;
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
    public Optional<AiProviderSettings> get(String caller) { return store.get(caller).map(value -> new AiProviderSettings(value.providerId(), value.baseUrl(), value.model(), value.apiKey())); }
    public AiProviderSettings require(String caller) {
        return validated(get(caller).orElseThrow(() -> error(ControlError.AI_NOT_CONFIGURED)));
    }
    public AiProviderSettings resolve(String scope, AiProviderSettings settings) {
        return validated(resolveKey(scope, settings));
    }
    public AiProviderSettings resolveModels(String scope, AiProviderSettings settings) {
        return validated(resolveKey(scope, settings), false);
    }
    private AiProviderSettings resolveKey(String scope, AiProviderSettings settings) {
        if (settings.apiKey() == null || settings.apiKey().isBlank()) {
            AiProviderSettings saved = require(scope);
            if (!OpenAiCompatibleEndpointPolicy.normalizedBaseUrl(settings.baseUrl()).equals(saved.baseUrl())) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "a new API key is required when changing the provider endpoint"), null);
            }
            return new AiProviderSettings(settings.providerId(), settings.baseUrl(), settings.model(), saved.apiKey());
        }
        return settings;
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
        return validated(settings, true);
    }
    private static AiProviderSettings validated(AiProviderSettings settings, boolean requireModel) {
        String base = OpenAiCompatibleEndpointPolicy.normalizedBaseUrl(settings.baseUrl());
        if (!("openai-compatible".equals(settings.providerId()) || "openai-responses".equals(settings.providerId()))
                || requireModel && (settings.model() == null || settings.model().isBlank())
                || settings.model() != null && (settings.model().length() > 200 || settings.model().chars().anyMatch(Character::isISOControl))
                || settings.apiKey() == null || settings.apiKey().isBlank() || settings.apiKey().length() > 8192
                || settings.apiKey().chars().anyMatch(c -> c < 32 || c == 127)) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "provider settings require a supported HTTPS API endpoint, model and API key"), null);
        }
        return new AiProviderSettings(settings.providerId(), base, settings.model(), settings.apiKey());
    }
    public static TapstateException error(ControlError error) { return new TapstateException(error, Map.of(), null); }
}
