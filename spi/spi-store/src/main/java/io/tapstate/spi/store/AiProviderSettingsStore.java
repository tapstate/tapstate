package io.tapstate.spi.store;

import java.util.Optional;

/** Owner-scoped provider credentials; implementations encrypt secrets before durable writes. */
public interface AiProviderSettingsStore {
    String ON_PREM_OWNER = "[\"on-prem\"]";

    static boolean supportsProvider(String provider) {
        return "openai-compatible".equals(provider) || "openai-responses".equals(provider);
    }
    Optional<Settings> get(String owner);
    void put(String owner, Settings settings);
    void delete(String owner);

    /** Changes the active route without rewriting the saved provider or its credential. */
    default boolean selectManaged(String owner, boolean selected) {
        var saved = get(owner);
        saved.ifPresent(value -> put(owner, new Settings(value.providerId(), value.baseUrl(),
                value.model(), value.apiKey(), selected)));
        return saved.isPresent();
    }

    record Settings(String providerId, String baseUrl, String model, String apiKey, boolean managedSelected) {
        public Settings(String providerId, String baseUrl, String model, String apiKey) {
            this(providerId, baseUrl, model, apiKey, false);
        }
        @Override public String toString() { return "AiProviderSettings[redacted]"; }
    }
}
