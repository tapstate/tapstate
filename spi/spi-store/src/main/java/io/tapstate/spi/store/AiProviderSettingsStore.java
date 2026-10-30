package io.tapstate.spi.store;

import java.util.Optional;

/** Owner-scoped provider credentials; implementations encrypt secrets before durable writes. */
public interface AiProviderSettingsStore {
    Optional<Settings> get(String owner);
    void put(String owner, Settings settings);
    void delete(String owner);

    record Settings(String providerId, String baseUrl, String model, String apiKey) {
        @Override public String toString() { return "AiProviderSettings[redacted]"; }
    }
}
