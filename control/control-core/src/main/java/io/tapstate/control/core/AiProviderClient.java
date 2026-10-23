package io.tapstate.control.core;

import io.tapstate.spi.store.AiProviderSettingsStore.Settings;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/** Provider protocol boundary shared by connection checks and transient Builder proposals. */
public interface AiProviderClient {
    void check(Settings settings);

    Optional<Map<String, Object>> propose(Settings settings, List<Map<String, String>> messages,
            Map<String, Object> context, Consumer<String> text);
}
