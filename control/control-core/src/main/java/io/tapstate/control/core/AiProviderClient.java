package io.tapstate.control.core;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/** Provider protocol boundary shared by connection checks and transient Builder proposals. */
public interface AiProviderClient {
    void check(AiProviderSettings settings);

    default List<String> models(AiProviderSettings settings) {
        throw AiProviderService.error(ControlError.AI_PROVIDER_FAILED);
    }

    Optional<Map<String, Object>> propose(AiProviderSettings settings, List<Map<String, String>> messages,
            Map<String, Object> context, Consumer<String> text);

    default Optional<Map<String, Object>> propose(AiProviderSettings settings, List<Map<String, String>> messages,
            Map<String, Object> context, Output output) {
        return propose(settings, messages, context, (Consumer<String>) output);
    }

    interface Output extends Consumer<String> {
        default void reasoning(Reasoning part) { }
    }

    record Reasoning(String id, Phase phase, String text) {
        public enum Phase { START, DELTA, END }
    }
}
