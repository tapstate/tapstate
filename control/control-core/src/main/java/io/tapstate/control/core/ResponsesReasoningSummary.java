package io.tapstate.control.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** Translates public summary parts only; reasoning content never grants proposal authority. */
final class ResponsesReasoningSummary {
    private static final int MAX_PARTS = 128;
    private final Map<Key, Part> parts = new LinkedHashMap<>();
    private final Consumer<AiProviderClient.Reasoning> output;

    ResponsesReasoningSummary(Consumer<AiProviderClient.Reasoning> output) { this.output = output; }

    void accept(Map<String, Object> event) {
        String type = string(event.get("type"));
        switch (type) {
            case "response.reasoning_summary_part.added" -> part(event).append(summary(event.get("part")));
            case "response.reasoning_summary_text.delta" -> part(event).append(string(event.get("delta")));
            case "response.reasoning_summary_text.done" -> part(event).complete(string(event.get("text")));
            case "response.reasoning_summary_part.done" -> part(event).complete(summary(event.get("part")));
            case "response.output_item.done" -> completeItem(object(event.get("item")));
            default -> { /* Other event families are handled by the proposal translator. */ }
        }
    }

    void complete(List<?> items) {
        for (Object item : items) completeItem(object(item));
        for (Part part : parts.values()) part.end();
    }

    private void completeItem(Map<?, ?> item) {
        if (!"reasoning".equals(item.get("type"))) return;
        String source = string(item.get("id"));
        if (!(item.get("summary") instanceof List<?> summaries)) throw invalid();
        for (int index = 0; index < summaries.size(); index++) {
            part(new Key(source, index)).complete(summary(summaries.get(index)));
        }
    }

    private Part part(Map<String, Object> event) {
        String source = string(event.get("item_id"));
        if (!(event.get("summary_index") instanceof Number index)) throw invalid();
        try {
            int value = new java.math.BigDecimal(index.toString()).intValueExact();
            if (value < 0) throw invalid();
            return part(new Key(source, value));
        } catch (NumberFormatException | ArithmeticException malformed) { throw invalid(); }
    }

    private Part part(Key key) {
        Part existing = parts.get(key);
        if (existing != null) return existing;
        if (parts.size() >= MAX_PARTS) throw invalid();
        Part created = new Part("reasoning-" + parts.size());
        parts.put(key, created);
        output.accept(new AiProviderClient.Reasoning(created.id, AiProviderClient.Reasoning.Phase.START, ""));
        return created;
    }

    private record Key(String source, int index) { }

    private final class Part {
        private final String id;
        private final StringBuilder text = new StringBuilder();
        private boolean ended;

        private Part(String id) { this.id = id; }

        private void append(String delta) {
            if (ended) throw invalid();
            if (delta.isEmpty()) return;
            text.append(delta);
            output.accept(new AiProviderClient.Reasoning(id, AiProviderClient.Reasoning.Phase.DELTA, delta));
        }

        private void complete(String value) {
            if (!value.startsWith(text.toString())) throw invalid();
            if (value.length() > text.length()) append(value.substring(text.length()));
            end();
        }

        private void end() {
            if (ended) return;
            ended = true;
            output.accept(new AiProviderClient.Reasoning(id, AiProviderClient.Reasoning.Phase.END, ""));
        }
    }

    private static String summary(Object value) {
        Map<?, ?> part = object(value);
        if (!"summary_text".equals(part.get("type"))) throw invalid();
        return string(part.get("text"));
    }

    private static Map<?, ?> object(Object value) {
        if (!(value instanceof Map<?, ?> result)) throw invalid();
        return result;
    }

    private static String string(Object value) {
        if (!(value instanceof String result)) throw invalid();
        return result;
    }

    private static io.tapstate.core.common.TapstateException invalid() {
        return AiProviderService.error(ControlError.AI_INVALID_PROPOSAL);
    }
}
