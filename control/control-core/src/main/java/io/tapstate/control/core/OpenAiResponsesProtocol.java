package io.tapstate.control.core;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Stateless Responses wire translation; only completed output can become a Builder proposal. */
final class OpenAiResponsesProtocol {
    private OpenAiResponsesProtocol() { }

    static Map<String, Object> request(Map<String, Object> chat) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("model", chat.get("model"));
        result.put("input", chat.get("messages"));
        result.put("store", false);
        if (chat.containsKey("reasoning")) result.put("reasoning", chat.get("reasoning"));
        result.put("max_output_tokens", chat.getOrDefault("max_completion_tokens", chat.get("max_tokens")));
        if (chat.containsKey("stream")) result.put("stream", chat.get("stream"));
        if (chat.get("tools") instanceof List<?> tools) {
            result.put("tools", tools.stream().map(raw -> {
                Map<String, Object> function = new LinkedHashMap<>(object(object(raw).get("function")));
                function.put("type", "function");
                // Builder schemas include optional fields; do not let Responses normalize them to strict mode.
                function.put("strict", false);
                return function;
            }).toList());
            result.put("parallel_tool_calls", false);
        }
        if (chat.get("tool_choice") instanceof Map<?, ?> choice) {
            result.put("tool_choice", Map.of("type", "function", "name", object(choice.get("function")).get("name")));
        }
        return result;
    }

    static List<?> validateCompleted(Map<String, Object> response) {
        if (!"completed".equals(response.get("status")) || response.get("error") != null
                || !(response.get("output") instanceof List<?> output) || output.isEmpty()) throw invalid();
        return output;
    }

    static final class Stream {
        private final AiProviderClient.Output text;
        private final ResponsesReasoningSummary reasoning;
        private final boolean proposalEnabled;
        private final ToolExplanationStream explanation = new ToolExplanationStream();
        private final StringBuilder arguments = new StringBuilder();
        private String toolId;
        private String channel;
        private boolean textEmitted;
        private boolean argumentsEmitted;
        private Optional<Map<String, Object>> result = Optional.empty();

        Stream(AiProviderClient.Output text, boolean proposalEnabled) {
            this.text = text;
            this.reasoning = new ResponsesReasoningSummary(text::reasoning);
            this.proposalEnabled = proposalEnabled;
        }

        boolean accept(Map<String, Object> event) {
            if (!(event.get("type") instanceof String type)) throw invalid();
            reasoning.accept(event);
            switch (type) {
                case "error", "response.failed", "response.incomplete" -> throw invalid();
                case "response.output_item.added" -> {
                    Map<String, Object> item = object(event.get("item"));
                    if ("function_call".equals(item.get("type"))) {
                        if (!proposalEnabled || toolId != null || !"propose_pipeline".equals(item.get("name"))
                                || !(item.get("id") instanceof String id) || id.isBlank()) throw invalid();
                        toolId = id;
                    }
                }
                case "response.output_text.delta" -> {
                    textEmitted = true;
                    emit(string(event.get("delta")), "text");
                }
                case "response.function_call_arguments.delta" -> {
                    if (toolId == null || !toolId.equals(event.get("item_id"))) throw invalid();
                    String delta = string(event.get("delta"));
                    arguments.append(delta);
                    argumentsEmitted = true;
                    explanation.accept(delta, value -> emit(value, "tool"));
                }
                case "response.completed" -> {
                    complete(object(event.get("response")));
                    return true;
                }
                default -> { /* Lifecycle and summary events carry no independent proposal authority. */ }
            }
            return false;
        }

        private void complete(Map<String, Object> response) {
            List<?> items = validateCompleted(response);
            reasoning.complete(items);
            Map<String, Object> call = null;
            StringBuilder completedText = new StringBuilder();
            for (Object raw : items) {
                Map<String, Object> item = object(raw);
                if ("function_call".equals(item.get("type"))) {
                    if (!proposalEnabled || call != null || !"propose_pipeline".equals(item.get("name"))
                            || item.get("status") != null && !"completed".equals(item.get("status"))) throw invalid();
                    call = item;
                } else if ("message".equals(item.get("type"))) {
                    if (!(item.get("content") instanceof List<?> content)) throw invalid();
                    for (Object part : content) {
                        Map<String, Object> value = object(part);
                        if ("output_text".equals(value.get("type"))) completedText.append(string(value.get("text")));
                    }
                }
            }
            if (call == null) {
                if (toolId != null || completedText.isEmpty()) throw invalid();
            } else {
                if (toolId != null && !toolId.equals(call.get("id"))) throw invalid();
                String completeArguments = string(call.get("arguments"));
                if (argumentsEmitted && !arguments.toString().equals(completeArguments)) throw invalid();
                Map<String, Object> parsed = object(JsonReader.parse(completeArguments));
                result = Optional.of(object(parsed.get("candidateWizard")));
                if (!argumentsEmitted && parsed.get("explanation") instanceof String value) emit(value, "tool");
            }
            if (!textEmitted && !completedText.isEmpty()) emit(completedText.toString(), "text");
        }

        private void emit(String value, String nextChannel) {
            if (value.isEmpty()) return;
            if (channel != null && !channel.equals(nextChannel)) text.accept("\n\n");
            channel = nextChannel;
            text.accept(value);
        }

        Optional<Map<String, Object>> result() { return result; }
    }

    private static String string(Object value) {
        if (!(value instanceof String result)) throw invalid();
        return result;
    }

    private static Map<String, Object> object(Object value) {
        if (!(value instanceof Map<?, ?> source)) throw invalid();
        Map<String, Object> result = new LinkedHashMap<>();
        for (var entry : source.entrySet()) {
            if (!(entry.getKey() instanceof String key)) throw invalid();
            result.put(key, entry.getValue());
        }
        return result;
    }

    private static TapstateException invalid() { return AiProviderService.error(ControlError.AI_INVALID_PROPOSAL); }
}
