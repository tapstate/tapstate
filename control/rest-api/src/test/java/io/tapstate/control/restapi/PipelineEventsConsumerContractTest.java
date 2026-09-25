package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlApiSchema;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Consumer examples for the prepared event contract, before the event operation is exposed. */
class PipelineEventsConsumerContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> FIXTURES = List.of(
            "events-failure-recovery.golden.json",
            "events-known-gap.golden.json",
            "events-empty.golden.json");

    @Test
    void everyEventExampleMatchesTheClosedResultSchema() throws Exception {
        Map<?, ?> definitions = (Map<?, ?>) ControlApiSchema.document().get("$defs");
        Map<?, ?> result = (Map<?, ?>) definitions.get("PipelineEventsResult");

        for (String fixture : FIXTURES) {
            Map<?, ?> page = JSON.readValue(golden(fixture), Map.class);
            conformsTo(result, page, fixture);
            assertThat(page.get("completeness")).isEqualTo("BEST_EFFORT");
            assertThat(page.get("nextCursor")).isNull();
            assertThat(page.get("pipelineId")).isEqualTo("orders");
            assertThat(page.get("knownGaps")).isInstanceOf(List.class);
        }
    }

    @Test
    void examplesKeepFailureRecoveryPageGapsAndEmptyHistoryDistinct() throws Exception {
        Map<?, ?> failure = JSON.readValue(golden(FIXTURES.get(0)), Map.class);
        List<?> failureEvents = (List<?>) failure.get("events");
        assertThat(failureEvents).hasSize(2);
        assertThat(((Map<?, ?>) failureEvents.get(0)).get("kind")).isEqualTo("FAILURE");
        assertThat(((Map<?, ?>) failureEvents.get(1)).get("kind")).isEqualTo("EXECUTION_RECOVERED");
        assertThat(failure.get("knownGaps")).isEqualTo(List.of());

        Map<?, ?> gap = JSON.readValue(golden(FIXTURES.get(1)), Map.class);
        List<?> gapEvents = (List<?>) gap.get("events");
        List<?> knownGaps = (List<?>) gap.get("knownGaps");
        assertThat(gapEvents).hasSize(1);
        assertThat(knownGaps).hasSize(1);
        assertThat(((Map<?, ?>) gapEvents.get(0)).get("kind")).isEqualTo("TELEMETRY_GAP");
        assertThat(((Map<?, ?>) knownGaps.get(0)).get("eventId"))
                .isEqualTo(((Map<?, ?>) gapEvents.get(0)).get("id"));
        assertThat(((Map<?, ?>) knownGaps.get(0)).get("reasons"))
                .isEqualTo(List.of("QUEUE_FULL", "WRITE_FAILURE"));

        Map<?, ?> empty = JSON.readValue(golden(FIXTURES.get(2)), Map.class);
        assertThat(empty.get("events")).isEqualTo(List.of());
        assertThat(empty.get("knownGaps")).isEqualTo(List.of());
        assertThat(empty.get("completeness")).isEqualTo("BEST_EFFORT");
    }

    private static String golden(String name) throws IOException {
        try (var input = PipelineEventsConsumerContractTest.class.getResourceAsStream(
                "/golden/observability-events/" + name)) {
            if (input == null) {
                throw new IOException("missing event consumer example: " + name);
            }
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(text).doesNotContain("\r").endsWith("\n");
            return text;
        }
    }

    /** Check the JSON Schema features used by this contract without adding a validator dependency. */
    private static void conformsTo(Map<?, ?> schema, Object value, String path) {
        if (schema.get("oneOf") instanceof List<?> alternatives) {
            List<?> matching = alternatives.stream()
                    .filter(candidate -> matchesType((Map<?, ?>) candidate, value))
                    .toList();
            assertThat(matching).as(path + " matches exactly one schema branch").hasSize(1);
            conformsTo((Map<?, ?>) matching.get(0), value, path);
            return;
        }
        if (schema.get("enum") instanceof List<?> values) {
            assertThat(values.contains(value)).as(path + " uses a declared enum value").isTrue();
        }
        Object type = schema.get("type");
        if (type == null) {
            return;
        }
        switch ((String) type) {
            case "object" -> {
                assertThat(value).as(path).isInstanceOf(Map.class);
                Map<?, ?> object = (Map<?, ?>) value;
                Map<?, ?> properties = (Map<?, ?>) schema.get("properties");
                if (schema.get("required") instanceof List<?> required) {
                    for (Object name : required) {
                        assertThat(object.containsKey(name)).as(path + "." + name + " is required").isTrue();
                    }
                }
                for (Map.Entry<?, ?> entry : object.entrySet()) {
                    Object propertySchema = properties == null ? null : properties.get(entry.getKey());
                    if (propertySchema instanceof Map<?, ?> nested) {
                        conformsTo(nested, entry.getValue(), path + "." + entry.getKey());
                    } else if (schema.get("additionalProperties") instanceof Map<?, ?> additional) {
                        conformsTo(additional, entry.getValue(), path + "." + entry.getKey());
                    } else {
                        assertThat(schema.get("additionalProperties"))
                                .as(path + "." + entry.getKey() + " is an allowed field")
                                .isEqualTo(true);
                    }
                }
            }
            case "array" -> {
                assertThat(value).as(path).isInstanceOf(List.class);
                List<?> items = (List<?>) value;
                if (schema.get("maxItems") instanceof Number max) {
                    assertThat(items.size()).as(path + " item count").isLessThanOrEqualTo(max.intValue());
                }
                if (Boolean.TRUE.equals(schema.get("uniqueItems"))) {
                    assertThat(items).as(path + " distinct items").doesNotHaveDuplicates();
                }
                for (int index = 0; index < items.size(); index++) {
                    conformsTo((Map<?, ?>) schema.get("items"), items.get(index), path + "[" + index + "]");
                }
            }
            case "string" -> {
                assertThat(value).as(path).isInstanceOf(String.class);
                String text = (String) value;
                if (schema.get("minLength") instanceof Number min) {
                    assertThat(text.length()).as(path + " length").isGreaterThanOrEqualTo(min.intValue());
                }
                if ("date-time".equals(schema.get("format"))) {
                    OffsetDateTime.parse(text);
                }
                if (schema.get("pattern") instanceof String pattern) {
                    assertThat(Pattern.compile(pattern).matcher(text).find()).as(path + " pattern").isTrue();
                }
            }
            case "null" -> assertThat(value).as(path).isNull();
            default -> throw new IllegalStateException("unsupported schema type in event contract: " + type);
        }
    }

    private static boolean matchesType(Map<?, ?> schema, Object value) {
        return switch ((String) schema.get("type")) {
            case "null" -> value == null;
            case "string" -> value instanceof String;
            default -> false;
        };
    }
}
