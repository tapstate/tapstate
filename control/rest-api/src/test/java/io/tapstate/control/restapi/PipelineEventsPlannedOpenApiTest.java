package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlApiSchema;
import io.tapstate.control.core.ControlOperations;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the static, unexposed HTTP contract against the shared control schema. */
class PipelineEventsPlannedOpenApiTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String RESOURCE =
            "/golden/observability-events/pipeline-events.planned.openapi.json";
    private static final String PATH = "/api/pipelines/{id}/events";

    @Test
    void preparedResponseSchemaIsExactlyTheSharedControlDefinition() throws Exception {
        Map<String, Object> document = contract();
        Map<String, Object> components = object(document.get("components"));
        Map<String, Object> schemas = object(components.get("schemas"));

        assertThat(document.get("openapi")).isEqualTo("3.1.0");
        assertThat(document.get("jsonSchemaDialect"))
                .isEqualTo("https://json-schema.org/draft/2020-12/schema");
        assertThat(schemas.get("PipelineEventsResult"))
                .isEqualTo(ControlApiSchema.resolve("#/$defs/PipelineEventsResult"));
    }

    @Test
    void plannedRouteHasTheBoundQueryBearerReadAndCodedResponses() throws Exception {
        Map<String, Object> document = contract();
        assertThat(document.get("x-tapstate-availability")).isEqualTo("planned-unexposed");
        Map<String, Object> paths = object(document.get("paths"));
        assertThat(paths.keySet()).containsExactly(PATH);
        Map<String, Object> path = object(paths.get(PATH));
        assertThat(path.keySet()).containsExactly("get");
        Map<String, Object> operation = object(path.get("get"));
        assertThat(operation.get("operationId")).isEqualTo("pipeline.events");
        assertThat(operation.get("x-tapstate-availability")).isEqualTo("planned-unexposed");
        assertThat(operation.get("x-tapstate-required-scope")).isEqualTo("READ");
        assertThat(operation.get("security")).isEqualTo(List.of(Map.of("bearerAuth", List.of())));
        Map<String, Object> components = object(document.get("components"));
        Map<String, Object> bearer = object(object(components.get("securitySchemes")).get("bearerAuth"));
        assertThat(bearer).containsEntry("type", "http").containsEntry("scheme", "bearer");
        assertThat(ControlOperations.registry().isRegistered("pipeline.events")).isFalse();

        Map<String, Object> request = ControlApiSchema.resolve("#/$defs/PipelineEventsRequest");
        Map<String, Object> requestProperties = object(request.get("properties"));
        List<?> parameters = (List<?>) operation.get("parameters");
        assertThat(parameters.stream().map(value -> String.valueOf(object(value).get("name"))).toList())
                .containsExactly("id", "from", "to", "limit", "cursor");
        for (Object value : parameters) {
            Map<String, Object> parameter = object(value);
            String name = (String) parameter.get("name");
            assertThat(parameter.get("in")).isEqualTo(name.equals("id") ? "path" : "query");
            assertThat(parameter.get("required")).isEqualTo(List.of("id", "from", "to").contains(name));
            assertThat(parameter.get("schema")).isEqualTo(requestProperties.get(name));
        }
        assertThat((String) operation.get("description"))
                .contains("half open", "15 days", "10 minutes");

        Map<String, Object> responses = object(operation.get("responses"));
        assertThat(responses.keySet()).containsExactlyInAnyOrder("200", "400", "401", "403", "404", "410");
        assertThat(responseSchema(responses, "200"))
                .isEqualTo(Map.of("$ref", "#/components/schemas/PipelineEventsResult"));
        Map<String, Object> noStore = object(object(object(responses.get("200")).get("headers"))
                .get("Cache-Control"));
        assertThat(object(noStore.get("schema")).get("const")).isEqualTo("no-store");
        Map<String, List<String>> codes = Map.of(
                "400", List.of("control.malformed-request", "monitor.invalid-cursor",
                        "monitor.query-budget-exceeded"),
                "401", List.of("control.unauthenticated"),
                "403", List.of("control.forbidden"),
                "404", List.of("lifecycle.unknown-pipeline"),
                "410", List.of("monitor.cursor-expired"));
        codes.forEach((status, expected) -> {
            assertThat(object(responses.get(status)).get("x-tapstate-error-codes")).isEqualTo(expected);
            assertThat(responseSchema(responses, status))
                    .isEqualTo(Map.of("$ref", "#/components/schemas/ErrorResponse"));
        });
    }

    private static Map<String, Object> responseSchema(Map<String, Object> responses, String status) {
        Map<String, Object> response = object(responses.get(status));
        return object(object(object(response.get("content")).get("application/json")).get("schema"));
    }

    private static Map<String, Object> contract() throws IOException {
        try (var input = PipelineEventsPlannedOpenApiTest.class.getResourceAsStream(RESOURCE)) {
            if (input == null) {
                throw new IOException("missing prepared OpenAPI contract: " + RESOURCE);
            }
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(text).doesNotContain("\r").endsWith("\n");
            return object(JSON.readValue(text, Map.class));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        assertThat(value).isInstanceOf(Map.class);
        return (Map<String, Object>) value;
    }
}
