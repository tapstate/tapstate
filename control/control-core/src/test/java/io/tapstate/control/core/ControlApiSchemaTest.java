package io.tapstate.control.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ControlApiSchemaTest {

    private static final Set<String> MCP_OPERATIONS = Set.of(
            "system.version",
            "connector.list", "connector.get",
            "source.draft",
            "connection.test", "connection.test-result", "connection.discover-schema", "connection.schema",
            "artifact.validate", "artifact.apply", "artifact.delete", "artifact.get",
            "pipeline.start", "pipeline.stop", "pipeline.pause", "pipeline.resume",
            "pipeline.status", "pipeline.metrics",
            "pipeline.snapshot", "pipeline.logs",
            "data-browser.collections", "data-browser.find", "data-browser.stats");

    @Test
    void mcpSurfaceIsTheOnlineAuthoringClosureAndTheReadFace() {
        Set<String> actual = ControlOperations.registry().exposedOn(Frontend.MCP).stream()
                .map(Operation::id)
                .collect(Collectors.toSet());

        assertThat(actual).isEqualTo(MCP_OPERATIONS);
    }

    /**
     * A draft may carry the same per-resource precondition the removal takes, and the schema has to say
     * so. This is not decoration: the draft object is {@code additionalProperties: false}, so a field the
     * schema does not declare is one a schema-checking caller is told never to send — the server would
     * accept it and no caller would ever offer it. It stays optional, because a draft without one is the
     * behaviour every existing caller already has.
     */
    @Test
    void anApplyDraftMayDeclareThePreconditionItIsEditingAgainst() {
        Map<?, ?> draft = applyDraftSchema();

        Map<?, ?> properties = (Map<?, ?>) draft.get("properties");
        assertThat(properties.keySet().stream().map(String::valueOf).toList())
                .contains("expectedContentHash");
        assertThat(draft.get("additionalProperties")).isEqualTo(false);
        List<?> required = (List<?>) draft.get("required");
        assertThat(required.stream().map(String::valueOf).toList())
                .as("a draft without a precondition keeps today's behaviour")
                // The list is pinned as non-empty by naming what does stay required: on an empty one
                // the absence below holds vacuously, so a schema that lost its required fields
                // altogether would read as proof that the precondition is optional.
                .contains("content")
                .doesNotContain("expectedContentHash");
    }

    private static Map<?, ?> applyDraftSchema() {
        Map<?, ?> request = ControlApiSchema.resolve(
                ControlOperations.registry().resolve("artifact.apply").schema().params());
        Map<?, ?> drafts = (Map<?, ?>) ((Map<?, ?>) request.get("properties")).get("drafts");
        return (Map<?, ?>) drafts.get("items");
    }

    @Test
    void everyMcpOperationHasResolvableRequestAndResultSchemaRefs() {
        Map<String, Object> document = ControlApiSchema.document();
        assertThat(document).containsEntry("$schema", "https://json-schema.org/draft/2020-12/schema");
        assertThat(document.get("$defs")).isInstanceOf(Map.class);
        Map<?, ?> definitions = (Map<?, ?>) document.get("$defs");

        for (Operation operation : ControlOperations.registry().exposedOn(Frontend.MCP)) {
            assertThat(operation.description()).as(operation.id() + " description").isNotBlank();
            assertThat(operation.schema()).as(operation.id()).isNotNull();
            assertThat(operation.schema().params()).as(operation.id() + " params").startsWith("#/$defs/");
            assertThat(operation.schema().result()).as(operation.id() + " result").startsWith("#/$defs/");
            assertThat(definitions.containsKey(
                    operation.schema().params().substring("#/$defs/".length()))).isTrue();
            assertThat(definitions.containsKey(
                    operation.schema().result().substring("#/$defs/".length()))).isTrue();
        }
    }

    /**
     * The delete tool's argument names are published the moment the tool is, and a remote model calls it
     * by those names alone. Both are required: an id with no precondition would let a caller discard a
     * version it never read, which is the one thing the conditional delete exists to prevent, and an
     * optional precondition is indistinguishable from none for a model that omits what it may omit.
     */
    @Test
    void artifactDeleteRequiresBothTheIdAndThePreconditionItWillBeCalledWith() {
        Map<?, ?> definitions = (Map<?, ?>) ControlApiSchema.document().get("$defs");
        Map<?, ?> request = (Map<?, ?>) definitions.get("ArtifactDeleteRequest");
        Map<?, ?> properties = (Map<?, ?>) request.get("properties");

        assertThat(properties.keySet().stream().map(String::valueOf).toList())
                .containsExactlyInAnyOrder("id", "expectedContentHash");
        assertThat(request.get("required")).isEqualTo(java.util.List.of("id", "expectedContentHash"));
        assertThat(request.get("additionalProperties"))
                .as("an unknown argument must be refused, not silently dropped")
                .isEqualTo(false);
    }

    @Test
    void sourceDraftDescriptionMakesTheServerOwnTheLiveConnectorContract() {
        assertThat(ControlOperations.SOURCE_DRAFT.description())
                .contains("known connector", "live connector contract", "canonical YAML");
    }

    @Test
    void sourceDraftLeavesOnlyConnectorConfigOpenForTheLiveContract() {
        Map<?, ?> definitions = (Map<?, ?>) ControlApiSchema.document().get("$defs");
        Map<?, ?> request = (Map<?, ?>) definitions.get("SourceDraftRequest");
        Map<?, ?> properties = (Map<?, ?>) request.get("properties");
        Map<?, ?> config = (Map<?, ?>) properties.get("config");

        assertThat(request.get("additionalProperties")).isEqualTo(false);
        assertThat(request.get("required")).isEqualTo(java.util.List.of("id", "connector", "config"));
        assertThat(properties.keySet().stream().map(String::valueOf).toList()).containsExactlyInAnyOrder(
                "id", "metadata", "connector", "config", "mode", "tables",
                "options", "srs", "experimental", "clearSecrets");
        assertThat(config.get("type")).isEqualTo("object");
        assertThat(config.get("additionalProperties")).isEqualTo(true);
        assertThat(definitions.get("SourceDraftResult")).isEqualTo(Map.of(
                "type", "object",
                "properties", Map.of("yaml", Map.of(
                        "type", "string", "minLength", 1,
                        "description", "Canonical tapstate/v1 Source YAML")),
                "additionalProperties", false,
                "required", java.util.List.of("yaml")));
    }

    @Test
    void sourceDraftSchemaConstrainsNestedFieldsButKeepsExtensionMapsOpen() {
        Map<?, ?> definitions = (Map<?, ?>) ControlApiSchema.document().get("$defs");
        Map<?, ?> request = (Map<?, ?>) definitions.get("SourceDraftRequest");
        Map<?, ?> properties = (Map<?, ?>) request.get("properties");

        Map<?, ?> metadata = (Map<?, ?>) properties.get("metadata");
        assertThat(metadata.get("additionalProperties")).isEqualTo(false);
        assertThat(((Map<?, ?>) metadata.get("properties")).keySet().stream().map(String::valueOf).toList())
                .containsExactlyInAnyOrder("labels", "description");

        Map<?, ?> table = (Map<?, ?>) ((Map<?, ?>) properties.get("tables")).get("items");
        assertThat(((Map<?, ?>) ((Map<?, ?>) table.get("properties")).get("type")).get("enum"))
                .isEqualTo(java.util.List.of("literal", "regex", "spec"));

        Map<?, ?> srs = (Map<?, ?>) properties.get("srs");
        assertThat(((Map<?, ?>) ((Map<?, ?>) srs.get("properties")).get("schemaEvolution")).get("enum"))
                .isEqualTo(java.util.List.of("track", "ignore"));
        assertThat(((Map<?, ?>) ((Map<?, ?>) srs.get("properties")).get("queryable")).get("type"))
                .isEqualTo("boolean");
        assertThat(((Map<?, ?>) properties.get("options")).get("additionalProperties"))
                .isEqualTo(true);
    }

    /**
     * The schema is where a caller picks the field it will match on, which makes it the one place the
     * choice can be priced before it is made. A name holding a dot is already described here, because
     * addressing it needs a spelling nobody guesses; what the description did not say is that reading on
     * it is the expensive spelling too. An index is written the same way a path is, so such a name cannot
     * have one, and every read matching on it walks the collection. That cost is invisible in the answer
     * - the rows are correct - so a caller told how to address the field and not what it costs will reach
     * for it exactly as readily as for an indexed one.
     */
    @Test
    void theFilterFieldSaysWhatMatchingOnANameHoldingADotCosts() {
        Map<?, ?> definitions = (Map<?, ?>) ControlApiSchema.document().get("$defs");
        Map<?, ?> request = (Map<?, ?>) definitions.get("DataBrowserFindRequest");
        Map<?, ?> filter = (Map<?, ?>) ((Map<?, ?>) request.get("properties")).get("filter");
        Map<?, ?> field = (Map<?, ?>) ((Map<?, ?>) filter.get("properties")).get("field");

        assertThat(String.valueOf(field.get("description")))
                .as("that matching on such a field reads the whole collection")
                .contains("every row")
                .as("and why, so it reads as a property of the name rather than of this one read")
                .contains("index");
    }
}
