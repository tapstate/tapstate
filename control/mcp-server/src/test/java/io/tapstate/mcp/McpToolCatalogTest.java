package io.tapstate.mcp;

import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema;
import io.tapstate.control.client.HttpControlClient;
import io.tapstate.control.core.ControlApiSchema;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.Frontend;
import io.tapstate.control.core.Operation;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class McpToolCatalogTest {

    private static final List<String> READ_TOOLS = List.of(
            "connector_list", "connector_get",
            "source_draft",
            "connection_test_result", "connection_schema", "artifact_validate", "artifact_get",
            "pipeline_status", "pipeline_metrics", "pipeline_snapshot", "pipeline_logs",
            "data_browser_collections", "data_browser_find", "data_browser_stats");

    private static final List<String> WRITE_TOOLS = List.of(
            "artifact_apply", "artifact_delete", "connection_test", "connection_discover_schema",
            "pipeline_start", "pipeline_stop", "pipeline_pause", "pipeline_resume");

    /**
     * The read that supplies the removal's precondition has to be reachable without write access.
     * Landing it in the write bucket would make the hash obtainable only in a session that already
     * holds the power to destroy, which defeats the point of reading before deciding to.
     */
    @Test
    void theReadThatSuppliesTheRemovalPreconditionIsAvailableWithoutWriteAccess() {
        assertThat(McpToolCatalog.operations(false).stream().map(McpToolCatalog::toolName))
                .contains("artifact_get");
    }

    @Test
    void defaultSurfaceContainsExactlyTheFourteenReadTools() {
        assertThat(McpToolCatalog.operations(false).stream().map(McpToolCatalog::toolName))
                .containsExactlyInAnyOrderElementsOf(READ_TOOLS);
    }

    @Test
    void theDataBrowserToolsAppearWithNoMcpCodeOfTheirOwn() {
        // The whole claim of this surface: a verb becomes a tool by being marked on its registry entry,
        // not by anything written here. These three carry no branch, no name and no schema in this
        // module — take the two marks off the entries and all three disappear.
        assertThat(McpToolCatalog.operations(false).stream().map(McpToolCatalog::toolName))
                .contains("data_browser_collections", "data_browser_find", "data_browser_stats");
    }

    @Test
    void tellsACallerWhatAnAbsentFieldListMeansAndHowToGetTheShapeAnyway() {
        // An agent that reads an absent `fields` as "no fields" stops there and reports an empty
        // collection. The schema is where it is told otherwise, and told what to do instead — nothing
        // else in the protocol carries that, and there is no person on this face to infer it.
        Map<String, Object> result = ControlApiSchema.resolve(
                ControlOperations.DATA_BROWSER_COLLECTIONS.schema().result());
        Map<?, ?> entry = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) result.get("properties"))
                .get("collections")).get("items");
        Map<?, ?> fields = (Map<?, ?>) ((Map<?, ?>) entry.get("properties")).get("fields");

        assertThat((String) fields.get("description"))
                .contains("Absent")
                .contains("not the same as")
                .contains("first page");
    }

    @Test
    void onlyTheNameIsPromisedForEveryCollection() {
        // `kind` is required of nothing, and the schema says why: the listing covers a whole database,
        // and answering "view" for a collection nobody declared would tell the caller a pipeline
        // materializes something made by hand. A required `kind` is exactly that claim, in the contract.
        Map<String, Object> result = ControlApiSchema.resolve(
                ControlOperations.DATA_BROWSER_COLLECTIONS.schema().result());
        Map<?, ?> entry = (Map<?, ?>) ((Map<?, ?>) ((Map<?, ?>) result.get("properties"))
                .get("collections")).get("items");

        assertThat(((List<?>) entry.get("required")).stream().map(String::valueOf).toList())
                .containsExactly("name");
        Map<?, ?> kind = (Map<?, ?>) ((Map<?, ?>) entry.get("properties")).get("kind");
        assertThat(((List<?>) kind.get("enum")).stream().map(String::valueOf).toList())
                .containsExactly("view");
        assertThat((String) kind.get("description"))
                .contains("Absent")
                .contains("not made here");
    }

    @Test
    void allowWriteAddsExactlyTheEightWriteTools() {
        assertThat(McpToolCatalog.operations(true).stream().map(McpToolCatalog::toolName))
                .containsExactlyInAnyOrderElementsOf(concat(READ_TOOLS, WRITE_TOOLS));
    }

    /**
     * There has to be a way to make a Pipeline stop moving that does not clear it. Without one, the only
     * "stop" a model can reach is the verb that drops the Pipeline's position and everything it
     * assembled -- and a caller that has to pick something is going to pick the thing that is there.
     * The required answer on the stop is a real question only while a second door exists.
     */
    @Test
    void thereIsAWayToHoldAPipelineThatClearsNothing() {
        assertThat(McpToolCatalog.operations(true).stream().map(McpToolCatalog::toolName))
                .contains("pipeline_pause", "pipeline_resume");
        // Write access, like every other verb that changes a Pipeline: holding one is not a read.
        assertThat(McpToolCatalog.operations(false).stream().map(McpToolCatalog::toolName))
                .doesNotContain("pipeline_pause", "pipeline_resume");
        // The description is what a model reads before choosing, and choosing between these two and the
        // stop is the whole point of opening them -- so it has to say that this one keeps everything.
        assertThat(ControlOperations.PIPELINE_PAUSE.description())
                .contains("Nothing is cleared")
                .contains("carries on");
    }

    /**
     * The one tool on this surface that destroys a named resource must not be reachable from a session
     * that was not started with write access. The exact-set assertions above would also catch it, but
     * only as one name among seventeen; this says which property is load-bearing, so a future edit that
     * re-scopes the operation fails against a test that explains why it may not.
     */
    @Test
    void theDestructiveToolIsAbsentFromAReadOnlySession() {
        assertThat(McpToolCatalog.operations(false).stream().map(McpToolCatalog::toolName))
                .doesNotContain("artifact_delete");
        assertThat(McpToolCatalog.operations(true).stream().map(McpToolCatalog::toolName))
                .contains("artifact_delete");
    }

    /**
     * The tool name is derived, and it is a published promise the moment the sidecar advertises it: a
     * model calls {@code artifact_delete} by that literal name. Pinning it here means a change to the
     * derivation, or to the operation id, breaks a test rather than a caller.
     */
    @Test
    void theRemovalToolIsNamedArtifactDelete() {
        assertThat(McpToolCatalog.toolName(ControlOperations.ARTIFACT_DELETE)).isEqualTo("artifact_delete");
    }

    @Test
    void sdkSpecificationsAreDerivedFromRegistryDescriptionsAndSchemas() {
        try (HttpControlClient client = new HttpControlClient()) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    URI.create("http://127.0.0.1:1"), "token", Map.of(), client);
            List<SyncToolSpecification> specifications = McpToolCatalog.specifications(false, executor);

            for (SyncToolSpecification specification : specifications) {
                Operation operation = ControlOperations.registry()
                        .exposedOn(Frontend.MCP).stream()
                        .filter(candidate -> McpToolCatalog.toolName(candidate).equals(specification.tool().name()))
                        .findFirst()
                        .orElseThrow();
                assertThat(specification.tool().description()).isEqualTo(operation.description());
                assertThat(specification.tool().inputSchema())
                        .isEqualTo(ControlApiSchema.resolve(operation.schema().params()));
                assertThat(specification.tool().outputSchema())
                        .isEqualTo(ControlApiSchema.resolve(operation.schema().result()));
            }
        }
    }

    @Test
    void sdkSpecificationHandlerProjectsExecutorResultIntoMcpResult() {
        try (HttpControlClient client = new HttpControlClient()) {
            McpOperationExecutor executor = new McpOperationExecutor(
                    URI.create("http://127.0.0.1:1"), "token", Map.of(), client);
            SyncToolSpecification specification = McpToolCatalog.specifications(false, executor).stream()
                    .filter(candidate -> candidate.tool().name().equals("connector_list"))
                    .findFirst()
                    .orElseThrow();

            McpSchema.CallToolResult result = specification.callHandler().apply(
                    null, new McpSchema.CallToolRequest("connector_list", Map.of()));

            assertThat(result.isError()).isTrue();
            assertThat(result.structuredContent()).isInstanceOf(Map.class);
            assertThat(result.content()).isNotEmpty();
        }
    }

    private static List<String> concat(List<String> left, List<String> right) {
        return java.util.stream.Stream.concat(left.stream(), right.stream()).toList();
    }
}
