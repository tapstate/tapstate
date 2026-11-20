package io.tapstate.app;

import io.tapstate.control.core.PipelineDraftCompiler;
import io.tapstate.control.restapi.AssistantProposalFixture;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.PipelineDraft;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Opt-in live smoke with synthetic input and the same signer and provider transport as the service. */
class ManagedBedrockLiveTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "TAPSTATE_BEDROCK_LIVE_TEST", matches = "true")
    void kimiStreamsConversationAndReturnsACompilablePipelineEdit() {
        var configuration = new ManagedAiConfiguration();
        try (var credentials = configuration.bedrockCredentials()) {
            var route = configuration.managedAiProvider(credentials, "ap-southeast-1", "global.moonshotai.kimi-k3", 4);
            var first = Map.of("role", "user", "content",
                    "Remember that the destination table I want is orders_archive_smoke. Reply only ACK.");
            var streamed = new StringBuilder();
            assertThat(route.client().propose(route.settings(), List.of(first),
                    Map.of("proposalEnabled", false), streamed::append)).isEmpty();
            assertThat(streamed.toString()).contains("ACK");

            var base = priorDraft();
            var context = Map.<String, Object>of("proposalEnabled", true,
                    "currentWizard", AssistantProposalFixture.view(base));
            var second = Map.of("role", "user", "content",
                    "Change only the destination table to the exact name I gave in the previous turn. Keep the root and all other settings.");
            var proposal = route.client().propose(route.settings(),
                    List.of(first, Map.of("role", "assistant", "content", streamed.toString()), second),
                    context, ignored -> { }).orElseThrow();
            assertThat(proposal).containsEntry("destinationTable", "orders_archive_smoke")
                    .containsEntry("destinationSourceId", "dst").containsEntry("destinationWriteMode", "upsert");
            var edited = AssistantProposalFixture.readCandidate(base, proposal);
            assertThat(edited.wizard().root()).isEqualTo(base.wizard().root());
            assertThat(edited.wizard().related()).isEmpty();
            assertThat(edited.wizard().transforms()).isEmpty();
            assertThat(new CanonicalWriter().write(new PipelineDraftCompiler().compileCandidate(edited)))
                    .contains("orders_archive_smoke");
        }
    }

    @Test void rejectsInvalidFieldsInTheReturnedProposalBeforeAcceptingTheDsl() {
        var base = priorDraft();
        var edited = new LinkedHashMap<>(AssistantProposalFixture.view(base));
        var invalidRoot = Map.of("table", "orders", "keys", List.of("id"), "columns", List.of(),
                "transforms", List.of());
        edited.put("root", invalidRoot);
        var compiler = new PipelineDraftCompiler();
        var draftWithoutSource = AssistantProposalFixture.readCandidate(base, edited);
        assertThatThrownBy(() -> compiler.compileCandidate(draftWithoutSource))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("wizard root source");

        edited.put("root", AssistantProposalFixture.view(base).get("root"));
        edited.put("transforms", List.of(Map.of("id", "invalid", "type", "not-a-transform", "config", Map.of())));
        assertThatThrownBy(() -> AssistantProposalFixture.readCandidate(base, edited))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unsupported transform kind");
    }

    private static PipelineDraft priorDraft() {
        var root = new PipelineDraft.Root("root", "src", "orders", List.of("id"), List.of());
        var output = new PipelineDraft.Output("atlas", Map.of("sourceId", "dst", "table", "orders", "writeMode", "upsert"));
        return new PipelineDraft("orders-smoke", 1, 1, PipelineDraft.Mode.WIZARD, "Orders", null, null,
                new PipelineDraft.Wizard(root, List.of(), List.of(), output),
                null, null, null, Instant.EPOCH, Instant.EPOCH, "smoke");
    }
}
