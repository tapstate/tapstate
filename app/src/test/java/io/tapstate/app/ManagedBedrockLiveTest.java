package io.tapstate.app;

import io.tapstate.control.core.PipelineDraftCompiler;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.PipelineDraft;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Opt-in live smoke with synthetic input and the same signer and provider transport as the service. */
@EnabledIfEnvironmentVariable(named = "TAPSTATE_BEDROCK_LIVE_TEST", matches = "true")
class ManagedBedrockLiveTest {
    @Test void kimiStreamsConversationAndReturnsACompilablePipelineEdit() {
        var configuration = new ManagedAiConfiguration();
        try (var credentials = configuration.bedrockCredentials()) {
            var route = configuration.managedAiProvider(credentials, "ap-southeast-1", "global.moonshotai.kimi-k3", 4);
            var first = Map.of("role", "user", "content",
                    "Remember that the destination table I want is orders_archive_smoke. Reply only ACK.");
            var streamed = new StringBuilder();
            assertThat(route.client().propose(route.settings(), List.of(first),
                    Map.of("proposalEnabled", false), streamed::append)).isEmpty();
            assertThat(streamed.toString()).contains("ACK");

            var root = Map.of("id", "root", "source", "src", "table", "orders", "primaryKey", List.of("id"),
                    "transforms", List.of());
            var wizard = Map.of("root", root, "relations", List.of(), "transforms", List.of(),
                    "destinationTable", "orders", "destinationSourceId", "dst", "destinationWriteMode", "upsert");
            var context = Map.<String, Object>of("proposalEnabled", true, "currentWizard", wizard);
            var second = Map.of("role", "user", "content",
                    "Change only the destination table to the exact name I gave in the previous turn. Keep the root and all other settings.");
            var proposal = route.client().propose(route.settings(),
                    List.of(first, Map.of("role", "assistant", "content", streamed.toString()), second),
                    context, ignored -> { }).orElseThrow();
            assertThat(proposal).containsEntry("destinationTable", "orders_archive_smoke")
                    .containsEntry("destinationSourceId", "dst").containsEntry("destinationWriteMode", "upsert");
            assertThat(proposal.get("root")).isInstanceOfSatisfying(Map.class,
                    result -> assertThat(result.get("table")).isEqualTo("orders"));
            var edited = new PipelineDraft("orders-smoke", 1, 1, PipelineDraft.Mode.WIZARD, "Orders", null, null,
                    new PipelineDraft.Wizard(new PipelineDraft.Root("root", "src", "orders", List.of("id"), List.of()),
                            List.of(), List.of(), new PipelineDraft.Output("atlas", Map.of(
                                    "sourceId", proposal.get("destinationSourceId"), "table", proposal.get("destinationTable"),
                                    "writeMode", proposal.get("destinationWriteMode")))),
                    null, null, null, Instant.EPOCH, Instant.EPOCH, "smoke");
            assertThat(new CanonicalWriter().write(new PipelineDraftCompiler().compileCandidate(edited)))
                    .contains("orders_archive_smoke");
        }
    }
}
