package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineDraftService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelinePreviewService;
import io.tapstate.control.core.OpenAiCompatibleClient;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineDraft;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RestApiConfigurationTest {

    @Test
    void mvcInstallsAnAsyncResponseTimeoutThatTracksThePreviewDeadlineAndItsCompletionBuffer() {
        InspectableAsyncSupportConfigurer configurer = new InspectableAsyncSupportConfigurer();

        new RestApiConfiguration().apiWebMvc().configureAsyncSupport(configurer);

        assertThat(configurer.timeout())
                .isEqualTo(Math.max(Math.max(PipelinePreviewService.DEADLINE.plusSeconds(15).toMillis(),
                        OpenAiCompatibleClient.maximumProposalDuration().plusSeconds(15).toMillis()), 105000L));
    }

    @Test
    void mvcAsyncResponseBudgetCoversProviderRetriesAndTheNonStreamingFallback() {
        InspectableAsyncSupportConfigurer configurer = new InspectableAsyncSupportConfigurer();

        new RestApiConfiguration().apiWebMvc().configureAsyncSupport(configurer);

        assertThat(configurer.timeout()).isGreaterThanOrEqualTo(285000L);
    }

    @Test
    void statusWatchTreatsEveryCatalogPipelineAsKnownDuringTheUnknownReadRace() {
        PipelineApiTest.FakeArtifactStore artifacts = new PipelineApiTest.FakeArtifactStore();
        artifacts.seed("""
                version: tapstate/v1
                kind: pipeline
                id: published
                source: src_x
                serve:
                  from: /.*/
                  sync:
                    - id: sink1
                      source: tgt_x
                      write_mode: upsert
                      ddl: apply
                """);
        ArtifactQueryService artifactQueries = new ArtifactQueryService(artifacts);
        PipelineApiTest.FakePipelineDraftStore drafts = new PipelineApiTest.FakePipelineDraftStore();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        drafts.create(new PipelineDraft("draft-only", 1, 1, PipelineDraft.Mode.DAG, "Draft", null,
                new PipelineDraft.Graph(List.of(), List.of(), new PipelineDraft.Viewport(0, 0, 1)),
                null, null, null, null, now, now, "test"));
        PipelineObservationQueryService observations = new PipelineObservationQueryService(
                artifactQueries, noObservations());
        DesiredStore unreadablePublishedIntent = new DesiredStore() {
            @Override
            public void save(DesiredState desired) {
                throw new AssertionError("no desired write is expected here");
            }

            @Override
            public Optional<DesiredState> read(String pipelineId) {
                if (pipelineId.equals("published")) {
                    throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                            Map.of("id", pipelineId, "field", "targetState"), null);
                }
                return Optional.empty();
            }

            @Override
            public List<String> pipelineIds() {
                return List.of();
            }

            @Override
            public void delete(String pipelineId) {
                throw new AssertionError("no desired delete is expected here");
            }
        };
        PipelineCatalogService pipelines = new PipelineCatalogService(
                artifactQueries,
                new PipelineDraftService(drafts),
                unreadablePublishedIntent, observations);

        var knownPipeline = PipelineStreamConfiguration.knownPipeline(pipelines);

        assertThat(knownPipeline.test("published")).isTrue();
        assertThat(knownPipeline.test("draft-only")).isTrue();
        assertThat(knownPipeline.test("ghost")).isFalse();
    }

    private static ObservationStore noObservations() {
        return new ObservationStore() {
            @Override
            public void delete(String pipelineId) {
                throw new UnsupportedOperationException("removal is not exercised by this double");
            }

            @Override
            public void save(Observation observation) {
            }

            @Override
            public Optional<Observation> read(String pipelineId) {
                return Optional.empty();
            }
        };
    }

    private static final class InspectableAsyncSupportConfigurer extends AsyncSupportConfigurer {
        Long timeout() {
            return getTimeout();
        }
    }
}
