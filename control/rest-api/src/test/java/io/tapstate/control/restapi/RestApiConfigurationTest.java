package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineDraftService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.control.core.PipelinePreviewService;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RestApiConfigurationTest {

    @Test
    void mvcInstallsAnAsyncResponseTimeoutThatTracksThePreviewDeadlineAndItsCompletionBuffer() {
        InspectableAsyncSupportConfigurer configurer = new InspectableAsyncSupportConfigurer();

        new RestApiConfiguration().apiWebMvc().configureAsyncSupport(configurer);

        assertThat(configurer.timeout())
                .isEqualTo(PipelinePreviewService.DEADLINE.plusSeconds(15).toMillis());
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
        PipelineObservationQueryService observations = new PipelineObservationQueryService(
                artifactQueries, noObservations());
        PipelineCatalogService pipelines = new PipelineCatalogService(
                artifactQueries,
                new PipelineDraftService(new PipelineApiTest.FakePipelineDraftStore()),
                new PipelineApiTest.FakeDesiredStore(), observations);

        var knownPipeline = PipelineStreamConfiguration.knownPipeline(pipelines);

        assertThat(knownPipeline.test("published")).isTrue();
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
