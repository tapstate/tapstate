package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelinePreviewService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RestApiConfigurationTest {

    @Test
    void asyncResponseTimeoutTracksThePreviewDeadlineAndItsCompletionBuffer() {
        assertThat(RestApiConfiguration.asyncRequestTimeoutMillis())
                .isEqualTo(PipelinePreviewService.DEADLINE.plusSeconds(15).toMillis());
    }
}
