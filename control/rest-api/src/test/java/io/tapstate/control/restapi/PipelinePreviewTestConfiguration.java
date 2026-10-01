package io.tapstate.control.restapi;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.PipelinePreviewService;
import io.tapstate.runtime.probe.PipelinePreviewProbe;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/** Inert preview execution for full-face tests that mount, but do not invoke, the preview endpoint. */
@Configuration(proxyBeanMethods = false)
final class PipelinePreviewTestConfiguration {

    @Bean
    PipelinePreviewService pipelinePreviewService(ApplyService compiler, Clock clock) {
        PipelinePreviewProbe unused = request -> {
            throw new AssertionError("this full-face test does not execute a Pipeline preview");
        };
        return new PipelinePreviewService(compiler, unused, clock);
    }
}
