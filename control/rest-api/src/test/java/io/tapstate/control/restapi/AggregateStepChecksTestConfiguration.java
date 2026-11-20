package io.tapstate.control.restapi;

import io.tapstate.control.core.AggregateStepChecks;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The aggregate check service, for contexts that import the whole HTTP face and do not exercise it.
 *
 * <p>Every controller in the bundle brings its service with it, so a context that mounts the face
 * without one does not start. It throws rather than answering: nothing in those contexts reaches these
 * routes, and a case that does has to supply its own.
 */
@Configuration(proxyBeanMethods = false)
final class AggregateStepChecksTestConfiguration {

    @Bean
    AggregateStepChecks aggregateStepChecks() {
        return new AggregateStepChecks() {

            @Override
            public Report check(String pipelineId, String step, Request request) {
                throw notHere();
            }

            @Override
            public Report repair(String principal, String pipelineId, String step, Request request) {
                throw notHere();
            }
        };
    }

    private static UnsupportedOperationException notHere() {
        return new UnsupportedOperationException(
                "no case in this context checks or repairs an aggregate step; one that does has to supply it");
    }
}
