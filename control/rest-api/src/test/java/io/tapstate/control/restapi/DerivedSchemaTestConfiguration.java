package io.tapstate.control.restapi;

import io.tapstate.control.core.DerivedSchemas;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * The derived-schema service, for contexts that import the whole HTTP face and do not exercise it.
 *
 * <p>The same requirement the position service carries, for the same reason: every controller in that
 * bundle brings its service with it, so a context that mounts the face without one does not start. That
 * is the design -- a face is either wired or it is not -- and it is what makes a controller that nobody
 * mounted visible here rather than as a 404 in the product.
 *
 * <p>It throws rather than answering. Nothing in those contexts reaches a derived-schema route, so an
 * answer here would be a fixture nobody asked for; a throw says plainly that a case which did reach one
 * should have supplied its own.
 */
@Configuration(proxyBeanMethods = false)
final class DerivedSchemaTestConfiguration {

    @Bean
    DerivedSchemas derivedSchemas() {
        return new DerivedSchemas() {

            @Override
            public List<StepReport> compare(String pipelineId) {
                throw notHere();
            }

            @Override
            public void accept(String principal, String pipelineId) {
                throw notHere();
            }
        };
    }

    private static UnsupportedOperationException notHere() {
        return new UnsupportedOperationException(
                "no case in this context reads or accepts a derived schema; one that does has to supply it");
    }
}
