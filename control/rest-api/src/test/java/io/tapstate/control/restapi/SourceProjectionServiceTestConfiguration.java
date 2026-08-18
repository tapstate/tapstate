package io.tapstate.control.restapi;

import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.core.catalog.TapstateCatalog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
final class SourceProjectionServiceTestConfiguration {

    @Bean
    SourceRepresentation sourceRepresentation() {
        return new SourceRepresentation(TapstateCatalog::load);
    }

    @Bean
    SourceProjectionService sourceProjectionService(
            ApplyService apply,
            ArtifactQueryService query,
            ArtifactMutationService mutation,
            SourceRepresentation representation) {
        return new SourceProjectionService(apply, query, mutation, representation);
    }
}
