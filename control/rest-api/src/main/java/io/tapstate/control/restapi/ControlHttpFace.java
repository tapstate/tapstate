package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.SourceDraft;
import io.tapstate.control.core.SourceTableView;
import io.tapstate.control.core.SourceView;
import io.tapstate.control.core.TokenAdminService;
import io.tapstate.control.core.TokenService;
import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import tools.jackson.databind.DeserializationFeature;

/**
 * The public assembly entry point for the whole HTTP control face: the path-prefix + interceptor
 * registration, every verb controller, the pre-authentication entry points, the anonymous probe, and the
 * coded-error advice — bundled with the authentication interceptor that guards them. The assembly root
 * imports this one configuration to serve the control plane over HTTP; the individual controllers and the
 * interceptor stay package-private and are wired here, from inside their own package.
 *
 * <p>Bundling the interceptor with the controllers is what makes the surface fail closed: the only way to
 * bring the verb controllers into a context is through this configuration, and it always supplies the
 * {@link AuthInterceptor} (which {@link RestApiConfiguration} then registers on {@code /api/**}). A context
 * that serves these verbs therefore cannot come up without the guard — the interceptor's own dependencies
 * ({@link OperationRegistry}, {@link CredentialAuthenticator}) must be present or the context fails to
 * start, rather than silently serving an unguarded surface.
 */
@Configuration
@Import({RestApiConfiguration.class, ArtifactController.class, ConnectionController.class,
        ConnectorController.class, ConnectorIconController.class, PipelineController.class, PipelineObservationController.class,
        PipelineLogsController.class, PipelineStreamConfiguration.class, ClusterController.class,
        HealthController.class, AuthController.class, TokenController.class, SourceController.class,
        SourceDraftController.class,
        ApiExceptionHandler.class})
public class ControlHttpFace {

    @Bean
    AuthInterceptor authInterceptor(OperationRegistry registry, CredentialAuthenticator credentials) {
        return new AuthInterceptor(registry, credentials);
    }

    @Bean
    TokenAdminService tokenAdminService(TokenService tokens, AuditGate auditGate) {
        return new TokenAdminService(tokens, auditGate);
    }

    @Bean
    JsonMapperBuilderCustomizer sourceJsonContract() {
        return builder -> builder
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .addMixIn(SourceView.class, NonNullSourceJson.class)
                .addMixIn(SourceTableView.class, NonNullSourceJson.class)
                .addMixIn(SourceDraft.SourceSrs.class, NonNullSourceJson.class);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private abstract static class NonNullSourceJson {
    }

}
