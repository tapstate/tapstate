package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
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
 * The public assembly entry point for the whole HTTP control face: the path-prefix configuration, Spring
 * Security chains, every verb controller, the pre-authentication entry points, the anonymous probe, and the
 * coded-error advice. The assembly root imports this one configuration to serve the control plane over HTTP;
 * the individual controllers and security adapters stay package-private and are wired here, from inside
 * their own package.
 *
 * <p>Bundling the security chains with the controllers is what makes the surface fail closed: the only way
 * to bring the verb controllers into a production context is through this configuration, and its security
 * configuration requires the operation registry and both credential verifiers. A production context cannot
 * come up with this HTTP face but without the guard.
 *
 * <p>The streaming channels are imported here too, and being here is the only thing that mounts them. A
 * websocket configuration left out of this list does not fail anything: no context misses a bean, no
 * projection gate covers a websocket path, and the one caller that asks for a follow registry asks
 * optionally and settles for a registry that does nothing. It shows up only as a handshake answered 404
 * by a product whose client, handler and tests all work.
 */
@Configuration
@Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class, ArtifactController.class, ConnectionController.class,
        ConnectorController.class, DataBrowserController.class, DataBrowserStreamConfiguration.class,
        PipelineController.class, PipelineObservationController.class, PipelineLogsController.class,
        PipelineStreamConfiguration.class, ClusterController.class, HealthController.class,
        AuthController.class, IssuerDiscoveryController.class, TokenController.class, SourceController.class,
        SourceDraftController.class,
        ApiExceptionHandler.class})
public class ControlHttpFace {

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
