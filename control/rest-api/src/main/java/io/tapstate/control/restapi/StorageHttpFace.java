package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.handler.AbstractHandlerMapping;

import java.io.IOException;
import java.util.Map;

/** The authenticated minimum HTTP face remains available before the data-plane runtime exists. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class, HealthController.class,
        VersionController.class, IssuerDiscoveryController.class, AuthController.class, BrowserAuthController.class, CloudAuthController.class,
        CurrentUserController.class, ClusterContextController.class, StorageControlController.class, StorageEntryController.class, InstallationController.class, ApiExceptionHandler.class})
public class StorageHttpFace {
    @Bean
    org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer sourceJsonContract() {
        return new ControlHttpFace().sourceJsonContract();
    }
    @Bean
    io.tapstate.control.core.CurrentUserQueryService currentUserQueryService(ObjectProvider<io.tapstate.control.core.AuthenticationMode> modes) {
        return new io.tapstate.control.core.CurrentUserQueryService(modes.getIfAvailable(() -> io.tapstate.control.core.AuthenticationMode.ON_PREM));
    }

    @Bean
    HandlerMapping admittedRuntimeHandlers(ObjectProvider<RuntimeHttpHandlers> runtimes, ApiSecurityErrorWriter errors,
            io.tapstate.control.core.OperationRegistry operations) {
        var declared = new RuntimeRouteMappings(operations);
        var unavailable = new UnavailableHandler(errors);
        var mapping = new AbstractHandlerMapping() {
            @Override protected Object getHandlerInternal(HttpServletRequest request) throws Exception {
                var runtime = runtimes.getIfAvailable();
                var handler = runtime == null ? null : runtime.handlerFor(request);
                if (handler != null) return handler;
                if (request.getRequestURI().startsWith(request.getContextPath() + "/api/") && declared.matches(request))
                    return unavailable;
                return null;
            }
        };
        mapping.setOrder(1);
        return mapping;
    }

    record UnavailableHandler(ApiSecurityErrorWriter errors) implements HttpRequestHandler {
        @Override
        public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
            errors.write(request, response, ControlError.STORAGE_UNAVAILABLE, Map.of());
        }
    }
}
