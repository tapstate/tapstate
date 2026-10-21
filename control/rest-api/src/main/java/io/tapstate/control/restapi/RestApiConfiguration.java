package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelinePreviewService;
import io.tapstate.messages.MessageCatalog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.Duration;

/**
 * The HTTP presentation configuration: the single Spring context's Web MVC face over the control
 * core. The control verbs are projected here in the one service context. The assembly may supply a
 * dedicated installation connector with its own channel and capability checks.
 *
 * <p>Every {@code @RestController} handler is mounted under the single {@code /api} prefix, so the
 * verb surface lives at one route root. A plain {@code @Controller} (not annotated
 * {@code @RestController}) is left unprefixed, so the root-level probe and the pre-authentication
 * entry points (login, bootstrap) are served outside {@code /api}.
 *
 * <p>{@link RestApiSecurityConfiguration} owns HTTP authentication and authorization when the full control
 * face is assembled. This configuration stays focused on path projection and common presentation beans, so
 * a focused MVC mapping test can instantiate only the handlers it needs without serving an authenticated
 * production surface.
 */
@Configuration
public class RestApiConfiguration {

    private static final Duration ASYNC_RESPONSE_BUFFER = Duration.ofSeconds(15);

    static long asyncRequestTimeoutMillis() {
        return PipelinePreviewService.DEADLINE.plus(ASYNC_RESPONSE_BUFFER).toMillis();
    }

    @Bean
    WebMvcConfigurer apiWebMvc() {
        return new WebMvcConfigurer() {
            @Override
            public void configurePathMatch(PathMatchConfigurer configurer) {
                configurer.addPathPrefix("/api", HandlerTypePredicate.forAnnotation(RestController.class));
            }

            @Override
            public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
                configurer.setDefaultTimeout(asyncRequestTimeoutMillis());
            }
        };
    }

    /**
     * The shared error-code catalog + renderer, so this HTTP face renders coded errors the same way every
     * other presentation face does (the offline CLI, the assembly root). The bundled {@code en} catalog is
     * immutable and stateless, so a single instance is shared.
     */
    @Bean
    MessageCatalog messageCatalog() {
        return MessageCatalog.bundled();
    }
}
