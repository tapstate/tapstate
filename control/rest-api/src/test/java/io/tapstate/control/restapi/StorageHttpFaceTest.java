package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.ClusterTopologyService;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.common.JsonReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import static org.assertj.core.api.Assertions.assertThat;

class StorageHttpFaceTest {
    @Test
    void anUnknownApiRouteReturnsJson404WithAnAdmittedRuntime() throws Exception {
        try (var context = new SpringApplicationBuilder(TestApp.class).run("--server.port=0")) {
            var base = URI.create("http://127.0.0.1:"
                    + ((WebServerApplicationContext) context).getWebServer().getPort());
            try (var client = HttpClient.newHttpClient()) {
                var anonymous = client.send(HttpRequest.newBuilder(base.resolve("/api/cluster/members")).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(anonymous.statusCode()).as("the admitted controller retains authentication").isEqualTo(401);
                var admitted = client.send(HttpRequest.newBuilder(base.resolve("/api/cluster/members"))
                        .header("Authorization", "Bearer staged-runtime-admin").GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(admitted.statusCode()).as("the staged face serves the admitted runtime: %s", admitted.body())
                        .isEqualTo(200);

                var missing = client.send(HttpRequest.newBuilder(base.resolve("/api/runtime-image-smoke-missing"))
                        .GET().build(), HttpResponse.BodyHandlers.ofString());
                var contentType = missing.headers().firstValue("Content-Type").orElse("");
                assertThat(contentType).startsWith("application/json");
                assertThat(JsonReader.parse(missing.body())).isInstanceOf(Map.class);
                assertThat(missing.statusCode())
                        .as("GET /api/runtime-image-smoke-missing; Content-Type: %s; body: %s", contentType, missing.body())
                        .isEqualTo(404);
            }
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(StorageHttpFace.class)
    static class TestApp {
        @Bean
        OperationRegistry operationRegistry() {
            return ControlOperations.registry();
        }

        @Bean
        CredentialAuthenticator credentialAuthenticator() {
            return new CredentialAuthenticator(credential -> "staged-runtime-admin".equals(credential)
                    ? Optional.of(new VerifiedToken("admin", Scope.ADMIN)) : Optional.empty());
        }

        @Bean(destroyMethod = "close")
        GenericWebApplicationContext admittedRuntime(WebApplicationContext parent) {
            var child = new GenericWebApplicationContext();
            child.setParent(parent);
            child.setServletContext(parent.getServletContext());
            child.registerBean(ClusterTopologyService.class,
                    () -> new ClusterTopologyTestConfiguration().clusterTopologyService());
            child.registerBean(ClusterController.class,
                    () -> new ClusterController(child.getBean(ClusterTopologyService.class)));
            child.registerBean(RequestMappingHandlerMapping.class, () -> {
                var mapping = new RequestMappingHandlerMapping();
                mapping.setOrder(0);
                mapping.setPathPrefixes(Map.of("/api", HandlerTypePredicate.forAnnotation(RestController.class)));
                return mapping;
            });
            child.refresh();
            return child;
        }

        @Bean
        RuntimeHttpHandlers runtimeHttpHandlers(
                @Qualifier("admittedRuntime") GenericWebApplicationContext runtime) {
            return runtime.getBean(RequestMappingHandlerMapping.class)::getHandler;
        }
    }
}
