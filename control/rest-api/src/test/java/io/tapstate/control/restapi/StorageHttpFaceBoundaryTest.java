package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlOperations;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import static org.assertj.core.api.Assertions.assertThat;

class StorageHttpFaceBoundaryTest {
    private static final AtomicReference<RuntimeHttpHandlers> runtime = new AtomicReference<>();
    private static ConfigurableApplicationContext context;
    private static GenericWebApplicationContext child;
    private static HttpClient client;
    private static URI base;

    @BeforeAll static void start() {
        context = new SpringApplicationBuilder(TestApp.class).run("--server.port=0", "--server.servlet.context-path=/stage");
        child = new StorageHttpFaceTest.TestApp().admittedRuntime((WebApplicationContext) context);
        client = HttpClient.newHttpClient();
        base = URI.create("http://127.0.0.1:" + ((WebServerApplicationContext) context).getWebServer().getPort());
    }

    @AfterEach void withdrawRuntime() { runtime.set(null); }
    @AfterAll static void close() {
        if (client != null) client.close();
        if (child != null) child.close();
        if (context != null) context.close();
    }

    private static void admit() {
        runtime.set(child.getBean(RequestMappingHandlerMapping.class)::getHandler);
    }

    private static HttpResponse<String> request(String method, String path, String credential, String... headers) throws Exception {
        var request = HttpRequest.newBuilder(base.resolve("/stage" + path));
        if (credential != null) request.header("Authorization", "Bearer " + credential);
        if (headers.length > 0) request.headers(headers);
        request.method(method, "POST".equals(method) ? HttpRequest.BodyPublishers.ofString("{}") : HttpRequest.BodyPublishers.noBody());
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void missing(HttpResponse<String> response) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(404);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("application/json");
        assertThat(JsonReader.parse(response.body())).isInstanceOf(Map.class);
    }

    private static void unavailable(HttpResponse<String> response) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(503);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("application/json");
        var body = (Map<?, ?>) JsonReader.parse(response.body());
        assertThat(body.get("code")).isEqualTo("control.storage-unavailable");
        assertThat(body.get("params")).isEqualTo(Map.of());
        assertThat(response.headers().firstValue("Cache-Control")).contains("no-store");
    }

    @Test void unknownRoutesRemainMissingBeforeAdmissionAndAfterWithdrawal() throws Exception {
        missing(request("GET", "/api/runtime-image-smoke-missing", null));
        unavailable(request("GET", "/api/cluster/members", null));
        admit();
        assertThat(request("GET", "/api/cluster/members", "admin").statusCode()).isEqualTo(200);
        missing(request("GET", "/api/runtime-image-smoke-missing", null));
        runtime.set(null);
        unavailable(request("GET", "/api/cluster/members", "admin"));
        missing(request("GET", "/api/runtime-image-smoke-missing", null));
        admit();
        assertThat(request("GET", "/api/cluster/members", "admin").statusCode()).isEqualTo(200);
    }

    @Test void knownParameterizedRoutesRemainUnavailableWithoutCreatingBusinessServices() throws Exception {
        unavailable(request("GET", "/api/pipelines/orders/derived-schema", "admin"));
        missing(request("GET", "/api/pipelines/orders/not-a-route", "admin"));
        missing(request("GET", "/api/users", "admin"));
    }

    @Test void methodMismatchesKeepMvcStatusAndAllowBeforeAndAfterAdmission() throws Exception {
        var cold = request("POST", "/api/cluster/members", "admin");
        assertThat(cold.statusCode()).as(cold.body()).isEqualTo(405);
        assertThat(cold.headers().firstValue("Allow").orElse("")).contains("GET");
        admit();
        var admitted = request("POST", "/api/cluster/members", "admin");
        assertThat(admitted.statusCode()).isEqualTo(405);
        assertThat(admitted.headers().firstValue("Allow")).isEqualTo(cold.headers().firstValue("Allow"));
    }

    @Test void consumesAndProducesMismatchesUseMvcErrorsRatherThanStorageErrors() throws Exception {
        var wrongContent = request("POST", "/api/validation:evaluate", "admin", "Content-Type", "text/plain");
        assertThat(wrongContent.statusCode()).as(wrongContent.body()).isEqualTo(415);
        var wrongAccept = request("POST", "/api/validation:evaluate", "admin", "Content-Type", "application/json", "Accept", "text/plain");
        assertThat(wrongAccept.statusCode()).as(wrongAccept.body()).isEqualTo(406);
        unavailable(request("POST", "/api/validation:evaluate", "admin", "Content-Type", "application/json"));
    }

    @Test void headAndOptionsKeepTheExistingBehaviorAtEachAdmissionStage() throws Exception {
        var head = request("HEAD", "/api/cluster/members", "admin");
        assertThat(head.statusCode()).isEqualTo(503);
        assertThat(head.body()).isEmpty();
        unavailable(request("OPTIONS", "/api/cluster/members", "admin"));
        admit();
        assertThat(request("HEAD", "/api/cluster/members", "admin").statusCode()).isEqualTo(200);
        var options = request("OPTIONS", "/api/cluster/members", "admin");
        // The existing authorization manager rejects Spring's synthetic OPTIONS handler without @Verb.
        // Compare with the non-staged face to keep that separate behavior unchanged by this routing fix.
        try (var baseline = new SpringApplicationBuilder(BaselineApp.class).run("--server.port=0")) {
            var uri = URI.create("http://127.0.0.1:"
                    + ((WebServerApplicationContext) baseline).getWebServer().getPort() + "/api/cluster/members");
            var direct = client.send(HttpRequest.newBuilder(uri).header("Authorization", "Bearer admin")
                    .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(options.statusCode()).as("OPTIONS retains the non-staged security/MVC behavior")
                    .isEqualTo(direct.statusCode());
            assertThat(options.headers().firstValue("Allow")).isEqualTo(direct.headers().firstValue("Allow"));
        }
    }

    @Test void existingAuthenticationAndScopeChecksPrecedeAdmittedOperations() throws Exception {
        admit();
        assertThat(request("GET", "/api/cluster/members", null).statusCode()).isEqualTo(401);
        assertThat(request("GET", "/api/cluster/members", "invalid").statusCode()).isEqualTo(401);
        assertThat(request("POST", "/api/storage/setup/commit", "reader", "Content-Type", "application/json").statusCode()).isEqualTo(403);
        assertThat(request("GET", "/api/runtime-image-smoke-missing", "invalid").statusCode()).isEqualTo(401);
        missing(request("GET", "/api/runtime-image-smoke-missing", "reader"));
    }

    @Test void minimumControlsKeepTheirOwnAuthorizationAndUnavailableDependencies() throws Exception {
        assertThat(request("GET", "/healthz", null).statusCode()).isEqualTo(200);
        assertThat(request("GET", "/api/auth/me", null).statusCode()).isEqualTo(401);
        assertThat(request("GET", "/api/auth/me", "admin").statusCode()).isEqualTo(200);
        unavailable(request("POST", "/api/storage/status", "admin", "Content-Type", "application/json"));
        assertThat(request("GET", "/not-public", null).statusCode()).isEqualTo(401);
    }

    @Test void declaredStreamsRemainUnavailableWithoutTreatingOtherPathsAsStreams() throws Exception {
        for (String path : new String[]{"/api/pipelines/orders/status/watch", "/api/pipelines/orders/logs/follow", "/api/data-browser/source/collection/tail"}) {
            unavailable(request("GET", path, null));
        }
        missing(request("GET", "/api/pipelines/orders/status/not-a-stream", null));
        missing(request("GET", "/api/data-browser/source/collection/not-a-stream", null));
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(StorageHttpFace.class)
    static class TestApp {
        @Bean OperationRegistry operations() { return ControlOperations.registry(); }
        @Bean CredentialAuthenticator credentials() {
            return new CredentialAuthenticator(value -> switch (value) {
                case "admin" -> Optional.of(new VerifiedToken("admin", Scope.ADMIN));
                case "reader" -> Optional.of(new VerifiedToken("reader", Scope.READ));
                default -> Optional.empty();
            });
        }
        @Bean RuntimeHttpHandlers runtimeHttpHandlers() {
            return request -> {
                var selected = runtime.get();
                return selected == null ? null : selected.handlerFor(request);
            };
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class, ApiExceptionHandler.class,
            ClusterController.class, ClusterTopologyTestConfiguration.class})
    static class BaselineApp {
        @Bean OperationRegistry operations() { return ControlOperations.registry(); }
        @Bean CredentialAuthenticator credentials() { return new TestApp().credentials(); }
    }
}
