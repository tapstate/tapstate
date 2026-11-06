package io.tapstate.app;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.adapters.mongostore.HttpDiagnosticsCommandCounter;
import io.tapstate.control.core.*;
import io.tapstate.control.restapi.ControlHttpFace;
import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Real HTTP/security/storage witness for opt-in timing and unchanged admission. */
@RequiresDocker
class CloudHttpPerformanceDiagnosticsIT {
    private static final Map<String, CompletableFuture<Integer>> COUNTS = new ConcurrentHashMap<>();

    @Test void enablingDiagnosticsPreservesMongoWorkResponsesAndRevocation() throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L))) {
            mongo.start();
            var stored = NativeAppStorage.create(mongo, "http_diag", StorageBinding.Mode.CLOUD, "diagnostic-cluster");
            var properties = new CloudProperties(); properties.setBaseUrl("https://cloud.example");
            properties.setToken("fixture-token"); properties.setClusterId(stored.clusterId());
            properties.setAtlasUri(stored.configurationUri());
            var settings = CloudRuntimeSettings.resolve(properties); var clock = Clock.systemUTC();
            var bridge = new CloudSdkBridge(settings, new Provider(stored.clusterId(), clock));
            try (var root = new StorageRoot(new MongoProperties(), settings, bridge, BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), clock, new byte[32]);
                 var http = HttpClient.newHttpClient()) {
                root.loadExisting(bound -> { var business = mock(NativeBusinessContext.class);
                    when(business.available()).thenReturn(true); return business; });
                String cookie = root.managedAuthentication().service().exchangeCode("fixture", "127.0.0.1").token();
                try (var context = new SpringApplicationBuilder(Assembly.class).environment(NativeAppStorage.isolatedEnvironment())
                        .initializers(app -> { app.getBeanFactory().registerSingleton("root", root);
                            app.getBeanFactory().registerSingleton("settings", settings); })
                        .run("--server.address=127.0.0.1", "--server.port=0", "--spring.main.banner-mode=off",
                                "--logging.level.root=WARN")) {
                    String origin = "http://127.0.0.1:" + ((WebServerApplicationContext) context).getWebServer().getPort();
                    Logger logger = (Logger) org.slf4j.LoggerFactory.getLogger("io.tapstate.diagnostics.http");
                    assertThat(logger.getEffectiveLevel()).isEqualTo(Level.INFO);
                    var events = new ListAppender<ILoggingEvent>(); events.start(); logger.addAppender(events);
                    try {
                        for (int i = 0; i < 4; i++) request(http, origin, cookie, 200);
                        assertThat(events.list).isEmpty();
                        logger.setLevel(Level.DEBUG);
                        for (int i = 0; i < 8; i++) request(http, origin, cookie, 200);
                        var offNanos = new ArrayList<Long>(); var onNanos = new ArrayList<Long>();
                        for (int i = 0; i < 16; i++) {
                            logger.setLevel(Level.INFO); events.list.clear();
                            var before = request(http, origin, cookie, 200); assertThat(events.list).isEmpty();
                            logger.setLevel(Level.DEBUG); var after = request(http, origin, cookie, 200);
                            offNanos.add(before.nanos()); onNanos.add(after.nanos());
                            assertThat(after.response().body()).isEqualTo(before.response().body());
                            assertThat(after.commands()).isEqualTo(before.commands());
                            assertThat(events.list).singleElement().satisfies(event -> {
                                @SuppressWarnings("unchecked") var value = (Map<String, Object>) JsonReader.parse(event.getFormattedMessage());
                                assertThat(value).containsEntry("event", "http_diag").containsEntry("complete", true)
                                        .containsEntry("request_id", after.response().headers().firstValue("X-Request-ID").orElseThrow());
                                var driver = (Map<?, ?>) value.get("mongo");
                                assertThat(((Number) driver.get("started")).intValue()).isEqualTo(after.commands());
                                assertThat(((Number) driver.get("completed")).intValue()).isEqualTo(after.commands());
                                assertThat(value.get("phases").toString()).contains("auth_entry", "auth_attempt", "auth_verify", "context_select");
                                assertThat(event.getFormattedMessage()).doesNotContain(cookie, "fixture-jwt", "fixture-token", "mongodb://");
                            });
                        }
                        System.out.println("HTTP_DIAG_OVERHEAD " + io.tapstate.core.common.JsonWriter.write(Map.of(
                                "pairs", offNanos.size(), "off_http_ns", offNanos, "on_http_ns", onNanos,
                                "note", "same_http_body_and_command_counts;localhost;not_a_production_overhead_bound")));
                        logger.setLevel(Level.INFO); root.managedAuthentication().service().invalidateJwt("jti");
                        request(http, origin, cookie, 401);
                        logger.setLevel(Level.DEBUG); request(http, origin, cookie, 401);
                    } finally { logger.detachAppender(events); events.stop(); logger.setLevel(Level.INFO); }
                }
            }
        }
    }

    private record Result(HttpResponse<String> response, int commands, long nanos) { }
    private static Result request(HttpClient http, String origin, String cookie, int status) throws Exception {
        String id = UUID.randomUUID().toString(); var count = new CompletableFuture<Integer>(); COUNTS.put(id, count);
        try {
            long started = System.nanoTime();
            var response = http.send(HttpRequest.newBuilder(URI.create(origin + "/api/auth/me"))
                    .timeout(Duration.ofSeconds(30)).header("Cookie", "__Host-tapstate-cloud-session=" + cookie)
                    .header("X-Probe-ID", id).GET().build(), HttpResponse.BodyHandlers.ofString());
            long nanos = System.nanoTime() - started;
            assertThat(response.statusCode()).isEqualTo(status);
            return new Result(response, count.get(5, TimeUnit.SECONDS), nanos);
        } finally { COUNTS.remove(id); }
    }

    @SpringBootConfiguration
    @ComponentScan(basePackageClasses = ControlHttpFace.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = "io\\.tapstate\\.control\\.restapi\\.(RestApiSecurityConfiguration|ApiExceptionHandler|CurrentUserController)"))
    static class Assembly extends CloudStatusConfigLatencyIT.Assembly {
        @Bean CurrentUserQueryService users() { return new CurrentUserQueryService(AuthenticationMode.CLOUD); }
        @Bean org.springframework.boot.web.servlet.FilterRegistrationBean<CloudHttpDiagnosticsFilter> diagnostics(CloudRuntimeSettings settings) {
            return new CloudRuntimeConfiguration().cloudHttpDiagnostics(settings);
        }
        @Override @Bean org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter> measurements() {
            var filter = new org.springframework.web.filter.OncePerRequestFilter() {
                @Override protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest request,
                        jakarta.servlet.http.HttpServletResponse response, jakarta.servlet.FilterChain chain)
                        throws jakarta.servlet.ServletException, java.io.IOException {
                    try (var counter = new HttpDiagnosticsCommandCounter()) {
                        try { chain.doFilter(request, response); }
                        finally { var target = COUNTS.get(request.getHeader("X-Probe-ID"));
                            if (target != null) target.complete(counter.count()); }
                    }
                }
            };
            var registration = new org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter>(filter);
            registration.setOrder(Integer.MIN_VALUE); return registration;
        }
    }

    private record Provider(String cluster, Clock clock) implements CloudSdkBridge.SdkRuntime {
        public CloudSdkBridge.ExchangeResult exchange(String code) { return new CloudSdkBridge.ExchangeResult("fixture-jwt", "organization", cluster, "Org", "Cluster", "local"); }
        public CloudSdkBridge.VerifiedClaims verify(String jwt) { return new CloudSdkBridge.VerifiedClaims("user", "organization", cluster, "jti", clock.instant().plusSeconds(3600), "https://cloud.example", "127.0.0.1", List.of("workload:write")); }
        public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() { return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization", cluster, null, "PROVISIONING"); }
        public boolean verifyCallback(String method, String time, String nonce, String data, String signature) { return false; }
        public void report(String cluster, String nonce, CloudRuntimeStatus status, String token) { throw new AssertionError("unexpected report"); }
    }
}
