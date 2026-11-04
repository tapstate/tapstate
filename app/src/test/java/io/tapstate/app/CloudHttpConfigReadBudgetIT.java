package io.tapstate.app;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.CloudReadCommandProbe;
import io.tapstate.control.core.*;
import io.tapstate.control.restapi.ControlHttpFace;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Real HTTP and persisted default attempts, with both find and aggregate delayed on CONFIG. */
@RequiresDocker
class CloudHttpConfigReadBudgetIT {
    private static final Map<String, CompletableFuture<CloudReadCommandProbe.Observation>> TRACES = new ConcurrentHashMap<>();
    private static final List<String> PATHS = List.of("/api/auth/me", "/api/storage/status", "/api/cluster/context");

    @Test
    void allReadFacesHaveBoundedCommandsAndRetainTheOriginalClaimAndSessionFences() throws Exception {
        var samples = new ArrayList<Sample>();
        try (var config = mongo(); var cache = mongo()) {
            config.start(); cache.start();
            var stored = NativeAppStorage.prepare(config.getReplicaSetUrl("budget_config"),
                    cache.getReplicaSetUrl("budget_cache"), cache.getReplicaSetUrl("budget_master"),
                    StorageBinding.Mode.CLOUD, "budget-cluster");
            var properties = new CloudProperties();
            properties.setBaseUrl("https://cloud.example"); properties.setToken("fixture-token");
            properties.setClusterId(stored.clusterId()); properties.setAtlasUri(stored.configurationUri());
            var cloud = CloudRuntimeSettings.resolve(properties);
            var clock = Clock.systemUTC();
            var bridge = new CloudSdkBridge(cloud, new Provider(stored.clusterId(), clock));
            try (var root = new StorageRoot(new MongoProperties(), cloud, bridge, BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), clock, new byte[32]);
                 var raw = MongoClients.create(stored.configurationUri());
                 var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
                var seed = new AtomicReference<Runnable>();
                var claims = new AtomicReference<WorkloadClaimStore>();
                var claim = new AtomicReference<WorkloadClaim>();
                var verification = new AtomicReference<DefaultSourceVerification>();
                var verificationClock = new AtomicReference<DisplayClock>();
                var connection = new AtomicReference<ConnectionConfig>();
                root.loadExisting(bound -> {
                    var configuration = root.runtime().requireConfiguration();
                    var port = new ThreeLayerStorePort(configuration, bound,
                            ignored -> { throw new AssertionError("unexpected pipeline preparation"); });
                    claims.set(port.workloadClaims());
                    var resultStore = port.connectionTestResults();
                    var display = new DisplayClock(clock.instant());
                    verificationClock.set(display);
                    var check = new DefaultSourceVerification(() -> {
                        bound.requireBusiness();
                        var binding = configuration.bindings().load().orElseThrow(StorageRuntime::unavailable);
                        if (!binding.equals(bound.binding())) return Optional.empty();
                        return configuration.artifacts().get(binding.defaultSourceId())
                                .filter(SourceResource.class::isInstance).map(SourceResource.class::cast);
                    }, resultStore, display, bound.binding().defaultSourceId(), () ->
                            new DefaultSourceVerification.Execution(claim.get().owner(), claim.get().claimGeneration()),
                            execution -> claims.get().read(claim.get().key()).filter(reading -> reading.leased()
                                    && reading.claim().owner().equals(execution.owner())
                                    && reading.claim().claimGeneration() == execution.generation()).isPresent(),
                            ignored -> false, Duration.ofSeconds(60));
                    verification.set(check);
                    seed.set(() -> {
                        var key = new WorkloadClaimKey(stored.clusterId(), WorkloadClaimType.NODE_SESSION, "fixture-node");
                        var acquired = claims.get().acquire(key, new WorkloadOwner(key.resourceId(), "fixture-boot"),
                                1, Duration.ofMinutes(5));
                        assertThat(acquired.acquired()).isTrue(); claim.set(acquired.claim());
                        var source = (SourceResource) configuration.artifacts().get(bound.binding().defaultSourceId()).orElseThrow();
                        connection.set(new ConnectionConfig(source.id(), source.connector(), source.config()));
                        var result = new ConnectionTestResult(source.id(), source.connector(),
                                ConnectionTestResult.Outcome.PASSED, List.of(), display.millis());
                        new ConnectionTestService(ignored -> result, resultStore,
                                new AuditGate(configuration.audit(), display), null,
                                new SourceConnectionResolver(configuration.artifacts()), check)
                                .test(source.id(), source.connector(), source.config(), "fixture-user");
                        display.now = clock.instant().plus(Duration.ofMinutes(11));
                        assertThat(check.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.EXPIRED);
                    });
                    var business = mock(NativeBusinessContext.class);
                    when(business.available()).thenReturn(true);
                    when(business.applicationDataVerification()).thenAnswer(ignored -> check.snapshot());
                    return business;
                });
                seed.get().run();
                String cookie = root.managedAuthentication().service().exchangeCode("fixture", "127.0.0.1").token();
                try (var context = new SpringApplicationBuilder(Assembly.class)
                        .environment(NativeAppStorage.isolatedEnvironment())
                        .initializers(app -> app.getBeanFactory().registerSingleton("latencyRoot", root))
                        .run("--server.address=127.0.0.1", "--server.port=0", "--spring.main.banner-mode=off")) {
                    String origin = "http://127.0.0.1:" + ((WebServerApplicationContext) context).getWebServer().getPort();
                    for (String path : PATHS) request(http, origin, cookie, path, 200, "EXPIRED");
                    for (int repeat = 0; repeat < 3; repeat++) {
                        var order = new ArrayList<>(List.of(0, 20, 40));
                        Collections.shuffle(order, new Random(7431L + repeat));
                        for (int millis : order) for (String path : PATHS) {
                            drain(root);
                            long before = delay(raw, millis);
                            Sample sample;
                            long after;
                            try { sample = request(http, origin, cookie, path, 200, "EXPIRED"); }
                            finally { drain(root); after = delay(raw, 0); }
                            if (millis > 0) assertThat(after - before).isPositive();
                            if (path.equals("/api/storage/status") && millis > 0)
                                assertThat(after - before).as("all CONFIG read commands, including aggregate, are bounded").isLessThanOrEqualTo(64);
                            System.out.printf("HTTP_READ_BUDGET path=%s repeat=%d delayMs=%d elapsedMs=%d delayHits=%d bytes=%d commands=%s%n",
                                    path, repeat, millis, sample.elapsedMs(), after - before, sample.observed().bytes(), sample.observed().commands());
                            int budget = path.equals("/api/storage/status") ? 185 : path.equals("/api/auth/me") ? 75 : 110;
                            assertThat(sample.observed().started()).as(path + " driver command budget").isLessThanOrEqualTo(budget);
                            assertThat(sample.observed().commands().getOrDefault("aggregate", 0)).isPositive();
                            samples.add(new Sample(path, millis, sample.elapsedMs(), sample.observed()));
                        }
                    }
                    drain(root);
                    for (String path : PATHS) request(http, origin, cookie, path, 200, "EXPIRED");
                    // The display association remains tied to the real persisted node tenure.
                    var original = claim.get();
                    assertThat(claims.get().release(original)).isTrue();
                    request(http, origin, cookie, "/api/storage/status", 200, "UNKNOWN");
                    var next = claims.get().acquire(original.key(), original.owner(), 2, Duration.ofMinutes(5));
                    assertThat(next.claim().claimGeneration()).isGreaterThan(original.claimGeneration());
                    claim.set(next.claim());
                    request(http, origin, cookie, "/api/storage/status", 200, "UNKNOWN");
                    try (var attempt = verification.get().begin(connection.get())) {
                        assertThat(claims.get().release(next.claim())).isTrue();
                        var result = new ConnectionTestResult(connection.get().id(), connection.get().connectorId(),
                                ConnectionTestResult.Outcome.PASSED, List.of(), verificationClock.get().millis(), true);
                        assertThatThrownBy(() -> attempt.publish(result, null)).isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
                    }
                    root.managedAuthentication().service().invalidateJwt("jti");
                    for (String path : PATHS) request(http, origin, cookie, path, 401, null);
                }
            }
        }
        assertThat(samples).hasSize(27);
        for (String path : PATHS) {
            long baseline = median(samples, path, 0), delayed = median(samples, path, 40);
            System.out.printf("HTTP_READ_MEDIAN path=%s baselineMs=%d delayedMs=%d increaseMs=%d%n", path, baseline, delayed, delayed - baseline);
            // This companion also delays aggregate, unlike the unchanged find-only acceptance case.
            assertThat(delayed - baseline).as(path + " find+aggregate delay sensitivity").isLessThan(4000);
        }
    }

    private static long median(List<Sample> samples, String path, int delay) {
        return samples.stream().filter(sample -> sample.path().equals(path) && sample.delayMs() == delay)
                .mapToLong(Sample::elapsedMs).sorted().toArray()[1];
    }
    private record Sample(String path, int delayMs, long elapsedMs, CloudReadCommandProbe.Observation observed) {}
    private static long delay(MongoClient raw, int millis) {
        var command = new Document("configureFailPoint", "failCommand").append("mode", millis == 0 ? "off" : "alwaysOn");
        if (millis > 0) command.append("data", new Document("failCommands", List.of("find", "aggregate", "getMore"))
                .append("blockConnection", true).append("blockTimeMS", millis));
        return ((Number) raw.getDatabase("admin").runCommand(command).get("count")).longValue();
    }
    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withLabel("io.tapstate.issue-loop.owner", System.getenv().getOrDefault("TAPSTATE_TEST_RESOURCE_OWNER", "1544b244689d41719d87288f458d22f4"))
                .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25", "--setParameter", "enableTestCommands=1");
    }
    private static void drain(StorageRoot root) throws Exception {
        var worker = (ExecutorService) org.springframework.test.util.ReflectionTestUtils.getField(root, "authenticationWorker");
        if (worker != null) worker.submit(() -> {}).get(15, TimeUnit.SECONDS);
    }
    private static Sample request(HttpClient http, String origin, String cookie, String path, int status, String verification) throws Exception {
        String id = UUID.randomUUID().toString();
        var trace = new CompletableFuture<CloudReadCommandProbe.Observation>(); TRACES.put(id, trace);
        try {
            var request = HttpRequest.newBuilder(URI.create(origin + path)).timeout(Duration.ofSeconds(35))
                    .header("Cookie", "__Host-tapstate-cloud-session=" + cookie).header("X-Latency-Probe", id);
            if (path.equals("/api/storage/status")) request.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{}"));
            else request.GET();
            long start = System.nanoTime();
            var response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            var observed = trace.get(5, TimeUnit.SECONDS);
            assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
            assertThat(observed.completed()).isEqualTo(observed.started());
            if (verification != null && path.equals("/api/storage/status"))
                assertThat(response.body()).contains("\"status\":\"" + verification + "\"");
            return new Sample(path, 0, elapsed, observed);
        } finally { TRACES.remove(id); }
    }
    private static final class DisplayClock extends Clock {
        Instant now;
        DisplayClock(Instant now) { this.now = now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(now, zone); }
        @Override public Instant instant() { return now; }
    }

    @SpringBootConfiguration
    @ComponentScan(basePackageClasses = ControlHttpFace.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = "io\\.tapstate\\.control\\.restapi\\.(RestApiSecurityConfiguration|ApiExceptionHandler|StorageControlController|CurrentUserController|ClusterContextController)"))
    static class Assembly extends CloudStatusConfigLatencyIT.Assembly {
        @Bean CurrentUserQueryService currentUser() { return new CurrentUserQueryService(AuthenticationMode.CLOUD); }
        @Override @Bean org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter> measurements() {
            var filter = new org.springframework.web.filter.OncePerRequestFilter() {
                @Override protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest request,
                        jakarta.servlet.http.HttpServletResponse response, jakarta.servlet.FilterChain chain)
                        throws jakarta.servlet.ServletException, java.io.IOException {
                    try (var probe = new CloudReadCommandProbe()) {
                        try { chain.doFilter(request, response); }
                        finally {
                            var target = TRACES.get(request.getHeader("X-Latency-Probe"));
                            if (target != null) target.complete(probe.observation());
                        }
                    }
                }
            };
            var result = new org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter>(filter);
            result.setOrder(Integer.MIN_VALUE); return result;
        }
    }
    private record Provider(String cluster, Clock clock) implements CloudSdkBridge.SdkRuntime {
        @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
            return new CloudSdkBridge.ExchangeResult("fixture-jwt", "organization", cluster, "Org", "Cluster", "local");
        }
        @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            return new CloudSdkBridge.VerifiedClaims("user", "organization", cluster, "jti", clock.instant().plusSeconds(3600),
                    "https://cloud.example", "127.0.0.1", List.of("workload:write"));
        }
        @Override public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() {
            return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization", cluster, null, "PROVISIONING");
        }
        @Override public boolean verifyCallback(String method, String time, String nonce, String data, String signature) { return false; }
        @Override public void report(String cluster, String nonce, CloudRuntimeStatus status, String token) { throw new AssertionError("unexpected report"); }
    }
}
