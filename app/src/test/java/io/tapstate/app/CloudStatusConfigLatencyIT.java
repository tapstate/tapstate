package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.CloudLatencyCommandProbe;
import io.tapstate.control.core.*;
import io.tapstate.control.restapi.ControlHttpFace;
import io.tapstate.control.restapi.RestApiConfiguration;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Real HTTP authorization and storage reads with independent CONFIG and CACHE delay domains. */
@RequiresDocker
class CloudStatusConfigLatencyIT {
    private static final Map<String, CompletableFuture<CloudLatencyCommandProbe.Observation>> TRACES =
            new ConcurrentHashMap<>();

    @Test
    void expiredStatusDoesNotAmplifyFortyMillisecondConfigReadsIntoMultiSecondLatency() throws Exception {
        var samples = new ArrayList<Sample>();
        try (var configMongo = mongo(); var cacheMongo = mongo()) {
            configMongo.start();
            cacheMongo.start();
            var stored = NativeAppStorage.prepare(configMongo.getReplicaSetUrl("latency_config"),
                    cacheMongo.getReplicaSetUrl("latency_cache"), cacheMongo.getReplicaSetUrl("latency_master"),
                    StorageBinding.Mode.CLOUD, "latency-cluster");
            var properties = new CloudProperties();
            properties.setBaseUrl("https://cloud.example");
            properties.setClusterId(stored.clusterId());
            properties.setAtlasUri(stored.configurationUri());
            properties.setToken("fixture-runtime-token");
            var cloud = CloudRuntimeSettings.resolve(properties);
            var clock = Clock.systemUTC();
            var bridge = new CloudSdkBridge(cloud, new Provider(stored.clusterId(), clock));
            try (var root = new StorageRoot(new MongoProperties(), cloud, bridge, BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), clock, new byte[32]);
                 var configRaw = MongoClients.create(stored.configurationUri());
                 var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
                var seed = new AtomicReference<Runnable>();
                root.loadExisting(bound -> {
                    var configuration = root.runtime().requireConfiguration();
                    var port = new ThreeLayerStorePort(configuration, bound,
                            ignored -> { throw new AssertionError("unexpected pipeline preparation"); });
                    var results = port.connectionTestResults();
                    var verificationClock = new VerificationClock(clock.instant());
                    var verification = new DefaultSourceVerification(() -> {
                        bound.requireBusiness();
                        var binding = configuration.bindings().load().orElseThrow(StorageRuntime::unavailable);
                        if (!binding.equals(bound.binding())) return Optional.empty();
                        return configuration.artifacts().get(binding.defaultSourceId())
                                .filter(SourceResource.class::isInstance).map(SourceResource.class::cast);
                    }, results, verificationClock);
                    seed.set(() -> {
                        var source = (SourceResource) configuration.artifacts()
                                .get(bound.binding().defaultSourceId()).orElseThrow();
                        var result = new ConnectionTestResult(source.id(), source.connector(),
                                ConnectionTestResult.Outcome.PASSED, List.of(), verificationClock.millis());
                        new ConnectionTestService(ignored -> result, results,
                                new AuditGate(configuration.audit(), verificationClock), null,
                                new SourceConnectionResolver(configuration.artifacts()), verification)
                                .test(source.id(), source.connector(), source.config(), "fixture-user");
                        // Publish within the real server deadline, then age only the display clock.
                        verificationClock.now = clock.instant().plus(Duration.ofMinutes(11));
                        assertThat(verification.snapshot().status()).isEqualTo(DefaultSourceVerification.Status.EXPIRED);
                    });
                    // Control lifecycle/member availability without starting Hazelcast or a connector.
                    var business = mock(NativeBusinessContext.class);
                    when(business.available()).thenReturn(true);
                    when(business.applicationDataVerification()).thenAnswer(ignored -> verification.snapshot());
                    return business;
                });
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
                seed.get().run();
                String cookie = root.managedAuthentication().service().exchangeCode("fixture", "127.0.0.1").token();
                try (var context = new SpringApplicationBuilder(Assembly.class)
                        .environment(NativeAppStorage.isolatedEnvironment())
                        .initializers(app -> app.getBeanFactory().registerSingleton("latencyRoot", root))
                        .run("--server.address=127.0.0.1", "--server.port=0", "--spring.main.banner-mode=off")) {
                    String origin = "http://127.0.0.1:"
                            + ((WebServerApplicationContext) context).getWebServer().getPort();
                    status(http, origin, cookie, true);
                    for (int repeat = 0; repeat < 3; repeat++) {
                        var scenarios = new ArrayList<>(List.of("baseline", "config-find-40", "baseline-restored"));
                        Collections.shuffle(scenarios, new Random(7431L + repeat));
                        for (String scenario : scenarios) {
                            int millis = scenario.equals("config-find-40") ? 40 : 0;
                            long before = delay(configRaw, millis);
                            Sample sample;
                            long after;
                            try {
                                sample = status(http, origin, cookie, true);
                            } finally {
                                after = delay(configRaw, 0);
                            }
                            if (millis > 0) assertThat(after - before).as("CONFIG find delay was exercised").isPositive();
                            samples.add(new Sample(scenario, sample.elapsedMs(), sample.observation()));
                            System.out.printf("CONFIG_LATENCY scenario=%s repeat=%d elapsedMs=%d commands=%d bindingReads=%d delayHits=%d%n",
                                    scenario, repeat, sample.elapsedMs(), sample.observation().started(),
                                    sample.observation().bindingReads(), after - before);
                        }
                    }
                    root.managedAuthentication().service().invalidateJwt("jti");
                    status(http, origin, cookie, false);
                }
            }
        }
        assertThat(samples).hasSize(9);
        long baseline = median(samples, "baseline");
        long delayed = median(samples, "config-find-40");
        long restored = median(samples, "baseline-restored");
        System.out.printf("CONFIG_LATENCY_MEDIANS baselineMs=%d delayedMs=%d restoredMs=%d increaseMs=%d%n",
                baseline, delayed, restored, delayed - baseline);
        assertThat(delayed - baseline)
                .as("CONFIG find +40 ms must not amplify repeated synchronous validation into more than 3 seconds; "
                        + "baseline=%d ms, delayed=%d ms, restored=%d ms", baseline, delayed, restored)
                .isLessThanOrEqualTo(3000);
    }

    private static long median(List<Sample> samples, String scenario) {
        return samples.stream().filter(sample -> scenario.equals(sample.scenario()))
                .mapToLong(Sample::elapsedMs).sorted().toArray()[1];
    }

    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withLabels(System.getenv("TAPSTATE_TEST_RESOURCE_OWNER") == null ? Map.of()
                        : Map.of("io.tapstate.issue-loop.owner", System.getenv("TAPSTATE_TEST_RESOURCE_OWNER")))
                .withTmpFs(Map.of("/data/db", "rw", "/data/configdb", "rw"))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25",
                        "--setParameter", "enableTestCommands=1");
    }

    private static long delay(MongoClient raw, int millis) {
        var command = new Document("configureFailPoint", "failCommand")
                .append("mode", millis == 0 ? "off" : "alwaysOn");
        if (millis > 0) command.append("data", new Document("failCommands", List.of("find"))
                .append("blockConnection", true).append("blockTimeMS", millis));
        return ((Number) raw.getDatabase("admin").runCommand(command).get("count")).longValue();
    }

    private static Sample status(HttpClient http, String origin, String cookie, boolean authorized) throws Exception {
        String id = UUID.randomUUID().toString();
        var trace = new CompletableFuture<CloudLatencyCommandProbe.Observation>();
        TRACES.put(id, trace);
        try {
            var request = HttpRequest.newBuilder(URI.create(origin + "/api/storage/status"))
                    .timeout(Duration.ofSeconds(35)).header("Cookie", "__Host-tapstate-cloud-session=" + cookie)
                    .header("X-Latency-Probe", id).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{}")).build();
            long started = System.nanoTime();
            var response = http.send(request, HttpResponse.BodyHandlers.ofString());
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            var observed = trace.get(5, TimeUnit.SECONDS);
            assertThat(response.statusCode()).as(response.body()).isEqualTo(authorized ? 200 : 401);
            assertThat(observed.completed()).as("all synchronous driver commands completed").isEqualTo(observed.started());
            if (authorized) {
                assertThat(response.body()).contains("\"status\":\"EXPIRED\"");
                assertThat(observed.bindingReads()).as("real CONFIG publication checks were exercised").isGreaterThan(1);
            }
            return new Sample("", elapsed, observed);
        } finally {
            TRACES.remove(id);
        }
    }

    private record Sample(String scenario, long elapsedMs, CloudLatencyCommandProbe.Observation observation) {}

    private static final class VerificationClock extends Clock {
        private Instant now;
        VerificationClock(Instant now) { this.now = now; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(now, zone); }
        @Override public Instant instant() { return now; }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import(RestApiConfiguration.class)
    @ComponentScan(basePackageClasses = ControlHttpFace.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = "io\\.tapstate\\.control\\.restapi\\.(RestApiSecurityConfiguration|ApiExceptionHandler|StorageControlController)"))
    static class Assembly {
        @Bean AuthenticationMode mode() { return AuthenticationMode.CLOUD; }
        @Bean OperationRegistry operations() { return ControlOperations.registry(); }
        @Bean ManagedAuthenticationContext.Provider managed(StorageRoot root) { return root::managedAuthentication; }
        @Bean CredentialAuthenticator credentials(StorageRoot root) {
            return CredentialAuthenticator.scoped(root::authenticateCredentials);
        }
        @Bean StorageControlCaller callers(StorageRoot root) {
            return io.tapstate.control.restapi.StorageControlCallers.ordinary((principal, proof) ->
                    StorageControlAccess.ordinary(root.mode(),
                            () -> root.runtime().requireConfiguration().setupStates().load().scope(), principal, proof));
        }
        @Bean StorageControlDispatcher dispatcher(StorageRoot root, OperationRegistry operations) {
            return StorageControlDispatcher.staged(operations, () -> null, () -> null, () -> null,
                    (caller, active) -> root.snapshotProjection(caller));
        }
        @Bean org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter> measurements() {
            var filter = new org.springframework.web.filter.OncePerRequestFilter() {
                @Override protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest request,
                        jakarta.servlet.http.HttpServletResponse response, jakarta.servlet.FilterChain chain)
                        throws jakarta.servlet.ServletException, java.io.IOException {
                    try (var probe = new CloudLatencyCommandProbe()) {
                        try { chain.doFilter(request, response); }
                        finally {
                            var target = TRACES.get(request.getHeader("X-Latency-Probe"));
                            if (target != null) target.complete(probe.observation());
                        }
                    }
                }
            };
            var registration = new org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter>(filter);
            registration.setOrder(Integer.MIN_VALUE);
            return registration;
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
        @Override public void report(String cluster, String nonce, CloudRuntimeStatus status, String token) {
            throw new AssertionError("unexpected report");
        }
    }
}
