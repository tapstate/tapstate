package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import static org.assertj.core.api.Assertions.assertThat;

/** Real HTTP security and storage dispatch retain one authenticated identity for each request. */
class StorageHttpAuthenticationTest {
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "installation", "operation", 1);
    private static final AtomicBoolean REVOKED = new AtomicBoolean();
    private static final AtomicBoolean OWNER_ACTIVE = new AtomicBoolean(true);
    private static final AtomicInteger AUTHENTICATIONS = new AtomicInteger();
    private static final AtomicInteger REAUTHENTICATIONS = new AtomicInteger();
    private static final AtomicInteger LOCAL_CHECKS = new AtomicInteger();
    private static final AtomicInteger PROBES = new AtomicInteger();
    private static final AtomicInteger SETUP_CHECKS = new AtomicInteger();
    private static final AtomicLong GENERATION = new AtomicLong(1);
    private static final AtomicReference<Runnable> DURING_PROBE = new AtomicReference<>(() -> {});
    private static final AtomicReference<Runnable> DURING_NORMALIZATION = new AtomicReference<>(() -> {});
    private static final AtomicReference<Scope> SCOPE = new AtomicReference<>(Scope.WRITE);
    private static ConfigurableApplicationContext context;
    private static HttpClient client;
    private static URI endpoint;

    @BeforeAll static void start() {
        context = new SpringApplicationBuilder(Assembly.class).run("--server.address=127.0.0.1", "--server.port=0");
        client = HttpClient.newHttpClient();
        endpoint = URI.create("http://127.0.0.1:"
                + ((WebServerApplicationContext) context).getWebServer().getPort() + "/api/storage/test");
    }
    @AfterAll static void close() {
        if (client != null) client.close();
        if (context != null) context.close();
    }
    @BeforeEach void reset() {
        REVOKED.set(false); OWNER_ACTIVE.set(true); GENERATION.set(1); SCOPE.set(Scope.WRITE);
        AUTHENTICATIONS.set(0); REAUTHENTICATIONS.set(0); LOCAL_CHECKS.set(0); PROBES.set(0); SETUP_CHECKS.set(0);
        DURING_PROBE.set(() -> {});
        DURING_NORMALIZATION.set(() -> {});
    }

    @Test void storageTestAuthenticatesOnceAcrossDispatcherAndTransactionGuards() throws Exception {
        var response = request("tcs_fixture");
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        assertThat(response.body()).contains("testStamp", "cache-database-empty");
        assertThat(AUTHENTICATIONS).hasValue(1);
        assertThat(REAUTHENTICATIONS).as("request guards retain the entry authentication").hasValue(0);
        assertThat(LOCAL_CHECKS.get()).isPositive();
        assertThat(SETUP_CHECKS).hasValue(2);
        assertThat(PROBES).hasValue(1);
    }

    @Test void revocationTakesEffectOnTheNextRequest() throws Exception {
        DURING_PROBE.set(() -> REVOKED.set(true));
        assertThat(request("tcs_fixture").statusCode()).isEqualTo(200);
        assertThat(request("tcs_fixture").statusCode()).isEqualTo(401);
        assertThat(AUTHENTICATIONS).hasValue(2);
        assertThat(REAUTHENTICATIONS).hasValue(0);
        assertThat(PROBES).hasValue(1);
    }

    @Test void installationGenerationIsStillCheckedAfterTheProbe() throws Exception {
        DURING_PROBE.set(() -> GENERATION.incrementAndGet());
        var response = request("tcs_fixture");
        assertThat(response.statusCode()).as(response.body()).isEqualTo(409);
        assertThat(response.body()).contains("control.storage-scope-conflict").doesNotContain("testStamp");
        assertThat(AUTHENTICATIONS).hasValue(1);
        assertThat(REAUTHENTICATIONS).hasValue(0);
        assertThat(SETUP_CHECKS).hasValue(2);
    }

    @Test void installationGenerationIsCheckedAfterNormalizationBeforeAnyProbe() throws Exception {
        DURING_NORMALIZATION.set(() -> GENERATION.incrementAndGet());
        var response = request("tcs_fixture");
        assertThat(response.statusCode()).as(response.body()).isEqualTo(409);
        assertThat(PROBES).hasValue(0);
        assertThat(SETUP_CHECKS).hasValue(1);
    }

    @Test void closingTheSelectedOwnerStillRejectsTheRequest() throws Exception {
        DURING_PROBE.set(() -> OWNER_ACTIVE.set(false));
        var response = request("tcs_fixture");
        assertThat(response.statusCode()).as(response.body()).isEqualTo(503);
        assertThat(response.body()).doesNotContain("testStamp");
        assertThat(LOCAL_CHECKS.get()).isPositive();
    }

    @Test void invalidCookiesAndInsufficientPermissionsAreRejectedAtEntry() throws Exception {
        assertThat(request("tcs_invalid").statusCode()).isEqualTo(401);
        SCOPE.set(Scope.READ);
        assertThat(request("tcs_fixture").statusCode()).isEqualTo(403);
        assertThat(PROBES).hasValue(0);
        assertThat(REAUTHENTICATIONS).hasValue(0);
    }

    private static HttpResponse<String> request(String cookie) throws Exception {
        var candidate = Map.of("cache", connection("cache"), "master", connection("master"));
        String body = JsonWriter.write(Map.of("operationId", SETUP.operationId(), "generation", 1,
                "candidate", candidate, "layer", "CACHE"));
        return client.send(HttpRequest.newBuilder(endpoint)
                .header("Cookie", "__Host-tapstate-cloud-session=" + cookie)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    private static Map<String,Object> connection(String database) {
        return Map.of("kind", "ATLAS", "uri", "mongodb://fixture.invalid/" + database, "database", database);
    }
    private static StorageSetupScope currentScope() {
        return new StorageSetupScope(SETUP.clusterId(), SETUP.installationId(), SETUP.operationId(), GENERATION.get());
    }
    private static void localCheck() {
        LOCAL_CHECKS.incrementAndGet();
        if (!OWNER_ACTIVE.get()) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
    }
    private static final class States implements StorageSetupStateStore {
        private final Runnable guard;
        States(Runnable guard) { this.guard = guard; }
        public StorageSetupStateStore guardedBy(Runnable active) { return new States(active); }
        public Status load() {
            guard.run();
            var result = new Status(currentScope(), Phase.CONFIG_PREPARED, 0);
            guard.run();
            return result;
        }
        public View loadView() { return new View(load(), Optional.empty(), Optional.empty()); }
        public View requireCurrentView(StorageSetupScope expected, Purpose purpose) {
            requireCurrent(expected, purpose); return loadView();
        }
        public Status rotate(StorageSetupScope expected, Purpose purpose) { throw new AssertionError("unexpected rotate"); }
        public void requireCurrent(StorageSetupScope expected, Purpose purpose) {
            guard.run();
            SETUP_CHECKS.incrementAndGet();
            if (!currentScope().equals(expected))
                throw new TapstateException(ControlError.STORAGE_SCOPE_CONFLICT, Map.of(), null);
            guard.run();
        }
    }

    @SpringBootConfiguration @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class, StorageControlController.class, ApiExceptionHandler.class})
    static class Assembly {
        @Bean AuthenticationMode mode() { return AuthenticationMode.CLOUD; }
        @Bean OperationRegistry operations() { return ControlOperations.registry(); }
        @Bean CredentialAuthenticator credentials() {
            return CredentialAuthenticator.scoped(raw -> {
                AUTHENTICATIONS.incrementAndGet();
                if (!"tcs_fixture".equals(raw) || REVOKED.get()) return Optional.empty();
                return Optional.of(new CredentialAuthentication(new VerifiedToken("user", SCOPE.get()), Optional.empty(),
                        Optional.of("organization"), () -> {
                            REAUTHENTICATIONS.incrementAndGet();
                            localCheck();
                            if (REVOKED.get()) throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
                        }, Optional.of(new CredentialEvidence(CredentialType.CLOUD_SESSION, Optional.empty(),
                                StorageHttpAuthenticationTest::localCheck)), Optional.of(SETUP)));
            });
        }
        @Bean StorageControlCaller caller() {
            return StorageControlCallers.ordinary((principal, original) ->
                    StorageControlAccess.ordinary(StorageBinding.Mode.CLOUD, StorageHttpAuthenticationTest::currentScope,
                            principal, original));
        }
        @Bean StorageSetupService setup() {
            var bindings = (StorageBindingStore) Proxy.newProxyInstance(StorageBindingStore.class.getClassLoader(),
                    new Class<?>[]{StorageBindingStore.class}, (proxy, method, args) -> { throw new AssertionError("unexpected binding access"); });
            var probe = new StorageConnectionProbe() {
                public StorageBinding.Connection normalize(StorageBinding.Connection candidate) {
                    DURING_NORMALIZATION.get().run(); return candidate;
                }
                public void test(StorageBinding.Connection candidate) { throw new AssertionError("unexpected generic probe"); }
                public CacheTestResult testCache(StorageBinding.Connection candidate) {
                    PROBES.incrementAndGet(); DURING_PROBE.get().run(); return CacheTestResult.emptyDatabase();
                }
            };
            var target = new StorageSetupService.DefaultTarget() {
                public StorageSetupService.ConnectorIdentity identity(StorageBinding.Connection c) { throw new AssertionError("unexpected target"); }
                public SourceResource definition(StorageBinding.Connection c, String id) { throw new AssertionError("unexpected target"); }
                public void test(StorageBinding.Connection c, SourceResource source, StorageSetupService.ConnectorIdentity id) { throw new AssertionError("unexpected target"); }
            };
            var config = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://fixture.invalid/config", "config", null);
            return new StorageSetupService(StorageBinding.Mode.CLOUD, bindings, new States(() -> {}), probe, target,
                    () -> Optional.of(config), new AuditGate(record -> {}, Clock.systemUTC()), Clock.systemUTC(), () -> {});
        }
        @Bean StorageControlDispatcher dispatcher(OperationRegistry operations, StorageSetupService setup) {
            return StorageControlDispatcher.staged(operations, () -> setup, () -> null, () -> null,
                    (caller, active) -> { throw new AssertionError("unexpected snapshot"); });
        }
    }
}
