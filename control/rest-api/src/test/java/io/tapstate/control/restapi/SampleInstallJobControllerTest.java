package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.SampleSourceCredentialsProvider;
import io.tapstate.control.core.SampleSourceService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.PlanAdvisories;
import io.tapstate.control.core.ResourceAttributionPolicy;
import io.tapstate.control.core.SchemaDerivation;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.control.core.StateDatabasePolicy;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.core.VerifiedToken;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceModel;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real sample service with controlled credentials boundaries; no connector or remote database is used. */
class SampleInstallJobControllerTest {
    private static final String SAMPLE = "sample_core_banking";
    private static final String OTHER = "sample_cards_crm";

    @Test
    void installationStatusIsReachableThroughHttpWithoutCompilerParameterMetadata() throws Exception {
        try (Fixture fixture = new Fixture(SampleInstallJobControllerTest::codedFailure)) {
            String id = fixture.start(SAMPLE).id();
            MockMvcBuilders.standaloneSetup(fixture.controller).build()
                    .perform(get("/sample-sources/installations/{jobId}", id))
                    .andExpect(status().isOk());
        }
    }

    @AfterEach
    void clearAuthentication() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void refusesExcessQueuedWorkWithoutStartingMoreCredentialRequests() throws Exception {
        try (BlockingSettings settings = new BlockingSettings(); Fixture fixture = new Fixture(settings)) {
            for (int i = 0; i < 18; i++) fixture.start(SAMPLE);
            assertThat(settings.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> fixture.start(SAMPLE)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ControlError.UNREACHABLE));
            assertThat(settings.calls.get()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void completedJobsHaveABoundedRetentionAndNewWorkCanStillBeAdmitted() throws Exception {
        try (Fixture fixture = new Fixture(SampleInstallJobControllerTest::codedFailure)) {
            String first = fixture.start(SAMPLE).id();
            fixture.finished(first);
            for (int i = 0; i < 64; i++) fixture.finished(fixture.start(SAMPLE).id());
            assertThatThrownBy(() -> fixture.controller.status(first))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
            assertThat(fixture.finished(fixture.start(SAMPLE).id()).finished()).isTrue();
        }
    }

    @Test
    void contextCloseInterruptsOwnedWorkAndDiscardsQueuedJobs() throws Exception {
        try (BlockingSettings settings = new BlockingSettings(); Fixture fixture = new Fixture(settings)) {
            for (int i = 0; i < 18; i++) fixture.start(SAMPLE);
            assertThat(settings.entered.await(5, TimeUnit.SECONDS)).isTrue();
            fixture.context.close();
            assertThat(settings.interrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(settings.calls.get()).isEqualTo(2);
            assertThatThrownBy(() -> fixture.start(SAMPLE)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ControlError.UNREACHABLE));
        }
    }

    @Test
    void programmerRuntimeFailureIsPreservedForTheAuthenticatedStatusBoundary() throws Exception {
        IllegalStateException defect = new IllegalStateException("owned-programmer-defect");
        try (Fixture fixture = new Fixture(() -> { throw defect; })) {
            String id = fixture.start(SAMPLE).id();
            assertThat(fixture.failure(id)).isSameAs(defect);
            authenticate("different-owner");
            assertThatThrownBy(() -> fixture.controller.status(id)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
        }
    }

    @Test
    void programmerErrorDoesNotLeaveAPermanentlyUnfinishedInstallation() throws Exception {
        AssertionError defect = new AssertionError("owned-invariant-defect");
        try (Fixture fixture = new Fixture(() -> { throw defect; })) {
            String id = fixture.start(SAMPLE).id();
            assertThat(fixture.failure(id)).isSameAs(defect);
            for (int i = 0; i < 64; i++) assertThat(fixture.failure(fixture.start(SAMPLE).id())).isSameAs(defect);
            assertThatThrownBy(() -> fixture.controller.status(id)).isInstanceOf(TapstateException.class);
        }
    }

    @Test
    void codedFailuresExposeTheCanonicalCodeWithoutRawDiagnosticArguments() throws Exception {
        try (Fixture fixture = new Fixture(SampleInstallJobControllerTest::codedFailure)) {
            var status = fixture.finished(fixture.start(SAMPLE).id());
            assertThat(status.items()).singleElement().satisfies(item -> {
                assertThat(item.stage()).isEqualTo("FAILED");
                assertThat(item.errorCode()).isEqualTo(ControlError.MALFORMED_REQUEST.code());
                assertThat(item.errorDetail()).isNullOrEmpty();
            });
            assertThat(status.toString()).doesNotContain("owned-password-sentinel");
        }
    }

    @Test
    void invalidSelectionsNeverFetchCredentials() {
        AtomicInteger calls = new AtomicInteger();
        try (Fixture fixture = new Fixture(() -> { calls.incrementAndGet(); return codedFailure(); })) {
            for (List<String> ids : List.of(List.<String>of(), List.of(SAMPLE, SAMPLE), List.of("unknown"))) {
                assertThatThrownBy(() -> fixture.controller.start(new SampleInstallJobController.Request(ids)))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(ControlError.MALFORMED_REQUEST));
            }
            assertThatThrownBy(() -> fixture.controller.start(new SampleInstallJobController.Request(null)))
                    .isInstanceOf(TapstateException.class);
            assertThat(calls).hasValue(0);
        }
    }

    @Test
    void ordinaryCodedFailureDoesNotSkipTheOtherSelectedSampleAndJobsBelongToTheirCaller() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Fixture fixture = new Fixture(() -> { calls.incrementAndGet(); return codedFailure(); })) {
            var status = fixture.finished(fixture.start(SAMPLE, OTHER).id());
            assertThat(calls).hasValue(2);
            assertThat(status.items()).extracting(SampleInstallJobController.Item::stage)
                    .containsExactly("FAILED", "FAILED");
            authenticate("different-owner");
            assertThatThrownBy(() -> fixture.controller.status(status.id())).isInstanceOf(TapstateException.class);
        }
    }

    @Test
    void cloudCreationCapturesTheVerifiedUserIdBeforeTheRequestContextChangesAndRefreshPreservesIt() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        String verifiedUserId = "cloud-user-uuid-42";
        try (Fixture fixture = new Fixture(() -> {
            assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
            if (calls.getAndIncrement() == 0) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("owned Cloud request not released");
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("owned Cloud request interrupted", error);
                }
            }
            return Map.of("host", "sample.invalid", "port", 3306, "database", "sample",
                    "username", "sample", "password", "owned-sample-password");
        }, true)) {
            SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(
                    TapstatePrincipal.cloudSession(new VerifiedToken(verifiedUserId, Scope.WRITE))));
            String id = fixture.start(SAMPLE).id();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            authenticate("other-request-subject");
            release.countDown();
            authenticate(verifiedUserId);
            assertThat(fixture.finished(id).items()).extracting(SampleInstallJobController.Item::stage)
                    .containsExactly("READY");
            var stored = fixture.store.get(SAMPLE).orElseThrow();
            assertThat(stored.metadata().cloud()).isTrue();
            assertThat(stored.metadata().userId()).isEqualTo(verifiedUserId);
            assertThat(fixture.sources.get(SAMPLE).config()).doesNotContainKey("password");

            authenticate("second-verified-user");
            fixture.finished(fixture.start(SAMPLE).id());
            assertThat(fixture.store.get(SAMPLE).orElseThrow().metadata().userId()).isEqualTo(verifiedUserId);
        } finally {
            release.countDown();
        }
    }

    private static Map<String, Object> codedFailure() {
        throw new TapstateException(ControlError.MALFORMED_REQUEST,
                Map.of("reason", "owned-password-sentinel"), null);
    }

    private static void authenticate(String subject) {
        var principal = TapstatePrincipal.humanJwt(new VerifiedToken(subject, Scope.WRITE));
        SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(principal));
    }

    private static final class Fixture implements AutoCloseable {
        private final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        private final SampleInstallJobController controller;
        private final PipelineApiTest.FakeArtifactStore store = new PipelineApiTest.FakeArtifactStore();
        private final SourceProjectionService sources;

        private Fixture(Supplier<Map<String, Object>> settings) {
            this(settings, false);
        }

        private Fixture(Supplier<Map<String, Object>> settings, boolean cloud) {
            Clock clock = Clock.systemUTC();
            var audit = new AuditGate(new PipelineApiTest.RecordingAuditStore(), clock);
            var services = new PipelineApiTest.TestApp();
            SchemaStore schemas = new SchemaStore() {
                @Override public void save(DiscoveredSourceModel model) { }
                @Override public Optional<DiscoveredSourceModel> get(String id) { return Optional.empty(); }
            };
            var apply = cloud ? new ApplyService(TapstateCatalog::load, store, audit, schemas,
                    PlanAdvisories.none(), SchemaDerivation.none(), null, ResourceAttributionPolicy.managedCloud(),
                    StateDatabasePolicy.CLOUD)
                    : services.applyService(store, audit);
            sources = new SourceProjectionService(apply,
                    services.artifactQueryService(store), services.artifactMutationService(store, audit),
                    new SourceRepresentation(TapstateCatalog::load));
            var unusedProbes = new AuthTest.TestApp();
            var connections = cloud ? new ConnectionTestService(config -> new ConnectionTestResult(
                    config.id(), config.connectorId(), ConnectionTestResult.Outcome.PASSED, List.of(), clock.millis()),
                    new ConnectionTestResultStore() {
                        @Override public void save(ConnectionTestResult result) { }
                        @Override public Optional<ConnectionTestResult> find(String id) { return Optional.empty(); }
                    }, audit, null,
                    io.tapstate.control.core.SourceConnectionResolver.forAdHocConnections())
                    : unusedProbes.connectionTestService(audit);
            var discovery = cloud ? new SchemaDiscoveryService(config -> new SourceModel(List.of()),
                    schemas, audit, clock, null,
                    io.tapstate.control.core.SourceConnectionResolver.forAdHocConnections())
                    : unusedProbes.schemaDiscoveryService(audit);
            var samples = new SampleSourceService(sources, discovery, connections,
                    catalog(), new SampleSourceCredentialsProvider() {
                        @Override public boolean available() { return true; }
                        @Override public Credentials fetch() { throw new AssertionError("legacy fetch is not selected"); }
                        @Override public Map<String, Object> settingsFor(String id) { return settings.get(); }
                    });
            context.registerBean(SampleInstallJobController.class, () -> new SampleInstallJobController(samples));
            context.refresh();
            controller = context.getBean(SampleInstallJobController.class);
            authenticate("sample-owner");
        }

        private SampleInstallJobController.Status start(String... ids) {
            return controller.start(new SampleInstallJobController.Request(List.of(ids)));
        }

        private SampleInstallJobController.Status finished(String id) throws InterruptedException {
            for (int i = 0; i < 500; i++) {
                var status = controller.status(id);
                if (status.finished()) return status;
                Thread.sleep(10);
            }
            throw new AssertionError("installation did not finish");
        }

        private Throwable failure(String id) throws InterruptedException {
            for (int i = 0; i < 500; i++) {
                try { controller.status(id); }
                catch (RuntimeException | Error error) { return error; }
                Thread.sleep(10);
            }
            throw new AssertionError("original installation defect was not observable");
        }

        @Override public void close() { context.close(); }
    }

    private static ConnectorCatalogView catalog() {
        TapstateCatalog bundled = TapstateCatalog.load();
        ConnectorCatalogStore store = new ConnectorCatalogStore() {
            @Override public void upsert(ConnectorCatalogEntry entry) { throw new AssertionError("no registration"); }
            @Override public Optional<ConnectorCatalogEntry> get(String id) { return Optional.empty(); }
            @Override public List<ConnectorCatalogEntry> list() { return bundled.all(); }
        };
        ConnectorSpecStore specs = new ConnectorSpecStore() {
            @Override public void put(String hash, byte[] bytes) { throw new AssertionError("no spec write"); }
            @Override public Optional<byte[]> get(String hash) { throw new AssertionError("no spec read"); }
        };
        ConnectorRegistry registry = new ConnectorRegistry() {
            @Override public io.tapstate.spi.store.RegistrationOutcome register(String id, String version,
                    io.tapstate.spi.store.RegistrationSource source, byte[] bytes) {
                throw new AssertionError("no connector registration");
            }
            @Override public List<io.tapstate.spi.store.ConnectorRegistration> list() {
                throw new AssertionError("no connector lookup");
            }
            @Override public Optional<byte[]> artifact(String hash) { throw new AssertionError("no connector load"); }
            @Override public boolean hasArtifact(String hash) { throw new AssertionError("no connector lookup"); }
        };
        return new ConnectorCatalogView(bundled, store, specs, registry);
    }

    private static final class BlockingSettings implements Supplier<Map<String, Object>>, AutoCloseable {
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(2);
        private final CountDownLatch interrupted = new CountDownLatch(2);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override public Map<String, Object> get() {
            calls.incrementAndGet();
            entered.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("owned credentials were not released");
            } catch (InterruptedException error) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
            }
            return codedFailure();
        }

        @Override public void close() { release.countDown(); }
    }
}
