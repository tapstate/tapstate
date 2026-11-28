package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.spi.store.AlertNodeLifecycleStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;

@RequiresDocker
class NativeNodeLifecycleIT {
    @Container static final MongoDBContainer MONGO = AlertEvaluationHostIT.MONGO;
    @TempDir Path work;

    @Test void anOrderlyShutdownLeavesLegacyRetirementHistoryUnchangedAcrossReopen() {
        exercise(false);
    }

    @Test void aFailedBusinessOwnerDoesNotMarkLegacyExpectationAsAnOrderlyStop() {
        exercise(true);
    }

    private void exercise(boolean failClosed) {
        var storage = NativeAppStorage.createOnPremWithAdmin(MONGO, "native_node", "node-admin", "isolated-node-password");
        var settings = new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5));
        try (var observer = MongoClients.create(storage.configurationUri())) {
            var db = observer.getDatabase(storage.configurationDatabase());
            var rows = SystemCollections.ALERT_NODE_LIFECYCLES.on(db);
            try {
                var historical = seedLegacyBoot(storage, settings, failClosed);
                Document retained = rows.find().first();
                assertThat(retained).isNotNull();
                try (var application = new SpringApplicationBuilder(Bootstrap.class)
                        .environment(CloudFixtureEnvironment.isolated()).run(
                                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                                "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                                "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"),
                                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                                "--tapstate.control.auth.jwt-secret=isolated-node-test-signing")) {
                    NativeAppStorage.awaitBusiness(application);
                    var owner = (NativeBusinessContext) application.getBean(StorageRoot.class).runtime().business().orElseThrow();
                    var lease = NativeAppStorage.business(application).getBean(NodeSessionLease.class);
                    assertThat(lease.currentFence()).hasValueSatisfying(fence -> {
                        assertThat(fence.owner().nodeId()).isEqualTo(historical.registration().key().nodeId());
                        assertThat(fence.owner().bootId()).isNotEqualTo(historical.registration().key().bootId());
                    });
                    await().atMost(Duration.ofSeconds(15)).until(() -> owner.alertHealth()
                            .map(health -> health.maintenance().get("lastSuccess") != null).orElse(false));
                    assertThat(rows.find()).containsExactly(retained);
                    assertThat(SystemCollections.ALERT_RULES.on(db).countDocuments()).isZero();
                    assertThat(SystemCollections.ALERT_INSTANCES.on(db).countDocuments()).isZero();
                    if (failClosed) {
                        owner.close();
                        assertThat(rows.find()).containsExactly(retained);
                    }
                }
                assertThat(rows.find()).containsExactly(retained);
                try (var reopened = MongoConfigurationDomain.open(settings, storage.clusterId())) {
                    var setup = historical.registration().setup();
                    var lifecycle = reopened.alertNodeLifecycles(historical.registration().key().scope(), setup, () -> {});
                    assertThat(lifecycle.list()).containsExactly(historical);
                    assertThat(rows.find()).containsExactly(retained);
                }
            } finally {
                db.drop();
                observer.getDatabase(storage.cacheDatabase()).drop();
                observer.getDatabase(storage.masterDatabase()).drop();
            }
        }
    }
    private static AlertNodeLifecycleStore.Row seedLegacyBoot(NativeAppStorage storage,
            MongoConnectionSettings settings, boolean leaveExpected) {
        try (var domain = MongoConfigurationDomain.open(settings, storage.clusterId())) {
            var setup = domain.bindings().load().orElseThrow().setup();
            var scope = new AlarmKey.Scope(setup.installationId(), storage.clusterId());
            var historicalBoot = new WorkloadOwner(storage.clusterId(), UUID.randomUUID().toString());
            var key = new AlertNodeLifecycleStore.Key(scope, historicalBoot.nodeId(), historicalBoot.bootId());
            var session = new WorkloadClaimFence(
                    new WorkloadClaimKey(storage.clusterId(), WorkloadClaimType.NODE_SESSION, historicalBoot.nodeId()),
                    historicalBoot, 1, 0, 1);
            var registration = new AlertNodeLifecycleStore.Registration(key, setup, storage.preparation().authScope(),
                    session, UUID.randomUUID().toString(), 1);
            var lifecycle = domain.alertNodeLifecycles(scope, setup, () -> {});
            var registeredAt = Instant.parse("2026-01-01T00:00:00Z");
            var expected = lifecycle.register(registration, registeredAt, () -> {});
            return leaveExpected ? expected : lifecycle.retire(registration, expected.revision(),
                    UUID.randomUUID().toString(), registeredAt.plusSeconds(1), () -> {});
        }
    }
}
