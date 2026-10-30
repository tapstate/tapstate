package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** One real production member with independently faultable configuration and cache replica sets. */
@RequiresDocker
class SplitStoreLeaseLifecycleIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");
    private static final Duration TTL = Duration.ofSeconds(10);
    @TempDir Path work;

    @Test
    void configurationOutageDoesNotStopCacheRenewalsButAnUnansweredCacheRenewalLosesTheOriginalProof() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "split_config_" + suffix, cacheDb = "split_cache_" + suffix;
        String configUri = options(CONFIG.getReplicaSetUrl(configDb), "socketTimeoutMS=1500&connectTimeoutMS=1500");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                options(CACHE.getReplicaSetUrl(cacheDb), "socketTimeoutMS=30000"), cacheDb, null);
        var scope = new StorageSetupScope("split-" + suffix, "installation", "setup-" + suffix, 1);
        try (var configuration = MongoConfigurationBootstrap.prepare(new MongoConnectionSettings(configUri, null, Duration.ofSeconds(3)),
                scope.clusterId(), scope.installationId(), scope.operationId(), 1)) {
            var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", CACHE.getReplicaSetUrl("split_views_" + suffix)),
                    null, null, null, null);
            var committed = configuration.bindings().commitWithFirstCache(scope, StorageBinding.Mode.OP, binding, target);
            var preparation = configuration.bindings().cachePreparation().orElseThrow();
            try (var cache = MongoCacheDomain.connect(binding, Duration.ofSeconds(3), "split-fixture")) {
                cache.prepareFirstAuth(committed.firstCreation().orElseThrow());
                cache.prepareInitialBusiness(preparation, () -> {});
            }
            configuration.bindings().complete(scope);
            configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                    new io.tapstate.spi.store.User("admin", new BCryptPasswordHasher().hash("split-test-password"), "admin"));
        }

        try (var app = start(configUri, "original");
             MongoClient observer = MongoClients.create(CACHE.getReplicaSetUrl(cacheDb))) {
            var root = app.getBean(StorageRoot.class);
            await().atMost(Duration.ofSeconds(20)).until(root.runtime()::businessStarted);
            var child = (ConfigurableApplicationContext) ReflectionTestUtils.getField(root.runtime().business().orElseThrow(), "context");
            var lease = child.getBean(NodeSessionLease.class);
            var member = child.getBean(HazelcastInstance.class);
            var originalFence = lease.currentFence().orElseThrow();
            var bound = root.runtime().current().orElseThrow();
            var claims = bound.cache().workloadClaims(bound.preparation().businessScope());
            var initial = claims.read(originalFence.key()).orElseThrow().claim();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(claims.read(initial.key()).orElseThrow().claim().leaseUntil()).isAfter(initial.leaseUntil()));
            assertThat(root.runtime().requireConfiguration().artifacts().get("views")).isPresent();
            try (MongoClient configObserver = MongoClients.create(CONFIG.getReplicaSetUrl(configDb))) {
                assertThat(configObserver.getDatabase(configDb).listCollectionNames().into(new ArrayList<>()))
                        .isNotEmpty().doesNotContain("workload_claims", "keyring_acks", "source_config_keyring_sessions");
            }

            CONFIG.getDockerClient().pauseContainerCmd(CONFIG.getContainerId()).exec();
            try {
                assertThatThrownBy(() -> root.runtime().requireConfiguration().artifacts().get("views"))
                        .isInstanceOf(TapstateException.class);
                try (var unavailable = start(configUri, "unavailable")) {
                    var blocked = unavailable.getBean(StorageRoot.class);
                    assertThat(blocked.phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_UNAVAILABLE);
                    assertThat(blocked.runtime().business()).isEmpty();
                }
                await().atMost(TTL.plusSeconds(5)).untilAsserted(() -> {
                    var renewed = claims.read(initial.key()).orElseThrow();
                    assertThat(renewed.leased()).isTrue();
                    assertThat(renewed.claim().leaseUntil()).isAfter(initial.leaseUntil().plus(TTL));
                    assertThat(lease.currentFence()).contains(originalFence);
                    assertThat(member.getLifecycleService().isRunning()).isTrue();
                });
            } finally { CONFIG.getDockerClient().unpauseContainerCmd(CONFIG.getContainerId()).exec(); }
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(root.runtime().requireConfiguration().artifacts().get("views")).isPresent());

            var admin = observer.getDatabase("admin");
            String appName = "tapstate/" + app.getBean(BootId.class).value();
            long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("findAndModify"))
                            .append("blockConnection", true).append("blockTimeMS", 25000).append("appName", appName)))
                    .get("count")).longValue();
            try {
                admin.runCommand(new Document("waitForFailPoint", "failCommand").append("timesEntered", entered + 1).append("maxTimeMS", 5000));
                long originalDeadline = (long) ReflectionTestUtils.getField(lease, "provenUntil");
                assertThat(originalDeadline - System.nanoTime()).isPositive();
                // Observe closure without invoking a lease accessor that can trigger expiry itself.
                AtomicBoolean closed = (AtomicBoolean) ReflectionTestUtils.getField(lease, "closed");
                await().atMost(TTL.plusSeconds(2)).untilTrue(closed);
                assertThat(lease.currentFence()).isEmpty();
                assertThat(System.nanoTime()).isGreaterThanOrEqualTo(originalDeadline);
                assertThat(ReflectionTestUtils.getField(lease, "provenUntil")).isEqualTo(originalDeadline);
                await().atMost(Duration.ofSeconds(10)).until(() -> !member.getLifecycleService().isRunning());
                assertThat(root.runtime().requireConfiguration().artifacts().get("views")).isPresent();
                assertThat(observer.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isEqualTo(1);
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
            assertThat(lease.currentFence()).isEmpty();
        }
    }

    private ConfigurableApplicationContext start(String configUri, String name) {
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.store.mongo.uri=" + configUri, "--tapstate.store.mongo.server-selection-timeout=2s",
                "--tapstate.cluster.node-session-ttl=" + TTL.toSeconds() + "s", "--tapstate.cluster.node-session-renew-interval=1s",
                "--tapstate.control.auth.jwt-secret=split-store-stable-test-key",
                "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"),
                "--tapstate.connectors.plugins-dir=" + work.resolve(name));
    }

    private static String options(String uri, String options) { return uri + (uri.contains("?") ? "&" : "?") + options; }
}
