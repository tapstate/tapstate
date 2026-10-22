package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.app.BCryptPasswordHasher;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.User;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** Native split stores, three packaged processes, and a real configuration-only outage. */
@RequiresDocker
class SplitStoreProcessLeaseIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    private static final Duration TTL = Duration.ofSeconds(10);
    // ClientSession.withTransaction retries transient failures for up to 120 seconds.
    private static final Duration CONFIGURATION_REFUSAL = Duration.ofSeconds(150);
    private static final List<String> NODES = List.of("split-a", "split-b", "split-c");
    @TempDir Path work;

    @Test @Timeout(300)
    void threeProcessesRenewWithoutConfigurationAndAReplacementReusesTheOriginalKey() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String clusterId = "split-process-" + suffix;
        String configDb = "config_" + suffix, cacheDb = "cache_" + suffix;
        String configUri = CONFIG.getReplicaSetUrl(configDb) + "?socketTimeoutMS=1500&connectTimeoutMS=1500";
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                CACHE.getReplicaSetUrl(cacheDb), cacheDb, null);
        var scope = new StorageSetupScope(clusterId, "installation", "setup", 1);
        try (var configuration = MongoConfigurationBootstrap.prepare(
                new MongoConnectionSettings(configUri, null, Duration.ofSeconds(3)), clusterId, "installation", "setup", 1)) {
            var target = new SourceResource("views", null, "mongodb",
                    Map.of("isUri", true, "uri", CACHE.getReplicaSetUrl("master_" + suffix)), null, null, null, null);
            var committed = configuration.bindings().commitWithFirstCache(scope, StorageBinding.Mode.OP, binding, target);
            try (var cache = MongoCacheDomain.connect(binding, Duration.ofSeconds(3), "fixture")) {
                cache.prepareFirstAuth(committed.firstCreation().orElseThrow());
                cache.prepareInitialBusiness(committed.preparation(), () -> {});
            }
            configuration.bindings().complete(scope);
            configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                    new User("admin", new BCryptPasswordHasher().hash("split-process-password"), "admin"));
        }
        var arguments = List.of("--tapstate.cluster.node-session-ttl=10s", "--tapstate.cluster.node-session-renew-interval=1s",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"));
        try (MongoClient configObserver = MongoClients.create(CONFIG.getReplicaSetUrl(configDb));
             MongoClient cacheObserver = MongoClients.create(CACHE.getReplicaSetUrl(cacheDb));
             var cluster = PartitionableCluster.startPrepared(configUri, clusterId, NODES,
                     "admin", "split-process-password", arguments)) {
            for (String node : NODES) assertThat(cluster.awaitMembers(node, 3)).containsExactlyInAnyOrderElementsOf(NODES);
            var keys = configObserver.getDatabase(configDb).getCollection("cluster_keys").find().into(new ArrayList<>());
            assertThat(keys).hasSize(1);
            assertThat(cluster.member(NODES.getFirst()).artifactIds()).contains("views");
            var original = claims(cacheObserver, cacheDb);
            assertThat(original.keySet()).containsExactlyInAnyOrderElementsOf(NODES);
            CONFIG.getDockerClient().pauseContainerCmd(CONFIG.getContainerId()).exec();
            try {
                long pausedAt = System.nanoTime();
                var paused = claims(cacheObserver, cacheDb);
                assertThat(cluster.member(NODES.getFirst()).artifactIdsIfConfigurationAvailable(CONFIGURATION_REFUSAL)).isEmpty();
                Await.until("all three original boots to renew beyond one lease while configuration is paused",
                        TTL.plusSeconds(15), () -> {
                            var current = claims(cacheObserver, cacheDb);
                            return System.nanoTime() - pausedAt > TTL.toNanos() && NODES.stream().allMatch(node -> current.containsKey(node)
                                    && current.get(node).getString("ownerBootId").equals(original.get(node).getString("ownerBootId"))
                                    && current.get(node).get("claimGeneration").equals(original.get(node).get("claimGeneration"))
                                    && current.get(node).getDate("leaseUntil").toInstant()
                                            .isAfter(paused.get(node).getDate("leaseUntil").toInstant().plus(TTL)));
                        }, () -> "original node leases did not all advance");
                for (String node : NODES) assertThat(cluster.isAlive(node)).isTrue();
            } finally { CONFIG.getDockerClient().unpauseContainerCmd(CONFIG.getContainerId()).exec(); }
            Await.until("configuration reads to recover", CONFIGURATION_REFUSAL,
                    () -> cluster.member(NODES.getFirst()).artifactIdsIfConfigurationAvailable(CONFIGURATION_REFUSAL)
                            .filter(ids -> ids.contains("views")).isPresent(), () -> "configuration unavailable");
            String oldBoot = original.get(NODES.getFirst()).getString("ownerBootId");
            long generation = ((Number) original.get(NODES.getFirst()).get("claimGeneration")).longValue();
            cluster.restart(NODES.getFirst());
            cluster.awaitMembers(NODES.getFirst(), 3);
            var replacement = claims(cacheObserver, cacheDb).get(NODES.getFirst());
            assertThat(replacement.getString("ownerBootId")).isNotEqualTo(oldBoot);
            assertThat(((Number) replacement.get("claimGeneration")).longValue()).isGreaterThan(generation);
            assertThat(configObserver.getDatabase(configDb).getCollection("cluster_keys").find().into(new ArrayList<>())).isEqualTo(keys);
            assertThat(configObserver.getDatabase(configDb).listCollectionNames().into(new ArrayList<>()))
                    .doesNotContain("workload_claims", "keyring_acks", "source_config_keyring_sessions");
            assertThat(cluster.unattributedConnections()).isZero();
        }
    }

    private static Map<String, Document> claims(MongoClient observer, String database) {
        var rows = new LinkedHashMap<String, Document>();
        observer.getDatabase(database).getCollection("workload_claims")
                .find(new Document("resourceType", "NODE_SESSION"))
                .forEach(row -> rows.put(row.getString("resourceId"), row));
        return rows;
    }

    private static MongoDBContainer mongo() {
        return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withMemory(768L * 1024 * 1024).withNanoCPUs(1_000_000_000L));
    }
}
