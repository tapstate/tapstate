package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
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
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

/** Native split stores and three packaged processes with independently faulted authorization paths. */
@RequiresDocker
class SplitStoreProcessLeaseIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo()
            .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1");
    // Keep the production lease length: packaged multi-member cold starts can exceed ten seconds.
    private static final Duration TTL = Duration.ofSeconds(30);
    // ClientSession.withTransaction retries transient failures for up to 120 seconds.
    private static final Duration CONFIGURATION_REFUSAL = Duration.ofSeconds(150);
    private static final List<String> NODES = List.of("split-a", "split-b", "split-c");
    @TempDir Path work;

    @Test @Timeout(300)
    void threeProcessesRenewWithoutConfigurationAndAReplacementReusesTheOriginalKey() {
        Prepared prepared = prepare();
        String configDb = prepared.configDb(), cacheDb = prepared.cacheDb();
        try (MongoClient configObserver = MongoClients.create(CONFIG.getReplicaSetUrl(configDb));
             MongoClient cacheObserver = MongoClients.create(CACHE.getReplicaSetUrl(cacheDb));
             var cluster = start(prepared)) {
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

    @Test @Timeout(300)
    void aLateCacheRenewalCannotReclaimTheNodeFromItsReplacement() {
        Prepared prepared = prepare();
        String oldNode = NODES.getFirst(), survivor = NODES.get(1);
        try (MongoClient observer = MongoClients.create(CACHE.getReplicaSetUrl(prepared.cacheDb()));
             var cluster = start(prepared)) {
            for (String node : NODES) cluster.awaitMembers(node, 3);
            Document original = claims(observer, prepared.cacheDb()).get(oldNode);
            String oldBoot = original.getString("ownerBootId");
            String appName = "tapstate/" + oldBoot;
            assertThat(cluster.member(survivor).clusterMembers().stream().filter(row -> row.nodeId().equals(oldNode)).toList())
                    .singleElement().satisfies(row -> assertThat(row.bootId()).isEqualTo(oldBoot));
            MongoDatabase admin = observer.getDatabase("admin");
            long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("findAndModify"))
                            .append("blockConnection", true).append("blockTimeMS", 120000).append("appName", appName)))
                    .get("count")).longValue();
            try {
                admin.runCommand(new Document("waitForFailPoint", "failCommand").append("timesEntered", entered + 1).append("maxTimeMS", 5000));
                Object held = Await.answered("the held command to be this boot's exact node renewal", Duration.ofSeconds(5),
                        () -> heldRenewal(admin, appName, oldNode, oldBoot)).get("opid");
                assertThat(held).isNotNull();
                assertThat(cluster.awaitExactly(survivor, 2, TTL.plusSeconds(30)))
                        .containsExactlyInAnyOrderElementsOf(NODES.subList(1, NODES.size()));
                assertThat(cluster.isAlive(oldNode)).as("the expired boot remains a live JVM").isTrue();
                try (var replacement = cluster.launchReplacementPrepared(oldNode)) {
                    var newControl = new ControlPlane(replacement.baseUrl());
                    newControl.login("admin", "split-process-password");
                    cluster.awaitMembers(survivor, 3);
                    Document admitted = claims(observer, prepared.cacheDb()).get(oldNode);
                    String newBoot = admitted.getString("ownerBootId");
                    assertThat(newBoot).isNotEqualTo(oldBoot);
                    assertThat(((Number) admitted.get("claimGeneration")).longValue())
                            .isGreaterThan(((Number) original.get("claimGeneration")).longValue());
                    assertThat(currentOperations(admin).stream().anyMatch(op -> held.equals(op.get("opid"))))
                            .as("replacement is admitted before the original server command resumes").isTrue();
                    Await.until("the delayed original command to finish", Duration.ofSeconds(135),
                            () -> currentOperations(admin).stream().noneMatch(op -> held.equals(op.get("opid"))),
                            () -> "original command still held");
                    Document afterReturn = claims(observer, prepared.cacheDb()).get(oldNode);
                    Await.until("the replacement to retain and renew its exact claim after the delayed operation", TTL.plusSeconds(15), () -> {
                        Document current = claims(observer, prepared.cacheDb()).get(oldNode);
                        assertThat(current.getString("ownerBootId")).isEqualTo(newBoot);
                        assertThat(current.get("claimGeneration")).isEqualTo(admitted.get("claimGeneration"));
                        return current.getDate("leaseUntil").toInstant().isAfter(afterReturn.getDate("leaseUntil").toInstant().plus(TTL));
                    }, () -> "replacement lease did not progress");
                    assertThat(cluster.member(survivor).clusterMembers().stream().filter(row -> row.nodeId().equals(oldNode)).toList())
                            .singleElement().satisfies(row -> assertThat(row.bootId()).isEqualTo(newBoot));
                    assertThat(newControl.artifactIds()).contains("views");
                    assertThat(cluster.isAlive(oldNode)).isTrue();
                    assertThat(cluster.unattributedConnections()).isZero();
                }
            } finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
        }
    }

    private Prepared prepare() {
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
        return new Prepared(clusterId, configDb, cacheDb, configUri);
    }

    private PartitionableCluster start(Prepared prepared) {
        var arguments = List.of("--tapstate.cluster.node-session-ttl=" + TTL.toSeconds() + "s", "--tapstate.cluster.node-session-renew-interval=1s",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"));
        return PartitionableCluster.startPrepared(prepared.configUri(), prepared.clusterId(), NODES,
                "admin", "split-process-password", arguments);
    }

    private record Prepared(String clusterId, String configDb, String cacheDb, String configUri) {}

    private static List<Document> currentOperations(MongoDatabase admin) {
        return admin.runCommand(new Document("currentOp", 1).append("$all", true)).getList("inprog", Document.class);
    }

    private static Optional<Document> heldRenewal(MongoDatabase admin, String appName, String node, String boot) {
        return currentOperations(admin).stream().filter(op -> appName.equals(op.getString("appName")))
                .filter(op -> op.get("command") instanceof Document command && "workload_claims".equals(command.get("findAndModify"))
                        && containsField(command.get("query"), "resourceId", node)
                        && containsField(command.get("query"), "ownerBootId", boot)).findFirst();
    }

    private static boolean containsField(Object value, String key, Object expected) {
        if (value instanceof Map<?, ?> map)
            return expected.equals(map.get(key)) || map.values().stream().anyMatch(child -> containsField(child, key, expected));
        if (value instanceof List<?> list) return list.stream().anyMatch(child -> containsField(child, key, expected));
        return false;
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
