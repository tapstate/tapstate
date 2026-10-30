package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ControlError;
import io.tapstate.spi.store.StorageBinding;
import java.util.List;
import java.util.UUID;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** A stateless bearer still checks the original durable authentication scope on every HTTP request. */
@io.tapstate.testsupport.RequiresDocker
class NativeAuthenticationStoreOutageIT {
    enum Layer { CONFIG, CACHE }
    enum LateFault { CREDENTIAL_RECORD, CUMULATIVE_CHECKS }

    @ParameterizedTest
    @EnumSource(LateFault.class)
    void credentialReadsAndRepeatedChecksShareTheAuthenticationDeadline(LateFault fault) throws Exception {
        try (NetworkedMongo mongo = NetworkedMongo.startWithTestCommands();
             PreparedNativeStore storage = PreparedNativeStore.createOn(mongo);
             var raw = MongoClients.create(storage.configurationUri());
             RealProcessServer server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                     port -> List.of("--tapstate.cluster.id=" + storage.clusterId(), "--tapstate.hz.member-port=0",
                             "--tapstate.hz.jet.cooperative-thread-count=2"));
             HttpClient http = HttpClient.newHttpClient()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            String machine = control.mintToken("ADMIN");
            var configuration = raw.getDatabase(storage.configurationDatabase());
            var cache = raw.getDatabase(storage.cacheDatabase());
            Document token = SystemCollections.CONFIG_TOKENS.on(configuration).find().first();
            Document auth = authIdentity(SystemCollections.CACHE_IDENTITY.on(cache).find().first());
            var admin = raw.getDatabase("admin");
            boolean credentialRead = fault == LateFault.CREDENTIAL_RECORD;
            String namespace = credentialRead
                    ? storage.configurationDatabase() + "." + SystemCollections.CONFIG_TOKENS.collectionName()
                    : storage.cacheDatabase() + "." + SystemCollections.CACHE_IDENTITY.collectionName();
            long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("find"))
                            .append("namespace", namespace).append("blockConnection", true)
                            .append("blockTimeMS", credentialRead ? 20000 : 4000))).get("count")).longValue();
            try {
                if (credentialRead) {
                    // Original-scope checks are healthy: only the machine-token record is blocked.
                    assertThat(control.storageStatus().get("clusterId")).isEqualTo(storage.clusterId());
                }
                var response = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/api/cluster/members"))
                        .timeout(Duration.ofSeconds(20))
                        .header("Authorization", "Bearer " + (credentialRead ? machine : control.credential()))
                        .GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(503);
                assertThat(ControlPlane.interpretClusterRefusal(response.statusCode(), response.body()))
                        .contains(ControlError.STORAGE_UNAVAILABLE.code());
            } finally {
                long finished = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", "off")).get("count")).longValue();
                assertThat(finished - entered).isGreaterThanOrEqualTo(credentialRead ? 1L : 2L);
            }
            assertThat(control.storageStatus().get("clusterId")).isEqualTo(storage.clusterId());
            assertThat(SystemCollections.CONFIG_TOKENS.on(configuration).find().first()).isEqualTo(token);
            assertThat(authIdentity(SystemCollections.CACHE_IDENTITY.on(cache).find().first())).isEqualTo(auth);
        }
    }

    @ParameterizedTest
    @EnumSource(Layer.class)
    void anUnavailableAuthenticationDependencyReturnsACodeAndKeepsItsOriginalScope(Layer layer) {
        String cluster = "auth-outage-" + UUID.randomUUID();
        try (NetworkedMongo config = NetworkedMongo.start(); NetworkedMongo cache = NetworkedMongo.start()) {
            String configurationUri = config.uriForThisHost("configuration");
            var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    cache.uriForThisHost("cache"), "cache", null);
            NativeMongoStorageFixture.prepare(configurationUri, binding, config.uriForThisHost("master"), cluster);
            try (var rawConfig = MongoClients.create(configurationUri); var rawCache = MongoClients.create(binding.uri());
                 RealProcessServer server = RealProcessServer.startPrepared(configurationUri, "127.0.0.1",
                         port -> List.of("--tapstate.cluster.id=" + cluster, "--tapstate.hz.member-port=0",
                                 "--tapstate.hz.jet.cooperative-thread-count=2"))) {
                var configuration = rawConfig.getDatabase("configuration");
                var cacheDatabase = rawCache.getDatabase("cache");
                Document keys = SystemCollections.CLUSTER_KEYS.on(configuration).find().first();
                Document bound = SystemCollections.STORAGE_BINDING.on(configuration).find().first();
                Document auth = authIdentity(SystemCollections.CACHE_IDENTITY.on(cacheDatabase).find().first());
                var control = new ControlPlane(server.baseUrl());
                control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
                Await.until("native authentication and business storage to be ready", () ->
                        "READY".equals(control.storageStatus().get("businessPhase")), () -> String.valueOf(control.storageStatus()));
                assertThat(control.clusterMemberNodeIds()).hasSize(1);
                NetworkedMongo unavailable = layer == Layer.CONFIG ? config : cache;
                unavailable.freeze();
                try {
                    assertThat(control.clusterReadRefusal()).contains(ControlError.STORAGE_UNAVAILABLE.code());
                } finally {
                    unavailable.thaw();
                }
                assertThat(control.storageStatus().get("clusterId")).isEqualTo(cluster);
                assertThat(SystemCollections.CLUSTER_KEYS.on(configuration).find().first()).isEqualTo(keys);
                assertThat(SystemCollections.STORAGE_BINDING.on(configuration).find().first()).isEqualTo(bound);
                assertThat(authIdentity(SystemCollections.CACHE_IDENTITY.on(cacheDatabase).find().first())).isEqualTo(auth);
                assertThat(SystemCollections.OP_SESSIONS.on(cacheDatabase).countDocuments()).isZero();
            }
        }
    }

    private static Document authIdentity(Document row) {
        var identity = new Document();
        for (String name : List.of("clusterId", "bindingVersion", "cacheIncarnation", "authGeneration", "authPhase")) {
            identity.put(name, row.get(name));
        }
        return identity;
    }
}
