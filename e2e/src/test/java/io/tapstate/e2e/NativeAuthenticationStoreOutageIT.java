package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
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
    enum LateFault {
        CREDENTIAL_RECORD(20000, 0, false),
        WITHIN_SHARED_BUDGET(0, 4000, true),
        EXCEEDS_SHARED_BUDGET(3000, 4000, false);

        final int credentialDelay;
        final int scopeDelay;
        final boolean succeeds;

        LateFault(int credentialDelay, int scopeDelay, boolean succeeds) {
            this.credentialDelay = credentialDelay;
            this.scopeDelay = scopeDelay;
            this.succeeds = succeeds;
        }
    }

    @ParameterizedTest
    @EnumSource(LateFault.class)
    void credentialReadsAndRepeatedChecksShareTheAuthenticationDeadline(LateFault fault) throws Exception {
        String cluster = "auth-budget-" + UUID.randomUUID();
        try (var configMongo = NetworkedMongo.startWithTestCommands();
             var cacheMongo = NetworkedMongo.startWithTestCommands()) {
            String configUri = configMongo.uriForThisHost("configuration");
            String cacheUri = cacheMongo.uriForThisHost("cache");
            var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, cacheUri, "cache", null);
            NativeMongoStorageFixture.prepare(configUri, binding, configMongo.uriForThisHost("master"), cluster);
            try (var rawConfig = MongoClients.create(configUri);
                 var rawCache = MongoClients.create(cacheUri);
                 var server = RealProcessServer.startPrepared(configUri, "127.0.0.1",
                         port -> List.of("--tapstate.cluster.id=" + cluster, "--tapstate.hz.member-port=0",
                                 "--tapstate.hz.jet.cooperative-thread-count=2"));
                 var http = HttpClient.newHttpClient()) {
                var control = new ControlPlane(server.baseUrl());
                control.login(NativeMongoStorageFixture.USER, NativeMongoStorageFixture.PASSWORD);
                Await.until("the installation to be ready for the same authenticated request",
                        () -> "READY".equals(control.storageStatus().get("businessPhase")),
                        () -> String.valueOf(control.storageStatus()));
                String machine = control.mintToken("ADMIN");
                var tokens = SystemCollections.CONFIG_TOKENS.on(rawConfig.getDatabase("configuration"));
                var cacheIdentity = SystemCollections.CACHE_IDENTITY.on(rawCache.getDatabase("cache"));
                Document token = tokens.find().first();
                Document auth = authIdentity(cacheIdentity.find().first());
                assertThat(members(http, server, machine).statusCode()).isEqualTo(200);

                try (var credential = holdReads(rawConfig.getDatabase("admin"),
                             "configuration." + SystemCollections.CONFIG_TOKENS.collectionName(), fault.credentialDelay, 1);
                     var scope = holdReads(rawCache.getDatabase("admin"),
                             "cache." + SystemCollections.CACHE_IDENTITY.collectionName(), fault.scopeDelay, 2)) {
                    if (fault == LateFault.CREDENTIAL_RECORD) {
                        assertThat(members(http, server, control.credential()).statusCode()).isEqualTo(200);
                    }
                    var response = members(http, server, machine);
                    if (fault.succeeds) {
                        assertThat(response.statusCode()).isEqualTo(200);
                    } else {
                        assertThat(response.statusCode()).isEqualTo(503);
                        assertThat(ControlPlane.interpretClusterRefusal(response.statusCode(), response.body()))
                                .contains(ControlError.STORAGE_UNAVAILABLE.code());
                    }
                }

                assertThat(members(http, server, machine).statusCode()).isEqualTo(200);
                assertThat(tokens.find().first()).isEqualTo(token);
                assertThat(authIdentity(cacheIdentity.find().first())).isEqualTo(auth);
            }
        }
    }

    private static HttpResponse<String> members(HttpClient http, RealProcessServer server, String credential)
            throws Exception {
        return http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/api/cluster/members"))
                .timeout(Duration.ofSeconds(20))
                .header("Authorization", "Bearer " + credential)
                .GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static AutoCloseable holdReads(MongoDatabase admin, String namespace, int delayMillis, int count) {
        if (delayMillis == 0) return () -> {};
        long entered = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("find"))
                        .append("namespace", namespace).append("blockConnection", true)
                        .append("blockTimeMS", delayMillis))).get("count")).longValue();
        return () -> {
            long finished = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "off")).get("count")).longValue();
            assertThat(finished - entered).as("blocked Mongo reads on %s", namespace).isGreaterThanOrEqualTo(count);
        };
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
                assertThat(control.clusterMembers()).singleElement()
                        .satisfies(member -> assertThat(member.bootId()).isNotBlank());
                control.awaitInitialAlertMaintenance();
                Document readyKeys=SystemCollections.CLUSTER_KEYS.on(configuration).find().first();
                assertThat(keyIdentity(readyKeys)).isEqualTo(keyIdentity(keys));
                assertThat(readyKeys.getLong("sourceWriteFence")).isGreaterThanOrEqualTo(keys.getLong("sourceWriteFence"));
                NetworkedMongo unavailable = layer == Layer.CONFIG ? config : cache;
                unavailable.freeze();
                try {
                    assertThat(control.clusterReadRefusal()).contains(ControlError.STORAGE_UNAVAILABLE.code());
                } finally {
                    unavailable.thaw();
                }
                assertThat(control.storageStatus().get("clusterId")).isEqualTo(cluster);
                assertThat(SystemCollections.CLUSTER_KEYS.on(configuration).find().first()).isEqualTo(readyKeys);
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
    private static Document keyIdentity(Document row){var identity=new Document(row);identity.remove("sourceWriteFence");return identity;}
}
