package io.tapstate.app;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Enforced Mongo authorization, not an unauthenticated simulation of database routing. */
@RequiresDocker
class ManagedMongoStoreIsolationIT {

    @Test void encryptedConfigurationMovesBetweenIsolatedRootsAndRetainsItsOriginalResultAfterCacheLoss() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String sourceDb = "transfer_source_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            var from = seedTransferRoot(admin, "from_" + suffix, sourceDb, "portable-admin", "portable-password");
            var to = seedTransferRoot(admin, "to_" + suffix, sourceDb, "destination-admin", "destination-password");
            admin.getDatabase(sourceDb).getCollection("orders").insertMany(List.of(new Document("_id", 1).append("name", "first").append("quantity", 7),
                    new Document("_id", 2).append("name", "second").append("quantity", 19)));
            var sourceConfig = admin.getDatabase(from.config()); var destinationConfig = admin.getDatabase(to.config());
            String fromKey = sourceConfig.getCollection("cluster_keys").find().first().getString("keyId");
            String toKey = destinationConfig.getCollection("cluster_keys").find().first().getString("keyId");
            assertThat(toKey).isNotEqualTo(fromKey);
            Document targetBinding = destinationConfig.getCollection("storage_binding").find().first();
            Document targetBootstrap = destinationConfig.getCollection("op_bootstrap").find().first();
            String key = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
            Map<?,?> exported;
            Map<String,Object> sourceSettings = Map.of("isUri", true, "uri", uri(from.user(), sourceDb));
            try (var application = transferRoot(from)) {
                var client = transferClient(application, from.username(), from.password());
                assertThat(client.get().uri("/api/sources").retrieve().body(String.class)).contains("\"store\":\"true\"");
                application.getBean(StorageRoot.class).runtime().requireConfiguration().artifacts().save(new SourceResource("secret_only", null, "mysql",
                        Map.of("host", "unused.invalid", "port", 3306, "database", "unused", "username", "unused", "password", "unique-import-catalog-secret"), null, null, null, null));
                client.post().uri("/api/sources").body(Map.of("id", "portable_input", "connector", "mongodb", "config", sourceSettings,
                        "mode", "snapshot", "tables", List.of(Map.of("type", "literal", "name", "orders")))).retrieve().toBodilessEntity();
                client.post().uri("/api/connections:discover-schema").body(Map.of("id", "portable_input", "connectorId", "mongodb", "settings", sourceSettings)).retrieve().toBodilessEntity();
                client.post().uri("/api/pipelines").body(Map.of("id", "portable_flow", "sources", List.of(Map.of("id", "portable_input", "srs", false)),
                        "settings", Map.of("readMode", "snapshot_only"), "serve", Map.of("id", "output", "from", "/.*/",
                                "sync", List.of(Map.of("id", "copy", "source", "views", "onFullLoad", "clear"))))).retrieve().toBodilessEntity();
                client.post().uri("/api/pipelines/portable_flow:start").retrieve().toBodilessEntity();
                var sourceTarget = admin.getDatabase(from.master()).getCollection("orders");
                var sourceRuntime = admin.getDatabase(from.cache());
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(sourceTarget.countDocuments()).isEqualTo(2);
                    assertThat(sourceTarget.find(new Document("_id", 1)).first().getString("name")).isEqualTo("first");
                    assertThat(((Number)sourceTarget.find(new Document("_id", 2)).first().get("quantity")).longValue()).isEqualTo(19);
                    var checkpoint = SystemCollections.CACHE_PIPELINE_STATE.on(sourceRuntime).find(new Document("_id", "portable_flow")).first();
                    assertThat(checkpoint).isNotNull().containsEntry("stateJson", "COMPLETED");
                    var configured = application.getBean(StorageRoot.class).runtime().requireConfiguration();
                    var business = configured.bindings().cachePreparation().orElseThrow().businessScope();
                    long generation = configured.pipelineStates().read("portable_flow").orElseThrow().stateGeneration();
                    var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), "portable_flow", generation);
                    assertThat(checkpoint.get("scope", Document.class)).isEqualTo(new Document("clusterId", scope.clusterId())
                            .append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation())
                            .append("pipelineId", scope.pipelineId()).append("stateGeneration", scope.stateGeneration()));
                    var intent = configured.intents().load("portable_flow").orElseThrow();
                    assertThat(intent.scope()).isEqualTo(scope);
                    assertThat(intent.desired().targetState()).isEqualTo(io.tapstate.core.lifecycle.PipelineState.RUNNING);
                });
                // The source now holds real run state and business rows; none belongs to the transfer.
                sourceTarget.insertOne(new Document("_id", 999).append("name", "source-only"));
                exported = client.post().uri("/api/configuration/export").body(Map.of("resourceIds", List.of("portable_flow", "secret_only"),
                        "usernames", List.of(from.username()), "transferKey", key)).retrieve().body(Map.class);
                assertThat(exported.get("defaultSourceId")).isEqualTo("views");
                assertThat(io.tapstate.core.common.JsonWriter.write(exported)).doesNotContain(PASSWORD, key, "portable-password");
            }
            String prepared; String importId; Map<?,?> receipt;
            try (var application = transferRoot(to)) {
                var client = transferClient(application, to.username(), to.password());
                assertThatThrownBy(() -> client.post().uri("/api/configuration/import/preflight").body(Map.of("transfer", exported.get("transfer"),
                        "transferKey", key, "defaultSourceMapping", Map.of())).retrieve().toBodilessEntity())
                        .isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                assertThat(destinationConfig.getCollection("configuration_imports").countDocuments()).isZero();
                var preflight = client.post().uri("/api/configuration/import/preflight").body(Map.of("transfer", exported.get("transfer"),
                        "transferKey", key, "defaultSourceMapping", Map.of("views", "views"))).retrieve().body(Map.class);
                prepared = (String)preflight.get("preparedTransfer");
                importId = (String)((Map<?,?>)((Map<?,?>)preflight.get("operation")).get("ticket")).get("id");
                assertThat(destinationConfig.getCollection("pipelines").countDocuments()).isZero();
                var confirmation = Map.of("importId", importId, "transfer", prepared, "transferKey", key, "defaultSourceMapping", Map.of("views", "views"));
                receipt = client.post().uri("/api/configuration/import/commit").body(confirmation).retrieve().body(Map.class);
                var business = application.getBean(StorageRoot.class).runtime().business().orElseThrow();
                var child = (ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(business, "context");
                assertThat(child.getBean(io.tapstate.core.logging.SecretRedactor.class).redact("unique-import-catalog-secret")).isEqualTo("********");
                assertThat(destinationConfig.getCollection("storage_binding").find().first()).isEqualTo(targetBinding);
                assertThat(destinationConfig.getCollection("op_bootstrap").find().first()).isEqualTo(targetBootstrap);
                assertThat(destinationConfig.getCollection("cluster_keys").find().first().getString("keyId")).isEqualTo(toKey);
                String originalCipher = sourceConfig.getCollection("sources").find(new Document("_id", "portable_input")).first().get("body", Document.class).getString("config");
                String destinationCipher = destinationConfig.getCollection("sources").find(new Document("_id", "portable_input")).first().get("body", Document.class).getString("config");
                assertThat(originalCipher).contains(fromKey).doesNotContain(PASSWORD);
                assertThat(destinationCipher).contains(toKey).doesNotContain(fromKey, PASSWORD).isNotEqualTo(originalCipher);
                assertThat(admin.getDatabase(to.master()).getCollection("orders").countDocuments()).isZero();
                assertThat(SystemCollections.PIPELINE_INTENTS.on(destinationConfig).countDocuments()).isZero();
                var destinationRuntime = admin.getDatabase(to.cache());
                for (var table : List.of(SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION,
                        SystemCollections.DIRECT_CHANNEL_OFFSETS, SystemCollections.DIRECT_BATCHES, SystemCollections.SRS_CONSUMERS,
                        SystemCollections.CACHE_SRS_CONSUMER_OFFSETS)) {
                    assertThat(table.on(destinationRuntime).countDocuments()).as("no imported run records in %s", table.collectionName()).isZero();
                }
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(destinationRuntime)
                        .countDocuments(new Document("resourceType", "PIPELINE_ACTUATION"))).isZero();
                try (var definitions = io.tapstate.adapters.mongostore.MongoConfigurationDomain.openExisting(
                        new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(to.user(), to.config()), null, Duration.ofSeconds(5)))) {
                    // Import creates a fresh CONFIG mapping, without carrying the source's prepared runtime.
                    var preparation = definitions.preparations().read("portable_flow").orElseThrow();
                    assertThat(preparation.phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                    assertThat(preparation.scope()).isNull();
                    assertThat(preparation.preparationId()).isNull();
                    assertThat(preparation.issuerNode()).isNull();
                }
                assertThat(SystemCollections.CAPTURE_PREPARATIONS.on(destinationConfig).countDocuments()).isZero();
                var importedClient = transferClient(application, from.username(), from.password());
                var tested = importedClient.post().uri("/api/connections:test").body(Map.of("id", "portable_input", "connectorId", "mongodb", "settings", sourceSettings)).retrieve().body(Map.class);
                assertThat(tested.get("outcome")).isEqualTo("PASSED");
                importedClient.post().uri("/api/connections:discover-schema").body(Map.of("id", "portable_input", "connectorId", "mongodb", "settings", sourceSettings)).retrieve().toBodilessEntity();
                importedClient.post().uri("/api/pipelines/portable_flow:start").retrieve().toBodilessEntity();
                var target = admin.getDatabase(to.master()).getCollection("orders");
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(target.countDocuments()).isEqualTo(2);
                    assertThat(target.find(new Document("_id", 1)).first().getString("name")).isEqualTo("first");
                    assertThat(((Number)target.find(new Document("_id", 2)).first().get("quantity")).longValue()).isEqualTo(19);
                });
                assertThat(admin.getDatabase(from.master()).getCollection("orders").find(new Document("_id", 999)).first()).isNotNull();
                var definitions = application.getBean(StorageRoot.class).runtime().requireConfiguration().artifacts();
                var previous = (SourceResource)definitions.get("portable_input").orElseThrow();
                var changed = new SourceResource(previous.id(), new Metadata(Map.of(), "edited-after-import"), previous.connector(), previous.config(),
                        previous.mode(), previous.tables(), previous.srs(), previous.execution(), previous.experimental());
                definitions.save(changed);
                assertThat(client.post().uri("/api/configuration/import/commit").body(confirmation).retrieve().body(Map.class)).isEqualTo(receipt);
                assertThat(((SourceResource)definitions.get("portable_input").orElseThrow()).metadata().description()).isEqualTo("edited-after-import");
            }
            admin.getDatabase(to.cache()).drop();
            try (var restarted = transferRoot(to)) {
                var client = transferClient(restarted, from.username(), from.password());
                assertThat(restarted.getBean(StorageRoot.class).runtime().business()).isEmpty();
                var status = client.post().uri("/api/configuration/import/status").body(Map.of("importId", importId)).retrieve().body(Map.class);
                assertThat(status.get("phase")).isEqualTo("COMMITTED"); assertThat(status.get("receipt")).isEqualTo(receipt);
                assertThat(client.post().uri("/api/configuration/import/commit").body(Map.of("importId", importId, "transfer", prepared,
                        "transferKey", key, "defaultSourceMapping", Map.of("views", "views"))).retrieve().body(Map.class)).isEqualTo(receipt);
                assertThat(restarted.getBean(StorageRoot.class).runtime().business()).isEmpty();
                assertThat(admin.getDatabase(to.cache()).listCollectionNames().into(new ArrayList<>())).doesNotContain(
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.collectionName(), SystemCollections.CACHE_PIPELINE_STATE.collectionName(),
                        SystemCollections.CACHE_PIPELINE_OBSERVATION.collectionName());
            }
        }
    }

    private record TransferDeployment(String config, String cache, String master, String user, String username, String password) {}
    private TransferDeployment seedTransferRoot(MongoClient admin, String name, String sourceDb, String username, String password) {
        var deployment = new TransferDeployment("tx_config_" + name, "tx_cache_" + name, "tx_master_" + name, "tx_user_" + name, username, password);
        createUser(admin, deployment.user(), List.of(deployment.config(), deployment.cache(), deployment.master(), sourceDb));
        var setup = new StorageSetupScope("tx_" + name, "installation", "setup", 1);
        var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri(deployment.user(), deployment.cache()), deployment.cache(), null);
        try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(deployment.user(), deployment.config()), null, Duration.ofSeconds(5)),
                setup.clusterId(), setup.installationId(), setup.operationId(), 1)) {
            var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", uri(deployment.user(), deployment.master())), null, null, null, null);
            var initial = configuration.bindings().commitWithFirstCache(setup, StorageBinding.Mode.OP, cache, target);
            try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cache, Duration.ofSeconds(5), "transfer-fixture")) {
                cached.prepareFirstAuth(initial.firstCreation().orElseThrow()); cached.prepareInitialBusiness(initial.preparation(), () -> {});
            }
            configuration.bindings().complete(setup);
            configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(), new User(username, new BCryptPasswordHasher().hash(password), "admin"));
        }
        return deployment;
    }
    private ConfigurableApplicationContext transferRoot(TransferDeployment deployment) {
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0", "--logging.level.root=ERROR",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.store.mongo.uri=" + uri(deployment.user(), deployment.config()), "--tapstate.control.auth.jwt-secret=transfer-fixture-signing-secret",
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(), "--tapstate.connectors.plugins-dir=" + work.resolve(deployment.config()));
    }
    private static org.springframework.web.client.RestClient transferClient(ConfigurableApplicationContext application, String username, String password) {
        var client = rootClient(application);
        var login = client.post().uri("/auth/login").body(Map.of("username", username, "password", password, "createSession", true))
                .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
        return client.mutate().defaultHeader("Authorization", "Bearer " + login.token()).build();
    }

    @Test void opInstallationRestartsCreatesItsFirstAdministratorAndCopiesRealSourceValues() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "op_config_" + suffix, cacheDb = "op_cache_" + suffix, masterDb = "op_master_" + suffix, sourceDb = "op_source_" + suffix;
        String user = "op_" + suffix;
        var files = InstallationFiles.initialize(work.resolve("op-installation"));
        var setup = InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC());
        Path setupSeeds = Files.createDirectory(work.resolve("op-setup-seeds"));
        Files.copy(CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar"), setupSeeds.resolve("mongodb-connector.jar"));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb, sourceDb));
            var source = admin.getDatabase(sourceDb).getCollection("orders");
            source.insertMany(List.of(new Document("_id", 1).append("name", "first").append("quantity", 7),
                    new Document("_id", 2).append("name", "second").append("quantity", 19)));
            long initialPid;
            try (var process = installationProcess(files, setupSeeds, "first")) {
                initialPid = process.process.pid();
                var management = process.management(files, setup.token());
                Map<String,Object> candidate = Map.of("configuration", Map.of("kind", "MONGODB", "uri", uri(user, configDb), "database", configDb),
                        "cache", Map.of("kind", "MONGODB", "uri", uri(user, cacheDb), "database", cacheDb),
                        "master", Map.of("kind", "MONGODB", "uri", uri(user, masterDb), "database", masterDb));
                var stamps = new java.util.LinkedHashMap<String,Object>();
                for (var layer : io.tapstate.control.core.StorageSetupService.Layer.values()) {
                    var tested = management.post().uri("/api/storage/test").body(Map.of("operationId", setup.scope().operationId(),
                            "generation", setup.scope().generation(), "candidate", candidate, "layer", layer.name())).retrieve().body(Map.class);
                    stamps.put(layer.name(), tested.get("testStamp"));
                }
                assertThat(files.configurationUri()).isEmpty();
                var committed = management.post().uri("/api/storage/setup/commit").body(Map.of("operationId", setup.scope().operationId(),
                        "generation", setup.scope().generation(), "candidate", candidate, "testStamps", stamps)).retrieve().body(Map.class);
                assertThat(committed.get("setupPhase")).isEqualTo("COMPLETE");
                assertThat(admin.getDatabase(configDb).getCollection("schema_migrations").find(new Document("_id", "config")).first().getInteger("installedVersion")).isEqualTo(2);
                assertThat(admin.getDatabase(cacheDb).getCollection("schema_migrations").find(new Document("_id", "business")).first().getInteger("installedVersion")).isEqualTo(2);
                assertThat(admin.getDatabase(cacheDb).listCollectionNames()).contains("validation_runs");
                assertThat(process.publicClient().get().uri("/auth/storage-entry").retrieve().body(Map.class))
                        .containsExactlyInAnyOrderEntriesOf(Map.of("mode", "OP", "entry", "RESTART_REQUIRED", "management", false));
                assertThat(files.stage().handoff()).isEqualTo(InstallationFiles.Handoff.RESTART_REQUIRED);
                assertThat(files.capability()).isEmpty();
                assertThat(files.configurationUri()).contains(uri(user, configDb));
                assertThat(admin.getDatabase(configDb).getCollection("users").countDocuments()).isZero();
                assertThat(admin.getDatabase(configDb).getCollection("sources").countDocuments()).isOne();
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isZero();
            }
            String key = admin.getDatabase(configDb).getCollection("cluster_keys").find().first().getString("keyId");
            try (var process = installationProcess(files, setupSeeds, "restarted")) {
                assertThat(process.process.pid()).isNotEqualTo(initialPid);
                assertThat(files.stage().handoff()).isEqualTo(InstallationFiles.Handoff.RESTARTED);
                assertThat(process.publicClient().get().uri("/auth/storage-entry").retrieve().body(Map.class))
                        .containsExactlyInAnyOrderEntriesOf(Map.of("mode", "OP", "entry", "FIRST_ADMIN_REQUIRED", "management", false));
                var firstAdmin = InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC());
                assertThat(firstAdmin.purpose()).isEqualTo(io.tapstate.spi.store.StorageSetupStateStore.Purpose.FIRST_ADMIN);
                var created = process.management(files, firstAdmin.token()).post().uri("/installation/first-admin")
                        .body(Map.of("username", "admin", "password", "op-installation-password")).retrieve().body(Map.class);
                assertThat(created.get("phase")).isEqualTo("COMPLETE");
                assertThat(process.publicClient().get().uri("/auth/storage-entry").retrieve().body(Map.class))
                        .containsExactlyInAnyOrderEntriesOf(Map.of("mode", "OP", "entry", "LOGIN", "management", false));
                assertThat(files.capability()).isEmpty();
                var publicClient = process.publicClient();
                var login = publicClient.post().uri("/auth/login").body(Map.of("username", "admin", "password", "op-installation-password", "createSession", true))
                        .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
                var client = publicClient.mutate().defaultHeader("Authorization", "Bearer " + login.token()).build();
                var admitted = client.post().uri("/api/storage/status").body(Map.of()).retrieve().body(Map.class);
                assertThat(((List<?>)admitted.get("allowedActions")).containsAll(List.of("configuration.export", "configuration.import.preflight",
                        "configuration.import.commit", "configuration.import.status"))).isTrue();
                Map<String,Object> settings = Map.of("isUri", true, "uri", uri(user, sourceDb));
                client.post().uri("/api/sources").body(Map.of("id", "input", "connector", "mongodb", "config", settings,
                        "mode", "snapshot", "tables", List.of(Map.of("type", "literal", "name", "orders")))).retrieve().toBodilessEntity();
                var test = client.post().uri("/api/connections:test").body(Map.of("id", "input", "connectorId", "mongodb", "settings", settings)).retrieve().body(Map.class);
                assertThat(test.get("outcome")).isEqualTo("PASSED");
                client.post().uri("/api/connections:discover-schema").body(Map.of("id", "input", "connectorId", "mongodb", "settings", settings))
                        .retrieve().toBodilessEntity();
                client.post().uri("/api/pipelines").body(Map.of("id", "first_copy", "sources", List.of(Map.of("id", "input", "srs", false)),
                        "settings", Map.of("readMode", "snapshot_only"), "serve", Map.of("id", "output", "from", "/.*/",
                                "sync", List.of(Map.of("id", "copy", "source", "views", "onFullLoad", "clear"))))).retrieve().toBodilessEntity();
                client.post().uri("/api/pipelines/first_copy:start").retrieve().toBodilessEntity();
                var target = admin.getDatabase(masterDb).getCollection("orders");
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    assertThat(target.countDocuments()).isEqualTo(2);
                    var first = target.find(new Document("_id", 1)).first(); var second = target.find(new Document("_id", 2)).first();
                    assertThat(first.getString("name")).isEqualTo("first"); assertThat(((Number)first.get("quantity")).longValue()).isEqualTo(7);
                    assertThat(second.getString("name")).isEqualTo("second"); assertThat(((Number)second.get("quantity")).longValue()).isEqualTo(19);
                });
                publicClient.post().uri("/auth/logout").header("Authorization", "TapstateSession " + login.sessionToken()).retrieve().toBodilessEntity();
                var fresh = publicClient.post().uri("/auth/login").body(Map.of("username", "admin", "password", "op-installation-password", "createSession", true))
                        .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
                assertThat(fresh.sessionToken()).isNotEqualTo(login.sessionToken());
                assertThat(publicClient.get().uri("/api/pipelines/first_copy").header("Authorization", "Bearer " + fresh.token()).retrieve().body(String.class)).contains("first_copy");
                assertThat(admin.getDatabase(configDb).getCollection("cluster_keys").find().first().getString("keyId")).isEqualTo(key);
                assertThat(admin.getDatabase(configDb).getCollection("users").countDocuments()).isOne();
                assertThat(admin.getDatabase(cacheDb).getCollection("users").countDocuments()).isZero();
                assertThat(admin.getDatabase(configDb).getCollection("op_sessions").countDocuments()).isZero();
                assertThat(admin.getDatabase(cacheDb).getCollection("op_sessions").countDocuments()).isPositive();
                assertThatThrownBy(() -> InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC())).isInstanceOf(TapstateException.class);
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"intent", "uri", "configuration", "birth", "complete"})
    void opRetainedInstallationBoundariesResumeTheOriginalOperation(String boundary) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String config = "resume_config_" + suffix, cache = "resume_cache_" + suffix, master = "resume_master_" + suffix, user = "resume_" + suffix;
        var files = InstallationFiles.initialize(work.resolve("resume-installation"));
        var original = InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC());
        Path seeds = Files.createDirectory(work.resolve("resume-seeds"));
        Files.copy(CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar"), seeds.resolve("mongodb-connector.jar"));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(config, cache, master));
            seedOpBoundary(files, user, config, cache, master, boundary);
            String oldKey = admin.getDatabase(config).getCollection("cluster_keys").find().first() == null ? null
                    : admin.getDatabase(config).getCollection("cluster_keys").find().first().getString("keyId");
            try (var process = installationProcess(files, seeds, "resume-" + boundary)) {
                if (List.of("intent", "uri", "configuration").contains(boundary)) {
                    var renewed = InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC());
                    assertThat(renewed.scope().operationId()).isEqualTo(original.scope().operationId());
                    assertThat(renewed.scope().generation()).isGreaterThan(original.scope().generation());
                    var management = process.management(files, renewed.token());
                    var status = management.post().uri("/api/storage/status").body(Map.of()).retrieve().body(Map.class);
                    assertThat(((List<?>)status.get("allowedActions")).containsAll(List.of("storage.test", "storage.setup.commit"))).isTrue();
                    var candidate = Map.of("configuration", Map.of("kind", "MONGODB", "uri", uri(user, config), "database", config),
                            "cache", Map.of("kind", "MONGODB", "uri", uri(user, cache), "database", cache),
                            "master", Map.of("kind", "MONGODB", "uri", uri(user, master), "database", master));
                    var stamps = new java.util.LinkedHashMap<String,Object>();
                    for (var layer : io.tapstate.control.core.StorageSetupService.Layer.values()) {
                        var tested = management.post().uri("/api/storage/test").body(Map.of("operationId", renewed.scope().operationId(), "generation", renewed.scope().generation(),
                                "candidate", candidate, "layer", layer.name())).retrieve().body(Map.class);
                        stamps.put(layer.name(), tested.get("testStamp"));
                    }
                    management.post().uri("/api/storage/setup/commit").body(Map.of("operationId", renewed.scope().operationId(), "generation", renewed.scope().generation(),
                            "candidate", candidate, "testStamps", stamps)).retrieve().toBodilessEntity();
                }
                assertThat(files.stage().scope().operationId()).isEqualTo(original.scope().operationId());
                assertThat(files.stage().handoff()).isEqualTo(boundary.equals("complete") ? InstallationFiles.Handoff.RESTARTED : InstallationFiles.Handoff.RESTART_REQUIRED);
                var database = admin.getDatabase(config);
                assertThat(database.getCollection("storage_setup").find().first().getString("phase")).isEqualTo("COMPLETE");
                assertThat(database.getCollection("storage_binding").countDocuments()).isOne();
                assertThat(database.getCollection("sources").countDocuments()).isOne();
                assertThat(database.getCollection("users").countDocuments()).isZero();
                if (oldKey != null) assertThat(database.getCollection("cluster_keys").find().first().getString("keyId")).isEqualTo(oldKey);
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void firstAdministratorTransactionHasOneWinnerAcrossIndependentProcesses(boolean sameUsername) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String config = "race_config_" + suffix, cache = "race_cache_" + suffix, master = "race_master_" + suffix, user = "race_" + suffix;
        var files = InstallationFiles.initialize(work.resolve("admin-race"));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(config, cache, master)); seedOpBoundary(files, user, config, cache, master, "complete");
            var children = new ArrayList<Process>(); var outputs = new ArrayList<Path>();
            try {
                for (int i = 0; i < 2; i++) {
                    var output = work.resolve("admin-race-" + i + ".log"); outputs.add(output);
                    children.add(new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx128m", "-cp",
                            System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")), FirstAdministratorTransactionProcess.class.getName(), files.directory().toString())
                            .redirectErrorStream(true).redirectOutput(output.toFile()).start());
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> outputs.stream().allMatch(output -> {
                    try { return Files.readString(output).contains("ADMIN_TRANSACTION_READY"); } catch (java.io.IOException invalid) { return false; }
                }));
                for (int i = 0; i < 2; i++) {
                    var process = children.get(i);
                    process.getOutputStream().write(io.tapstate.core.common.JsonWriter.write(Map.of("username", i == 0 || sameUsername ? "first" : "second", "password", "password-" + i)).getBytes(StandardCharsets.UTF_8));
                    process.getOutputStream().close();
                }
                for (var child : children) assertThat(child.waitFor(20, TimeUnit.SECONDS)).isTrue();
                var results = new ArrayList<String>(); for (Path output : outputs) results.add(Files.readString(output));
                assertThat(results.stream().filter(value -> value.contains("ADMIN_TRANSACTION_COMMITTED")).count()).isOne();
                assertThat(results.stream().filter(value -> value.contains("ADMIN_TRANSACTION_REFUSED")).count()).isOne();
                var database = admin.getDatabase(config);
                assertThat(database.getCollection("users").countDocuments()).isOne();
                var receipt = database.getCollection("op_bootstrap").find().first();
                assertThat(receipt.getString("phase")).isEqualTo("COMPLETE");
                assertThat(database.getCollection("users").find(new Document("_id", receipt.getString("username"))).first()).isNotNull();
            } finally {
                for (var child : children) if (child.isAlive()) { child.destroy(); if (!child.waitFor(5, TimeUnit.SECONDS)) child.destroyForcibly().waitFor(5, TimeUnit.SECONDS); }
            }
        }
    }

    @Test void firstAdminAuditFailureAndLostHttpResponseNeverOverwriteOrReopenTheAdministrator() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String config = "audit_config_" + suffix, cache = "audit_cache_" + suffix, master = "audit_master_" + suffix, user = "audit_" + suffix;
        var files = InstallationFiles.initialize(work.resolve("admin-response-loss"));
        Path seeds = Files.createDirectory(work.resolve("admin-response-seeds"));
        Files.copy(CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar"), seeds.resolve("mongodb-connector.jar"));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(config, cache, master)); seedOpBoundary(files, user, config, cache, master, "complete");
            try (var process = installationProcess(files, seeds, "admin-response")) {
                var issued = InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC());
                var database = admin.getDatabase(config);
                var administratorAudit = new Document("operationId", io.tapstate.control.core.ControlOperations.USER_CREATE.id());
                if (!database.listCollectionNames().into(new ArrayList<>()).contains("audit")) database.createCollection("audit");
                database.runCommand(new Document("collMod", "audit").append("validator", new Document("$expr", new Document("$eq", List.of(1, 0)))).append("validationLevel", "strict"));
                var management = process.management(files, issued.token());
                assertThatThrownBy(() -> management.post().uri("/installation/first-admin").body(Map.of("username", "admin", "password", "original-admin-password"))
                        .retrieve().toBodilessEntity()).isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getResponseBodyAsString()).contains("control.audit-blocked"));
                assertThat(database.getCollection("users").countDocuments()).isZero();
                assertThat(database.getCollection("op_bootstrap").find().first().getString("phase")).isEqualTo("PENDING");
                assertThat(database.getCollection("audit").countDocuments(administratorAudit)).isZero();
                database.runCommand(new Document("collMod", "audit").append("validator", new Document()));
                var address = java.net.URI.create(files.stage().managementUrl());
                byte[] body = io.tapstate.core.common.JsonWriter.write(Map.of("username", "admin", "password", "original-admin-password")).getBytes(StandardCharsets.UTF_8);
                try (var socket = new java.net.Socket("127.0.0.1", address.getPort())) {
                    String headers = "POST /installation/first-admin HTTP/1.1\r\nHost: 127.0.0.1:" + address.getPort()
                            + "\r\nOrigin: " + address + "\r\nContent-Type: application/json\r\nContent-Length: " + body.length
                            + "\r\n" + OnPremInstallation.CAPABILITY_HEADER + ": " + issued.token() + "\r\nConnection: close\r\n\r\n";
                    socket.getOutputStream().write(headers.getBytes(StandardCharsets.US_ASCII)); socket.getOutputStream().write(body);
                    socket.getOutputStream().flush(); socket.shutdownOutput();
                    // The caller deliberately receives no response; the native transaction must be reconciled.
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> database.getCollection("users").countDocuments() == 1);
                var saved = database.getCollection("users").find().first();
                assertThat(new BCryptPasswordHasher().matches("original-admin-password", saved.getString("passwordHash"))).isTrue();
                assertThat(database.getCollection("op_bootstrap").find().first().getString("phase")).isEqualTo("COMPLETE");
                assertThat(database.getCollection("audit").countDocuments(administratorAudit)).isOne();
                assertThatThrownBy(() -> management.post().uri("/installation/first-admin").body(Map.of("username", "admin", "password", "replacement-password"))
                        .retrieve().toBodilessEntity()).isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                assertThat(database.getCollection("users").find().first()).isEqualTo(saved);
                assertThat(process.publicClient().post().uri("/auth/login").body(Map.of("username", "admin", "password", "original-admin-password", "createSession", true))
                        .retrieve().body(io.tapstate.control.restapi.LoginResponse.class).token()).isNotBlank();
                assertThatThrownBy(() -> InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC())).isInstanceOf(TapstateException.class);
                database.getCollection("users").deleteMany(new Document());
                assertThatThrownBy(() -> InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC())).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> management.post().uri("/installation/first-admin").body(Map.of("username", "new", "password", "replacement-password"))
                        .retrieve().toBodilessEntity()).isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                assertThat(database.getCollection("users").countDocuments()).isZero();
                assertThat(database.getCollection("op_bootstrap").find().first().getString("phase")).isEqualTo("COMPLETE");
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void lostInitialOpCreationRequiresOwnerConfirmationAndAControlledRestart(boolean runningWithoutIdentity) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String config = "recover_config_" + suffix, cache = "recover_cache_" + suffix, master = "recover_master_" + suffix, user = "recover_" + suffix;
        var files = InstallationFiles.initialize(work.resolve("lost-installation"));
        var setup = InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC());
        Path seeds = Files.createDirectory(work.resolve("lost-seeds"));
        Files.copy(CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar"), seeds.resolve("mongodb-connector.jar"));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(config, cache, master)); seedOpBoundary(files, user, config, cache, master, "lost");
            String predecessor = null;
            if (runningWithoutIdentity) {
                try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationDomain.openExisting(OnPremInstallation.settings(files, new MongoProperties()))) {
                    var scope = configuration.bindings().cachePreparation().orElseThrow().authScope();
                    var source = configuration.initialOpRecoverySource(scope, false);
                    var properties = new ConnectorPluginProperties(); properties.setSeedDir(seeds); properties.setPluginsDir(work.resolve("retained-plan-plugins"));
                    var target = new InstalledConnectorTests(properties, java.time.Clock.systemUTC()).target();
                    var masterConnection = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, uri(user, master), master, null);
                    String dependency = OnPremInstallation.recoveryDependency(configuration.bindings().load().orElseThrow(), scope, target.identity(masterConnection));
                    var manifest = new io.tapstate.spi.store.StorageRecoveryCommands.Manifest(new io.tapstate.spi.store.StorageRecoveryCommands.Selection(
                            io.tapstate.spi.store.StorageRecoveryCommands.Extent.CLUSTER, java.util.Set.of()), java.util.Set.of(),
                            Map.of(source.id(), io.tapstate.core.model.canonical.CanonicalHash.of(source)), dependency, configuration.configurationRevision());
                    var commands = configuration.recoveryCommands(); var confirmed = commands.confirm(commands.plan(scope, manifest, null, null));
                    predecessor = configuration.startInitialOpRecovery(confirmed, () -> {}).plan().operationId();
                }
            }
            long firstPid;
            try (var process = installationProcess(files, seeds, "lost")) {
                firstPid = process.process.pid(); var management = process.management(files, setup.token());
                assertThat(admin.getDatabase(cache).listCollectionNames().into(new ArrayList<>())).isEmpty();
                var initial = management.post().uri("/api/storage/status").body(Map.of()).retrieve().body(Map.class);
                assertThat(initial.get("businessPhase")).isEqualTo("AWAITING_REBUILD_CONFIRMATION");
                assertThatThrownBy(() -> process.publicClient().post().uri("/api/storage/recovery/plan")
                        .header(OnPremInstallation.CAPABILITY_HEADER, setup.token()).body(Map.of("scope", "CLUSTER", "pipelineIds", List.of()))
                        .retrieve().toBodilessEntity()).isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                var input = new java.util.LinkedHashMap<String,Object>(); input.put("scope", "CLUSTER"); input.put("pipelineIds", List.of());
                if (predecessor != null) input.put("previousOperationId", predecessor);
                var planned = management.post().uri("/api/storage/recovery/plan").body(input).retrieve().body(Map.class);
                assertThat(planned.get("phase")).isEqualTo("PLANNED");
                assertThat(admin.getDatabase(cache).listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(admin.getDatabase(config).getCollection("users").countDocuments()).isZero();
                var confirm = new java.util.LinkedHashMap<String,Object>(); confirm.put("operationId", planned.get("operationId")); confirm.put("planHash", planned.get("planHash"));
                confirm.put("controlRevision", planned.get("controlRevision")); confirm.put("confirmed", false);
                assertThatThrownBy(() -> management.post().uri("/api/storage/recovery/confirm").body(confirm).retrieve().toBodilessEntity())
                        .isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                assertThat(admin.getDatabase(cache).listCollectionNames().into(new ArrayList<>())).isEmpty();
                confirm.put("confirmed", true);
                var completed = management.post().uri("/api/storage/recovery/confirm").body(confirm).retrieve().body(Map.class);
                assertThat(completed.get("phase")).isEqualTo("COMPLETE");
                assertThat(files.stage().handoff()).isEqualTo(InstallationFiles.Handoff.RESTART_REQUIRED);
                assertThat(admin.getDatabase(config).getCollection("schema_migrations").find(new Document("_id", "config")).first().getInteger("installedVersion")).isEqualTo(2);
                assertThat(admin.getDatabase(cache).getCollection("schema_migrations").find(new Document("_id", "business")).first().getInteger("installedVersion")).isEqualTo(2);
                assertThat(admin.getDatabase(cache).listCollectionNames()).contains("validation_runs");
                assertThat(files.capability()).isEmpty();
                var identity = admin.getDatabase(cache).getCollection("cache_identity").find().first();
                assertThat(identity.getString("authPreparationId")).isEqualTo(planned.get("operationId"));
                assertThat(identity.getString("businessPreparationId")).isEqualTo(planned.get("operationId"));
                assertThat(identity.getLong("authGeneration")).isEqualTo(1L);
                assertThat(admin.getDatabase(cache).getCollection("workload_claims").countDocuments()).isZero();
                assertThat(admin.getDatabase(config).getCollection("op_bootstrap").find().first().getString("phase")).isEqualTo("PENDING");
                assertThatThrownBy(() -> management.post().uri("/api/storage/recovery/confirm").body(confirm).retrieve().toBodilessEntity())
                        .isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
            }
            try (var process = installationProcess(files, seeds, "recovered")) {
                assertThat(process.process.pid()).isNotEqualTo(firstPid);
                assertThat(files.stage().handoff()).isEqualTo(InstallationFiles.Handoff.RESTARTED);
                var firstAdmin = InstallationCommand.authorize(files, new MongoProperties(), java.time.Clock.systemUTC());
                process.management(files, firstAdmin.token()).post().uri("/installation/first-admin")
                        .body(Map.of("username", "admin", "password", "recovered-admin-password")).retrieve().toBodilessEntity();
                var login = process.publicClient().post().uri("/auth/login").body(Map.of("username", "admin", "password", "recovered-admin-password", "createSession", true))
                        .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
                assertThat(process.publicClient().get().uri("/api/sources").header("Authorization", "Bearer " + login.token()).retrieve().body(String.class)).contains("views");
                assertThat(admin.getDatabase(config).getCollection("sources").countDocuments()).isOne();
                assertThat(admin.getDatabase(config).getCollection("users").countDocuments()).isOne();
                assertThat(admin.getDatabase(cache).getCollection("op_sessions").countDocuments()).isOne();
            }
        }
    }

    private static void seedOpBoundary(InstallationFiles files, String user, String config, String cache, String master, String boundary) {
        var endpoint = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, uri(user, config), config, null);
        var scope = files.stage().scope(); String hash = OnPremInstallation.configurationHash(endpoint);
        files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.INTENT, hash));
        if (boundary.equals("intent")) return;
        files.saveConnection(endpoint.uri(), null); files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.URI_WRITTEN, hash));
        if (boundary.equals("uri")) return;
        files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CREATE_STARTED, hash));
        try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(OnPremInstallation.settings(files, new MongoProperties()),
                scope.clusterId(), scope.installationId(), scope.operationId(), scope.generation())) {
            if (boundary.equals("configuration")) return;
            files.saveStage(files.stage().withHandoff(InstallationFiles.Handoff.CONFIGURED, hash));
            var connection = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, uri(user, cache), cache, null);
            var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", uri(user, master)), null, null, null, null);
            var committed = configuration.bindings().commitWithFirstCache(scope, io.tapstate.spi.store.StorageBinding.Mode.OP, connection, target);
            if (boundary.equals("lost")) return;
            try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(connection, Duration.ofSeconds(5), "retained-op-boundary")) {
                cached.prepareFirstAuth(committed.firstCreation().orElseThrow());
                if (boundary.equals("birth")) return;
                cached.prepareInitialBusiness(committed.preparation(), () -> {});
            }
            configuration.bindings().complete(scope);
        }
    }

    private InstallationProcess installationProcess(InstallationFiles files, Path setupSeeds, String name) throws Exception {
        Path output = work.resolve("installation-" + name + ".log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx768m",
                "--add-opens=java.base/java.lang=ALL-UNNAMED", "-cp", classpath, OnPremInstallationProcess.class.getName(),
                files.directory().toString(), work.resolve("op-plugins").toString(), CloudConnectorTestInputs.seedDirectory().toString(), setupSeeds.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        var result = new InstallationProcess(process, output);
        try {
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> {
                if (!process.isAlive()) throw new AssertionError("installation child exited: " + Files.readString(output).replace(PASSWORD, "<redacted>"));
                return Files.readString(output).contains("INSTALLATION_READY ");
            });
            return result;
        } catch (RuntimeException | Error failure) { result.close(); throw failure; }
    }
    private static final class InstallationProcess implements AutoCloseable {
        final Process process; final Path output;
        InstallationProcess(Process process, Path output) { this.process = process; this.output = output; }
        org.springframework.web.client.RestClient management(InstallationFiles files, String capability) {
            String base = files.stage().managementUrl();
            return org.springframework.web.client.RestClient.builder().baseUrl(base).defaultHeader("Origin", base)
                    .defaultHeader(OnPremInstallation.CAPABILITY_HEADER, capability).build();
        }
        org.springframework.web.client.RestClient publicClient() throws Exception {
            var line = Files.readAllLines(output).stream().filter(value -> value.startsWith("INSTALLATION_READY ")).findFirst().orElseThrow();
            return org.springframework.web.client.RestClient.builder().baseUrl("http://127.0.0.1:" + line.split(" ")[1]).build();
        }
        @Override public void close() throws Exception {
            process.getOutputStream().close();
            if (!process.waitFor(30, TimeUnit.SECONDS)) { process.destroy(); if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly().waitFor(10, TimeUnit.SECONDS); }
            assertThat(process.isAlive()).isFalse();
        }
    }
    private static final String PASSWORD = "controlled-mongo-permissions-password";
    private static final String REPLICA_SET = "permissions-rs";
    private static final TestTrust TLS = TestTrust.create();
    @Container
    private static final GenericContainer<?> MONGO = mongo();
    @TempDir Path work;
    private final Set<String> ownedDatabases = new java.util.LinkedHashSet<>();

    private String ownedDatabase(String database) {
        if (Set.of("admin", "config", "local").contains(database)) throw new IllegalArgumentException("a fixture must name its own database");
        ownedDatabases.add(database);
        return database;
    }

    @org.junit.jupiter.api.AfterEach
    void releaseThisInvocationsDatabases() {
        if (ownedDatabases.isEmpty()) return;
        // Preserve every restart assertion, then release only this invocation's exact databases.
        // Native CONFIG, CACHE and target names are independent, so no suffix is inferred here.
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            long connectorBytes = 0;
            for (String database : ownedDatabases) {
                for (Document artifact : admin.getDatabase(database).getCollection("connector_artifacts.files")
                        .find().projection(new Document("length", 1))) {
                    connectorBytes += artifact.get("length", Number.class).longValue();
                }
                admin.getDatabase(database).drop();
            }
            assertThat(admin.listDatabaseNames().into(new ArrayList<>())).doesNotContainAnyElementsOf(ownedDatabases);
            System.out.printf("Managed store fixture released %d databases and %d connector bytes%n", ownedDatabases.size(), connectorBytes);
        }
    }

    private static GenericContainer<?> mongo() {
        byte[] key = new byte[64];
        new java.security.SecureRandom().nextBytes(key);
        return new GenericContainer<>(DockerImageName.parse("mongo:7.0"))
                .withExposedPorts(27017)
                .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
                .withCopyToContainer(Transferable.of(Base64.getEncoder().encode(key), 0400), "/data/configdb/member.key")
                .withCopyToContainer(Transferable.of(TLS.server(), 0400), "/data/configdb/server.pem")
                .withCopyToContainer(Transferable.of(TLS.ca().getBytes(StandardCharsets.UTF_8), 0444), "/data/configdb/ca.pem")
                .withCommand("--replSet", REPLICA_SET, "--keyFile", "/data/configdb/member.key", "--bind_ip_all",
                        "--setParameter", "enableTestCommands=1", "--tlsMode", "allowTLS", "--tlsCertificateKeyFile", "/data/configdb/server.pem",
                        "--tlsCAFile", "/data/configdb/ca.pem", "--tlsAllowConnectionsWithoutCertificates")
                // The image opens a temporary unauthenticated server while creating its administrator.
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*MongoDB init process complete; ready for start up.*", 1))
                        .withStrategy(Wait.forLogMessage("(?i).*waiting for connections.*", 2))
                        .withStrategy(Wait.forListeningPort())).withStartupTimeout(Duration.ofSeconds(90));
    }

    /** Test-only ephemeral material; never reads, replaces or trusts any operator's retained certificate. */
    private record TestTrust(String ca, byte[] server) {
        static TestTrust create() {
            Path directory = null;
            try {
                directory = Files.createTempDirectory("tapstate-mongo-fixture-trust-");
                Path store = directory.resolve("server.p12"), output = directory.resolve("keytool.log");
                var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                        "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12",
                        "-keystore", store.toString(), "-storepass", "fixture-only-password", "-dname", "CN=localhost",
                        "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-ext", "BC=ca:true", "-validity", "2", "-noprompt")
                        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
                if (!process.waitFor(30, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new AssertionError("test key generation timed out"); }
                if (process.exitValue() != 0) throw new AssertionError("test key generation failed: " + Files.readString(output));
                var keys = java.security.KeyStore.getInstance("PKCS12");
                try (var input = Files.newInputStream(store)) { keys.load(input, "fixture-only-password".toCharArray()); }
                String ca = pem("CERTIFICATE", keys.getCertificate("server").getEncoded());
                String key = pem("PRIVATE KEY", keys.getKey("server", "fixture-only-password".toCharArray()).getEncoded());
                return new TestTrust(ca, (key + ca).getBytes(StandardCharsets.UTF_8));
            } catch (Exception failure) { throw new IllegalStateException("cannot prepare test TLS material", failure); }
            finally {
                if (directory != null) try {
                    Files.deleteIfExists(directory.resolve("server.p12")); Files.deleteIfExists(directory.resolve("keytool.log")); Files.delete(directory);
                } catch (java.io.IOException failure) { throw new IllegalStateException("cannot remove test TLS material", failure); }
            }
        }
        private static String pem(String type, byte[] bytes) {
            return "-----BEGIN " + type + "-----\n" + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(bytes)
                    + "\n-----END " + type + "-----\n";
        }
    }

    @BeforeAll
    static void initiateAuthenticatedReplicaSet() throws Exception {
        var initiated = MONGO.execInContainer("mongosh", "--quiet", "--username", "root", "--password", PASSWORD,
                "--authenticationDatabase", "admin", "--eval", "rs.initiate({_id:'" + REPLICA_SET
                        + "',members:[{_id:0,host:'localhost:27017'}]})");
        assertThat(initiated.getExitCode()).withFailMessage("authenticated replica initialization failed: %s %s; container: %s",
                initiated.getStdout().replace(PASSWORD, "<redacted>"),
                initiated.getStderr().replace(PASSWORD, "<redacted>"), MONGO.getLogs().replace(PASSWORD, "<redacted>")).isZero();
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                if (Boolean.TRUE.equals(admin.getDatabase("admin").runCommand(new Document("hello", 1))
                        .getBoolean("isWritablePrimary"))) return;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
        throw new AssertionError("the authenticated replica set did not elect a primary");
    }

    @Test
    void storageProbeRequiresTransactionalCrudAndIndexPrivilegesWithoutKeepingProbeNamespaces() {
        String database = "setup_probe_" + UUID.randomUUID().toString().replace("-", "");
        String writer = "writer_" + database;
        String reader = "reader_" + database;
        String noIndex = "noindex_" + database;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, writer, List.of(database));
            admin.getDatabase("admin").runCommand(new Document("createUser", reader).append("pwd", PASSWORD)
                    .append("roles", List.of(new Document("role", "read").append("db", database))));
            admin.getDatabase(database).runCommand(new Document("createRole", "crud_without_indexes")
                    .append("roles", List.of()).append("privileges", List.of(new Document("resource",
                            new Document("db", database).append("collection", ""))
                            .append("actions", List.of("find", "insert", "update", "remove", "createCollection", "listCollections")))));
            admin.getDatabase("admin").runCommand(new Document("createUser", noIndex).append("pwd", PASSWORD)
                    .append("roles", List.of(new Document("role", "crud_without_indexes").append("db", database))));
            for (String user : List.of(writer, reader, noIndex)) {
                var candidate = new io.tapstate.spi.store.StorageBinding.Connection(
                        io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, uri(user, database), database, null);
                try (var connection = io.tapstate.adapters.mongostore.MongoConnection.forCache(candidate, Duration.ofSeconds(5), null)) {
                    connection.verifyConnectivity();
                    var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
                    var normalized = probe.normalize(candidate);
                    if (user.equals(writer)) probe.test(normalized);
                    else assertThatThrownBy(() -> probe.test(normalized))
                            .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(
                                            io.tapstate.adapters.mongostore.StoreError.DATABASE_ACCESS_FAILED));
                }
                assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>())).isEmpty();
            }
        }
    }

    @Test
    void invalidStorageCredentialsAreDiagnosedSeparatelyFromNetworkFailureWithoutExposingSecrets() {
        String database = "credential_probe_" + UUID.randomUUID().toString().substring(0, 8);
        String user = "credential_user_" + database;
        String rejectedPassword = "rejected-password-sentinel";
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(database));
            var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
            var input = probe.normalize(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    uri(user, database, rejectedPassword), database, null));
            var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(io.tapstate.adapters.mongostore.MongoConnection.class);
            var previousLevel = logger.getLevel();
            var events = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
            events.start(); logger.setLevel(ch.qos.logback.classic.Level.WARN); logger.addAppender(events);
            try {
                assertThatThrownBy(() -> probe.test(input)).isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.STORE_UNAUTHORIZED);
                    assertThat(failure.args()).isEmpty();
                    assertThat(failure.getCause()).isNull();
                    assertThat(failure.getMessage()).doesNotContain(user, PASSWORD, rejectedPassword, "mongodb://");
                });
                String logged = events.list.stream().map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
                        .collect(java.util.stream.Collectors.joining("\n"));
                assertThat(logged).contains("code=io.store-unauthorized", "driver_type=MongoSecurityException", "cause_code=18")
                        .doesNotContain(user, PASSWORD, rejectedPassword, "mongodb://");
                assertThat(events.list).allSatisfy(event -> assertThat(event.getThrowableProxy()).isNull());
            } finally { logger.detachAppender(events); events.stop(); logger.setLevel(previousLevel); }
            assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>())).isEmpty();
        }
    }

    @Test
    void aSocketClosedDuringAuthenticationIsNotReportedAsRejectedCredentials() {
        String database = "auth_network_" + UUID.randomUUID().toString().substring(0, 8);
        String user = "auth_network_user_" + database;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(database));
            admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 20)).append("data", new Document("failCommands", List.of("saslStart", "saslContinue"))
                            .append("closeConnection", true).append("appName", "storage-auth-transport")));
            try {
                var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
                var input = probe.normalize(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                        uri(user, database) + "&appName=storage-auth-transport", database, null));
                assertThatThrownBy(() -> probe.test(input)).isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(StoreError.UNREACHABLE);
                    assertThat(failure.getCause()).isNull();
                    assertThat(failure.getMessage()).doesNotContain(user, PASSWORD, "mongodb://");
                });
            } finally {
                admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void atlasPasswordAuthenticationNegotiatesAvailableScramAndPrefersSha256(boolean sha256Available) {
        String database = ownedDatabase("scram_choice_" + UUID.randomUUID().toString().substring(0, 8));
        String user = "scram_user_" + database;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD)
                    .append("roles", List.of(new Document("role", "readWrite").append("db", database)))
                    .append("mechanisms", sha256Available ? List.of("SCRAM-SHA-1", "SCRAM-SHA-256") : List.of("SCRAM-SHA-1")));
            var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
            var input = probe.normalize(new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS,
                    uri(user, database) + "&tls=true", database, TLS.ca()));
            assertThat(new com.mongodb.ConnectionString(input.uri()).getCredential().getAuthenticationMechanism()).isNull();
            var before = scramSuccesses(admin);
            probe.test(input);
            var after = scramSuccesses(admin);
            String expected = sha256Available ? "SCRAM-SHA-256" : "SCRAM-SHA-1";
            String other = sha256Available ? "SCRAM-SHA-1" : "SCRAM-SHA-256";
            assertThat(after.get(expected)).isGreaterThan(before.get(expected));
            assertThat(after.get(other)).isEqualTo(before.get(other));
            // An explicit choice is binding; an authentication failure must not start a weaker retry.
            long sha1Attempts = scramCounts(admin, "received").get("SCRAM-SHA-1");
            var invalid = probe.normalize(new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS,
                    uri(user, database, "wrong-scram-sentinel") + "&tls=true&authMechanism=SCRAM-SHA-256", database, TLS.ca()));
            assertThatThrownBy(() -> probe.test(invalid)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(sha256Available ? IoError.STORE_UNAUTHORIZED : StoreError.STORAGE_CONNECTION_UNSUPPORTED));
            assertThat(scramSuccesses(admin).get("SCRAM-SHA-1")).isEqualTo(after.get("SCRAM-SHA-1"));
            assertThat(scramCounts(admin, "received").get("SCRAM-SHA-1")).isEqualTo(sha1Attempts);
        }
    }

    private static Map<String, Long> scramSuccesses(MongoClient admin) {
        return scramCounts(admin, "successful");
    }

    private static Map<String, Long> scramCounts(MongoClient admin, String counter) {
        var mechanisms = admin.getDatabase("admin").runCommand(new Document("serverStatus", 1))
                .get("security", Document.class).get("authentication", Document.class).get("mechanisms", Document.class);
        var counts = new java.util.HashMap<String, Long>();
        for (String name : List.of("SCRAM-SHA-1", "SCRAM-SHA-256")) {
            counts.put(name, ((Number) mechanisms.get(name, Document.class).get("authenticate", Document.class).get(counter)).longValue());
        }
        return counts;
    }

    @Test
    void anAtlasSaslRejectionIsDiagnosedAsAuthenticationInsteadOfNetworkFailure() {
        String database = "atlas_auth_" + UUID.randomUUID().toString().substring(0, 8);
        String user = "atlas_auth_user_" + database;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(database));
            admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", new Document("times", 20)).append("data", new Document("failCommands", List.of("saslStart", "saslContinue"))
                            .append("errorCode", 8000).append("appName", "storage-atlas-auth-rejection")));
            try {
                var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
                var input = probe.normalize(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                        uri(user, database) + "&authMechanism=SCRAM-SHA-256&appName=storage-atlas-auth-rejection", database, null));
                assertThatThrownBy(() -> probe.test(input)).isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(IoError.STORE_UNAUTHORIZED);
                    assertThat(failure.args()).isEmpty();
                    assertThat(failure.getCause()).isNull();
                    assertThat(failure.getMessage()).doesNotContain(user, PASSWORD, "mongodb://");
                });
            } finally {
                admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        }
    }

    @Test
    void unboundCloudRootNeedsOnlyConfigAndDoesNotBuildRuntimeOrConnectorTestsForStatus() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "unbound_config_" + suffix, configUser = "unbound_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, configUser, List.of(configDb));
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser, configDb), null, Duration.ofSeconds(5));
            String cluster = "unbound-" + suffix;
            try (var ignored = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings, cluster, "installation", "setup", 1)) { }
            try (var application = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.cloud.base-url=https://cloud.example", "--tapstate.cloud.token=fixture-runtime-token",
                    "--tapstate.cloud.cluster-id=" + cluster, "--tapstate.cloud.atlas-uri=" + uri(configUser, configDb),
                    "--tapstate.control.auth.jwt-secret=unbound-test-key", "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"))) {
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.SETUP_REQUIRED);
                assertThat(root.runtime().current()).isEmpty(); assertThat(root.runtime().business()).isEmpty();
                assertThat(root.authentication().managed().service().identity().clusterId()).isEqualTo(cluster);
                assertThat(application.getBeansOfType(com.hazelcast.core.HazelcastInstance.class)).isEmpty();
                assertThat(application.getBeansOfType(io.tapstate.spi.store.UserStore.class)).isEmpty();
                assertThat(application.getBean(io.tapstate.control.core.CloudRuntimeStatusProvider.class).snapshot().activePipelines()).isZero();
                var original = root.runtime().requireConfiguration().setupStates().load().scope();
                assertThat(application.getBean(io.tapstate.control.core.StorageSetupService.class).status(original, () -> {}).bindingVersion()).isZero();
                assertThat(application.getBeanFactory().containsSingleton("installedConnectorTests")).isFalse();
                try (MongoClient limited = MongoClients.create(uri(configUser, configDb))) {
                    assertDenied(limited, configDb + "_operator"); assertDenied(limited, configDb + "_views");
                }
                assertThat(admin.getDatabase(configDb).listCollectionNames().into(new ArrayList<>()))
                        .doesNotContain("cloud_sessions", "workload_claims", "system_meta");
            }
        }
    }

    /** Low-level fixture preparation isolates production-root loading; it does not certify the installer or a registered target JAR. */
    @Test
    void wholeCacheLossUsesNormalOpLoginAndDoesNotRecreateBusinessState() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "loss_config_" + suffix, cacheDb = "loss_cache_" + suffix, masterDb = "loss_master_" + suffix;
        String user = "loss_" + suffix;
        var original = new io.tapstate.spi.store.StorageSetupScope("loss-" + suffix, "installation", "setup", 1);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var cacheConnection = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB,
                    uri(user, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, configDb), null, Duration.ofSeconds(5)),
                    original.clusterId(), original.installationId(), original.operationId(), 1)) {
                var target = new SourceResource("views", null, "mongodb", Map.of("uri", uri(user, masterDb)), null, null, null, null);
                var initial = configuration.bindings().commitWithFirstCache(original, io.tapstate.spi.store.StorageBinding.Mode.OP, cacheConnection, target);
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "loss-fixture")) {
                    cached.prepareFirstAuth(initial.firstCreation().orElseThrow()); cached.prepareInitialBusiness(initial.preparation(), () -> {});
                }
                configuration.bindings().complete(original);
                configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                        new io.tapstate.spi.store.User("admin", new BCryptPasswordHasher().hash("sentinel-login-password"), "admin"));
            }
            var config = admin.getDatabase(configDb);
            String keyId = config.getCollection("cluster_keys").find().first().getString("keyId");
            Document binding = config.getCollection("storage_binding").find().first();
            Document firstAdmin = config.getCollection("op_bootstrap").find().first();
            io.tapstate.control.restapi.LoginResponse old;
            try (var first = nativeRoot(uri(user, configDb))) { old = ordinaryLogin(first, true); }
            admin.getDatabase(cacheDb).drop();
            verifyNewAuthenticationProcess(uri(user, configDb), original.clusterId(), false);
            try (var restarted = nativeRoot(uri(user, configDb))) {
                assertThatThrownBy(() -> rootClient(restarted).post().uri("/auth/login")
                        .body(Map.of("username", "admin", "password", "incorrect-recovery-password", "createSession", true))
                        .retrieve().body(String.class))
                        .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                assertThat(admin.getDatabase(cacheDb).getCollection("op_sessions").countDocuments()).isZero();
                var fresh = ordinaryLogin(restarted, true);
                var root = restarted.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
                assertThat(root.runtime().business()).isEmpty();
                var current = root.runtime().current().orElseThrow().preparation().authScope();
                assertThat(current.authGeneration()).isEqualTo(2);
                var client = rootClient(restarted);
                assertThatThrownBy(() -> client.post().uri("/api/storage/status").header("Authorization", "Bearer " + old.token())
                        .body(Map.of()).retrieve().body(String.class)).isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                assertThatThrownBy(() -> client.post().uri("/auth/session").header("Authorization", "TapstateSession " + old.sessionToken())
                        .retrieve().body(String.class)).isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                String status = client.post().uri("/api/storage/status").header("Authorization", "Bearer " + fresh.token())
                        .body(Map.of()).retrieve().body(String.class);
                assertThat(((Map<?,?>)io.tapstate.core.common.JsonReader.parse(status)).get("businessPhase")).isEqualTo("AWAITING_REBUILD_CONFIRMATION");
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>()))
                        .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "op_sessions");
                assertThat(config.getCollection("cluster_keys").find().first().getString("keyId")).isEqualTo(keyId);
                assertThat(binding.equals(config.getCollection("storage_binding").find().first())).isTrue();
                assertThat(firstAdmin.equals(config.getCollection("op_bootstrap").find().first())).isTrue();
            }
        }
    }

    /** Real production roots and CONFIG/CACHE boundaries; each selected full executes the installed connector against actual source and target values. */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void wholeCacheRecoveryPublishesAllStoppedBeforeStartingTheNewRuntime(boolean cloud) {
        String suffix=UUID.randomUUID().toString().replace("-","");String configDb="stopped_config_"+suffix,cacheDb="stopped_cache_"+suffix,masterDb="stopped_master_"+suffix;
        String sourceDb="stopped_source_"+suffix;String user="stopped_"+suffix;var setup=new StorageSetupScope("stopped-"+suffix,"installation","setup",1);
        var mode=cloud?StorageBinding.Mode.CLOUD:StorageBinding.Mode.OP;var sdk=new RecoverySdk(setup.clusterId());
        try(var admin=MongoClients.create(uri("root","admin"))) {
            createUser(admin,user,List.of(configDb,cacheDb,masterDb,sourceDb));
            admin.getDatabase(sourceDb).getCollection("orders").insertMany(List.of(new Document("_id",1).append("value","first"),new Document("_id",2).append("value","second")));
            admin.getDatabase(masterDb).getCollection("orders").insertOne(new Document("_id",99).append("value","stale"));
            var cache=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,uri(user,cacheDb),cacheDb,null);
            try(var configuration=io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user,configDb),null,Duration.ofSeconds(5)),setup.clusterId(),setup.installationId(),setup.operationId(),1)) {
                var target=new SourceResource(StorageBinding.defaultSourceId(mode),null,"mongodb",Map.of("isUri",true,"uri",uri(user,masterDb)),null,null,null,null);
                var initial=configuration.bindings().commitWithFirstCache(setup,mode,cache,target);
                try(var old=io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cache,Duration.ofSeconds(5),"old-fixture")) {
                    old.prepareFirstAuth(initial.firstCreation().orElseThrow());old.prepareInitialBusiness(initial.preparation(),() -> {});
                }
                configuration.bindings().complete(setup);
                if(!cloud)configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                        new User("admin",new BCryptPasswordHasher().hash("sentinel-login-password"),"admin"));
                var source=new SourceResource("input",null,"mongodb",Map.of("isUri",true,"uri",uri(user,sourceDb)),SourceMode.SNAPSHOT,List.of(TableRef.literal("orders")),null,null);
                configuration.artifacts().saveAll(List.of(source));
                for(String id:List.of("a","b")) {
                    var pipeline=new PipelineResource(id,null,List.of(SourceRef.spec("input",false)),null,null,
                            new ServeBlock.Inline("serve",FromRef.literal("orders"),List.of(new SyncElement("sync",target.id(),null,null,null,OnFullLoad.CLEAR)),null,null),
                            new Settings(null,null,null,null,ReadMode.SNAPSHOT_ONLY,null),null);
                    configuration.artifacts().saveAll(List.of(pipeline));
                    var scope=new PipelineIntentScope(setup.clusterId(),1,initial.preparation().authScope().cacheIncarnation(),id,1);
                    configuration.intents().compareAndSet(scope,0,new io.tapstate.core.lifecycle.DesiredState(id,
                            id.equals("a")?io.tapstate.core.lifecycle.PipelineState.RUNNING:io.tapstate.core.lifecycle.PipelineState.STOPPED,
                            io.tapstate.core.model.canonical.CanonicalHash.of(pipeline),false,null,false,null));
                }
            }
            // Every fixture-owned old client is closed before the operator's explicit recovery confirmation.
            admin.getDatabase(cacheDb).drop();
            try(var application=cloud?cloudRecoveryRoot(uri(user,configDb),sdk,CloudConnectorTestInputs.seedDirectory()):nativeRoot(uri(user,configDb),CloudConnectorTestInputs.seedDirectory())) {
                String credential=cloud?exchangeCookie(application,"fresh"):ordinaryLogin(application,false).token();
                java.util.function.BiFunction<String,Map<String,Object>,Map<?,?>> post=(path,body) -> cloud?cloudPost(application,credential,path,body):
                        rootClient(application).post().uri(path).header("Authorization","Bearer "+credential).body(body).retrieve().body(Map.class);
                var root=application.getBean(StorageRoot.class);assertThat(root.runtime().business()).isEmpty();
                var planned=post.apply("/api/storage/recovery/plan",Map.of("scope","CLUSTER","pipelineIds",List.of()));
                assertThat(planned.get("affectedPipelineIds")).isEqualTo(List.of("a","b"));
                post.apply("/api/storage/recovery/confirm",Map.of("operationId",planned.get("operationId"),"planHash",planned.get("planHash"),
                        "controlRevision",planned.get("controlRevision"),"confirmed",true));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(35)).untilAsserted(() -> {
                    var failed=root.recoveryFailure().orElse(null);
                    if(failed!=null)throw failed;
                    assertThat(post.apply("/api/storage/recovery/status",Map.of("operationId",planned.get("operationId"))).get("phase")).isEqualTo("COMPLETE");
                    assertThat(root.runtime().businessStarted()).isTrue();
                });
                for(String id:List.of("a","b")) {
                    var intent=root.runtime().requireConfiguration().intents().load(id).orElseThrow();
                    assertThat(intent.desired().targetState()).isEqualTo(io.tapstate.core.lifecycle.PipelineState.STOPPED);
                    assertThat(intent.scope().stateGeneration()).isEqualTo(2);
                    assertThat(root.runtime().requireConfiguration().preparations().read(id).orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                }
                assertThat(admin.getDatabase(cacheDb).getCollection("pipeline_state").countDocuments()).isZero();
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isOne();
                var businessOwner=(NativeBusinessContext)root.runtime().business().orElseThrow();
                var child=(org.springframework.context.ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(businessOwner,"context");
                var catalog=child.getBean(io.tapstate.control.core.PipelineCatalogService.class);
                for(String id:List.of("a","b")) {
                    var item=catalog.find(id).orElseThrow();
                    assertThat(item.status().displayState()).isEqualTo(io.tapstate.control.core.PipelineCatalogItem.UiState.RECOVERY_REQUIRED);
                    assertThat(item.status().observedState()).isNull();
                }
                var recoveryService=application.getBean(io.tapstate.control.core.StorageRecoveryService.class);
                var delegate=(io.tapstate.control.core.StorageRecoveryService.Inspector)org.springframework.test.util.ReflectionTestUtils.getField(recoveryService,"inspector");
                org.springframework.test.util.ReflectionTestUtils.setField(recoveryService,"inspector",new io.tapstate.control.core.StorageRecoveryService.Inspector() {
                    private <T> T checked(String stage,java.util.function.Supplier<T> call) {
                        long start=System.nanoTime();System.out.println("CACHE_FULL mode="+mode+" stage="+stage+" begin");
                        try {return call.get();}
                        catch(TapstateException failure){System.out.println("CACHE_FULL mode="+mode+" stage="+stage+" code="+failure.code().code());throw failure;}
                        finally {System.out.println("CACHE_FULL mode="+mode+" stage="+stage+" elapsedMs="+Duration.ofNanos(System.nanoTime()-start).toMillis());}
                    }
                    public StorageRecoveryCommands.Manifest inspect(CacheAuthScope scope,StorageRecoveryCommands.Selection selection) {return checked("inspect",() -> delegate.inspect(scope,selection));}
                    public void requireCurrent(CacheAuthScope scope,StorageRecoveryCommands.Manifest manifest) {checked("current",() -> {delegate.requireCurrent(scope,manifest);return null;});}
                    public void verifyExternal(CacheAuthScope scope,StorageRecoveryCommands.Manifest manifest) {checked("external",() -> {delegate.verifyExternal(scope,manifest);return null;});}
                    public io.tapstate.control.core.StorageRecoveryService.Presentation present(StorageRecoveryCommands.Plan plan) {return checked("present",() -> delegate.present(plan));}
                });
                post.apply("/api/connections:discover-schema",Map.of("id","input","connectorId","mongodb","settings",Map.of("isUri",true,"uri",uri(user,sourceDb))));
                var full=post.apply("/api/storage/recovery/plan",Map.of("scope","PIPELINE","pipelineIds",List.of("a"),"previousOperationId",planned.get("operationId")));
                assertThat(full.get("preflightCurrent")).isEqualTo(true);
                assertThat((List<?>)full.get("targetPolicies")).hasSize(1);
                post.apply("/api/storage/recovery/confirm",Map.of("operationId",full.get("operationId"),"planHash",full.get("planHash"),"controlRevision",full.get("controlRevision"),"confirmed",true));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                    var failed=root.recoveryFailure().orElse(null);if(failed!=null)throw failed;
                    assertThat(post.apply("/api/storage/recovery/status",Map.of("operationId",full.get("operationId"))).get("phase")).isEqualTo("COMPLETE");
                    var business=(NativeBusinessContext)root.runtime().business().orElseThrow();
                    assertThat(io.tapstate.core.lifecycle.StateJson.parse(business.statusStore().state().read("a").orElseThrow().stateJson())).isEqualTo(io.tapstate.core.lifecycle.PipelineState.COMPLETED);
                    assertThat(catalog.find("a").orElseThrow().status().displayState()).isEqualTo(io.tapstate.control.core.PipelineCatalogItem.UiState.COMPLETED);
                });
                var rows=admin.getDatabase(masterDb).getCollection("orders").find().sort(new Document("_id",1)).into(new ArrayList<>());
                assertThat(rows).containsExactly(new Document("_id",1L).append("value","first"),new Document("_id",2L).append("value","second"));
                assertThat(root.runtime().requireConfiguration().intents().load("b").orElseThrow().desired().targetState()).isEqualTo(io.tapstate.core.lifecycle.PipelineState.STOPPED);
                assertThat(catalog.find("b").orElseThrow().status().displayState()).isEqualTo(io.tapstate.control.core.PipelineCatalogItem.UiState.RECOVERY_REQUIRED);
                assertThat(root.runtime().requireConfiguration().preparations().read("b").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                assertThatThrownBy(() -> post.apply("/api/pipelines/b:start",Map.of())).isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                admin.getDatabase(masterDb).getCollection("orders").insertOne(new Document("_id",100).append("value","after-completion"));
                post.apply("/api/storage/recovery/confirm",Map.of("operationId",full.get("operationId"),"planHash",full.get("planHash"),"controlRevision",full.get("controlRevision"),"confirmed",true));
                assertThat(admin.getDatabase(masterDb).getCollection("orders").countDocuments()).isEqualTo(3);

            }
        }
    }

    /** Real production-root routing and Mongo persistence; the SDK boundary remains a local fixture. */
    @Test
    void wholeCacheLossUsesOriginalCloudExchangeAndDurableRevocationWithoutBusinessState() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "cloud_config_" + suffix, cacheDb = "cloud_cache_" + suffix, masterDb = "cloud_master_" + suffix;
        String user = "cloud_" + suffix;
        var original = new io.tapstate.spi.store.StorageSetupScope("cloud-" + suffix, "installation", "setup", 1);
        var sdk = new RecoverySdk(original.clusterId());
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var cache = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB,
                    uri(user, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, configDb), null, Duration.ofSeconds(5)),
                    original.clusterId(), original.installationId(), original.operationId(), 1)) {
                var target = new SourceResource("atlas-store", null, "mongodb", Map.of("uri", uri(user, masterDb)), null, null, null, null);
                var initial = configuration.bindings().commitWithFirstCache(original, io.tapstate.spi.store.StorageBinding.Mode.CLOUD, cache, target);
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cache, Duration.ofSeconds(5), "cloud-fixture")) {
                    cached.prepareFirstAuth(initial.firstCreation().orElseThrow());
                }
                configuration.bindings().complete(original);
            }
            var configuration = admin.getDatabase(configDb);
            Document binding = configuration.getCollection("storage_binding").find().first();
            Document keys = configuration.getCollection("cluster_keys").find().first();
            String old;
            try (var first = cloudRecoveryRoot(uri(user, configDb), sdk)) { old = exchangeCookie(first, "old"); }
            admin.getDatabase(cacheDb).drop();
            verifyNewAuthenticationProcess(uri(user, configDb), original.clusterId(), true);
            try (var restarted = cloudRecoveryRoot(uri(user, configDb), sdk)) {
                var root = restarted.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
                assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(2);
                assertCloudStatus(restarted, old, 401);
                assertThatThrownBy(() -> exchangeCookie(restarted, "old"))
                        .isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                String timestamp = Long.toString(Instant.now().getEpochSecond());
                rootClient(restarted).post().uri("/auth/invalidate-session?jti=blocked&ts=" + timestamp + "&nonce=fixture-nonce&sign=fixture-signature")
                        .retrieve().toBodilessEntity();
                assertThat(admin.getDatabase(cacheDb).getCollection("cloud_revocations").countDocuments()).isOne();
            }
            // The blocked code has never been consumed. Only the durable callback survives this restart.
            try (var restarted = cloudRecoveryRoot(uri(user, configDb), sdk)) {
                var root = restarted.getBean(StorageRoot.class);
                assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(2);
                assertThatThrownBy(() -> exchangeCookie(restarted, "blocked"))
                        .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                String fresh = exchangeCookie(restarted, "fresh");
                assertCloudStatus(restarted, fresh, 200);
                assertThat(root.runtime().business()).isEmpty();
                assertThat(restarted.getBeansOfType(com.hazelcast.core.HazelcastInstance.class)).isEmpty();
                assertThat(restarted.getBeansOfType(io.tapstate.spi.store.UserStore.class)).isEmpty();
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>()))
                        .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "cloud_sessions", "cloud_revocations");
                assertThat(binding.equals(configuration.getCollection("storage_binding").find().first())).isTrue();
                var currentKey = configuration.getCollection("cluster_keys").find().first();
                var materialBefore = new Document(keys); materialBefore.remove("sourceWriteFence");
                var materialAfter = new Document(currentKey); materialAfter.remove("sourceWriteFence");
                assertThat(materialBefore.equals(materialAfter)).isTrue();
                assertThat(configuration.getCollection("users").countDocuments()).isZero();
            }
        }
    }

    /** The SDK and connector are owned fixtures; storage, TLS, HTTP and the business child are real. */
    @Test
    void cloudConfigurationInitializationRetriesConfirmationInTheSameProcessAndNeverRecreatesLostKeys() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "first_config_" + suffix, user = "config_" + suffix;
        var sdk = new RecoverySdk("new-" + suffix);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb));
            sdk.configurationHash = io.tapstate.core.model.canonical.CanonicalHash.ofText(uri(user, configDb));
            sdk.failCompletionResponses = 1;
            Document key;
            try (var application = cloudRecoveryRoot(uri(user, configDb), sdk)) {
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_UNAVAILABLE);
                key = admin.getDatabase(configDb).getCollection("cluster_keys").find().first();
                assertThat(key).isNotNull();
                assertThat(root.runtime().configuration()).isEmpty();
                assertThat(sdk.configurationClaims).hasValue(1);
                String temporary = exchangeCookie(application, "old");
                assertThat(temporary).isNotBlank();
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.SETUP_REQUIRED);
                assertThat(root.runtime().requireConfiguration().setupStates().load().scope().operationId()).isEqualTo(sdk.configurationOperation);
                assertThat(sdk.configurationClaims).hasValue(1); assertThat(sdk.configurationCompletions).hasValue(2);
                assertThat(admin.getDatabase(configDb).getCollection("cluster_keys").find().first()).isEqualTo(key);
                assertThat(root.runtime().current()).isEmpty(); assertThat(root.runtime().business()).isEmpty();
            }
            try (var restarted = cloudRecoveryRoot(uri(user, configDb), sdk)) {
                assertThat(restarted.getBean(StorageRoot.class).phase()).isEqualTo(StorageRoot.Phase.SETUP_REQUIRED);
                assertThat(sdk.configurationClaims).hasValue(1);
                assertThat(admin.getDatabase(configDb).getCollection("cluster_keys").find().first()).isEqualTo(key);
            }
            admin.getDatabase(configDb).drop();
            try (var lost = cloudRecoveryRoot(uri(user, configDb), sdk)) {
                var root = lost.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_UNAVAILABLE);
                assertThatThrownBy(() -> exchangeCookie(lost, "fresh")).isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_UNAVAILABLE);
                assertThat(root.runtime().configuration()).isEmpty();
                assertThat(admin.getDatabase(configDb).listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(sdk.configurationClaims).hasValue(2);
            }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"normal", "retry", "loss", "restart"})
    void lostInitialCacheCreationNeedsFreshCloudLoginAndExplicitOriginalRecovery(String fault) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "initial_config_" + suffix, cacheDb = "initial_cache_" + suffix, masterDb = "initial_master_" + suffix;
        String configUser = "config_" + suffix, customerUser = "customer_" + suffix;
        var scope = new StorageSetupScope("initial-" + suffix, "installation", "setup", 1);
        var sdk = new RecoverySdk(scope.clusterId());
        Path trace = work.resolve("initial-target-tests"), seeds = Files.createDirectory(work.resolve("initial-seeds"));
        Files.copy(io.tapstate.adapters.pdk.SyntheticSinkJars.registeredStorageProbe(work.resolve("initial-probe-build"), trace,
                "mongodb-atlas", "mongodb://"), seeds.resolve("mongodb-atlas.jar"));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, configUser, List.of(configDb)); createUser(admin, customerUser, List.of(cacheDb, masterDb));
            var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.ATLAS,
                    uri(customerUser, cacheDb) + "&tls=true&authMechanism=SCRAM-SHA-256", cacheDb, TLS.ca());
            var source = new SourceResource("atlas-store", null, "mongodb-atlas", Map.of("isUri", true,
                    "uri", uri(customerUser, masterDb) + "&tls=true&authMechanism=SCRAM-SHA-256", "tlsCaPem", TLS.ca()), null, null, null, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser, configDb), null, Duration.ofSeconds(5)),
                    scope.clusterId(), scope.installationId(), scope.operationId(), 1)) {
                configuration.bindings().commitWithFirstCache(scope, StorageBinding.Mode.CLOUD, cache, source);
                // The first process lost its non-serializable creation invocation before any CACHE transaction.
            }
            var originalBinding = admin.getDatabase(configDb).getCollection("storage_binding").find().first();
            try (var application = cloudRecoveryRoot(uri(configUser, configDb), sdk, CloudConnectorTestInputs.seedDirectory(), seeds)) {
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
                String cookie = exchangeCookie(application, "fresh");
                assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(2);
                assertThat(cloudPost(application, cookie, "/api/storage/status", Map.of()))
                        .containsEntry("businessPhase", "AWAITING_REBUILD_CONFIRMATION");
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>()))
                        .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "cloud_sessions", "cloud_revocations");
                var orphan = admin.getDatabase(configDb).getCollection("pdk_note_migrations");
                orphan.insertOne(new Document("_id", "unknown-history"));
                String initialCookie = cookie;
                assertThatThrownBy(() -> cloudPost(application, initialCookie, "/api/storage/recovery/plan", Map.of("scope", "CLUSTER", "pipelineIds", List.of())))
                        .isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                orphan.deleteOne(new Document("_id", "unknown-history"));
                var planned = cloudPost(application, cookie, "/api/storage/recovery/plan", Map.of("scope", "CLUSTER", "pipelineIds", List.of()));
                assertThat(planned).containsEntry("phase", "PLANNED").containsEntry("affectedPipelineIds", List.of());
                assertThat(((Map<?,?>)planned.get("definitionHashes")).containsKey("atlas-store")).isTrue();
                assertThat(root.runtime().business()).isEmpty();
                var recoverySnapshot = cloudPost(application, cookie, "/api/storage/status", Map.of());
                assertThat(recoverySnapshot).containsEntry("operationId", planned.get("operationId"));
                assertThat(((List<?>)recoverySnapshot.get("allowedActions")).contains("storage.recovery.confirm")).isTrue();
                assertThat(((List<?>)recoverySnapshot.get("allowedActions")).contains("storage.setup.commit")).isFalse();
                if (!fault.equals("normal")) failNextSetupCompletion(root.runtime().requireConfiguration());
                var confirmation = Map.<String,Object>of("operationId", planned.get("operationId"), "planHash", planned.get("planHash"),
                        "controlRevision", planned.get("controlRevision"), "confirmed", true);
                assertThat(cloudPost(application, cookie, "/api/storage/recovery/confirm", confirmation)).containsEntry("phase", "CONFIRMED");
                if (!fault.equals("normal")) {
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() ->
                            Boolean.TRUE.equals(org.springframework.test.util.ReflectionTestUtils.getField(root, "recoveryFailed")));
                    assertThat(root.runtime().requireConfiguration().setupStates().load().phase()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
                    assertThat(root.runtime().business()).isEmpty();
                    assertThat(cloudPost(application, cookie, "/api/storage/status", Map.of())).containsEntry("operationId", planned.get("operationId"));
                    if (fault.equals("restart")) {
                        root.close();
                        try (var resumed = cloudRecoveryRoot(uri(configUser, configDb), new RecoverySdk(scope.clusterId()),
                                CloudConnectorTestInputs.seedDirectory(), seeds)) {
                            var resumedRoot = resumed.getBean(StorageRoot.class);
                            String resumedCookie = exchangeCookie(resumed, "restarted"); var originalPlan = planned;
                            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                                assertThat(cloudPost(resumed, resumedCookie, "/api/storage/recovery/status", Map.of("operationId", originalPlan.get("operationId"))))
                                        .containsEntry("phase", "COMPLETE");
                                assertThat(resumedRoot.runtime().businessStarted()).isTrue();
                            });
                            assertThat(admin.getDatabase(configDb).getCollection("storage_binding").find().first()).isEqualTo(originalBinding);
                            assertThat(admin.getDatabase(cacheDb).getCollection("cache_identity").find().first().get("businessPreparationId"))
                                    .isEqualTo(originalPlan.get("operationId"));
                            assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isOne();
                        }
                        return;
                    }
                    if (fault.equals("loss")) {
                        var oldPlan = planned; var oldConfirmation = confirmation; String oldCookie = cookie;
                        admin.getDatabase(cacheDb).drop();
                        cookie = exchangeCookie(application, "restarted");
                        assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(3);
                        assertCloudStatus(application, oldCookie, 401);
                        String currentCookie = cookie;
                        assertThat(cloudPost(application, cookie, "/api/storage/status", Map.of())).containsEntry("operationId", oldPlan.get("operationId"));
                        assertThat(cloudPost(application, cookie, "/api/storage/recovery/status", Map.of("operationId", oldPlan.get("operationId"))))
                                .containsEntry("phase", "RUNNING");
                        assertThatThrownBy(() -> cloudPost(application, currentCookie, "/api/storage/recovery/confirm", oldConfirmation))
                                .isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                        planned = cloudPost(application, cookie, "/api/storage/recovery/plan", Map.of("scope", "CLUSTER", "pipelineIds", List.of(),
                                "previousOperationId", oldPlan.get("operationId")));
                        assertThat(planned).containsEntry("phase", "PLANNED");
                        assertThat(planned.get("operationId")).isNotEqualTo(oldPlan.get("operationId"));
                        assertThat(root.runtime().business()).isEmpty();
                        confirmation = Map.of("operationId", planned.get("operationId"), "planHash", planned.get("planHash"),
                                "controlRevision", planned.get("controlRevision"), "confirmed", true);
                    }
                    cloudPost(application, cookie, "/api/storage/recovery/confirm", confirmation);
                }
                String acceptedCookie = cookie; var acceptedPlan = planned;
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                    assertThat(cloudPost(application, acceptedCookie, "/api/storage/recovery/status", Map.of("operationId", acceptedPlan.get("operationId"))))
                            .containsEntry("phase", "COMPLETE");
                    assertThat(root.runtime().businessStarted()).isTrue();
                });
                assertThat(root.runtime().requireConfiguration().setupStates().load().phase()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
                assertThat(cloudPost(application, cookie, "/api/storage/recovery/confirm", confirmation)).containsEntry("phase", "COMPLETE");
                assertThat(admin.getDatabase(configDb).getCollection("storage_binding").find().first()).isEqualTo(originalBinding);
                assertThat(admin.getDatabase(configDb).getCollection("sources").countDocuments()).isOne();
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isOne();
                assertThat(admin.getDatabase(cacheDb).getCollection("cache_identity").find().first().get("businessPreparationId"))
                        .isEqualTo(planned.get("operationId"));
            }
        }
    }

    private static void failNextSetupCompletion(io.tapstate.adapters.mongostore.MongoConfigurationDomain configuration) {
        var original = configuration.bindings(); var bindings = org.mockito.Mockito.spy(original);
        var fail = new java.util.concurrent.atomic.AtomicBoolean(true);
        org.mockito.Mockito.doAnswer(call -> {
            var guarded = org.mockito.Mockito.spy((io.tapstate.spi.store.StorageBindingStore)call.callRealMethod());
            org.mockito.Mockito.doAnswer(completion -> {
                var result = completion.callRealMethod();
                if (fail.compareAndSet(true, false)) throw StorageRuntime.unavailable();
                return result;
            }).when(guarded).complete(org.mockito.ArgumentMatchers.any());
            return guarded;
        }).when(bindings).guardedBy(org.mockito.ArgumentMatchers.any());
        org.springframework.test.util.ReflectionTestUtils.setField(configuration, "bindings", bindings);
    }

    @Test void aDisconnectedCloudSetupCallerCannotCloseItsRetainedNativeCandidate() throws Exception {
        String suffix=UUID.randomUUID().toString().replace("-","");
        String configDb="disconnect_config_"+suffix,cacheDb="disconnect_cache_"+suffix,masterDb="disconnect_master_"+suffix;
        String configUser="disconnect_config_"+suffix,customerUser="disconnect_customer_"+suffix;
        String cluster="disconnect-"+suffix; var sdk=new RecoverySdk(cluster);
        Path trace=work.resolve("disconnect-target-tests"),seeds=Files.createDirectory(work.resolve("disconnect-setup-seeds"));
        Path artifact=io.tapstate.adapters.pdk.SyntheticSinkJars.registeredStorageProbe(work.resolve("disconnect-probe"),trace,"mongodb-atlas","mongodb://");
        Files.copy(artifact,seeds.resolve("mongodb-atlas.jar"));
        var paused=new java.util.concurrent.CountDownLatch(1); var publish=new java.util.concurrent.CountDownLatch(1);
        var childRef=new AtomicReference<ConfigurableApplicationContext>(); var ownerRef=new AtomicReference<NativeBusinessContext>();
        var assemblies=new java.util.concurrent.atomic.AtomicInteger(); var response=new DisconnectedResponse();
        try (MongoClient admin=MongoClients.create(uri("root","admin"))) {
            createUser(admin,configUser,List.of(configDb)); createUser(admin,customerUser,List.of(cacheDb,masterDb));
            var settings=new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser,configDb),null,Duration.ofSeconds(5));
            try (var ignored=io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings,cluster,"installation","setup",1)) { }
            try (var application=cloudRecoveryRoot(settings.uri(),sdk,CloudConnectorTestInputs.seedDirectory(),seeds,context -> {
                var factory=new org.springframework.beans.factory.support.RootBeanDefinition(StorageBusinessFactory.class,
                        () -> (StorageBusinessFactory)bound -> {
                            assemblies.incrementAndGet(); var owner=new NativeBusinessContext(context.getBean(StorageRoot.class),bound,context);
                            ownerRef.set(owner); return owner;
                        }); factory.setPrimary(true);
                var registry=(org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory();
                registry.registerBeanDefinition("observedNativeSetupFactory",factory);
                registry.registerBeanDefinition("disconnectResponseProbe",new org.springframework.beans.factory.support.RootBeanDefinition(
                        org.springframework.boot.web.servlet.FilterRegistrationBean.class,() -> new org.springframework.boot.web.servlet.FilterRegistrationBean<>(response)));
                context.addApplicationListener((org.springframework.context.ApplicationListener<org.springframework.context.event.ContextRefreshedEvent>)event -> {
                    if (event.getApplicationContext() instanceof ConfigurableApplicationContext child && child.getParent()==context
                            && child.containsLocalBean("nativeBusinessContext")) {
                        childRef.set(child); paused.countDown();
                        try { if (!publish.await(60,java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("the test did not release its native candidate"); }
                        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
                    }
                });
            })) {
                String temporary=exchangeCookie(application,"old"); var root=application.getBean(StorageRoot.class);
                var original=root.runtime().requireConfiguration().setupStates().load().scope();
                var candidate=Map.of("cache",Map.of("kind","ATLAS","uri",uri(customerUser,cacheDb)+"&tls=true&authMechanism=SCRAM-SHA-256","database",cacheDb,"caPem",TLS.ca()),
                        "master",Map.of("kind","ATLAS","uri",uri(customerUser,masterDb)+"&tls=true&authMechanism=SCRAM-SHA-256","database",masterDb,"caPem",TLS.ca()));
                var stamps=new LinkedHashMap<String,Object>();
                for (var layer:io.tapstate.control.core.StorageSetupService.Layer.values()) {
                    var tested=cloudPost(application,temporary,"/api/storage/test",Map.of("operationId",original.operationId(),"generation",original.generation(),
                            "candidate",candidate,"layer",layer.name())); stamps.put(layer.name(),tested.get("testStamp"));
                }
                var request=Map.<String,Object>of("operationId",original.operationId(),"generation",original.generation(),"candidate",candidate,"testStamps",stamps);
                int port=((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
                byte[] body=io.tapstate.core.common.JsonWriter.write(request).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                try {
                    try (var socket=new java.net.Socket("127.0.0.1",port)) {
                        socket.setSoLinger(true,0);
                        String headers="POST /api/storage/setup/commit HTTP/1.1\r\nHost: 127.0.0.1:"+port+"\r\nOrigin: http://127.0.0.1:"+port
                                +"\r\nCookie: "+temporary+"\r\nContent-Type: application/json\r\nContent-Length: "+body.length+"\r\nConnection: close\r\n\r\n";
                        socket.getOutputStream().write(headers.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                        socket.getOutputStream().write(body); socket.getOutputStream().flush();
                        assertThat(paused.await(60,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                        assertThat(assemblies.get()).isEqualTo(1);
                        assertThat(admin.getDatabase(configDb).getCollection("storage_setup").find().first()).containsEntry("phase","COMPLETE");
                    }
                    var child=childRef.get(); var member=child.getBean(com.hazelcast.core.HazelcastInstance.class);
                    assertThat(child.isActive()).isTrue(); assertThat(member.getLifecycleService().isRunning()).isTrue();
                    publish.countDown();
                    assertThat(response.finished.await(60,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    assertThat(response.failure.get()).as("the original HTTP reply meets an actually reset client socket").isNotNull();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(root.runtime()::businessStarted);
                    assertThat(root.runtime().business()).contains(ownerRef.get());
                    assertThat(child.isActive()).isTrue(); assertThat(member.getLifecycleService().isRunning()).isTrue();
                    assertCloudStatus(application,temporary,401);
                    String fresh=exchangeCookie(application,"fresh");
                    assertThat(cloudPost(application,fresh,"/api/storage/setup/commit",request)).containsEntry("setupPhase","COMPLETE");
                    assertThat(cloudPost(application,fresh,"/api/storage/status",Map.of())).containsEntry("businessPhase","READY");
                    assertThat(assemblies.get()).isEqualTo(1); assertThat(root.runtime().business()).contains(ownerRef.get());
                    String sources=rootClient(application).get().uri("/api/sources").header("Cookie",fresh).retrieve().body(String.class);
                    assertThat(sources).contains("atlas-store").doesNotContain(PASSWORD,"BEGIN CERTIFICATE");
                    assertThat(admin.getDatabase(configDb).getCollection("sources").countDocuments()).isOne();
                    assertThat(admin.getDatabase(configDb).getCollection("storage_binding").countDocuments()).isOne();
                    assertThat(Files.readAllLines(trace)).hasSize(2);
                } finally { publish.countDown(); }
            } finally { publish.countDown(); }
        }
    }

    private static final class DisconnectedResponse implements jakarta.servlet.Filter {
        final java.util.concurrent.CountDownLatch finished=new java.util.concurrent.CountDownLatch(1);
        final AtomicReference<java.io.IOException> failure=new AtomicReference<>();
        private final java.util.concurrent.atomic.AtomicBoolean first=new java.util.concurrent.atomic.AtomicBoolean(true);
        @Override public void doFilter(jakarta.servlet.ServletRequest request,jakarta.servlet.ServletResponse response,jakarta.servlet.FilterChain chain)
                throws java.io.IOException,jakarta.servlet.ServletException {
            if (!(request instanceof jakarta.servlet.http.HttpServletRequest http) || !http.getRequestURI().equals("/api/storage/setup/commit")
                    || !first.compareAndSet(true,false)) { chain.doFilter(request,response); return; }
            var wrapped=new jakarta.servlet.http.HttpServletResponseWrapper((jakarta.servlet.http.HttpServletResponse)response) {
                private jakarta.servlet.ServletOutputStream observed;
                @Override public jakarta.servlet.ServletOutputStream getOutputStream() throws java.io.IOException {
                    if (observed!=null) return observed; var output=super.getOutputStream();
                    observed=new jakarta.servlet.ServletOutputStream() {
                        @Override public boolean isReady() { return output.isReady(); }
                        @Override public void setWriteListener(jakarta.servlet.WriteListener listener) { output.setWriteListener(listener); }
                        @Override public void write(int value) throws java.io.IOException { try { output.write(value); } catch (java.io.IOException error) { failure.compareAndSet(null,error); throw error; } }
                        @Override public void write(byte[] value,int offset,int length) throws java.io.IOException { try { output.write(value,offset,length); } catch (java.io.IOException error) { failure.compareAndSet(null,error); throw error; } }
                        @Override public void flush() throws java.io.IOException { try { output.flush(); } catch (java.io.IOException error) { failure.compareAndSet(null,error); throw error; } }
                        @Override public void close() throws java.io.IOException { try { output.close(); } catch (java.io.IOException error) { failure.compareAndSet(null,error); throw error; } }
                    }; return observed;
                }
            };
            try { chain.doFilter(request,wrapped); wrapped.flushBuffer(); }
            catch (java.io.IOException error) { failure.compareAndSet(null,error); throw error; }
            finally { finished.countDown(); }
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true})
    void cloudFirstSetupContinuesInTheSameProcessAndReauthenticatesAfterCutover(boolean failAfterCompletion) throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "setup_config_" + suffix, cacheDb = "setup_cache_" + suffix, masterDb = "setup_master_" + suffix;
        String configUser = "config_" + suffix, customerUser = "customer_" + suffix;
        String cluster = "setup-" + suffix; var sdk = new RecoverySdk(cluster);
        Path trace = work.resolve("target-tests"), seeds = Files.createDirectory(work.resolve("setup-seeds"));
        Path artifact = io.tapstate.adapters.pdk.SyntheticSinkJars.registeredStorageProbe(work.resolve("probe-build"), trace, "mongodb-atlas", "mongodb://");
        Files.copy(artifact, seeds.resolve("mongodb-atlas.jar"));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, configUser, List.of(configDb)); createUser(admin, customerUser, List.of(cacheDb, masterDb));
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser, configDb), null, Duration.ofSeconds(5));
            try (var ignored = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings, cluster, "installation", "setup", 1)) { }
            try (var application = cloudRecoveryRoot(uri(configUser, configDb), sdk, CloudConnectorTestInputs.seedDirectory(), seeds)) {
                long pid = ProcessHandle.current().pid(); var root = application.getBean(StorageRoot.class);
                var configuration = root.runtime().requireConfiguration();
                var cutoverFault = new java.util.concurrent.atomic.AtomicBoolean(failAfterCompletion);
                var injected = new AtomicReference<Throwable>();
                var bindings = org.mockito.Mockito.spy(configuration.bindings());
                var committedBoundary = new java.util.concurrent.CountDownLatch(1);
                var deliverGrant = new java.util.concurrent.CountDownLatch(1);
                if (!failAfterCompletion) org.mockito.Mockito.doAnswer(call -> {
                    var guarded = org.mockito.Mockito.spy((io.tapstate.spi.store.StorageBindingStore)call.callRealMethod());
                    org.mockito.Mockito.doAnswer(commit -> {
                        var value = commit.callRealMethod(); committedBoundary.countDown();
                        assertThat(deliverGrant.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); return value;
                    }).when(guarded).commitWithFirstCache(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
                            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
                    return guarded;
                }).when(bindings).guardedBy(org.mockito.ArgumentMatchers.any());
                org.mockito.Mockito.doAnswer(call -> {
                    Object result = call.callRealMethod();
                    if (configuration.setupStates().load().phase() == StorageSetupStateStore.Phase.COMPLETE && cutoverFault.compareAndSet(true, false)) {
                        var failure = StorageRuntime.unavailable(); injected.set(failure); throw failure;
                    }
                    return result;
                }).when(bindings).load();
                org.springframework.test.util.ReflectionTestUtils.setField(configuration, "bindings", bindings);
                String temporary = exchangeCookie(application, "old");
                var oldContext = root.managedAuthentication();
                var original = configuration.setupStates().load().scope();
                String cacheUri = uri(customerUser, cacheDb) + "&tls=true&authMechanism=SCRAM-SHA-256";
                String masterUri = uri(customerUser, masterDb) + "&tls=true&authMechanism=SCRAM-SHA-256";
                var candidate = Map.of("cache", Map.of("kind", "ATLAS", "uri", cacheUri, "database", cacheDb, "caPem", TLS.ca()),
                        "master", Map.of("kind", "ATLAS", "uri", masterUri, "database", masterDb, "caPem", TLS.ca()));
                var stamps = new java.util.LinkedHashMap<String,Object>();
                for (var layer : io.tapstate.control.core.StorageSetupService.Layer.values()) {
                    var tested = cloudPost(application, temporary, "/api/storage/test", Map.of("operationId", original.operationId(),
                            "generation", original.generation(), "candidate", candidate, "layer", layer.name()));
                    stamps.put(layer.name(), tested.get("testStamp"));
                }
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>())).isEmpty();
                var request = Map.<String,Object>of("operationId", original.operationId(), "generation", original.generation(),
                        "candidate", candidate, "testStamps", stamps);
                if (failAfterCompletion) assertThatThrownBy(() -> cloudPost(application, temporary, "/api/storage/setup/commit", request))
                        .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).withFailMessage("%s; injected at %s",
                                        failure.getResponseBodyAsString(), injected.get() == null ? "never" : java.util.Arrays.toString(injected.get().getStackTrace())).isEqualTo(503));
                else try (var callers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
                    var commit = callers.submit(() -> cloudPost(application, temporary, "/api/storage/setup/commit", request));
                    try {
                        assertThat(committedBoundary.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                        var loginEntered = new java.util.concurrent.CountDownLatch(1);
                        var login = callers.submit(() -> { loginEntered.countDown(); return root.managedAuthentication(); });
                        assertThat(loginEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                        var whileCommitting = login.get(5, java.util.concurrent.TimeUnit.SECONDS);
                        assertThat(whileCommitting.service()).isSameAs(oldContext.service());
                        whileCommitting.requireActive().run();
                        assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>())).isEmpty();
                        deliverGrant.countDown();
                        assertThat(commit.get(60, java.util.concurrent.TimeUnit.SECONDS)).containsEntry("setupPhase", "COMPLETE");
                        assertThatThrownBy(() -> whileCommitting.requireActive().run()).isInstanceOf(TapstateException.class);
                        root.managedAuthentication().requireActive().run();
                        assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(1);
                    } finally { deliverGrant.countDown(); }
                }
                assertThat(configuration.setupStates().load().phase()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
                assertThat(admin.getDatabase(configDb).getCollection("schema_migrations").find(new Document("_id", "config")).first().getInteger("installedVersion")).isEqualTo(2);
                assertThat(admin.getDatabase(cacheDb).getCollection("schema_migrations").find(new Document("_id", "business")).first().getInteger("installedVersion")).isEqualTo(2);
                assertThat(admin.getDatabase(cacheDb).listCollectionNames()).contains("validation_runs");
                assertThat(admin.getDatabase(cacheDb).getCollection("cloud_sessions").countDocuments()).isZero();
                assertCloudStatus(application, temporary, 401);
                assertThatThrownBy(() -> oldContext.service().exchangeCode("late", "127.0.0.1")).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> exchangeCookie(application, "old")).isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                String fresh = exchangeCookie(application, "fresh");
                assertThat(cloudPost(application, fresh, "/api/storage/setup/commit", request)).containsEntry("setupPhase", "COMPLETE");
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(root.runtime()::businessStarted);
                assertThat(cloudPost(application, fresh, "/api/storage/status", Map.of())).containsEntry("businessPhase", "READY");
                assertThat(ProcessHandle.current().pid()).isEqualTo(pid);
                String sources = rootClient(application).get().uri("/api/sources").header("Cookie", fresh).retrieve().body(String.class);
                assertThat(sources).contains("atlas-store").doesNotContain(PASSWORD, "BEGIN CERTIFICATE");
                var saved = (SourceResource)configuration.artifacts().get("atlas-store").orElseThrow();
                assertThat(saved.config()).containsEntry("tlsCaPem", TLS.ca());
                assertThat(root.runtime().current().orElseThrow().binding().cache().caPem()).isEqualTo(TLS.ca());
                assertThat(Files.readAllLines(trace)).hasSize(2);
                assertThat(admin.getDatabase(configDb).getCollection("sources").countDocuments()).isOne();
                assertThat(admin.getDatabase(configDb).getCollection("storage_binding").countDocuments()).isOne();
                assertThat(admin.getDatabase(configDb).getCollection("users").countDocuments()).isZero();
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isOne();
                try (MongoClient limited = MongoClients.create(uri(configUser, configDb))) {
                    assertDenied(limited, cacheDb); assertDenied(limited, masterDb);
                    assertDenied(limited, configDb + "_operator"); assertDenied(limited, configDb + "_views");
                }
                // A real CACHE authorization failure cannot reopen the closed bootstrap owner.
                var bindingBeforeOutage = configuration.bindings().load().orElseThrow();
                var sourceBeforeOutage = configuration.artifacts().get("atlas-store").orElseThrow();
                var setupBeforeOutage = configuration.setupStates().load();
                admin.getDatabase("admin").runCommand(new Document("updateUser", customerUser)
                        .append("roles", List.of(new Document("role", "readWrite").append("db", masterDb))));
                try {
                    assertThatThrownBy(() -> cloudPost(application, fresh, "/api/storage/status", Map.of()))
                            .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class, failure -> {
                                assertThat(failure.getStatusCode().value()).isEqualTo(503);
                                assertThat(failure.getResponseBodyAsString()).contains("control.storage-unavailable");
                            });
                    rootClient(application).get().uri("/auth/exchange?code=cache-outage").exchange((outageRequest, response) -> {
                        assertThat(response.getStatusCode().value()).isEqualTo(503);
                        assertThat(response.getHeaders().getFirst("Set-Cookie")).isNull();
                        assertThat(response.bodyTo(String.class)).contains("control.storage-unavailable");
                        return null;
                    });
                    assertThat(sdk.consumed).doesNotContain("cache-outage");
                    assertThatThrownBy(() -> oldContext.service().exchangeCode("late-during-cache-outage", "127.0.0.1"))
                            .isInstanceOfSatisfying(TapstateException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(io.tapstate.control.core.ControlError.UNAUTHENTICATED));
                    assertThat(sdk.consumed).contains("late-during-cache-outage");
                    assertThat(configuration.setupStates().load().phase()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
                    assertThat(admin.getDatabase(configDb).getCollection("sources").countDocuments()).isOne();
                    assertThat(admin.getDatabase(configDb).getCollection("storage_binding").countDocuments()).isOne();
                } finally {
                    admin.getDatabase("admin").runCommand(new Document("updateUser", customerUser).append("roles",
                            List.of(cacheDb, masterDb).stream().map(database ->
                                    new Document("role", "readWrite").append("db", database)).toList()));
                }
                assertCloudStatus(application, temporary, 401);
                String afterOutage = exchangeCookie(application, "after-cache-outage");
                assertThat(cloudPost(application, afterOutage, "/api/storage/status", Map.of()))
                        .containsEntry("setupPhase", "COMPLETE");
                assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(1);
                assertThat(configuration.bindings().load().orElseThrow().equals(bindingBeforeOutage)).isTrue();
                assertThat(configuration.artifacts().get("atlas-store").orElseThrow().equals(sourceBeforeOutage)).isTrue();
                assertThat(configuration.setupStates().load()).isEqualTo(setupBeforeOutage);
                var owner = (NativeBusinessContext)root.runtime().business().orElseThrow();
                var child = (org.springframework.context.ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(owner, "context");
                var member = child.getBean(com.hazelcast.core.HazelcastInstance.class);
                root.close(); assertThat(member.getLifecycleService().isRunning()).isFalse();
                assertThat(rootClient(application).get().uri("/healthz").retrieve().toBodilessEntity().getStatusCode().value()).isEqualTo(200);
                var claims = admin.getDatabase(cacheDb).getCollection("workload_claims").aggregate(List.of(
                        new Document("$project", new Document("leased", new Document("$gt", List.of("$leaseUntil", "$$NOW")))))).into(new ArrayList<>());
                assertThat(claims).hasSize(1);
                assertThat(claims.getFirst().get("leased")).isEqualTo(false);
            }
            // A new process context reconstructs clients from the same persisted bundle, never a probe file.
            if (failAfterCompletion) admin.getDatabase("admin").runCommand(new Document("updateUser", configUser).append("roles", List.of()));
            try (var restarted = cloudRecoveryRoot(uri(configUser, configDb), new RecoverySdk(cluster), CloudConnectorTestInputs.seedDirectory(), seeds)) {
                if (failAfterCompletion) {
                    assertThat(restarted.getBean(StorageRoot.class).phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_UNAVAILABLE);
                    admin.getDatabase("admin").runCommand(new Document("updateUser", configUser)
                            .append("roles", List.of(new Document("role", "readWrite").append("db", configDb))));
                    String reauthenticated = exchangeCookie(restarted, "restarted");
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20))
                            .until(restarted.getBean(StorageRoot.class).runtime()::businessStarted);
                    assertThat(cloudPost(restarted, reauthenticated, "/api/storage/status", Map.of())).containsEntry("businessPhase", "READY");
                }
                assertThat(restarted.getBean(StorageRoot.class).phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
                assertThat(admin.getDatabase(configDb).getCollection("sources").countDocuments()).isOne();
            }
        }
    }

    @Test void setupCommitCannotLoseItsLiveCreationGrantToConcurrentLogin() throws Exception {
        cloudFirstSetupContinuesInTheSameProcessAndReauthenticatesAfterCutover(false);
    }

    @SuppressWarnings("unchecked")
    private static Map<String,Object> cloudPost(ConfigurableApplicationContext application, String cookie, String path, Map<String,?> body) {
        int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
        return rootClient(application).post().uri(path).header("Cookie", cookie).header("Origin", "http://127.0.0.1:" + port)
                .body(body).retrieve().body(Map.class);
    }

    private ConfigurableApplicationContext cloudRecoveryRoot(String configurationUri, RecoverySdk sdk) {
        return cloudRecoveryRoot(configurationUri, sdk, work.resolve("no-seeds"));
    }
    private ConfigurableApplicationContext cloudRecoveryRoot(String configurationUri, RecoverySdk sdk, Path seeds) {
        return cloudRecoveryRoot(configurationUri, sdk, seeds, null);
    }
    private ConfigurableApplicationContext cloudRecoveryRoot(String configurationUri, RecoverySdk sdk, Path seeds, Path setupFixtureSeeds) {
        return cloudRecoveryRoot(configurationUri,sdk,seeds,setupFixtureSeeds,ignored -> {});
    }
    private ConfigurableApplicationContext cloudRecoveryRoot(String configurationUri, RecoverySdk sdk, Path seeds, Path setupFixtureSeeds,
            java.util.function.Consumer<ConfigurableApplicationContext> fixtureInitialization) {
        return new SpringApplicationBuilder(Bootstrap.class, RecoverySdkConfiguration.class)
                .environment(CloudFixtureEnvironment.isolated())
                .initializers(context -> {
                    context.getBeanFactory().registerSingleton("fixtureRecoverySdk", sdk);
                    if (setupFixtureSeeds != null) {
                        var properties = new ConnectorPluginProperties(); properties.setSeedDir(setupFixtureSeeds);
                        properties.setPluginsDir(work.resolve("setup-fixture-plugins"));
                        var definition = new org.springframework.beans.factory.support.RootBeanDefinition(InstalledConnectorTests.class,
                                () -> new InstalledConnectorTests(properties, java.time.Clock.systemUTC()));
                        definition.setPrimary(true);
                        ((org.springframework.beans.factory.support.BeanDefinitionRegistry)context.getBeanFactory())
                                .registerBeanDefinition("fixtureInstalledConnectorTests", definition);
                    }
                    fixtureInitialization.accept(context);
                }).run(
                        "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                        "--tapstate.cloud.base-url=https://cloud.example", "--tapstate.cloud.token=fixture-runtime-token",
                        "--tapstate.cloud.cluster-id=" + sdk.cluster, "--tapstate.cloud.atlas-uri=" + configurationUri,
                        "--tapstate.control.auth.jwt-secret=cloud-loss-stable-test-key",
                        "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                        "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"), "--tapstate.connectors.seed-dir=" + seeds);
    }
    private static String exchangeCookie(ConfigurableApplicationContext application, String code) {
        int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
        var client = org.springframework.web.client.RestClient.builder().baseUrl("http://127.0.0.1:" + port)
                .requestFactory(new org.springframework.http.client.JdkClientHttpRequestFactory(java.net.http.HttpClient.newBuilder()
                        .followRedirects(java.net.http.HttpClient.Redirect.NEVER).build())).build();
        var response = client.get().uri("/auth/exchange?code=" + code).retrieve().toBodilessEntity();
        assertThat(response.getStatusCode().value()).isEqualTo(302);
        String cookie = response.getHeaders().getFirst("Set-Cookie");
        assertThat(cookie).contains("Secure", "HttpOnly");
        return cookie.split(";", 2)[0];
    }
    private static void assertCloudStatus(ConfigurableApplicationContext application, String cookie, int expected) {
        int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
        rootClient(application).post().uri("/api/storage/status").header("Cookie", cookie)
                .header("Origin", "http://127.0.0.1:" + port).body(Map.of()).exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(expected);
                    if (expected == 200) assertThat(response.bodyTo(Map.class).get("businessPhase")).isEqualTo("AWAITING_REBUILD_CONFIRMATION");
                    return null;
                });
    }
    @org.springframework.boot.test.context.TestConfiguration(proxyBeanMethods = false)
    static class RecoverySdkConfiguration {
        @org.springframework.context.annotation.Bean
        @org.springframework.context.annotation.Primary
        CloudSdkBridge recoveryBridge(CloudRuntimeSettings settings, RecoverySdk sdk) { return new CloudSdkBridge(settings, sdk); }
    }
    static final class RecoverySdk implements CloudSdkBridge.SdkRuntime {
        final String cluster;
        final java.util.Set<String> consumed = java.util.concurrent.ConcurrentHashMap.newKeySet();
        final String configurationOperation = UUID.randomUUID().toString();
        final AtomicInteger configurationClaims = new AtomicInteger(), configurationCompletions = new AtomicInteger();
        String configurationHash, configurationOwner, configurationPhase = "AVAILABLE";
        int failCompletionResponses;
        RecoverySdk(String cluster) { this.cluster = cluster; }
        @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
            if (!List.of("old", "blocked", "fresh", "late", "restarted", "cache-outage", "late-during-cache-outage",
                    "after-cache-outage").contains(code)
                    || !consumed.add(code)) throw new CloudSdkBridge.ProviderFailure();
            return new CloudSdkBridge.ExchangeResult(code, "organization", cluster, "Fixture", "Cluster", "local");
        }
        @Override public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() {
            return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization", cluster, null, "PROVISIONING");
        }
        @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            return new CloudSdkBridge.VerifiedClaims("cloud-user", "organization", cluster, jwt, Instant.now().plusSeconds(600),
                    "https://cloud.example", "127.0.0.1", List.of("workload:write"));
        }
        @Override public boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature) {
            return method.equals("POST") && nonce.equals("fixture-nonce") && signature.equals("fixture-signature") && data.equals("blocked");
        }
        @Override public void report(String cluster, String nonce, io.tapstate.control.core.CloudRuntimeStatus status, String token) {
            throw new AssertionError("the fixture disables status reporting");
        }
        @Override public synchronized CloudSdkBridge.ConfigurationInitialization claimConfiguration(String boot, String hash) {
            configurationClaims.incrementAndGet();
            if (configurationHash == null || !configurationHash.equals(hash)) throw new CloudSdkBridge.ProviderFailure();
            if (configurationPhase.equals("AVAILABLE")) { configurationPhase = "CLAIMED"; configurationOwner = boot; }
            return new CloudSdkBridge.ConfigurationInitialization(cluster, configurationOperation, configurationPhase, hash,
                    configurationPhase.equals("CLAIMED") && boot.equals(configurationOwner));
        }
        @Override public synchronized CloudSdkBridge.ConfigurationInitialization completeConfiguration(String operation, String hash) {
            configurationCompletions.incrementAndGet();
            if (!configurationOperation.equals(operation) || !hash.equals(configurationHash) || configurationPhase.equals("AVAILABLE"))
                throw new CloudSdkBridge.ProviderFailure();
            configurationPhase = "COMPLETE";
            if (failCompletionResponses-- > 0) throw new CloudSdkBridge.ProviderFailure();
            return new CloudSdkBridge.ConfigurationInitialization(cluster, operation, "COMPLETE", hash, false);
        }
    }

    /** Native ports seed exact historical receipts; HTTP/production DI perform the actual mixed-source edit. */
    @Test void originalPositionApiCommitsSharedAndDirectEditsTogetherAndAllowsASecondEdit() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "position_cfg_" + suffix, cacheDb = "position_cache_" + suffix, masterDb = "position_master_" + suffix;
        String user = "position_" + suffix;
        var setup = new StorageSetupScope("position-" + suffix, "installation", "setup", 1);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri(user, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, configDb), null, Duration.ofSeconds(5)),
                    setup.clusterId(), setup.installationId(), setup.operationId(), 1)) {
                var target = new SourceResource("views", null, "mongodb", Map.of("uri", uri(user, masterDb)), null, null, null, null);
                var committed = configuration.bindings().commitWithFirstCache(setup, StorageBinding.Mode.OP, cacheConnection, target);
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "position-fixture")) {
                    cached.prepareFirstAuth(committed.firstCreation().orElseThrow()); cached.prepareInitialBusiness(committed.preparation(), () -> {});
                }
                configuration.bindings().complete(setup);
                configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                        new User("admin", new BCryptPasswordHasher().hash("sentinel-login-password"), "admin"));
            }
            try (var application = nativeRoot(uri(user, configDb))) {
                var root = application.getBean(StorageRoot.class); assertThat(root.phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
                var bound = root.runtime().current().orElseThrow(); var cache = bound.cache(); var business = bound.preparation().businessScope();
                var configuration = root.runtime().requireConfiguration();
                var child = (ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(root.runtime().business().orElseThrow(), "context");
                var sources = new java.util.LinkedHashMap<String,SourceResource>();
                var table = new SourceTable("orders", List.of(new SourceField("id", "bigint", io.tapstate.core.common.TapstateType.INT64)), List.of("id"), List.of());
                for (String id : List.of("shared_source", "direct_source")) {
                    var source = new SourceResource(id, null, "mongodb", Map.of("uri", uri(user, masterDb)), SourceMode.SNAPSHOT,
                            List.of(TableRef.literal("orders")), null, null);
                    configuration.artifacts().create(source); sources.put(id, source);
                    cache.schemas(business, () -> {}).save(new DiscoveredSourceModel(id, "mongodb", 1, new SourceModel(List.of(table))));
                }
                var pipeline = new PipelineResource("position_flow", null, List.of(SourceRef.spec("shared_source", true), SourceRef.spec("direct_source", false)), null, null, null, null, null);
                configuration.artifacts().create(pipeline);
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), pipeline.id(), 1);
                var inputs = SourceModelSnapshot.capture(pipeline.id(), List.copyOf(sources.values()), cache.schemas(business, () -> {}))
                        .preparationInputs(scope, pipeline, pipeline, sources);
                child.getBean(PipelineFirstStatePreparer.class).prepare(scope, inputs);
                var ownership = child.getBean(PipelineActuationOwnership.class);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> ownership.permit(scope).granted());
                var permit = ownership.permit(scope); assertThat(permit.granted()).isTrue();
                var execution = ownership.beginExecution(scope, WorkloadClaimFence.from(permit.claim()));
                assertThat(execution.allowed()).isTrue(); var pipelineFence = execution.claimFence();
                var node = child.getBean(NodeSessionLease.class).currentFence().orElseThrow();
                var captures = child.getBean(CaptureOwnership.class);
                var srs = cache.srs(business, () -> {});
                String sharedId = null; DirectChannelContext directContext = null; DirectBatch pending = null;
                for (String source : sources.keySet()) {
                    var issued = (CaptureCreationResult.Issued)configuration.capturePreparations().issueFirstCreate(new CapturePreparationRequest(scope, pipelineFence, node, source, inputs));
                    var prepared = cache.capturePreparations(business, () -> {}).createFirst(issued.creation()).preparation();
                    var capture = captures.acquire(new io.tapstate.runtime.srs.CaptureId(prepared.captureId()));
                    assertThat(capture.acquired()).isTrue();
                    try {
                        if (prepared.mode() == CapturePreparation.Mode.SHARED_SRS) {
                            sharedId = prepared.chainId();
                            srs.chains().requestTables(sharedId, java.util.Set.of("orders"), capture.fence());
                            srs.chains().openEpoch(sharedId, 0, capture.fence()).orElseThrow();
                            srs.chains().publishServingTables(sharedId, 1, java.util.Set.of("orders"), capture.fence());
                            var key = new SrsModelKey(sharedId, "orders", business.cacheIncarnation(), 1, 0);
                            srs.models().publishInitial(key, io.tapstate.runtime.srs.SrsRingbuffer.ringName(sharedId, "orders"), table, capture.fence());
                            srs.chains().checkpoint(sharedId, 0, new SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "old-shared"),
                                    Map.of("orders", new SrsTableCheckpoint(key, -1))), capture.fence()).orElseThrow();
                            var consumer = new SrsConsumerContext(new SrsConsumerKey(sharedId, pipeline.id(), source), 1);
                            srs.consumers().attach(consumer, 0, java.util.Set.of("orders"), pipelineFence).orElseThrow();
                            srs.consumers().beginWriterRun(consumer, 1, "old-writer", Map.of("orders", List.of("writer")), pipelineFence);
                        } else {
                            directContext = inputs.directChannels().getFirst().context();
                            var direct = cache.directChannels(business, () -> {}); var before = direct.load(directContext);
                            long epoch = direct.beginCapture(directContext, before.revision(), 0, capture.fence(), pipelineFence).orElseThrow();
                            direct.confirmAnchor(directContext, epoch, "old-direct", capture.fence(), pipelineFence);
                            assertThat(direct.pinSnapshot(directContext, direct.load(directContext).revision(), epoch, "old-direct", pipelineFence)).isTrue();
                            direct.beginWriterRun(directContext, epoch, "old-writer", Map.of("orders", List.of("writer")), pipelineFence);
                            pending = new DirectBatch(directContext, epoch, 1, new ChainPosition(new SourceOrder(epoch, 0), "pending"), Map.of("orders", 0L));
                            direct.recordBatch(pending, 0, capture.fence(), pipelineFence);
                        }
                    } finally { captures.release(capture.claim()); }
                }
                var login = ordinaryLogin(application, false); var client = rootClient(application);
                var reading = client.get().uri("/api/pipelines/position_flow/position").header("Authorization", "Bearer " + login.token())
                        .retrieve().body(io.tapstate.control.core.PipelinePosition.class);
                assertThat(reading.chains()).extracting(value -> value.resumeFrom().token()).containsExactly("old-shared", "old-direct");
                for (String token : List.of("first", "second")) {
                    var request = Map.of("chains", List.of(Map.of("chainId", sharedId, "resumeFrom", Map.of("token", token + "-shared")),
                            Map.of("chainId", directContext.channelId(), "resumeFrom", Map.of("token", token + "-direct"))));
                    var result = client.put().uri("/api/pipelines/position_flow/position").header("Authorization", "Bearer " + login.token())
                            .body(request).retrieve().body(io.tapstate.control.core.PipelinePosition.class);
                    assertThat(result.chains()).extracting(value -> value.resumeFrom().token()).containsExactly(token + "-shared", token + "-direct");
                    var projected = child.getBean(StorePort.class).meta();
                    assertThat(projected.durableSourceRead(sharedId)).isEmpty();
                    assertThat(projected.durableSourceRead(directContext.channelId())).isEmpty();
                    assertThat(List.of(projected.read(sharedId).orElseThrow(), projected.read(directContext.channelId()).orElseThrow()))
                            .extracting(SrsMeta::sourceReadDurable).containsExactly(false, false);
                }
                assertThat(srs.chains().load(sharedId).orElseThrow().checkpoint().position().token()).isEqualTo("old-shared");
                var direct = cache.directChannels(business, () -> {}); var changed = direct.load(directContext);
                assertThat(changed.cursor().sinkAcked().token()).isEqualTo("old-direct"); assertThat(changed.writerRun()).isNull();
                assertThat(direct.pending(directContext, 1)).containsExactly(pending);
                var oldConsumer = new SrsConsumerContext(new SrsConsumerKey(sharedId, pipeline.id(), "shared_source"), 1);
                assertThatThrownBy(() -> srs.consumers().beginWriterRun(oldConsumer, 1, "old-writer", Map.of("orders", List.of("writer")), pipelineFence))
                        .isInstanceOf(TapstateException.class);
                var nextPermit = ownership.permit(scope);
                var nextExecution = ownership.beginExecution(scope, WorkloadClaimFence.from(nextPermit.claim()));
                var nextCapture = captures.acquire(new io.tapstate.runtime.srs.CaptureId(directContext.captureId()));
                assertThat(nextCapture.acquired()).isTrue();
                try {
                    long epoch = direct.beginCapture(directContext, changed.revision(), 1, nextCapture.fence(), nextExecution.claimFence()).orElseThrow();
                    var opened = cache.directChannels(business, () -> {}).load(directContext);
                    assertThat(opened.resumeToken()).isEqualTo("second-direct");
                    assertThat(opened.cursor().sinkAcked().token()).isEqualTo("old-direct");
                    assertThat(opened.retained().pendingCount()).isOne();
                    assertThat(direct.beginWriterRun(directContext, epoch, "next-writer", Map.of("orders", List.of("writer")), nextExecution.claimFence())).isTrue();
                    assertThat(direct.advanceWriter(directContext, epoch, "next-writer", "orders", "writer",
                            new WriterProgress(SourceOrder.snapshotRow(1),new ChainPosition(SourceOrder.snapshotRow(1),"old-direct")),nextExecution.claimFence())).isTrue();
                    var loaded = direct.load(directContext);
                    assertThat(loaded.cursor().snapshotCompletedTables()).containsExactly("orders");
                    assertThat(loaded.cursor().sinkAcked().token()).isEqualTo("old-direct");
                    assertThat(loaded.resumeOverride().token()).isEqualTo("second-direct");
                    var fixedDirect = directContext;
                    assertThatThrownBy(() -> direct.advanceWriter(fixedDirect,epoch,"next-writer","orders","writer",
                            new WriterProgress(new SourceOrder(epoch,0),new ChainPosition(new SourceOrder(epoch,0),"unstarted-cdc")),nextExecution.claimFence()))
                            .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.DIRECT_CHANNEL_UNAVAILABLE));
                    assertThat(direct.load(directContext)).isEqualTo(loaded);
                    assertThat(direct.confirmAnchor(directContext, epoch, "second-direct", nextCapture.fence(), nextExecution.claimFence())).isTrue();
                    assertThat(direct.load(directContext).resumeOverride()).isNull();
                    assertThat(direct.load(directContext).cursor().sinkAcked().order()).isEqualTo(SourceOrder.snapshotRow(epoch));
                    assertThat(admin.getDatabase(cacheDb).getCollection("direct_batches").countDocuments()).isZero();
                } finally { captures.release(nextCapture.claim()); }
                // A required second-source fact disappears after preflight/audit. The first source must stay unchanged.
                var sharedCollection = admin.getDatabase(cacheDb).getCollection("srs_chains");
                var cursorCollection = admin.getDatabase(cacheDb).getCollection("srs_consumer_offsets");
                var channelCollection = admin.getDatabase(cacheDb).getCollection("direct_channel_offsets");
                var sharedBefore = sharedCollection.find(new Document("_id", sharedId)).first();
                var cursorBefore = cursorCollection.find().first(); var channelBefore = channelCollection.find().first();
                var service = child.getBean(io.tapstate.control.core.PipelinePositionService.class);
                var writer = child.getBean(io.tapstate.control.core.PipelinePositionService.WriteBack.class);
                org.springframework.test.util.ReflectionTestUtils.setField(service, "nativeWriteBack",
                        (io.tapstate.control.core.PipelinePositionService.WriteBack)(id, before, moves) -> {
                            var command = writer.prepare(id, before, moves);
                            return () -> { channelCollection.deleteOne(new Document("_id", channelBefore.get("_id"))); command.run(); };
                        });
                try {
                    var refused = Map.of("chains", List.of(Map.of("chainId", sharedId, "resumeFrom", Map.of("token", "never-shared")),
                            Map.of("chainId", directContext.channelId(), "resumeFrom", Map.of("token", "never-direct"))));
                    assertThatThrownBy(() -> client.put().uri("/api/pipelines/position_flow/position").header("Authorization", "Bearer " + login.token())
                            .body(refused).retrieve().toBodilessEntity()).isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                    failure -> assertThat(failure.getResponseBodyAsString()).contains("io.direct-channel-unavailable"));
                    assertThat(sharedCollection.find(new Document("_id", sharedId)).first()).isEqualTo(sharedBefore);
                    assertThat(cursorCollection.find().first()).isEqualTo(cursorBefore);
                    assertThat(channelCollection.countDocuments()).isZero();
                } finally {
                    channelCollection.insertOne(channelBefore);
                    org.springframework.test.util.ReflectionTestUtils.setField(service, "nativeWriteBack", writer);
                }
            }
        }
    }

    ConfigurableApplicationContext restartedAuthenticationRoot(String configurationUri,String cluster,boolean cloud) {
        return cloud ? cloudRecoveryRoot(configurationUri,new RecoverySdk(cluster)) : nativeRoot(configurationUri);
    }
    private void verifyNewAuthenticationProcess(String configurationUri,String cluster,boolean cloud) {
        Path request=work.resolve("auth-recovery-"+cloud+".json"),output=work.resolve("auth-recovery-"+cloud+".log");
        Process process=null;
        try {
            Files.createFile(request,java.nio.file.attribute.PosixFilePermissions.asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
            Files.writeString(request,io.tapstate.core.common.JsonWriter.write(Map.of("configurationUri",configurationUri,"cluster",cluster,
                    "cloud",cloud,"work",Files.createDirectory(work.resolve("auth-recovery-child-"+cloud)).toString())));
            process=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx512m","-XX:ActiveProcessorCount=2",
                    "--add-opens=java.base/java.lang=ALL-UNNAMED","-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                    AuthenticationRecoveryRestartProcess.class.getName(),request.toString()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
            assertThat(process.pid()).isNotEqualTo(ProcessHandle.current().pid());
            assertThat(process.waitFor(60,TimeUnit.SECONDS)).as("the new auth-only process must finish within its startup bound").isTrue();
            assertThat(process.exitValue()).as("new-process authentication recovery: %s",Files.readString(output)).isZero();
            assertThat(Files.readString(output)).contains("AUTH_RECOVERY_PROCESS_VERIFIED "+process.pid()+" "+cloud);
        } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
        finally {
            if(process!=null && process.isAlive()) {
                process.destroyForcibly();
                try { assertThat(process.waitFor(5,TimeUnit.SECONDS)).as("the exact owned recovery process must stop").isTrue(); }
                catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
            }
            if(process==null || !process.isAlive()) {
                try { Files.deleteIfExists(request); } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); }
            }
        }
    }
    private ConfigurableApplicationContext nativeRoot(String configurationUri) {return nativeRoot(configurationUri,work.resolve("no-seeds"));}
    private ConfigurableApplicationContext nativeRoot(String configurationUri, Path seeds, String... overrides) {
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                java.util.stream.Stream.concat(java.util.stream.Stream.of(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0", "--logging.level.root=ERROR",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.store.mongo.uri=" + configurationUri, "--tapstate.control.auth.jwt-secret=loss-restart-stable-test-key",
                "--tapstate.connectors.seed-dir=" + seeds, "--tapstate.connectors.plugins-dir=" + work.resolve("plugins")),
                java.util.Arrays.stream(overrides)).toArray(String[]::new));
    }
    private static org.springframework.web.client.RestClient rootClient(ConfigurableApplicationContext application) {
        int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
        // A 503 after a committed command is an unknown result, never an instruction to repeat POST.
        return org.springframework.web.client.RestClient.builder().baseUrl("http://127.0.0.1:" + port)
                .requestFactory(new org.springframework.http.client.JdkClientHttpRequestFactory(java.net.http.HttpClient.newBuilder()
                        .followRedirects(java.net.http.HttpClient.Redirect.NEVER).build())).build();
    }
    private static io.tapstate.control.restapi.LoginResponse ordinaryLogin(ConfigurableApplicationContext application, boolean session) {
        return rootClient(application).post().uri("/auth/login")
                .body(Map.of("username", "admin", "password", "sentinel-login-password", "createSession", session))
                .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void nativeProductionRootLoadsConfigAndAuthBeforeAdmittingItsCacheRuntime(boolean businessPrepared) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "root_config_" + suffix, cacheDb = "root_cache_" + suffix, masterDb = "root_master_" + suffix;
        String configUser = "config_" + suffix, cacheUser = "cache_" + suffix, masterUser = "master_" + suffix;
        var original = new io.tapstate.spi.store.StorageSetupScope("root-" + suffix, "installation", "setup-" + suffix, 1);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, configUser, List.of(configDb)); createUser(admin, cacheUser, List.of(cacheDb)); createUser(admin, masterUser, List.of(masterDb));
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser, configDb), null, Duration.ofSeconds(5));
            var cache = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB,
                    uri(cacheUser, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings,
                    original.clusterId(), original.installationId(), original.operationId(), 1)) {
                var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", uri(masterUser, masterDb)), null, null, null, null);
                var published = configuration.bindings().commitWithFirstCache(original, io.tapstate.spi.store.StorageBinding.Mode.OP, cache, target);
                var preparation = configuration.bindings().cachePreparation().orElseThrow();
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cache, Duration.ofSeconds(5), "fixture")) {
                    cached.prepareFirstAuth(published.firstCreation().orElseThrow());
                    if (businessPrepared) cached.prepareInitialBusiness(preparation, () -> {});
                }
                configuration.bindings().complete(original);
                configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                        new io.tapstate.spi.store.User("admin", new BCryptPasswordHasher().hash("sentinel-login-password"), "admin"));
            }
            try (var application = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.store.mongo.uri=" + uri(configUser, configDb), "--tapstate.control.auth.jwt-secret=native-root-test-key",
                    "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"), "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"))) {
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(businessPrepared ? StorageRoot.Phase.BUSINESS_READY : StorageRoot.Phase.AUTH_READY);
                if (businessPrepared) org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(root.runtime()::businessStarted);
                else assertThat(root.runtime().business()).isEmpty();
                int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
                var client = org.springframework.web.client.RestClient.create("http://127.0.0.1:" + port);
                var login = client.post().uri("/auth/login").body(Map.of("username", "admin", "password", "sentinel-login-password"))
                        .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
                assertThat(login).isNotNull();
                String snapshot = client.post().uri("/api/storage/status").header("Authorization", "Bearer " + login.token())
                        .body(Map.of()).retrieve().body(String.class);
                assertThat(((Map<?,?>)io.tapstate.core.common.JsonReader.parse(snapshot)).get("businessPhase"))
                        .isEqualTo(businessPrepared ? "READY" : "AUTH_READY");
                assertThat(snapshot).contains("COMPLETE").doesNotContain(PASSWORD, "sentinel-login-password", "mongodb://");
                if (!businessPrepared) {
                    assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>()))
                            .doesNotContain("workload_claims", "pipeline_state", "srs_chain_state");
                    assertThatThrownBy(() -> client.get().uri("/api/sources").header("Authorization", "Bearer " + login.token()).retrieve().body(String.class))
                            .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                    failure -> assertThat(failure.getStatusCode().value()).isEqualTo(503));
                    return;
                }
                String sources = client.get().uri("/api/sources").header("Authorization", "Bearer " + login.token()).retrieve().body(String.class);
                assertThat(sources).contains("views").doesNotContain(PASSWORD, "sentinel-login-password");
                try (MongoClient limited = MongoClients.create(uri(configUser, configDb))) {
                    assertDenied(limited, cacheDb); assertDenied(limited, masterDb);
                }
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isEqualTo(1);
                assertThat(admin.getDatabase(configDb).listCollectionNames().into(new ArrayList<>())).doesNotContain(
                        "workload_claims", "system_meta", "operator_state", "op_sessions", "srs_meta");
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>())).doesNotContain("users", "cluster_keys", "system_meta", "operator_state", "srs_meta");
                var current = root.runtime().current().orElseThrow();
                var modelSource = new SourceResource("model_source", null, "mongodb", Map.of("uri", uri(masterUser, masterDb)),
                        io.tapstate.core.model.SourceMode.SNAPSHOT, List.of(io.tapstate.core.model.TableRef.literal("orders")), null, null);
                root.runtime().requireConfiguration().artifacts().create(modelSource);
                root.runtime().requireConfiguration().artifacts().create(new io.tapstate.core.model.PipelineResource("model_flow", null,
                        List.of(io.tapstate.core.model.SourceRef.spec("model_source", false)), null, null, null, null, null));
                current.cache().schemas(current.preparation().businessScope(), () -> {}).save(new io.tapstate.spi.store.DiscoveredSourceModel(
                        "model_source", "mongodb", 1L, new io.tapstate.spi.store.SourceModel(List.of(new io.tapstate.spi.store.SourceTable("orders",
                        List.of(new io.tapstate.spi.store.SourceField("id", "bigint", io.tapstate.core.common.TapstateType.INT64)), List.of("id"), List.of())))));
                client.post().uri("/api/pipelines/model_flow:accept-derived-schema").header("Authorization", "Bearer " + login.token())
                        .retrieve().toBodilessEntity();
                var modelScope = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "model_flow", 1);
                assertThat(current.cache().unpreparedPipelineDerivedSchemas(modelScope, null, () -> {}).latest("model_flow", "model_source.orders").orElseThrow().schema())
                        .containsEntry("id", "INT64 NULL");
                root.runtime().requireConfiguration().artifacts().create(new SourceResource("pending_model", null, "mongodb",
                        Map.of("uri", uri(masterUser, masterDb)), io.tapstate.core.model.SourceMode.SNAPSHOT, null, null, null));
                root.runtime().requireConfiguration().artifacts().create(new io.tapstate.core.model.PipelineResource("partial_model_flow", null,
                        List.of(io.tapstate.core.model.SourceRef.spec("pending_model", false), io.tapstate.core.model.SourceRef.spec("model_source", false)),
                        null, null, null, null, null));
                var child = (ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(root.runtime().business().orElseThrow(), "context");
                child.getBean(io.tapstate.control.core.SchemaDerivation.class).derive("partial_model_flow");
                var partialScope = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "partial_model_flow", 1);
                assertThat(current.cache().unpreparedPipelineDerivedSchemas(partialScope, null, () -> {}).latest("partial_model_flow", "model_source.orders"))
                        .isPresent();
                assertThat(root.runtime().requireConfiguration().preparations().read("model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelinePreparation.Phase.UNASSIGNED);
                assertThat(root.runtime().requireConfiguration().preparations().read("partial_model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelinePreparation.Phase.UNASSIGNED);
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isEqualTo(1);
                var originalModelFlow = root.runtime().requireConfiguration().artifacts().get("model_flow").orElseThrow();
                client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                        .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                        .retrieve().toBodilessEntity();
                assertThat(root.runtime().requireConfiguration().artifacts().get("model_flow")).isEmpty();
                assertThat(root.runtime().requireConfiguration().pipelineStates().read("model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRED);
                var configuration = root.runtime().requireConfiguration();
                configuration.artifacts().create(originalModelFlow);
                var reusedScope = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "model_flow", 2);
                var inputs = SourceModelSnapshot.capture("model_flow", List.of(modelSource),
                        current.cache().schemas(current.preparation().businessScope(), () -> {}))
                        .preparationInputs(reusedScope, (io.tapstate.core.model.PipelineResource)originalModelFlow,
                                (io.tapstate.core.model.PipelineResource)originalModelFlow, Map.of(modelSource.id(), modelSource));
                child.getBean(PipelineFirstStatePreparer.class).prepare(reusedScope, inputs);
                var driving=child.getBean(PipelineActuationOwnership.class);
                var permitted=driving.permit(reusedScope);
                assertThat(permitted.granted()).isTrue();
                var execution=driving.beginExecution(reusedScope,io.tapstate.spi.store.WorkloadClaimFence.from(permitted.claim()));
                assertThat(execution.allowed()).isTrue();
                var completedDag=new com.hazelcast.jet.core.DAG();
                completedDag.newVertex("done",com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne(
                        com.hazelcast.jet.core.ProcessorSupplier.of((com.hazelcast.function.SupplierEx<com.hazelcast.jet.core.Processor>) FinishedPipelineProcessor::new)));
                var retainedRun=child.getBean(io.tapstate.runtime.engine.Engine.class).submitRun("model_flow",completedDag,Map.of(),
                        io.tapstate.runtime.engine.nest.NestSettings.defaults(),new io.tapstate.runtime.engine.OperatorRunManifest("model_flow",execution.fence(),List.of()));
                assertThat(retainedRun.awaitTerminal(Duration.ofSeconds(15))).isTrue();
                String role = "retire_until_commit_" + suffix;
                admin.getDatabase("admin").runCommand(new Document("createRole", role).append("privileges", List.of(
                        new Document("resource", new Document("db", configDb).append("collection", ""))
                                .append("actions", List.of("find", "insert", "update", "createIndex", "listIndexes", "listCollections"))))
                        .append("roles", List.of()));
                admin.getDatabase("admin").runCommand(new Document("updateUser", configUser).append("roles", List.of(new Document("role", role).append("db", "admin"))));
                try {
                    assertThatThrownBy(() -> client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                            .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                            .retrieve().toBodilessEntity()).isInstanceOf(org.springframework.web.client.HttpServerErrorException.class);
                    assertThat(configuration.pipelineStates().read("model_flow").orElseThrow().phase())
                            .isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRING);
                    assertThat(configuration.artifacts().get("model_flow")).isPresent();
                    assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").find(new Document("pipelineScope.pipelineId", "model_flow"))
                            .first()).containsKey("pipelineRetirement");
                } finally {
                    admin.getDatabase("admin").runCommand(new Document("updateUser", configUser)
                            .append("roles", List.of(new Document("role", "readWrite").append("db", configDb))));
                }
                client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                        .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                        .retrieve().toBodilessEntity();
                assertThat(configuration.artifacts().get("model_flow")).isEmpty();
                assertThat(configuration.pipelineStates().read("model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRED);
                configuration.artifacts().create(originalModelFlow);
                assertThat(configuration.pipelineStates().read("model_flow").orElseThrow().stateGeneration()).isEqualTo(3);
                assertThat(child.getBean(io.tapstate.runtime.engine.Engine.class).captureRun("model_flow").orElseThrow().jobId()).isEqualTo(retainedRun.jobId());
                client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                        .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                        .retrieve().toBodilessEntity();
                assertThat(configuration.artifacts().get("model_flow")).isEmpty();
                configuration.artifacts().create(originalModelFlow);
                var beforeFull = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "model_flow", 4);
                var fullInputs = SourceModelSnapshot.capture("model_flow", List.of(modelSource),
                        current.cache().schemas(current.preparation().businessScope(), () -> {}))
                        .preparationInputs(beforeFull, (io.tapstate.core.model.PipelineResource)originalModelFlow,
                                (io.tapstate.core.model.PipelineResource)originalModelFlow, Map.of(modelSource.id(), modelSource));
                child.getBean(PipelineFirstStatePreparer.class).prepare(beforeFull, fullInputs);
                // The fixture seeds the accepted stop intent; the production driver performs the actual replacement.
                child.getBean(io.tapstate.spi.store.StorePort.class).desired().save(new io.tapstate.core.lifecycle.DesiredState("model_flow",
                        io.tapstate.core.lifecycle.PipelineState.STOPPED, io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow), true));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                    child.getBean(ConvergenceDriver.class).reconcile();
                    var mapping = configuration.pipelineStates().read("model_flow").orElseThrow();
                    return mapping.phase() == io.tapstate.spi.store.PipelineStateMapping.Phase.ACTIVE && mapping.stateGeneration() == 5;
                });
                var serverFailure = new AtomicReference<Throwable>();
                var deletion = child.getBean(io.tapstate.control.core.ArtifactMutationService.PipelineDeletion.class);
                org.springframework.test.util.ReflectionTestUtils.setField(child.getBean(io.tapstate.control.core.ArtifactMutationService.class), "pipelineDeletion",
                        (io.tapstate.control.core.ArtifactMutationService.PipelineDeletion)(definition, expected) -> {
                            try {
                                var command = deletion.prepare(definition, expected);
                                return () -> { try { command.run(); } catch (RuntimeException | Error failure) { serverFailure.set(failure); throw failure; } };
                            } catch (RuntimeException | Error failure) { serverFailure.set(failure); throw failure; }
                        });
                // Publication can precede the previous synchronous pass's finally block; only its busy fence is retried.
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                    try {
                        client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                                .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                                .retrieve().toBodilessEntity();
                    } catch (org.springframework.web.client.RestClientResponseException busy) {
                        var body = (Map<?,?>)io.tapstate.core.common.JsonReader.parse(busy.getResponseBodyAsString());
                        if (busy.getStatusCode().value() == 500 && "io.workload-claim-fenced".equals(body.get("code")))
                            throw new AssertionError("the native control is not yet available",serverFailure.get());
                        throw busy;
                    }
                });
                assertThat(configuration.artifacts().get("model_flow")).isEmpty();
                var businessOwner = (NativeBusinessContext)root.runtime().business().orElseThrow();
                var businessContext = (org.springframework.context.ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(businessOwner, "context");
                var originalMember = businessContext.getBean(com.hazelcast.core.HazelcastInstance.class);
                var originalNode = businessContext.getBean(NodeSessionLease.class).currentFence().orElseThrow();
                businessOwner.close();
                assertThat(originalMember.getLifecycleService().isRunning()).isFalse();
                assertThat(current.cache().workloadClaims(current.preparation().businessScope()).read(originalNode.key()).orElseThrow().leased()).isFalse();
                assertThat(root.runtime().businessStarted()).isFalse();
                String degraded = client.post().uri("/api/storage/status").header("Authorization", "Bearer " + login.token())
                        .body(Map.of()).retrieve().body(String.class);
                assertThat(((Map<?,?>)io.tapstate.core.common.JsonReader.parse(degraded)).get("businessPhase"))
                        .isEqualTo("AUTH_READY");
                admin.getDatabase(cacheDb).getCollection("op_sessions").drop();
                assertThatThrownBy(() -> client.post().uri("/auth/login")
                        .body(Map.of("username", "admin", "password", "sentinel-login-password", "createSession", false))
                        .retrieve().body(String.class))
                        .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(503));
            }
        }
    }

    private static final class FinishedPipelineProcessor extends com.hazelcast.jet.core.AbstractProcessor {
        @Override public boolean complete() { return true; }
    }

    /** Native control-service composition; the target fixture does not certify a registered connector or TLS. */
    @Test
    void setupAndFirstAdministratorServicesKeepNativeAuthorityAcrossAuditFailureAndReopen() {
        String configDb = "control_config_" + UUID.randomUUID().toString().replace("-", "");
        String cacheDb = configDb + "_cache";
        String masterDb = configDb + "_master";
        String user = "owner_" + configDb;
        var clock = java.time.Clock.systemUTC();
        var allowedAudit = new java.util.concurrent.atomic.AtomicBoolean(true);
        var targetTests = new AtomicInteger();
        var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var config = new io.tapstate.spi.store.StorageBinding.Connection(
                    io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, uri(user, configDb), configDb, null);
            var cache = new io.tapstate.spi.store.StorageBinding.Connection(config.kind(), uri(user, cacheDb), cacheDb, null);
            var master = new io.tapstate.spi.store.StorageBinding.Connection(config.kind(), uri(user, masterDb), masterDb, null);
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(config.uri(), null, Duration.ofSeconds(5));
            var original = new io.tapstate.spi.store.StorageSetupScope("control-cluster", "installation", "original-setup", 1);
            var candidate = new io.tapstate.control.core.StorageSetupService.Candidate(config, cache, master);
            var target = new io.tapstate.control.core.StorageSetupService.DefaultTarget() {
                @Override public io.tapstate.control.core.StorageSetupService.ConnectorIdentity identity(
                        io.tapstate.spi.store.StorageBinding.Connection connection) {
                    return new io.tapstate.control.core.StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64));
                }
                @Override public SourceResource definition(io.tapstate.spi.store.StorageBinding.Connection connection, String id) {
                    return new SourceResource(id, null, "mongodb", Map.of("uri", connection.uri()), null, null, null, null);
                }
                @Override public void test(io.tapstate.spi.store.StorageBinding.Connection connection, SourceResource source,
                        io.tapstate.control.core.StorageSetupService.ConnectorIdentity identity) {
                    assertThat(source.config().get("uri")).isEqualTo(probe.normalize(master).uri());
                    targetTests.incrementAndGet();
                }
            };
            try (var domain = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    settings, original.clusterId(), original.installationId(), original.operationId(), original.generation())) {
                var audit = new io.tapstate.control.core.AuditGate(record -> {
                    if (!allowedAudit.get()) throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "audit-unavailable"), null);
                    domain.audit().record(record);
                }, clock);
                var authority = new io.tapstate.control.core.SetupCapabilityService(domain.setupStates(), clock, () -> {}, () -> {}, () -> {});
                var issued = authority.issue(original, io.tapstate.spi.store.StorageSetupStateStore.Purpose.SETUP);
                var permission = authority.authorize(issued.token(), issued.scope(), issued.purpose());
                var service = new io.tapstate.control.core.StorageSetupService(io.tapstate.spi.store.StorageBinding.Mode.OP,
                        domain.bindings(), domain.setupStates(), probe, target, () -> java.util.Optional.of(config), audit, clock, () -> {});
                var stamps = new java.util.EnumMap<io.tapstate.control.core.StorageSetupService.Layer, String>(
                        io.tapstate.control.core.StorageSetupService.Layer.class);
                for (var layer : io.tapstate.control.core.StorageSetupService.Layer.values())
                    stamps.put(layer, service.test(issued.scope(), permission, candidate, layer).testStamp());
                assertThat(targetTests).hasValue(1);
                allowedAudit.set(false);
                assertThatThrownBy(() -> service.commit("owner", issued.scope(), permission, candidate, stamps))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code())
                                .isEqualTo(io.tapstate.control.core.ControlError.AUDIT_BLOCKED));
                assertThat(domain.bindings().load()).isEmpty();
                assertThat(domain.artifacts().list()).isEmpty();
                allowedAudit.set(true);
                var bound = service.commit("owner", issued.scope(), permission, candidate, stamps);
                assertThat(bound.setupPhase()).isEqualTo(io.tapstate.spi.store.StorageSetupStateStore.Phase.STORAGE_BOUND);
                var first = service.pending(issued.scope(), permission).orElseThrow().firstCreation().orElseThrow();
                var originalPreparation = domain.bindings().cachePreparation().orElseThrow();
                try (var reopened = io.tapstate.adapters.mongostore.MongoConfigurationDomain.open(settings, original.clusterId())) {
                    var nextAuthority = new io.tapstate.control.core.SetupCapabilityService(reopened.setupStates(), clock, () -> {}, () -> {}, () -> {});
                    var next = nextAuthority.issue(issued.scope(), issued.purpose());
                    var nextPermission = nextAuthority.authorize(next.token(), next.scope(), next.purpose());
                    assertThatThrownBy(permission::run).isInstanceOf(TapstateException.class);
                    var resumed = new io.tapstate.control.core.StorageSetupService(io.tapstate.spi.store.StorageBinding.Mode.OP,
                            reopened.bindings(), reopened.setupStates(), probe, target, () -> java.util.Optional.of(config), audit, clock, () -> {});
                    assertThat(resumed.commit("owner", next.scope(), nextPermission, candidate, Map.of()).bindingVersion()).isEqualTo(1);
                    assertThat(resumed.pending(next.scope(), nextPermission).orElseThrow().firstCreation()).isEmpty();
                    assertThat(reopened.bindings().cachePreparation()).contains(originalPreparation);
                    assertThat(reopened.artifacts().list()).hasSize(1);
                    first.requireActive();
                    reopened.bindings().complete(next.scope());
                    assertThatThrownBy(nextPermission::run).isInstanceOf(TapstateException.class);
                    var adminIssued = nextAuthority.issue(next.scope(), io.tapstate.spi.store.StorageSetupStateStore.Purpose.FIRST_ADMIN);
                    var adminPermission = nextAuthority.authorize(adminIssued.token(), adminIssued.scope(), adminIssued.purpose());
                    var passwords = new BCryptPasswordHasher();
                    var administrators = new io.tapstate.control.core.FirstAdministratorService(reopened.firstAdmin(), passwords, audit);
                    allowedAudit.set(false);
                    assertThatThrownBy(() -> administrators.create(adminPermission, "first-admin", "sentinel-password"))
                            .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code())
                                    .isEqualTo(io.tapstate.control.core.ControlError.AUDIT_BLOCKED));
                    assertThat(reopened.users().isEmpty()).isTrue();
                    allowedAudit.set(true);
                    assertThat(administrators.create(adminPermission, "first-admin", "sentinel-password").phase())
                            .isEqualTo(io.tapstate.spi.store.FirstAdminStore.Phase.COMPLETE);
                    assertThat(passwords.matches("sentinel-password", reopened.users().find("first-admin").orElseThrow().passwordHash())).isTrue();
                    assertThatThrownBy(adminPermission::run).isInstanceOf(TapstateException.class);
                    assertThatThrownBy(() -> administrators.create(adminPermission, "another", "another-password"))
                            .isInstanceOf(TapstateException.class);
                    assertThat(reopened.users().find("another")).isEmpty();
                }
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(admin.getDatabase(masterDb).listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(SystemCollections.AUDIT.on(admin.getDatabase(configDb)).find().into(new ArrayList<>()).toString())
                        .doesNotContain(PASSWORD, "sentinel-password", "mongodb://");
            } finally {
                for (String database : List.of(configDb, cacheDb, masterDb)) admin.getDatabase(database).drop();
                admin.getDatabase("admin").runCommand(new Document("dropUser", user));
            }
        }
    }

    @Test void anUnstartedPipelineRetiresThroughTheNativeApplicationWithoutInventingRuntimeHistory() {
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            var databases = prepareNativeDatabases(admin, "delete_" + UUID.randomUUID().toString().substring(0, 8));
            try (var application = nativeRoot(uri(databases.configUser(), databases.config()))) {
                var root = application.getBean(StorageRoot.class);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(root.runtime()::businessStarted);
                var business = (NativeBusinessContext) root.runtime().business().orElseThrow();
                var child = (ConfigurableApplicationContext) org.springframework.test.util.ReflectionTestUtils.getField(business, "context");
                var configuration = root.runtime().requireConfiguration();
                var source = new SourceResource("input", null, "mongodb", Map.of("isUri", true, "uri", uri(databases.masterUser(), databases.master())),
                        SourceMode.CDC, List.of(TableRef.literal("orders")), null, null);
                var pipeline = new PipelineResource("never_run", null, List.of(SourceRef.spec("input", false)), null, null,
                        new ServeBlock.Inline("serve", FromRef.literal("orders"), List.of(new SyncElement("sync", "views", null, null, null, OnFullLoad.APPEND)), null, null),
                        null, null);
                configuration.artifacts().saveAll(List.of(source, pipeline));
                assertThat(configuration.preparations().read(pipeline.id()).orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
                assertThat(child.getBean(io.tapstate.runtime.engine.Engine.class).captureRun(pipeline.id())).isEmpty();
                var node = child.getBean(NodeSessionLease.class).currentFence().orElseThrow();
                var reading = business.statusStore().workloadClaims().read(node.key()).orElseThrow();
                assertThat(reading.leased()).isTrue();
                assertThat(WorkloadClaimFence.from(reading.claim())).isEqualTo(node);
                child.getBean(io.tapstate.control.core.ArtifactMutationService.class).delete("admin", pipeline.id(),
                        io.tapstate.core.model.canonical.CanonicalHash.of(pipeline));
                assertThat(configuration.artifacts().get(pipeline.id())).isEmpty();
                var mapping = configuration.pipelineStates().read(pipeline.id()).orElseThrow();
                assertThat(mapping.phase()).isEqualTo(PipelineStateMapping.Phase.RETIRED);
                assertThat(configuration.retirements().completed(pipeline.id(), mapping.operationId())).isPresent();
                assertThat(admin.getDatabase(databases.cache()).getCollection("workload_claims")
                        .countDocuments(new Document("resourceType", "PIPELINE_ACTUATION"))).isZero();
                for (String collection : List.of("pipeline_state", "srs_consumers", "nest_state", "join_facts"))
                    assertThat(admin.getDatabase(databases.cache()).getCollection(collection).countDocuments()).isZero();
                assertThat(admin.getDatabase(databases.config()).getCollection("sources").countDocuments()).isEqualTo(2);
            }
        }
    }

    @Test
    void twoClusterUsersCanUseOnlyTheirOwnThreeDatabasesAndRecoverAfterRestart() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            var a = prepareNativeDatabases(admin, "a_" + suffix);
            var b = prepareNativeDatabases(admin, "b_" + suffix);
            List<String> allDatabases = List.of(a.config(), a.cache(), a.master(), b.config(), b.cache(), b.master());
            byte[] artifact = Files.readAllBytes(CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar"));
            String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(artifact));
            for (var set : List.of(a, b)) {
                recordConfigurationFootprint(set, "installed-before-runtime");
                for (var entry : Map.of(set.configUser(), set.config(), set.cacheUser(), set.cache(), set.masterUser(), set.master()).entrySet()) {
                    try (MongoClient limited = MongoClients.create(uri(entry.getKey(), entry.getValue()))) {
                        for (String database : allDatabases) if (!database.equals(entry.getValue())) assertDenied(limited, database);
                    }
                }
                try (var application = nativeRoot(uri(set.configUser(), set.config()))) {
                    var root = application.getBean(StorageRoot.class);
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(root.runtime()::businessStarted);
                    var bound = root.runtime().current().orElseThrow();
                    var registry = bound.cache().connectors(bound.preparation().businessScope(), () -> {});
                    assertThat(registry.register("mongodb", "2.0.7-SNAPSHOT", RegistrationSource.REGISTER, artifact).newlyRegistered()).isTrue();
                    assertThat(registry.artifact(hash)).hasValueSatisfying(bytes -> assertThat(bytes).isEqualTo(artifact));
                    bound.cache().schemas(bound.preparation().businessScope(), () -> {}).save(new DiscoveredSourceModel(
                            "native-input", "mongodb", 1L, new SourceModel(List.of(new SourceTable("orders",
                            List.of(new SourceField("id", "bigint", io.tapstate.core.common.TapstateType.INT64)), List.of("id"), List.of())))));
                    try (MongoClient master = MongoClients.create(uri(set.masterUser(), set.master()))) {
                        master.getDatabase(set.master()).getCollection("orders").insertOne(new Document("_id", 1).append("owner", set.master()));
                    }
                    assertNativeDatabasePlacement(admin, set);
                }
                try (var restarted = nativeRoot(uri(set.configUser(), set.config()))) {
                    var root = restarted.getBean(StorageRoot.class);
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(root.runtime()::businessStarted);
                    var bound = root.runtime().current().orElseThrow();
                    assertThat(bound.preparation()).isEqualTo(set.preparation());
                    assertThat(bound.cache().connectors(bound.preparation().businessScope(), () -> {}).artifact(hash))
                            .hasValueSatisfying(bytes -> assertThat(bytes).isEqualTo(artifact));
                    assertThat(bound.cache().schemas(bound.preparation().businessScope(), () -> {}).get("native-input")).isPresent();
                    assertThat(ordinaryLogin(restarted, true)).isNotNull();
                    assertNativeDatabasePlacement(admin, set);
                    assertThat(admin.getDatabase(set.master()).getCollection("orders").find().first())
                            .containsEntry("_id", 1).containsEntry("owner", set.master());
                }
                recordConfigurationFootprint(set, "after-runtime-and-restart");
            }
        }
    }

    private static void recordConfigurationFootprint(NativeDatabases set, String phase) {
        // Observe the actual limited CONFIG identity, without logging stored values or credentials.
        try (MongoClient limited = MongoClients.create(uri(set.configUser(), set.config()))) {
            var database = limited.getDatabase(set.config());
            var actualNames = database.listCollectionNames().into(new ArrayList<>());
            assertThat(SystemCollections.physicalNamesIn(SystemCollections.Database.CONFIG)).containsAll(actualNames);
            var measurements = new ArrayList<Document>();
            for (var resource : SystemCollections.values()) {
                if (resource.database() != SystemCollections.Database.CONFIG) continue;
                for (String name : resource.physicalNames()) {
                    if (!actualNames.contains(name)) {
                        measurements.add(new Document("collection", name).append("present", false));
                        continue;
                    }
                    var collection = database.getCollection(name);
                    var indexes = collection.listIndexes().into(new ArrayList<>());
                    assertThat(indexes).anySatisfy(index -> assertThat(index.get("key", Document.class))
                            .isEqualTo(new Document("_id", 1)));
                    if (name.equals(resource.indexTarget())) for (var required : resource.indexes()) {
                        Document index = indexes.stream().filter(value -> required.indexName().equals(value.getString("name")))
                                .findFirst().orElseThrow();
                        var keys = new Document(); required.keys().forEach(key -> keys.append(key, 1));
                        assertThat(index.get("key", Document.class)).isEqualTo(keys);
                        assertThat(Boolean.TRUE.equals(index.getBoolean("unique"))).isEqualTo(required.unique());
                        if (required.partialFilterJson() != null)
                            assertThat(index.get("partialFilterExpression", Document.class))
                                    .isEqualTo(Document.parse(required.partialFilterJson()));
                    }
                    Document stats = database.runCommand(new Document("collStats", name).append("scale", 1));
                    long count = collection.countDocuments();
                    assertThat(stats.get("count", Number.class).longValue()).isEqualTo(count);
                    var measurement = new Document("collection", name).append("present", true).append("documents", count);
                    for (String metric : List.of("size", "storageSize", "totalIndexSize", "nindexes")) {
                        long value = stats.get(metric, Number.class).longValue();
                        assertThat(value).isGreaterThanOrEqualTo(0);
                        measurement.append(metric, value);
                    }
                    measurement.append("indexNames", indexes.stream().map(index -> index.getString("name")).sorted().toList());
                    measurements.add(measurement);
                }
            }
            Document stats = database.runCommand(new Document("dbStats", 1).append("scale", 1));
            var report = new Document("phase", phase).append("mode", "OP")
                    .append("sources", database.getCollection("sources").countDocuments())
                    .append("pipelines", database.getCollection("pipelines").countDocuments())
                    .append("users", database.getCollection("users").countDocuments());
            for (String metric : List.of("objects", "dataSize", "storageSize", "indexes", "indexSize"))
                report.append(metric, stats.get(metric, Number.class).longValue());
            report.append("collections", measurements);
            System.out.println("CONFIG_FOOTPRINT " + report.toJson());
        }
    }

    private record NativeDatabases(String config, String cache, String master, String configUser,
            String cacheUser, String masterUser, CachePreparation preparation) {
        StorageBinding.Connection cacheConnection() {
            return new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri(cacheUser, cache), cache, null);
        }
    }

    private NativeDatabases prepareNativeDatabases(MongoClient admin, String suffix) {
        String config = "native_config_" + suffix, cache = "native_cache_" + suffix, master = "native_master_" + suffix;
        String configUser = "config_" + suffix, cacheUser = "cache_" + suffix, masterUser = "master_" + suffix;
        createUser(admin, configUser, List.of(config)); createUser(admin, cacheUser, List.of(cache)); createUser(admin, masterUser, List.of(master));
        var setup = new StorageSetupScope("native-" + suffix, "installation", "setup-" + suffix, 1);
        var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri(cacheUser, cache), cache, null);
        try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser, config), null, Duration.ofSeconds(2)),
                setup.clusterId(), setup.installationId(), setup.operationId(), 1)) {
            var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", uri(masterUser, master)), null, null, null, null);
            var initial = configuration.bindings().commitWithFirstCache(setup, StorageBinding.Mode.OP, endpoint, target);
            try (var domain = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(endpoint, Duration.ofSeconds(2), "native-isolation-fixture")) {
                domain.prepareFirstAuth(initial.firstCreation().orElseThrow()); domain.prepareInitialBusiness(initial.preparation(), () -> {});
            }
            configuration.bindings().complete(setup);
            configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                    new User("admin", new BCryptPasswordHasher().hash("sentinel-login-password"), "admin"));
            return new NativeDatabases(config, cache, master, configUser, cacheUser, masterUser, initial.preparation());
        }
    }

    private static void assertNativeDatabasePlacement(MongoClient admin, NativeDatabases set) {
        var config = admin.getDatabase(set.config()); var cache = admin.getDatabase(set.cache());
        assertThat(config.getCollection("sources").countDocuments(new Document("_id", "views"))).isEqualTo(1);
        assertThat(config.getCollection("users").countDocuments()).isEqualTo(1);
        assertThat(config.getCollection("cluster_keys").countDocuments()).isEqualTo(1);
        assertThat(config.listCollectionNames().into(new ArrayList<>())).doesNotContain(
                "workload_claims", "pipeline_state", "srs_events", "connector_artifacts.files", "connector_artifacts.chunks", "schema_discovery", "orders");
        assertThat(cache.listCollectionNames().into(new ArrayList<>())).contains("connector_artifacts.files", "connector_artifacts.chunks", "workload_claims")
                .doesNotContain("sources", "users", "cluster_keys", "storage_binding", "system_meta", "orders");
        assertThat(cache.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(1);
        assertThat(cache.getCollection("connector_artifacts.chunks").countDocuments()).isPositive();
        assertThat(admin.getDatabase(set.master()).listCollectionNames().into(new ArrayList<>())).containsExactly("orders");
        for (String name : List.of(set.config(), set.cache(), set.master())) {
            assertThat(admin.getDatabase(name).listCollectionNames().into(new ArrayList<>()))
                    .noneMatch(value -> value.startsWith("tapstate_access_probe_"));
        }
    }

    @Test
    void changingHistoryRetentionNeedsExplicitOfflineMaintenanceOfOnlyItsCacheCollection() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            var set = prepareNativeDatabases(admin, "ttl_" + suffix);
            var database = admin.getDatabase(set.cache());
            var history = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(database);
            long originalSeconds = Duration.ofDays(15).toSeconds(), changedSeconds = Duration.ofDays(7).toSeconds();
            history.insertOne(new Document("_id", "retained-sample").append("observedAt", java.util.Date.from(Instant.now())));
            Document sample = history.find().first(); Document originalIndex = history.listIndexes().into(new ArrayList<>()).stream()
                    .filter(row -> row.containsKey("expireAfterSeconds")).findFirst().orElseThrow();
            assertThat(((Number) originalIndex.get("expireAfterSeconds")).longValue()).isEqualTo(originalSeconds);
            var alteration = new Document("collMod", history.getNamespace().getCollectionName()).append("index",
                    new Document("name", originalIndex.getString("name")).append("expireAfterSeconds", changedSeconds));
            try (MongoClient limited = MongoClients.create(uri(set.cacheUser(), set.cache()))) {
                assertThatThrownBy(() -> limited.getDatabase(set.cache()).runCommand(alteration))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            try (var application = nativeRoot(uri(set.configUser(), set.config()), work.resolve("no-seeds"),
                    "--tapstate.metrics.history.retention=7d")) {
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
                assertThat(root.runtime().businessStarted()).isFalse();
                assertThat(ordinaryLogin(application, true)).isNotNull();
            }
            assertThat(history.listIndexes().into(new ArrayList<>())).contains(originalIndex);
            String role = "alter_history_" + suffix;
            database.runCommand(new Document("createRole", role).append("roles", List.of()).append("privileges", List.of(
                    new Document("resource", new Document("db", set.cache()).append("collection", history.getNamespace().getCollectionName()))
                            .append("actions", List.of("collMod")))));
            admin.getDatabase("admin").runCommand(new Document("grantRolesToUser", set.cacheUser()).append("roles", List.of(new Document("role", role).append("db", set.cache()))));
            database.getCollection("unrelated_history").createIndex(new Document("observedAt", 1), new IndexOptions().name("observedAt_idx").expireAfter(originalSeconds, TimeUnit.SECONDS));
            // No application is running: maintenance is an explicit database operation between owners.
            try (MongoClient limited = MongoClients.create(uri(set.cacheUser(), set.cache()))) {
                assertThatThrownBy(() -> limited.getDatabase(set.cache()).runCommand(new Document(alteration).append("collMod", "unrelated_history")))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
                limited.getDatabase(set.cache()).runCommand(alteration);
            }
            try (var reopened = nativeRoot(uri(set.configUser(), set.config()), work.resolve("no-seeds"),
                    "--tapstate.metrics.history.retention=7d")) {
                var root = reopened.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
                assertThat(ordinaryLogin(reopened, true)).isNotNull();
            }
            assertThat(history.find().first()).isEqualTo(sample);
            assertThat(history.listIndexes().into(new ArrayList<>()).stream().filter(row -> row.containsKey("expireAfterSeconds")).findFirst().orElseThrow()
                    .get("expireAfterSeconds", Number.class).longValue()).isEqualTo(changedSeconds);
        }
    }

    @Test
    void missingNativeConfigurationIndexPrivilegeIsCodedAndTheOriginalPreparationCanResume() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String config = "native_privileges_" + suffix, user = "config_limited_" + suffix, role = "without_indexes_" + suffix;
        ownedDatabase(config);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            Document builtin = admin.getDatabase("admin").runCommand(new Document("rolesInfo",
                    new Document("role", "readWrite").append("db", config)).append("showPrivileges", true)).getList("roles", Document.class).getFirst();
            List<Document> privileges = builtin.getList("privileges", Document.class).stream().map(row -> new Document(row).append("actions",
                    row.getList("actions", String.class).stream().filter(action -> !"createIndex".equals(action)).toList())).toList();
            admin.getDatabase(config).runCommand(new Document("createRole", role).append("roles", List.of()).append("privileges", privileges));
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD)
                    .append("roles", List.of(new Document("role", role).append("db", config))));
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, config), null, Duration.ofSeconds(2));
            assertThatThrownBy(() -> io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings, config, "installation", "original", 1))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                        assertThat(error.args().toString()).contains("code=13").doesNotContain(PASSWORD, "mongodb://");
                        assertThat(error.getCause()).isNull();
                    });
            Document originalKey = admin.getDatabase(config).getCollection("cluster_keys").find().first();
            admin.getDatabase(config).runCommand(new Document("updateRole", role).append("privileges", builtin.getList("privileges", Document.class)));
            try (var prepared = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings, config, "installation", "original", 1)) {
                assertThat(prepared.setupStates().load().scope().operationId()).isEqualTo("original");
                assertThat(prepared.bindings().load()).isEmpty();
                if (originalKey != null) assertThat(admin.getDatabase(config).getCollection("cluster_keys").find().first()).isEqualTo(originalKey);
            }
            assertThat(admin.getDatabase(config).getCollection("cluster_keys").countDocuments()).isEqualTo(1);
        }
    }

    @Test
    void changingConfigurationCredentialsPreservesTheOriginalDefaultSourceAndCacheBinding() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String renewedPassword = "controlled-renewed-config-password";
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            var set = prepareNativeDatabases(admin, "renew_" + suffix);
            var configuration = admin.getDatabase(set.config());
            Document source = configuration.getCollection("sources").find(new Document("_id", "views")).first();
            Document binding = configuration.getCollection("storage_binding").find().first();
            Document key = configuration.getCollection("cluster_keys").find().first();
            try (var first = nativeRoot(uri(set.configUser(), set.config()))) {
                assertThat(ordinaryLogin(first, true)).isNotNull();
            }
            admin.getDatabase("admin").runCommand(new Document("updateUser", set.configUser()).append("pwd", renewedPassword));
            try (MongoClient revoked = MongoClients.create(uri(set.configUser(), set.config()))) {
                assertThatThrownBy(() -> revoked.getDatabase(set.config()).getCollection("sources").countDocuments()).isInstanceOf(MongoSecurityException.class);
            }
            try (var restarted = nativeRoot(uri(set.configUser(), set.config(), renewedPassword))) {
                assertThat(ordinaryLogin(restarted, true)).isNotNull();
                assertThat(restarted.getBean(StorageRoot.class).runtime().current().orElseThrow().preparation()).isEqualTo(set.preparation());
                assertThat(configuration.getCollection("sources").find(new Document("_id", "views")).first()).isEqualTo(source);
                assertThat(configuration.getCollection("storage_binding").find().first()).isEqualTo(binding);
                assertThat(configuration.getCollection("cluster_keys").find().first()).isEqualTo(key);
                assertThat(source.toJson()).doesNotContain(PASSWORD, renewedPassword);
            }
        }
    }

    private static void assertNoProbes(MongoClient admin, String metadata) {
        for (String database : List.of(metadata, metadata + "_operator", metadata + "_views")) {
            assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                    .noneMatch(name -> name.startsWith("tapstate_access_probe_"));
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void cloudSetupConsumesTheOriginalSdkWithoutAccessToLegacyDatabases(boolean legacyReadable) throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String config = "consumer_config_" + suffix, user = "consumer_" + suffix, cluster = "consumer-" + suffix;
        var legacy = List.of(config + "_operator", config + "_views");
        ownedDatabase(config); legacy.forEach(this::ownedDatabase);
        var marker = new Document("_id", "retained").append("value", "must-not-change");
        try (MongoClient admin = MongoClients.create(uri("root", "admin"));
                var wire = new CloudConsumerWireFixture(cluster)) {
            var roles = new ArrayList<Document>();
            roles.add(new Document("role", "readWrite").append("db", config));
            for (String database : legacy) {
                admin.getDatabase(database).getCollection("retained").insertOne(marker);
                if (legacyReadable) roles.add(new Document("role", "read").append("db", database));
            }
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD).append("roles", roles));
            try (MongoClient limited = MongoClients.create(uri(user, config))) {
                for (String database : legacy) {
                    if (legacyReadable) assertThat(limited.getDatabase(database).getCollection("retained").countDocuments()).isOne();
                    else assertDenied(limited, database);
                    assertThatThrownBy(() -> limited.getDatabase(database).getCollection("retained")
                            .insertOne(new Document("_id", "denied")))
                            .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
                }
            }
            // Seed only the original CONFIG authority; this test exercises consumption, not allocation.
            try (var ignored = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, config), null, Duration.ofSeconds(5)),
                    cluster, "installation", "setup", 1)) { }
            AtomicInteger ready = new AtomicInteger();
            try (var application = start(uri(user, config), cluster, ready, "--tapstate.cloud.base-url=" + wire.baseUrl())) {
                assertThat(ready).hasValue(1);
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.SETUP_REQUIRED);
                assertThat(root.configurationEndpoint().orElseThrow().uri()).isEqualTo(uri(user, config));
                assertThat(root.runtime().current()).isEmpty();
                assertThat(root.runtime().business()).isEmpty();
                assertThat(application.getBeansOfType(com.hazelcast.core.HazelcastInstance.class)).isEmpty();
                assertThat(application.getBeansOfType(UserStore.class)).isEmpty();
                assertThat(application.getBeansOfType(OnPremInstallation.class)).isEmpty();

                String cookie = exchangeCookie(application, "accepted");
                var snapshot = cloudPost(application, cookie, "/api/storage/status", Map.of());
                assertThat(snapshot).containsEntry("setupPhase", "CONFIG_PREPARED").containsEntry("businessPhase", "NOT_CONFIGURED");
                assertThat(snapshot.toString()).doesNotContain(PASSWORD, "controlled-sdk-token");
                assertThat(((List<?>) snapshot.get("allowedActions")).contains("storage.setup.commit")).isTrue();
                rootClient(application).post().uri(wire.invalidationPath("blocked")).retrieve().toBodilessEntity();
                assertThatThrownBy(() -> exchangeCookie(application, "blocked"))
                        .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                error -> assertThat(error.getStatusCode().value()).isEqualTo(401));
                rootClient(application).post().uri(wire.invalidationPath("accepted")).retrieve().toBodilessEntity();
                assertCloudStatus(application, cookie, 401);
                assertThat(wire.exchanges).hasSize(2).allSatisfy(request -> {
                    assertThat(request.method()).isEqualTo("POST");
                    assertThat(request.authorization()).isEqualTo("controlled-sdk-token");
                    assertThat(request.body().path("clusterId").asText()).isEqualTo(cluster);
                    assertThat(request.body().size()).isEqualTo(2);
                });

                var reporter = application.getBean(io.tapstate.control.core.CloudStatusReporter.class);
                assertThat(reporter.report().activePipelines()).isZero();
                assertThat(reporter.report().lastErrorAt()).isNull();
                assertThat(wire.reports).hasSize(2).allSatisfy(request -> {
                    assertThat(request.method()).isEqualTo("POST");
                    assertThat(request.authorization()).isEqualTo("Bearer controlled-sdk-token");
                    var fields = new ArrayList<String>(); request.body().fieldNames().forEachRemaining(fields::add);
                    assertThat(fields).contains("nonce", "runtimeVersion", "uptimeMs", "activePipelines")
                            .isSubsetOf("nonce", "runtimeVersion", "uptimeMs", "activePipelines", "lastErrorAt");
                    assertThat(request.body().path("activePipelines").asInt()).isZero();
                    assertThat(request.body().path("uptimeMs").asLong()).isNotNegative();
                    assertThat(request.body().toString()).doesNotContain(PASSWORD, "controlled-sdk-token", "mongodb", "consumer-user");
                });
                assertThat(wire.reports.get(0).body().path("nonce")).isNotEqualTo(wire.reports.get(1).body().path("nonce"));
                admin.getDatabase("admin").runCommand(new Document("updateUser", user).append("roles", List.of()));
                assertThatThrownBy(reporter::report).isInstanceOf(TapstateException.class);
                assertThat(wire.reports).as("unreadable CONFIG must not be reported as an empty healthy runtime").hasSize(2);
                assertThat(root.runtime().business()).isEmpty();
                assertThat(wire.unexpected).isEmpty();
            }
            for (String database : legacy) {
                assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>())).containsExactly("retained");
                assertThat(admin.getDatabase(database).getCollection("retained").find().into(new ArrayList<>())).containsExactly(marker);
            }
            assertThat(admin.getDatabase(config).getCollection("users").countDocuments()).isZero();
            assertThat(admin.getDatabase(config).getCollection("op_bootstrap").countDocuments()).isZero();
            assertThat(admin.getDatabase(config).getCollection("storage_binding").countDocuments()).isZero();
            assertNoProbes(admin, config);
        }
    }

    private ConfigurableApplicationContext start(String metadataUri, String clusterId, AtomicInteger ready, String... overrides) {
        List<String> args = new ArrayList<>(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.store.mongo.server-selection-timeout=2s",
                "--tapstate.connectors.plugins-dir=" + work.resolve(clusterId),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                "--tapstate.cloud.base-url=https://cloud.example.invalid", "--tapstate.cloud.token=controlled-sdk-token",
                "--tapstate.cloud.cluster-id=" + clusterId, "--tapstate.cloud.atlas-uri=" + metadataUri));
        for (String override : overrides) {
            String key = override.substring(0, override.indexOf('=') + 1);
            args.removeIf(argument -> argument.startsWith(key));
            args.add(override);
        }
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .listeners((ApplicationListener<ApplicationReadyEvent>) event -> ready.incrementAndGet())
                .run(args.toArray(String[]::new));
    }

    private void createUser(MongoClient admin, String user, List<String> databases) {
        databases.forEach(this::ownedDatabase);
        admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD)
                .append("roles", databases.stream().map(database -> new Document("role", "readWrite").append("db", database)).toList()));
    }

    private static void assertDenied(MongoClient client, String database) {
        assertThatThrownBy(() -> client.getDatabase(database).getCollection("proof").countDocuments())
                .isInstanceOfSatisfying(MongoCommandException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(13));
    }

    private static String uri(String user, String database) {
        return uri(user, database, PASSWORD);
    }

    private static String uri(String user, String database, String password) {
        return "mongodb://" + user + ":" + password + "@" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017)
                + "/" + database + "?replicaSet=" + REPLICA_SET + "&directConnection=true&authSource=admin";
    }
}
