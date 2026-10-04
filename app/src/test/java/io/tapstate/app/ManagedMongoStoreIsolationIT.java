package io.tapstate.app;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorePort;
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
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
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
    enum Missing { OPERATOR, VIEWS }
    private static final String PASSWORD = "controlled-mongo-permissions-password";
    private static final String REPLICA_SET = "permissions-rs";
    @Container
    private static final GenericContainer<?> MONGO = mongo();
    @TempDir Path work;

    private static GenericContainer<?> mongo() {
        byte[] key = new byte[64];
        new java.security.SecureRandom().nextBytes(key);
        return new GenericContainer<>(DockerImageName.parse("mongo:7.0"))
                .withExposedPorts(27017)
                .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
                .withCopyToContainer(Transferable.of(Base64.getEncoder().encode(key), 0400), "/data/configdb/member.key")
                .withCommand("--replSet", REPLICA_SET, "--keyFile", "/data/configdb/member.key", "--bind_ip_all")
                // The image opens a temporary unauthenticated server while creating its administrator.
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*MongoDB init process complete; ready for start up.*", 1))
                        .withStrategy(Wait.forLogMessage("(?i).*waiting for connections.*", 2))
                        .withStrategy(Wait.forListeningPort())).withStartupTimeout(Duration.ofSeconds(90));
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
    void twoClusterUsersCanUseOnlyTheirOwnThreeDatabasesAndRecoverAfterRestart() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String a = "isolation_a_" + suffix;
        String b = "isolation_b_" + suffix;
        String userA = "user_a_" + suffix;
        String userB = "user_b_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, userA, List.of(a, a + "_operator", a + "_views"));
            createUser(admin, userB, List.of(b, b + "_operator", b + "_views"));
            try (MongoClient ownA = MongoClients.create(uri(userA, a));
                 MongoClient ownB = MongoClients.create(uri(userB, b));
                 MongoClient anonymous = MongoClients.create("mongodb://" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017)
                         + "/" + a + "?directConnection=true")) {
                assertDenied(anonymous, a);
                for (String other : List.of(b, b + "_operator", b + "_views")) assertDenied(ownA, other);
                for (String other : List.of(a, a + "_operator", a + "_views")) assertDenied(ownB, other);

                for (String metadata : List.of(a, b)) {
                    String user = metadata.equals(a) ? userA : userB;
                    try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                        StorePort store = first.getBean(StorePort.class);
                        store.keyedState().save("permissions-proof", "same-key", metadata.getBytes(StandardCharsets.UTF_8));
                        SourceResource views = (SourceResource) store.artifacts().get("views").orElseThrow();
                        try (MongoClient view = MongoClients.create(String.valueOf(views.config().get("uri")))) {
                            view.getDatabase(metadata + "_views").getCollection("proof")
                                    .insertOne(new Document("_id", "same-key").append("owner", metadata));
                        }
                        assertThat(admin.getDatabase(metadata).getCollection(SystemCollections.ARTIFACTS.collectionName())
                                .find(new Document("_id", "views")).first().get("body", Document.class).get("config"))
                                .isInstanceOf(String.class);
                        assertNoProbes(admin, metadata);
                    }
                    try (ConfigurableApplicationContext restarted = start(uri(user, metadata), metadata)) {
                        assertThat(restarted.getBean(StorePort.class).keyedState().load("permissions-proof", "same-key"))
                                .hasValueSatisfying(value -> assertThat(value).isEqualTo(metadata.getBytes(StandardCharsets.UTF_8)));
                        assertThat(admin.getDatabase(metadata + "_views").getCollection("proof")
                                .find(new Document("_id", "same-key")).first().getString("owner")).isEqualTo(metadata);
                        assertNoProbes(admin, metadata);
                    }
                }
            }
        }
    }

    private ConfigurableApplicationContext start(String metadataUri, String clusterId) {
        return start(metadataUri, clusterId, new AtomicInteger());
    }

    @Test
    void managedViewsUseRenewedDeploymentCredentialsAfterRestart() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_renewed_" + suffix;
        String user = "user_renewed_" + suffix;
        String renewedPassword = "controlled-renewed-mongo-password";
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, metadata + "_operator", metadata + "_views"));
            try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                StorePort store = first.getBean(StorePort.class);
                store.keyedState().save("permissions-proof", "renewed-key", new byte[] {7});
                SourceResource views = (SourceResource) store.artifacts().get("views").orElseThrow();
                try (MongoClient materialization = MongoClients.create(String.valueOf(views.config().get("uri")))) {
                    materialization.getDatabase(metadata + "_views").getCollection("proof")
                            .insertOne(new Document("_id", "before-renewal").append("owner", metadata));
                }
            }
            admin.getDatabase("admin").runCommand(new Document("updateUser", user).append("pwd", renewedPassword));
            try (MongoClient revoked = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> revoked.getDatabase(metadata).getCollection("proof").countDocuments())
                        .as("the server no longer accepts the original password")
                        .isInstanceOf(MongoSecurityException.class);
            }
            try (ConfigurableApplicationContext restarted = start(uri(user, metadata, renewedPassword), metadata)) {
                StorePort store = restarted.getBean(StorePort.class);
                assertThat(store.keyedState().load("permissions-proof", "renewed-key"))
                        .hasValueSatisfying(value -> assertThat(value).isEqualTo(new byte[] {7}));
                SourceResource views = (SourceResource) store.artifacts().get("views").orElseThrow();
                try (MongoClient materialization = MongoClients.create(String.valueOf(views.config().get("uri")))) {
                    materialization.getDatabase(metadata + "_views").getCollection("proof")
                            .insertOne(new Document("_id", "after-renewal").append("owner", metadata));
                }
                assertThat(admin.getDatabase(metadata + "_views").getCollection("proof").countDocuments()).isEqualTo(2);
                Object rawConfig = admin.getDatabase(metadata).getCollection(SystemCollections.ARTIFACTS.collectionName())
                        .find(new Document("_id", "views")).first().get("body", Document.class).get("config");
                assertThat(rawConfig).isInstanceOf(String.class);
                assertThat((String) rawConfig).doesNotContain(PASSWORD, renewedPassword);
                assertNoProbes(admin, metadata);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Missing.class)
    void readableButUnwritableDerivedDatabaseCannotReportReady(Missing readOnly) {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_readonly_" + suffix;
        String denied = metadata + (readOnly == Missing.OPERATOR ? "_operator" : "_views");
        String other = metadata + (readOnly == Missing.OPERATOR ? "_views" : "_operator");
        String user = "user_readonly_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD).append("roles", List.of(
                    new Document("role", "readWrite").append("db", metadata),
                    new Document("role", "readWrite").append("db", other),
                    new Document("role", "read").append("db", denied))));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThat(limited.getDatabase(denied).getCollection("proof").countDocuments()).isZero();
                assertThatThrownBy(() -> limited.getDatabase(denied).getCollection("proof")
                        .insertOne(new Document("_id", "denied-write")))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger ready = new AtomicInteger();
            Throwable failure = catchThrowable(() -> {
                try (var unexpected = start(uri(user, metadata), metadata, ready)) {
                    throw new AssertionError("Cloud startup accepted a read-only deployment database");
                }
            });
            assertThat(ready.get()).isZero();
            Throwable coded = failure;
            for (int depth = 0; depth < 16 && coded != null && !(coded instanceof TapstateException); depth++) coded = coded.getCause();
            assertThat(coded).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(StoreError.DATABASE_ACCESS_FAILED);
                assertThat(error.args()).containsExactlyEntriesOf(java.util.Map.of("database", denied));
                assertThat(error.getCause()).isNull();
            });
            assertNoProbes(admin, metadata);
        }
    }

    private static void assertNoProbes(MongoClient admin, String metadata) {
        for (String database : List.of(metadata, metadata + "_operator", metadata + "_views")) {
            assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                    .noneMatch(name -> name.startsWith("tapstate_access_probe_"));
        }
    }

    @ParameterizedTest
    @EnumSource(Missing.class)
    void missingDerivedDatabaseRightsCannotReportReady(Missing missing) {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_missing_" + suffix;
        String denied = metadata + (missing == Missing.OPERATOR ? "_operator" : "_views");
        String allowed = metadata + (missing == Missing.OPERATOR ? "_views" : "_operator");
        String user = "user_missing_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, allowed));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) { assertDenied(limited, denied); }
            AtomicInteger ready = new AtomicInteger();
            AtomicReference<ConfigurableApplicationContext> started = new AtomicReference<>();
            Throwable failure = catchThrowable(() -> started.set(start(uri(user, metadata), metadata, ready)));
            if (started.get() != null) started.get().close();
            assertThat(failure).as("Cloud startup refuses the missing %s grant before reporting ready", missing).isNotNull();
            assertThat(ready.get()).as("no ApplicationReadyEvent was published").isZero();
            Throwable coded = failure;
            for (int depth = 0; depth < 16 && !(coded instanceof TapstateException) && coded != null; depth++) {
                coded = coded.getCause();
            }
            assertThat(coded).as("a storage authorization failure is diagnosable with a canonical code")
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(StoreError.DATABASE_ACCESS_FAILED);
                        assertThat(error.args()).containsExactlyEntriesOf(java.util.Map.of("database", denied));
                        assertThat(error.getCause()).isNull();
                    });
            for (String database : List.of(metadata, allowed, denied)) {
                assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                        .noneMatch(name -> name.startsWith("tapstate_access_probe_"));
            }
        }
    }

    private ConfigurableApplicationContext start(String metadataUri, String clusterId, AtomicInteger ready) {
        List<String> args = new ArrayList<>(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.store.mongo.server-selection-timeout=2s",
                "--tapstate.connectors.plugins-dir=" + work.resolve(clusterId),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                "--tapstate.cloud.base-url=https://cloud.example.invalid", "--tapstate.cloud.token=controlled-sdk-token",
                "--tapstate.cloud.cluster-id=" + clusterId, "--tapstate.cloud.atlas-uri=" + metadataUri));
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .listeners((ApplicationListener<ApplicationReadyEvent>) event -> ready.incrementAndGet())
                .run(args.toArray(String[]::new));
    }

    private static void createUser(MongoClient admin, String user, List<String> databases) {
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
