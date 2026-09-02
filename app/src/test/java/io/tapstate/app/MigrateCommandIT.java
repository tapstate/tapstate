package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The inspection mode of the server binary, against a real store.
 *
 * <p>What every case here is really checking is that it stays read-only. It exists to be run on an
 * installation the server is refusing to start against, so an operator reaching for it is already in
 * trouble; a command that quietly created a collection or built an index while answering would be
 * changing the thing under investigation, and the person running it would have no way to know.
 */
@RequiresDocker
class MigrateCommandIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static MongoClient client;

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void listingWhatThisBuildCarriesNeedsNoStoreAtAll() {
        // Asked of a build, not of an installation -- which is what the release comparison needs, and
        // why it must answer with nothing running and nothing configured.
        Output output = run("migrate", "--list", "--tapstate.store.mongo.uri=mongodb://127.0.0.1:1/nowhere");

        assertThat(output.exitCode).isZero();
        assertThat(output.out).contains("1 V1BaselineIndexes");
    }

    @Test
    void statusOnAStoreNobodyHasMigratedReportsWhatWouldRunAndTouchesNothing() {
        String database = "migrate_status";
        drop(database);

        Output output = run("migrate", "--status", uriArgument(database));

        assertThat(output.exitCode).isZero();
        assertThat(output.out).contains("installed: 0").contains("supported: 2")
                .contains("V1BaselineIndexes").contains("V2StructuredArtifacts");
        assertThat(collectionNames(database))
                .as("the command is read-only; it must not bring the store part way forward while "
                        + "reporting on it")
                .isEmpty();
    }

    @Test
    void aDryRunNamesTheIndexesItWouldBuildWithoutBuildingThem() {
        String database = "migrate_dryrun";
        drop(database);

        Output output = run("migrate", "--dry-run", uriArgument(database));

        assertThat(output.exitCode).isZero();
        assertThat(output.out).contains("sessions.secretHash_issuer_idx").contains("(to build)");
        assertThat(collectionNames(database)).isEmpty();
    }

    @Test
    void afterAStartHasMigratedTheStoreThereIsNothingPending() {
        String database = "migrate_done";
        drop(database);
        try (io.tapstate.adapters.mongostore.MongoConnection connection =
                new io.tapstate.adapters.mongostore.MongoConnection(
                        new io.tapstate.adapters.mongostore.MongoConnectionSettings(
                                uri(database), null, java.time.Duration.ofSeconds(5)))) {
            connection.verify();
        }

        Output output = run("migrate", "--status", uriArgument(database));

        assertThat(output.out)
                .contains("installed: "
                        + io.tapstate.adapters.mongostore.migration.MigrationRunner.SUPPORTED_VERSION)
                .contains("pending:   none");
    }

    @Test
    void aStoreThatCannotBeReachedIsReportedAsACodedDiagnosticRatherThanAStack() {
        // The state this command is most often reached in is one where something is wrong. A driver
        // stack here would be the same answer as no answer.
        Output output = run("migrate", "--status",
                "--tapstate.store.mongo.uri=mongodb://127.0.0.1:1/nowhere?replicaSet=rs0",
                "--tapstate.store.mongo.server-selection-timeout=1s");

        assertThat(output.exitCode).isEqualTo(1);
        assertThat(output.err).contains("Cannot reach the store");
        assertThat(output.err).doesNotContain("com.mongodb");
    }

    // ---- fixtures ----

    private record Output(int exitCode, String out, String err) {
    }

    private static Output run(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode = MigrateCommand.run(args,
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
        return new Output(exitCode, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
    }

    private static String uri(String database) {
        String base = REPLICA_SET.getReplicaSetUrl();
        return base.substring(0, base.lastIndexOf('/') + 1) + database;
    }

    private static String uriArgument(String database) {
        return "--tapstate.store.mongo.uri=" + uri(database);
    }

    private static MongoClient client() {
        if (client == null) {
            client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        }
        return client;
    }

    private static void drop(String database) {
        client().getDatabase(database).drop();
    }

    private static List<String> collectionNames(String database) {
        List<String> names = new ArrayList<>();
        client().getDatabase(database).listCollectionNames().into(names);
        return names;
    }
}
