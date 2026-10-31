package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A PostgreSQL that cannot do change data capture at all passes the product's connection test.
 *
 * <p>This documents behaviour rather than endorsing it, and it is written down because the behaviour is
 * easy to meet and hard to see. The image ships {@code wal_level=replica}, which serves physical
 * replication and not logical decoding, so no replication slot can ever be created on it. The
 * connector notices - it really does try to create one - but reports the failure as a warning, and the
 * product's overall outcome does not fail on warnings. So a user who points at such a server is told
 * the connection is fine, builds a pipeline on it, and finds out when the stream half fails.
 *
 * <p><b>Why the container is left at its defaults.</b> Every other PostgreSQL fixture here turns logical
 * decoding on, because they are about what the product does once it works. This one is about the server
 * nobody prepared, which is the one a first-time user actually has.
 *
 * <p><b>What this pins, and what it does not.</b> It pins today's contract: a warning-only test passes.
 * It takes no position on whether that is right - a failed change-data-capture probe arguably should
 * fail the test, and that judgement belongs with the plan that owns clean refusals, because the same
 * rule governs MySQL and changing it would change what existing MySQL users already see. What this test
 * guarantees is that the change cannot be made silently: whoever makes it will find this case red, with
 * the reasoning in front of them.
 *
 * <p>Needs Docker and the postgres connector jar.
 */
class PostgresWithoutLogicalDecodingStillPassesConnectionTestIT {

    /** The item the connector reports its change-stream probe under. */
    private static final String READ_LOG_CHECK = "Read log";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres");
    }

    @Test
    void aServerThatCannotDecodeLogicallyIsReportedAsPassingWithAWarning() throws Exception {
        // Deliberately no wal_level override: this is the server as the image ships it.
        try (PostgreSQLContainer<?> postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:16"))) {
            postgres.start();

            try (PreparedNativeStore storage = PreparedNativeStore.create();
                    ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));

                ControlPlane.ConnectionTest report =
                        control.testConnection("pg_unprepared", "postgres", settings(postgres));

                // The finding, in two halves that only mean something together.
                assertThat(report.outcome())
                        .as("a server that can never open a replication slot is still reported as "
                                + "connectable: %s", report.statusByCheck())
                        .isEqualTo("PASSED");
                assertThat(report.statusByCheck())
                        .as("and the change-stream probe is where it said otherwise, as a warning that "
                                + "does not reach the outcome: %s", report.statusByCheck())
                        .containsEntry(READ_LOG_CHECK, "WARNING");
            }
        }
    }

    /**
     * The connector's own spelling: the account is {@code user} here where MySQL says {@code username},
     * and a table is addressed by schema as well as by database.
     */
    private static Map<String, Object> settings(PostgreSQLContainer<?> postgres) {
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", postgres.getHost());
        settings.put("port", postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT));
        settings.put("database", postgres.getDatabaseName());
        settings.put("schema", "public");
        settings.put("user", postgres.getUsername());
        settings.put("password", postgres.getPassword());
        return settings;
    }
}
