package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/** A wide PostgreSQL source must not churn connection pools while starting CDC. */
class PostgresCdcStreamStaysOpenIT {

    private static final String DATABASE = "cdc_stream_stays_open_523";
    private static final String PIPELINE = "wide_postgres_source";
    private static final Duration START_BOUND = Duration.ofSeconds(120);

    @BeforeAll
    static void requireDockerAndConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void aFreshWideSourceDoesNotReopenItsCdcConnectionEverySecond() throws Exception {
        Map<String, Object> postgres = SharedPostgres.settings(DATABASE);
        List<String> tables = IntStream.range(0, 27).mapToObj(index -> "source_table_" + index).toList();
        try (Connection connection = SharedPostgres.connect(postgres);
                Statement statement = connection.createStatement()) {
            for (String table : tables) {
                statement.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, value TEXT)");
            }
            statement.execute("INSERT INTO source_table_0 VALUES (1, 'initial')");
        }

        String targetUri = SharedMongo.replicaSetUrl("cdc_stream_stays_open_target_523");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                SharedMongo.replicaSetUrl("cdc_stream_stays_open_store_523"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_pg
                    connector: postgres
                    config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                    mode: cdc
                    tables: [ %s ]
                    """.formatted(postgres.get("host"), postgres.get("port"), postgres.get("database"),
                    postgres.get("username"), postgres.get("password"), String.join(", ", tables)));
            resources.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: tgt_mongo
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(targetUri));
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_pg
                    settings: { read_mode: snapshot_and_cdc }
                    transforms:
                      - { id: all_rows, from: /source_table_.*/, type: filter, expr: "true" }
                    serve:
                      from: all_rows
                      sync:
                        - source: tgt_mongo
                    """.formatted(PIPELINE));
            control.apply(resources);
            Map<String, Object> discovery = new LinkedHashMap<>(postgres);
            discovery.put("user", discovery.remove("username"));
            discovery.put("schema", "public");
            control.discoverSchema("src_pg", "postgres", discovery);
            AtomicInteger poolStarts = new AtomicInteger();
            Logger hikari = (Logger) LoggerFactory.getLogger("com.zaxxer.hikari.HikariDataSource");
            AppenderBase<ILoggingEvent> observedStarts = new AppenderBase<>() {
                @Override
                protected void append(ILoggingEvent event) {
                    String message = event.getFormattedMessage();
                    if (message.startsWith("HikariPool-") && message.endsWith(" - Starting...")) {
                        poolStarts.incrementAndGet();
                    }
                }
            };
            observedStarts.start();
            hikari.addAppender(observedStarts);
            try {
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the snapshot row to reach MongoDB", START_BOUND,
                        () -> mongo.count(EndpointAddress.uri(targetUri), "source_table_0") == 1,
                        () -> control.logs(PIPELINE));
                Await.until("the fresh PostgreSQL slot to become active", START_BOUND,
                        () -> activeReplicationPids(postgres).size() == 1,
                        () -> "active replication PIDs: " + activeReplicationPids(postgres)
                                + "; pipeline logs: " + control.logs(PIPELINE));
            } finally {
                hikari.detachAppender(observedStarts);
                observedStarts.stop();
            }
            assertThat(poolStarts.get())
                    .as("a single fresh 27-table source opened a new pool pair for each table before "
                            + "CDC became active, matching the reported start/warn/shutdown churn")
                    .isBetween(1, 4);
        }
    }

    private static List<Integer> activeReplicationPids(Map<String, Object> postgres) {
        try (Connection connection = SharedPostgres.connect(postgres);
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT active_pid FROM pg_replication_slots "
                        + "WHERE database = '" + DATABASE + "' AND active ORDER BY slot_name")) {
            List<Integer> pids = new java.util.ArrayList<>();
            while (rows.next()) {
                pids.add(rows.getInt(1));
            }
            return pids;
        } catch (Exception failure) {
            throw new IllegalStateException("cannot read active replication slots", failure);
        }
    }
}
