package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A PostgreSQL source's replication slot follows the pipeline reading it: confirmed past each change once
 * that change has landed, and on to the head of the log once writing stops, rather than staying where the
 * slot was created and holding every log segment since.
 *
 * <p>The shape is the one the slot was first seen not moving in: accounts, their transactions and their
 * positions, nested into one document per account and served to MongoDB. Transactions are written one at a
 * time after the load; each is waited for in the view, and the slot has to move past it within half a minute
 * of it landing. Past a change means at or beyond where the server was about to write inside the change's
 * transaction, before its commit, which holds whichever end of a commit record the connector confirms.
 *
 * <p>The last reading is taken once writing has stopped: the slot has to reach the position the server's log
 * had reached at that moment. Nothing more is written to the captured tables, so only the runs that carry a
 * position and no change can take it there -- which is what keeps a quiet source from holding its log.
 */
class PostgresSlotFollowsDurablePositionIT {

    private static final Duration BOUND = Duration.ofSeconds(180);
    private static final Duration SLOT_FOLLOWS = Duration.ofSeconds(30);
    private static final Duration SLOT_REACHES_THE_HEAD = Duration.ofSeconds(90);
    private static final double FLUSH_LAG_LIMIT_SECONDS = 60;

    private static final String SOURCE_ID = "src_pg";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String PIPELINE = "accounts_view";
    private static final int ACCOUNTS = 100;
    private static final int TRANSACTIONS = 20;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void theSlotMovesPastEachTransactionOnceItLandsAndOnToTheHeadOnceWritingStops() throws Exception {
        Map<String, Object> source = SharedPostgres.settings("slot_follows_src");
        createTables(source);
        String storeUri = SharedMongo.replicaSetUrl("slot_follows_store");
        String targetUri = SharedMongo.replicaSetUrl("slot_follows_target");

        try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri); MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE_ID + ".tap.yml", sourceYaml(source));
            resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri));
            resources.put(PIPELINE + ".tap.yml", pipelineYaml());
            control.apply(resources);
            control.discoverSchema(SOURCE_ID, "postgres", discoveryConfig(source));

            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until(PIPELINE + " to reach RUNNING", BOUND,
                    () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> control.state(PIPELINE) + ", logs: " + control.logs(PIPELINE));
            Await.until("every account in the view", BOUND,
                    () -> mongo.documents(EndpointAddress.uri(targetUri), "accounts").size() == ACCOUNTS,
                    () -> mongo.documents(EndpointAddress.uri(targetUri), "accounts").size() + " documents");
            Await.until("the chain's reader to hold a slot", BOUND,
                    () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));

            List<Double> lags = new ArrayList<>();
            for (int k = 1; k <= TRANSACTIONS; k++) {
                String inside = transact(source, k);
                int transaction = k;
                Await.until("transaction " + k + " in the view", BOUND,
                        () -> holdsTransaction(mongo, targetUri, transaction),
                        () -> String.valueOf(account(mongo, targetUri, transaction)));
                Await.until("the slot to move past transaction " + k + " once it landed", SLOT_FOLLOWS,
                        () -> PostgresSlots.confirmedAtOrPast(source, inside),
                        () -> String.valueOf(PostgresSlots.of(source)));
                PostgresSlots.flushLagSeconds(source).ifPresent(lags::add);
            }
            assertThat(lags).as("flush lag while writing, in seconds").allSatisfy(
                    lag -> assertThat(lag).isLessThan(FLUSH_LAG_LIMIT_SECONDS));

            String head = PostgresSlots.writtenPosition(source);
            Await.until("the slot to reach the head of the log once writing stopped", SLOT_REACHES_THE_HEAD,
                    () -> PostgresSlots.confirmedAtOrPast(source, head),
                    () -> "head " + head + ", slots " + PostgresSlots.of(source));
            assertThat(PostgresSlots.names(source)).as("one slot throughout").hasSize(1);
        }
    }

    /**
     * Writes transaction {@code k} -- account {@code k}'s -- in a transaction of its own, answering where the
     * server was about to write inside it, before its commit.
     */
    private static String transact(Map<String, Object> source, int k) throws Exception {
        try (Connection connection = SharedPostgres.connect(source)) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO transactions (id, account_id, amount) VALUES ("
                        + k + ", " + k + ", " + (k * 10) + ")");
            }
            String inside = PostgresSlots.insertPosition(connection);
            connection.commit();
            return inside;
        }
    }

    private static boolean holdsTransaction(MongoEndpoints mongo, String targetUri, int k) {
        return account(mongo, targetUri, k)
                .map(document -> document.get("transactions"))
                .filter(List.class::isInstance)
                .map(transactions -> ((List<?>) transactions).stream()
                        .filter(Document.class::isInstance)
                        .map(Document.class::cast)
                        .anyMatch(transaction -> String.valueOf(transaction.get("id")).equals(String.valueOf(k))))
                .orElse(false);
    }

    private static Optional<Document> account(MongoEndpoints mongo, String targetUri, int k) {
        return mongo.documents(EndpointAddress.uri(targetUri), "accounts").stream()
                .filter(document -> String.valueOf(document.get("id")).equals(String.valueOf(k)))
                .findFirst();
    }

    private static void createTables(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedPostgres.connect(source);
                Statement statement = connection.createStatement()) {
            for (String table : List.of("positions", "transactions", "accounts")) {
                statement.execute("DROP TABLE IF EXISTS " + table);
            }
            statement.execute("CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("CREATE TABLE transactions (id INT PRIMARY KEY, account_id INT, amount INT)");
            statement.execute("CREATE TABLE positions (id INT PRIMARY KEY, account_id INT, symbol VARCHAR(16))");
            for (String table : List.of("accounts", "transactions", "positions")) {
                statement.execute("ALTER TABLE " + table + " REPLICA IDENTITY FULL");
            }
            for (int id = 1; id <= ACCOUNTS; id++) {
                statement.execute("INSERT INTO accounts (id, name) VALUES (" + id + ", 'a" + id + "')");
                statement.execute("INSERT INTO positions (id, account_id, symbol) VALUES ("
                        + id + ", " + id + ", 'S" + (id % 7) + "')");
            }
        }
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: account_doc
                    type: nest
                    from: { a: accounts, t: transactions, p: positions }
                    root:
                      from: a
                      key: [ id ]
                      embed:
                        - { from: t, on: { account_id: id }, as: array, path: transactions, arrayKey: [ id ] }
                        - { from: p, on: { account_id: id }, as: array, path: positions, arrayKey: [ id ] }
                serve:
                  from: account_doc
                  sync:
                    - source: %s
                """
                .formatted(PIPELINE, SOURCE_ID, TARGET_ID);
    }

    private static Map<String, Object> discoveryConfig(Map<String, Object> settings) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", settings.get("host"));
        config.put("port", settings.get("port"));
        config.put("database", settings.get("database"));
        config.put("schema", "public");
        config.put("user", settings.get("username"));
        config.put("password", settings.get("password"));
        return config;
    }

    private static String sourceYaml(Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: postgres
                config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                mode: cdc
                tables: [ accounts, transactions, positions ]
                """
                .formatted(SOURCE_ID, settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"));
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(TARGET_ID, targetUri);
    }
}
