package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.testsupport.DockerGate;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Rollup fields kept current through change data capture: every field of every document equals a SQL
 * recomputation on the source after a snapshot, inserts, updates, deletes, a change to a referenced row, an
 * element moving in and out of a filter and to another parent, and a restart.
 *
 * <p>What a wrong implementation gets wrong here: one that maintains a running delta gets the re-parent wrong
 * (the old document keeps the element's share); one that ignores reference touches misses the
 * {@code prize_value} update; one that emits the array and its total separately is caught by the consistency
 * check every read makes, which recomputes {@code amount} from the very {@code item_movement} array the same
 * document carries.
 */
class NestRollupFieldsCdcIT {

    /** The wallet pipelines write their documents under the prefix they rename by, plus the root table. */
    private static final String TARGET = "wallet_transactions";
    private static final List<String> WALLET_TABLES = List.of("transactions", "items", "prize_definitions", "ratings");

    @BeforeAll
    static void prerequisites() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    // ---- the change sequence ---------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void rollupFieldsFollowEveryChangeTheirRowsGoThroughAndSurviveARestart(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("rollup_cdc_src_" + suffix);
        seedWallet(mysql);
        String targetUri = SharedMongo.replicaSetUrl("rollup_cdc_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        String pipeline = "rollup_cdc_" + suffix;

        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = tier.launch(storage)) {
                ControlPlane control = start(server, storage, mysql, WALLET_TABLES, targetUri, pipeline,
                        walletPipeline(pipeline));
                Await.until("the four source snapshots landed", () -> {
                    var snapshots = control.snapshotTables(pipeline);
                    return snapshots.size() == 4 && snapshots.values().stream().allMatch(TableSnapshot::landed);
                }, () -> String.valueOf(control.snapshotTables(pipeline)));
                converge(mongo, target, mysql, "after the snapshot");

                sql(mysql, "INSERT INTO items VALUES (8, 1, 2, 2)");
                converge(mongo, target, mysql, "after an inserted item");
                sql(mysql, "UPDATE items SET amount = 6 WHERE id = 1");
                converge(mongo, target, mysql, "after an item's amount changed");
                // prize 1 is read by items of transactions 1 and 2: both documents must move
                sql(mysql, "UPDATE prize_definitions SET prize_value = 7 WHERE id = 1");
                converge(mongo, target, mysql, "after a referenced prize value changed");
                sql(mysql, "DELETE FROM items WHERE id = 2");
                converge(mongo, target, mysql, "after an item was deleted");
                sql(mysql, "UPDATE items SET prize_definition_id = 1 WHERE id = 5");
                converge(mongo, target, mysql, "after an item moved into the reward filter");
                sql(mysql, "UPDATE items SET prize_definition_id = 2 WHERE id = 5");
                converge(mongo, target, mysql, "after the item moved back out of the filter");
                sql(mysql, "UPDATE items SET transaction_id = 1 WHERE id = 6");
                converge(mongo, target, mysql, "after an item moved to another transaction");

                // The latest rating, MGM's shape: two ratings on one model date told apart by rating type,
                // arriving out of order over two batches, one with no model date (no candidate) and one
                // winner whose score is null (the field is null, with no fall back to the next one).
                sql(mysql, "INSERT INTO ratings VALUES (5, 2, 'B', '2026-02-01', 520)");
                converge(mongo, target, mysql, "after a same-day rating of a later type");
                sql(mysql, "INSERT INTO ratings VALUES (6, 2, 'A', '2026-01-15', 900)",
                        "INSERT INTO ratings VALUES (7, 2, 'C', NULL, 999)");
                converge(mongo, target, mysql, "after an older rating and one without a model date");
                assertThat(document(mongo, target, 2)).containsEntry("latest_score", 520L);
                sql(mysql, "INSERT INTO ratings VALUES (8, 2, 'C', '2026-02-01', NULL)");
                converge(mongo, target, mysql, "after a winner whose score is null");
                assertThat(document(mongo, target, 2)).containsEntry("latest_score", null);
                sql(mysql, "DELETE FROM ratings WHERE id = 8");
                converge(mongo, target, mysql, "after the winner was deleted");
                assertThat(document(mongo, target, 2)).containsEntry("latest_score", 520L);
            }

            // A running value would be counted twice on replay; a recomputation cannot be.
            try (ServerHandle server = tier.launch(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                Await.until("the restarted server to adopt the running nest", () -> control.state(pipeline)
                        .filter(PipelineState.RUNNING::equals).isPresent(), () -> String.valueOf(control.state(pipeline)));
                converge(mongo, target, mysql, "after a restart");
                sql(mysql, "DELETE FROM items WHERE transaction_id = 2");
                converge(mongo, target, mysql, "after the last item of a transaction went");
                assertThat(document(mongo, target, 2)).containsEntry("qty", 0L).containsEntry("amount", 0L)
                        .containsEntry("reward_qty", null);
                control.lifecycle(pipeline, LifecycleVerb.PAUSE);
            }
        }
    }

    // ---- the numeric contract ---------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void decimalRollupsAreExactAndWhatCannotBeExactStopsTheDocumentWithACode(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("rollup_num_src_" + suffix);
        sql(mysql,
                "CREATE TABLE tx (id INT PRIMARY KEY)",
                "CREATE TABLE prices (id INT PRIMARY KEY, tx_id INT NOT NULL, price DECIMAL(18,2) NOT NULL)",
                "CREATE TABLE wide (id INT PRIMARY KEY, tx_id INT NOT NULL, big DECIMAL(38,0) NOT NULL)",
                "CREATE TABLE counts (id INT PRIMARY KEY, tx_id INT NOT NULL, n BIGINT NOT NULL)",
                "INSERT INTO tx VALUES (1), (2), (3)",
                // tx 1: a sum a double would round, tx 2: integral, tx 3: nothing at all
                "INSERT INTO prices VALUES (1, 1, 9999999999999999.99), (2, 1, 0.01), (3, 1, 0.01),"
                        + " (4, 2, 2.00), (5, 2, 1.00)",
                "INSERT INTO wide VALUES (1, 1, 99999999999999999999999999999999999999),"
                        + " (2, 1, 99999999999999999999999999999999999999)",
                "INSERT INTO counts VALUES (1, 1, 9223372036854775807), (2, 1, 1)");
        String targetUri = SharedMongo.replicaSetUrl("rollup_num_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storage)) {
            String exact = "rollup_exact_" + suffix;
            ControlPlane control = start(server, storage, mysql, List.of("tx", "prices", "wide", "counts"), targetUri,
                    exact, rollupOver(exact, "prices",
                    Map.of("total", Map.of("sum", "$price", "default", 0),
                            "mean", Map.of("avg", "$price"))));
            for (String failing : List.of("decimal", "integer", "fraction")) {
                String id = "rollup_" + failing + "_" + suffix;
                Map<String, Object> pipeline = switch (failing) {
                    case "decimal" -> rollupOver(id, "wide", Map.of("total", Map.of("sum", "$big")));
                    case "integer" -> rollupOver(id, "counts", Map.of("total", Map.of("sum", "$n")));
                    default -> rollupOver(id, "prices", Map.of("whole", Map.of("sum", "$price", "type", "int64")));
                };
                control.sourceRequest("POST", "/api/pipelines", pipeline, null, 201);
                control.lifecycle(id, LifecycleVerb.START);
            }

            AtomicReference<String> last = new AtomicReference<>("not observed");
            Await.until("the exact sums and averages", () -> {
                try {
                    Map<String, Object> tx1 = mongo.fetch(target, exact + "_tx", Map.of("id", 1)).orElseThrow();
                    Map<String, Object> tx2 = mongo.fetch(target, exact + "_tx", Map.of("id", 2)).orElseThrow();
                    Map<String, Object> tx3 = mongo.fetch(target, exact + "_tx", Map.of("id", 3)).orElseThrow();
                    assertThat(decimal(tx1.get("total"))).isEqualTo(new BigDecimal("10000000000000000.01"));
                    assertThat(decimal(tx1.get("mean"))).isEqualTo(new BigDecimal("10000000000000000.01")
                            .divide(BigDecimal.valueOf(3), 6, RoundingMode.HALF_EVEN));
                    assertThat(decimal(tx2.get("total"))).isEqualTo(new BigDecimal("3.00"));
                    assertThat(decimal(tx2.get("mean"))).isEqualTo(new BigDecimal("1.500000"));
                    assertThat(decimal(tx3.get("total"))).isEqualTo(new BigDecimal("0.00"));
                    assertThat(tx3.get("mean")).isNull();
                    return true;
                } catch (AssertionError | RuntimeException pending) {
                    last.set(String.valueOf(pending.getMessage()));
                    return false;
                }
            }, last::get);
            for (var failing : Map.of("decimal", "nest.rollup-overflow", "integer", "nest.rollup-overflow",
                    "fraction", "nest.rollup-result-not-convertible").entrySet()) {
                String id = "rollup_" + failing.getKey() + "_" + suffix;
                Await.until(id + " to stop with " + failing.getValue(),
                        () -> control.failureCode(id).filter(failing.getValue()::equals).isPresent(),
                        () -> String.valueOf(control.failure(id)));
            }
        }
    }

    // ---- a reference that is missing ---------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aMissingReferenceMakesItsTotalNullUntilTheRowArrivesEvenAcrossARestart(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("rollup_ref_src_" + suffix);
        seedWallet(mysql);
        // item 9 names prize 4, which does not exist yet
        sql(mysql, "INSERT INTO items VALUES (9, 3, 4, 5)");
        String targetUri = SharedMongo.replicaSetUrl("rollup_ref_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        String pipeline = "rollup_ref_" + suffix;

        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = tier.launch(storage)) {
                ControlPlane control = start(server, storage, mysql, WALLET_TABLES, targetUri, pipeline,
                        walletPipeline(pipeline));
                AtomicReference<String> last = new AtomicReference<>("not observed");
                Await.until("transaction 3 written without its prize and with a null amount", () -> {
                    Map<String, Object> tx3 = mongo.fetch(target, TARGET, Map.of("id", 3)).orElse(null);
                    last.set(String.valueOf(tx3));
                    return tx3 != null && tx3.containsKey("amount") && tx3.get("amount") == null
                            && Long.valueOf(5L).equals(tx3.get("qty"));
                }, last::get);
                // the transactions that read no missing reference are whole and right
                converge(mongo, target, mysql, "beside the incomplete one", List.of(1, 2));
            }
            try (ServerHandle server = tier.launch(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                Await.until("the restarted server to adopt the running nest", () -> control.state(pipeline)
                        .filter(PipelineState.RUNNING::equals).isPresent(), () -> String.valueOf(control.state(pipeline)));
                assertThat(document(mongo, target, 3)).containsEntry("amount", null);
                sql(mysql, "INSERT INTO prize_definitions VALUES (4, 'cash', 11)");
                converge(mongo, target, mysql, "after the missing prize arrived");
                assertThat(document(mongo, target, 3)).containsEntry("amount", 55L);
                control.lifecycle(pipeline, LifecycleVerb.PAUSE);
            }
        }
    }

    // ---- fixtures ------------------------------------------------------------------------------------------

    private static ControlPlane start(ServerHandle server, PreparedNativeStore storage, Map<String, Object> mysql,
            List<String> tables, String targetUri, String pipeline, Map<String, Object> definition) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        storage.loginAndAwaitBusiness(control);
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: wallet-mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(mysql.get("host"), mysql.get("port"), mysql.get("database"),
                mysql.get("username"), mysql.get("password"), String.join(", ", tables)));
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: wallet-store
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        control.apply(resources);
        control.discoverSchema("wallet-mysql", "mysql", mysql);
        control.sourceRequest("POST", "/api/pipelines", definition, null, 201);
        control.lifecycle(pipeline, LifecycleVerb.START);
        return control;
    }

    /** The transaction document of the snapshot example, with items whose key may change. */
    private static Map<String, Object> walletPipeline(String id) {
        Map<String, Object> rollups = new LinkedHashMap<>();
        rollups.put("qty", Map.of("sum", "$amount", "default", 0));
        rollups.put("amount", Map.of("sum", "=after.amount * after.prize_definition.prize_value", "default", 0));
        rollups.put("reward_qty", Map.of("sum", "$amount",
                "filter", "=after.prize_definition.prize_code == 'reward-points'"));
        Map<String, Object> items = new LinkedHashMap<>();
        items.put("from", "i");
        items.put("on", Map.of("transaction_id", "id"));
        items.put("as", "array");
        items.put("path", "item_movement");
        items.put("arrayKey", List.of("id"));
        items.put("trackKeyChanges", true);
        items.put("embed", List.of(Map.of("from", "pd", "on", Map.of("id", "prize_definition_id"),
                "as", "object", "path", "prize_definition")));
        items.put("rollup", rollups);
        Map<String, Object> ratings = Map.of("from", "r", "on", Map.of("trans_id", "id"), "as", "rollup",
                "arrayKey", List.of("id"), "rollup", Map.of("latest_score",
                        Map.of("last", Map.of("value", "$score", "by", List.of("$model_date", "$rating_type")))));
        return pipelineOf(id, Map.of("t", "transactions", "i", "items", "pd", "prize_definitions", "r", "ratings"),
                "t", List.of(items, ratings));
    }

    /** A transaction document carrying only {@code rollups} over the rows of {@code table}. */
    private static Map<String, Object> rollupOver(String id, String table, Map<String, Object> rollups) {
        return pipelineOf(id, Map.of("t", "tx", "c", table), "t", List.of(Map.of("from", "c",
                "on", Map.of("tx_id", "id"), "as", "rollup", "arrayKey", List.of("id"), "rollup", rollups)));
    }

    private static Map<String, Object> pipelineOf(String id, Map<String, String> aliases, String root,
            List<Map<String, Object>> embeds) {
        return Map.of("id", id, "sources", List.of("wallet-mysql"),
                "settings", Map.of("read_mode", "snapshot_and_cdc"),
                "transforms", List.of(Map.of("id", "doc", "type", "nest", "from", aliases,
                        "root", Map.of("from", root, "key", List.of("id"), "embed", embeds))),
                "serve", Map.of("from", "doc", "sync", List.of(Map.of("source", "wallet-store",
                        "rename", Map.of("prefix", id.startsWith("rollup_cdc") || id.startsWith("rollup_ref")
                                ? "wallet_" : id + "_"),
                        "execution", Map.of("parallelism", 1)))));
    }

    private static void seedWallet(Map<String, Object> mysql) throws SQLException {
        sql(mysql,
                "CREATE TABLE transactions (id INT PRIMARY KEY, player_id INT NOT NULL)",
                "CREATE TABLE items (id INT PRIMARY KEY, transaction_id INT NOT NULL, prize_definition_id INT,"
                        + " amount INT NOT NULL)",
                "CREATE TABLE prize_definitions (id INT PRIMARY KEY, prize_code VARCHAR(32) NOT NULL,"
                        + " prize_value INT NOT NULL)",
                "CREATE TABLE ratings (id INT PRIMARY KEY, trans_id INT NOT NULL, rating_type VARCHAR(8) NOT NULL,"
                        + " model_date DATE, score INT)",
                "INSERT INTO transactions VALUES (1, 9), (2, 9), (3, 7)",
                "INSERT INTO prize_definitions VALUES (1, 'reward-points', 5), (2, 'cash', 3), (3, 'reward-points', 10)",
                "INSERT INTO items VALUES (1, 1, 1, 2), (2, 1, 2, 1), (3, 1, 3, 4), (4, 2, 1, 3), (5, 2, 2, 5),"
                        + " (6, 2, 2, 1), (7, 2, 3, 3)",
                "INSERT INTO ratings VALUES (1, 1, 'A', '2026-03-01', 700), (2, 1, 'B', '2026-03-05', 690),"
                        + " (3, 2, 'A', '2026-02-01', 500), (4, 1, 'A', '2026-03-05', 650)");
    }

    /** Waits until every transaction document equals the SQL recomputation, checking every read on the way. */
    private static void converge(MongoEndpoints mongo, EndpointAddress target, Map<String, Object> mysql, String when)
            throws SQLException {
        converge(mongo, target, mysql, when, null);
    }

    private static void converge(MongoEndpoints mongo, EndpointAddress target, Map<String, Object> mysql,
            String when, List<Integer> only) throws SQLException {
        AtomicReference<String> last = new AtomicReference<>("not observed");
        Await.until("every document equal to the SQL recomputation " + when, () -> {
            Map<Integer, Map<String, Object>> expected = recomputed(mysql);
            for (var entry : expected.entrySet()) {
                if (only != null && !only.contains(entry.getKey())) {
                    continue;
                }
                Map<String, Object> actual = mongo.fetch(target, TARGET, Map.of("id", entry.getKey())).orElse(null);
                if (actual == null) {
                    last.set("transaction " + entry.getKey() + " not written");
                    return false;
                }
                // Whatever version this read caught, its totals belong to the items it carries.
                assertThat(actual.get("amount")).as("%s: amount against its own item_movement", when)
                        .isEqualTo(amountFromItsOwnItems(actual));
                Map<String, Object> got = new TreeMap<>();
                for (String field : entry.getValue().keySet()) {
                    got.put(field, actual.get(field));
                }
                if (!got.equals(entry.getValue())) {
                    last.set("transaction " + entry.getKey() + ": " + got + " != " + entry.getValue());
                    return false;
                }
            }
            return true;
        }, last::get);
    }

    /** {@code amount} as the document's own items say it should be: null if any item lacks its prize. */
    private static Object amountFromItsOwnItems(Map<String, Object> document) {
        long total = 0;
        boolean any = false;
        for (Object item : (List<?>) document.get("item_movement")) {
            Document element = (Document) item;
            Document prize = (Document) element.get("prize_definition");
            if (prize == null) {
                return null;
            }
            total += ((Number) element.get("amount")).longValue() * ((Number) prize.get("prize_value")).longValue();
            any = true;
        }
        return any ? total : 0L;
    }

    /** Every transaction's rollup fields, computed by the source database itself. */
    private static Map<Integer, Map<String, Object>> recomputed(Map<String, Object> mysql) {
        Map<Integer, Map<String, Object>> out = new TreeMap<>();
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("""
                        SELECT t.id,
                               COALESCE(SUM(i.amount), 0) AS qty,
                               CASE WHEN SUM(i.id IS NOT NULL AND pd.id IS NULL) > 0 THEN NULL
                                    ELSE COALESCE(SUM(i.amount * pd.prize_value), 0) END AS amount,
                               CASE WHEN SUM(i.id IS NOT NULL AND pd.id IS NULL) > 0 THEN NULL
                                    ELSE SUM(CASE WHEN pd.prize_code = 'reward-points' THEN i.amount END) END AS reward_qty,
                               (SELECT r.score FROM ratings r WHERE r.trans_id = t.id AND r.model_date IS NOT NULL
                                 ORDER BY r.model_date DESC, r.rating_type DESC, r.id DESC LIMIT 1) AS latest_score
                        FROM transactions t
                        LEFT JOIN items i ON i.transaction_id = t.id
                        LEFT JOIN prize_definitions pd ON pd.id = i.prize_definition_id
                        GROUP BY t.id""")) {
            while (rows.next()) {
                Map<String, Object> fields = new TreeMap<>();
                fields.put("qty", longOrNull(rows, "qty"));
                fields.put("amount", longOrNull(rows, "amount"));
                fields.put("reward_qty", longOrNull(rows, "reward_qty"));
                fields.put("latest_score", longOrNull(rows, "latest_score"));
                out.put(rows.getInt("id"), fields);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return out;
    }

    private static Long longOrNull(ResultSet rows, String column) throws SQLException {
        long value = rows.getLong(column);
        return rows.wasNull() ? null : value;
    }

    private static Map<String, Object> document(MongoEndpoints mongo, EndpointAddress target, int id) {
        return mongo.fetch(target, TARGET, Map.of("id", id)).orElseThrow();
    }

    private static BigDecimal decimal(Object value) {
        return value instanceof Decimal128 d ? d.bigDecimalValue() : (BigDecimal) Objects.requireNonNull(value);
    }

    private static void sql(Map<String, Object> mysql, String... statements) throws SQLException {
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement()) {
            List<String> all = new ArrayList<>(List.of(statements));
            for (String one : all) {
                statement.execute(one);
            }
        }
    }
}
