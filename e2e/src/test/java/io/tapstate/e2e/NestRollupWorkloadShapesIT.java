package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Rollup fields on the two document shapes real wallet and player-profile workloads ask for, with synthetic data
 * and neutral names. Every expected value is computed by the source database itself, so each document is held to
 * an answer the product had no part in.
 *
 * <p>The ledger: one document per transaction whose movements come from several detail tables. Each table's
 * rows are mapped to a signed amount by its own formula, tagged with where they came from and merged into one
 * level, over which the transaction carries its quantity, its line count and per-bucket totals.
 *
 * <p>The player: one document per player carrying values no single row holds - the time of the latest session
 * across sharded session tables, the active card, the preferred phone and the latest photo picked by an
 * ordering among the rows a condition admits, exact per-bucket point balances and a count and total of active
 * comps - each kept right as the rows behind it change, move or go.
 */
class NestRollupWorkloadShapesIT {

    @BeforeAll
    static void prerequisites() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    // ---- the ledger ------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aLedgerTransactionTotalsMovementsMappedFromSeveralDetailTables(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("rollup_ledger_src_" + suffix);
        sql(mysql,
                "CREATE TABLE ledger_txn (id INT PRIMARY KEY, member_id INT NOT NULL)",
                "CREATE TABLE point_rows (id INT PRIMARY KEY, txn_id INT NOT NULL, bucket_group INT NOT NULL,"
                        + " game_pts INT NOT NULL, base_pts INT NOT NULL, bonus_pts INT NOT NULL, adj_cr INT NOT NULL,"
                        + " adj_dr INT NOT NULL, redeem_pts INT NOT NULL, expire_pts INT NOT NULL)",
                "CREATE TABLE promo_rows (id INT PRIMARY KEY, txn_id INT NOT NULL, promo INT NOT NULL,"
                        + " bonus_promo INT NOT NULL, promo_used INT NOT NULL, adj_cr INT NOT NULL, adj_dr INT NOT NULL,"
                        + " expired INT NOT NULL)",
                "INSERT INTO ledger_txn VALUES (1, 501), (2, 501), (3, 777)",
                "INSERT INTO point_rows VALUES (1, 1, 2, 100, 20, 5, 0, 0, 0, 0), (2, 1, 5, 40, 0, 0, 10, 3, 0, 0),"
                        + " (3, 2, 2, 0, 0, 0, 0, 0, 60, 0), (4, 2, 2, 10, 10, 0, 0, 0, 0, 25)",
                "INSERT INTO promo_rows VALUES (1, 1, 50, 10, 0, 0, 0, 0), (2, 2, 30, 0, 45, 0, 0, 0)");
        String targetUri = SharedMongo.replicaSetUrl("rollup_ledger_tgt_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        String pipeline = "rollup_ledger_" + suffix;

        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storage)) {
            ControlPlane control = start(server, storage, mysql, List.of("ledger_txn", "point_rows", "promo_rows"),
                    targetUri, "pipeline.tap.yml", ledgerPipeline(pipeline), pipeline);
            String collection = pipeline + "_ledger_txn";
            converge(mongo, target, collection, mysql, LEDGER_SQL, "after the snapshot");

            sql(mysql, "INSERT INTO point_rows VALUES (5, 3, 5, 7, 0, 0, 0, 0, 0, 0)");
            converge(mongo, target, collection, mysql, LEDGER_SQL, "after a points row for an empty transaction");
            sql(mysql, "UPDATE point_rows SET game_pts = 400 WHERE id = 1");
            converge(mongo, target, collection, mysql, LEDGER_SQL, "after a formula input changed");
            sql(mysql, "UPDATE point_rows SET bucket_group = 5 WHERE id = 4");
            converge(mongo, target, collection, mysql, LEDGER_SQL, "after a row moved from one bucket to another");
            sql(mysql, "DELETE FROM promo_rows WHERE id = 1");
            converge(mongo, target, collection, mysql, LEDGER_SQL, "after a promo row was deleted");
            sql(mysql, "UPDATE point_rows SET txn_id = 2 WHERE id = 2");
            converge(mongo, target, collection, mysql, LEDGER_SQL, "after a points row moved to another transaction");
            control.lifecycle(pipeline, LifecycleVerb.PAUSE);
        }
    }

    private static String ledgerPipeline(String id) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: shapes-mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: point_movement
                    type: map
                    from: [ point_rows ]
                    fields:
                      movement_key: "='points-' + string(after.id)"
                      txn_id: $txn_id
                      origin: points
                      prize_code: "=after.bucket_group == 2 ? 'reward-points' : (after.bucket_group == 5 ? 'tier-points' : 'other')"
                      amount: "=after.game_pts + after.base_pts + after.bonus_pts + after.adj_cr - after.adj_dr - after.redeem_pts - after.expire_pts"
                      id: false
                      bucket_group: false
                      game_pts: false
                      base_pts: false
                      bonus_pts: false
                      adj_cr: false
                      adj_dr: false
                      redeem_pts: false
                      expire_pts: false
                  - id: promo_movement
                    type: map
                    from: [ promo_rows ]
                    fields:
                      movement_key: "='promo-' + string(after.id)"
                      txn_id: $txn_id
                      origin: promo
                      prize_code: bonus
                      amount: "=after.promo + after.bonus_promo - after.promo_used + after.adj_cr - after.adj_dr - after.expired"
                      id: false
                      promo: false
                      bonus_promo: false
                      promo_used: false
                      adj_cr: false
                      adj_dr: false
                      expired: false
                  - id: movements
                    type: union
                    from: [ point_movement, promo_movement ]
                  - id: ledger_doc
                    type: nest
                    from: { t: ledger_txn, m: movements }
                    root:
                      from: t
                      key: [ id ]
                      embed:
                        - from: m
                          on: { txn_id: id }
                          as: array
                          path: item_movement
                          key: [ movement_key ]
                          arrayKey: [ movement_key ]
                          trackKeyChanges: true
                          rollup:
                            qty:           { sum: $amount, default: 0 }
                            lines:         { count: true }
                            reward_points: { sum: $amount, filter: "=after.prize_code == 'reward-points'", default: 0 }
                            tier_points:   { sum: $amount, filter: "=after.prize_code == 'tier-points'", default: 0 }
                serve:
                  from: ledger_doc
                  sync:
                    - source: shapes-store
                      rename: { prefix: %s_ }
                      execution: { parallelism: 1 }
                """.formatted(id, id);
    }

    /** The ledger's rollups, computed by the source database from the same formulas. */
    private static final String LEDGER_SQL = """
            SELECT t.id,
                   COALESCE(SUM(m.amount), 0) AS qty,
                   COUNT(m.amount) AS `lines`,
                   COALESCE(SUM(CASE WHEN m.prize_code = 'reward-points' THEN m.amount END), 0) AS reward_points,
                   COALESCE(SUM(CASE WHEN m.prize_code = 'tier-points' THEN m.amount END), 0) AS tier_points
            FROM ledger_txn t LEFT JOIN (
                SELECT txn_id, game_pts + base_pts + bonus_pts + adj_cr - adj_dr - redeem_pts - expire_pts AS amount,
                       CASE bucket_group WHEN 2 THEN 'reward-points' WHEN 5 THEN 'tier-points' ELSE 'other' END AS prize_code
                FROM point_rows
                UNION ALL
                SELECT txn_id, promo + bonus_promo - promo_used + adj_cr - adj_dr - expired, 'bonus' FROM promo_rows
            ) m ON m.txn_id = t.id
            GROUP BY t.id""";

    // ---- the player ------------------------------------------------------------------------------------------

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aPlayerProfileKeepsItsPicksBalancesAndLatestSessionThroughEveryChange(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("rollup_player_src_" + suffix);
        sql(mysql,
                "CREATE TABLE player (id INT PRIMARY KEY, home_site INT NOT NULL)",
                "CREATE TABLE session_0 (id INT PRIMARY KEY, player_id INT NOT NULL, ended_at DATETIME(3) NOT NULL)",
                "CREATE TABLE session_1 LIKE session_0", "CREATE TABLE session_2 LIKE session_0",
                "CREATE TABLE session_3 LIKE session_0",
                "CREATE TABLE card (id INT PRIMARY KEY, player_id INT NOT NULL, card_no VARCHAR(24) NOT NULL,"
                        + " status CHAR(1) NOT NULL, activated_at DATETIME(3) NOT NULL)",
                "CREATE TABLE phone (id INT PRIMARY KEY, player_id INT NOT NULL, phone VARCHAR(24) NOT NULL,"
                        + " preferred CHAR(1) NOT NULL, status CHAR(1) NOT NULL, type_id INT NOT NULL)",
                "CREATE TABLE image (id INT PRIMARY KEY, player_id INT NOT NULL, image_type INT NOT NULL,"
                        + " status CHAR(1) NOT NULL, taken_at DATETIME(3) NOT NULL)",
                "CREATE TABLE point_balance (id INT PRIMARY KEY, player_id INT NOT NULL, site_id INT NOT NULL,"
                        + " bucket INT NOT NULL, balance DECIMAL(20,8) NOT NULL)",
                "CREATE TABLE comp (id INT PRIMARY KEY, player_id INT NOT NULL, status CHAR(1) NOT NULL,"
                        + " comp_value DECIMAL(18,2) NOT NULL)",
                "INSERT INTO player VALUES (1, 1), (2, 1)",
                "INSERT INTO session_0 VALUES (1, 1, '2026-03-01 10:00:00.000')",
                "INSERT INTO session_1 VALUES (2, 1, '2026-03-05 22:15:00.500'), (3, 2, '2026-02-01 08:00:00.000')",
                "INSERT INTO session_2 VALUES (4, 1, '2026-03-04 09:00:00.000')",
                "INSERT INTO session_3 VALUES (5, 2, '2026-02-03 01:30:00.000')",
                "INSERT INTO card VALUES (1, 1, 'C-001', 'I', '2026-01-01 00:00:00.000'),"
                        + " (2, 1, 'C-002', 'A', '2026-02-01 00:00:00.000'), (3, 2, 'C-201', 'A', '2026-01-15 00:00:00.000')",
                "INSERT INTO phone VALUES (1, 1, '+10000000001', 'Y', 'A', 4), (2, 1, '+10000000002', 'N', 'A', 1),"
                        + " (3, 2, '+10000000003', 'N', 'A', 4)",
                "INSERT INTO image VALUES (1, 1, 1, 'A', '2026-01-10 00:00:00.000'), (2, 1, 1, 'I', '2026-02-10 00:00:00.000'),"
                        + " (3, 1, 2, 'A', '2026-03-10 00:00:00.000')",
                "INSERT INTO point_balance VALUES (1, 1, 1, 2, 1250.50000000), (2, 1, 1, 2, -0.00000001),"
                        + " (3, 1, 1, 5, 99999999999.12345678), (4, 1, 2, 2, 500.00000000), (5, 2, 1, 2, -12.75000000)",
                "INSERT INTO comp VALUES (1, 1, 'A', 120.00), (2, 1, 'A', 80.55), (3, 1, 'V', 999.99), (4, 2, 'A', 15.10)");
        String targetUri = SharedMongo.replicaSetUrl("rollup_player_tgt_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        String pipeline = "rollup_player_" + suffix;

        try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storage)) {
            ControlPlane control = start(server, storage, mysql, List.of("player", "session_0", "session_1", "session_2",
                    "session_3", "card", "phone", "image", "point_balance", "comp"), targetUri, "pipeline.tap.yml",
                    playerPipeline(pipeline), pipeline);
            String collection = pipeline + "_player";
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after the snapshot");

            // the active card: a newer active card wins, then the current one is deactivated, then a tie on time
            sql(mysql, "INSERT INTO card VALUES (4, 1, 'C-003', 'A', '2026-03-01 00:00:00.000')");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after a newer active card");
            sql(mysql, "UPDATE card SET status = 'I' WHERE id = 4");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after the active card was deactivated");
            sql(mysql, "INSERT INTO card VALUES (5, 1, 'C-004', 'A', '2026-02-01 00:00:00.000')");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after a card activated at the same time");
            // the preferred phone flips, and a photo becomes the latest active one
            sql(mysql, "UPDATE phone SET preferred = 'N' WHERE id = 1", "UPDATE phone SET preferred = 'Y' WHERE id = 2");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after the preferred phone changed");
            sql(mysql, "UPDATE image SET status = 'A' WHERE id = 2");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after a later photo became active");
            // the latest session: deleted, an end time moved, a session moved to the other player
            sql(mysql, "DELETE FROM session_1 WHERE id = 2");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after the latest session was deleted");
            sql(mysql, "UPDATE session_3 SET ended_at = '2026-04-01 00:00:00.000' WHERE id = 5");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after a session's end time moved");
            sql(mysql, "UPDATE session_3 SET player_id = 1 WHERE id = 5");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after a session moved to another player");
            // balances at the edges of their type, and comps going away entirely
            sql(mysql, "UPDATE point_balance SET balance = -99999999999.99999999 WHERE id = 3",
                    "INSERT INTO point_balance VALUES (6, 1, 1, 2, 0.00000001)");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after balances moved to the edges of their type");
            sql(mysql, "DELETE FROM comp WHERE player_id = 1");
            converge(mongo, target, collection, mysql, PLAYER_SQL, "after every comp of a player went");
            control.lifecycle(pipeline, LifecycleVerb.PAUSE);
        }
    }

    private static String playerPipeline(String id) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: shapes-mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: sessions
                    type: union
                    from: [ session_0, session_1, session_2, session_3 ]
                  - id: player_doc
                    type: nest
                    from: { p: player, s: sessions, c: card, ph: phone, i: image, b: point_balance, cp: comp }
                    root:
                      from: p
                      key: [ id ]
                      embed:
                        - from: s
                          on: { player_id: id }
                          as: rollup
                          key: [ id ]
                          arrayKey: [ id ]
                          trackKeyChanges: true
                          rollup:
                            last_played: { max: $ended_at }
                        - from: c
                          on: { player_id: id }
                          as: array
                          path: cards
                          arrayKey: [ id ]
                          rollup:
                            active_card: { last: { value: $card_no, by: [ $activated_at, $id ] }, filter: "=after.status == 'A'" }
                        - from: ph
                          on: { player_id: id }
                          as: array
                          path: phones
                          arrayKey: [ id ]
                          rollup:
                            preferred_phone: { first: { value: $phone, by: [ $type_id, $id ] }, filter: "=after.preferred == 'Y' && after.status == 'A'" }
                        - from: i
                          on: { player_id: id }
                          as: rollup
                          arrayKey: [ id ]
                          rollup:
                            latest_photo: { last: { value: $id, by: $taken_at }, filter: "=after.image_type == 1 && after.status == 'A'" }
                        - from: b
                          on: { player_id: id }
                          as: rollup
                          arrayKey: [ id ]
                          rollup:
                            reward_balance: { sum: $balance, filter: "=after.bucket == 2 && after.site_id == 1", default: 0 }
                            tier_balance:   { sum: $balance, filter: "=after.bucket == 5 && after.site_id == 1", default: 0 }
                        - from: cp
                          on: { player_id: id }
                          as: rollup
                          arrayKey: [ id ]
                          rollup:
                            active_comps:     { count: true, filter: "=after.status == 'A'" }
                            active_comp_total: { sum: $comp_value, filter: "=after.status == 'A'", default: 0 }
                serve:
                  from: player_doc
                  sync:
                    - source: shapes-store
                      rename: { prefix: %s_ }
                      execution: { parallelism: 1 }
                """.formatted(id, id);
    }

    /** The player's rollups, computed by the source database: each pick, balance and count written as SQL. */
    private static final String PLAYER_SQL = """
            SELECT p.id,
                   (SELECT MAX(ended_at) FROM (SELECT * FROM session_0 UNION ALL SELECT * FROM session_1
                        UNION ALL SELECT * FROM session_2 UNION ALL SELECT * FROM session_3) s
                     WHERE s.player_id = p.id) AS last_played,
                   (SELECT card_no FROM card WHERE player_id = p.id AND status = 'A'
                     ORDER BY activated_at DESC, id DESC LIMIT 1) AS active_card,
                   (SELECT phone FROM phone WHERE player_id = p.id AND preferred = 'Y' AND status = 'A'
                     ORDER BY type_id, id LIMIT 1) AS preferred_phone,
                   (SELECT id FROM image WHERE player_id = p.id AND image_type = 1 AND status = 'A'
                     ORDER BY taken_at DESC, id DESC LIMIT 1) AS latest_photo,
                   (SELECT COALESCE(SUM(balance), 0) FROM point_balance WHERE player_id = p.id AND bucket = 2
                     AND site_id = 1) AS reward_balance,
                   (SELECT COALESCE(SUM(balance), 0) FROM point_balance WHERE player_id = p.id AND bucket = 5
                     AND site_id = 1) AS tier_balance,
                   (SELECT COUNT(*) FROM comp WHERE player_id = p.id AND status = 'A') AS active_comps,
                   (SELECT COALESCE(SUM(comp_value), 0) FROM comp WHERE player_id = p.id AND status = 'A')
                     AS active_comp_total
            FROM player p""";

    // ---- fixtures --------------------------------------------------------------------------------------------

    private static ControlPlane start(ServerHandle server, PreparedNativeStore storage, Map<String, Object> mysql,
            List<String> tables, String targetUri, String pipelineFile, String pipelineYaml, String pipeline) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        storage.loginAndAwaitBusiness(control);
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        Map<String, String> sources = new LinkedHashMap<>();  // one batch: a pipeline sees only what it is applied with
        sources.put("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: shapes-mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(mysql.get("host"), mysql.get("port"), mysql.get("database"), mysql.get("username"),
                mysql.get("password"), String.join(", ", tables)));
        sources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: shapes-store
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        control.apply(sources);
        // Discovered before the pipeline is applied: its row expressions are checked against the discovered
        // columns at apply, and its rollups typed from them when the run is prepared.
        control.discoverSchema("shapes-mysql", "mysql", mysql);
        sources.put(pipelineFile, pipelineYaml);
        control.apply(sources);
        control.lifecycle(pipeline, LifecycleVerb.START);
        return control;
    }

    /** Waits until every document's rollup fields equal what {@code recompute} answers on the source. */
    private static void converge(MongoEndpoints mongo, EndpointAddress target, String collection,
            Map<String, Object> mysql, String recompute, String when) {
        AtomicReference<String> last = new AtomicReference<>("not observed");
        Await.until("every document equal to the source's own answer " + when, () -> {
            Map<Integer, Map<String, Object>> expected = query(mysql, recompute);
            for (var entry : expected.entrySet()) {
                Map<String, Object> actual = mongo.fetch(target, collection, Map.of("id", entry.getKey())).orElse(null);
                if (actual == null) {
                    last.set("document " + entry.getKey() + " not written");
                    return false;
                }
                Map<String, Object> got = new TreeMap<>();
                for (String field : entry.getValue().keySet()) {
                    got.put(field, normalized(actual.get(field)));
                }
                if (!got.equals(entry.getValue())) {
                    last.set("document " + entry.getKey() + ": " + got + " != " + entry.getValue());
                    return false;
                }
            }
            return true;
        }, last::get);
    }

    /** Rows keyed by {@code id}, every other column normalized so a number or an instant compares by value. */
    private static Map<Integer, Map<String, Object>> query(Map<String, Object> mysql, String select) {
        Map<Integer, Map<String, Object>> out = new TreeMap<>();
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(select)) {
            ResultSetMetaData meta = rows.getMetaData();
            while (rows.next()) {
                Map<String, Object> fields = new TreeMap<>();
                for (int c = 1; c <= meta.getColumnCount(); c++) {
                    String name = meta.getColumnLabel(c);
                    if (!name.equals("id")) {
                        fields.put(name, normalized(rows.getObject(c)));
                    }
                }
                out.put(rows.getInt("id"), fields);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        return out;
    }

    private static Object normalized(Object value) {
        return switch (value) {
            case null -> null;
            case Decimal128 d -> d.bigDecimalValue().stripTrailingZeros();
            case BigDecimal d -> d.stripTrailingZeros();
            case Number n -> new BigDecimal(n.toString()).stripTrailingZeros();
            case Timestamp t -> t.toInstant().toEpochMilli();
            case java.time.LocalDateTime t -> t.toInstant(java.time.ZoneOffset.UTC).toEpochMilli();
            case Date d -> d.getTime();
            default -> value.toString();
        };
    }

    private static void sql(Map<String, Object> mysql, String... statements) throws SQLException {
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement()) {
            for (String one : statements) {
                statement.execute(one);
            }
        }
    }
}
