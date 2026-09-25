package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Frozen source data and target answers for the three real-connector pipeline workloads.
 *
 * <p>The driver may launch different application jars, but it must use these same source statements,
 * resource documents and phase boundaries for every fork. Each fork needs a fresh source database and
 * must call {@link Workload#resetTargets(String, String, String)} after its previous server has stopped:
 * the managed view and operator-state databases are shared by every server on the test Mongo replica set.
 */
final class BenchmarkWorkloadDefinitions {

    static final int SNAPSHOT_ROWS = 12_000;
    static final long SEED = 424_242L;

    private static final String COPY = "copy";
    private static final String STATELESS = "stateless";
    private static final String STATEFUL = "stateful";

    private static final String COPY_PIPELINE = "bench_copy";
    private static final String STATELESS_PIPELINE = "bench_stateless";
    private static final String JOIN_PIPELINE = "bench_stateful_join";
    private static final String NEST_PIPELINE = "bench_stateful_nest";

    private static final String COPY_TABLE = "bench_copy_orders";
    private static final String STATELESS_TABLE = "bench_stateless_orders";
    private static final String JOIN_ORDERS = "bench_join_orders";
    private static final String JOIN_CUSTOMERS = "bench_join_customers";
    private static final String NEST_ORDERS = "bench_nest_orders";
    private static final String NEST_ITEMS = "bench_nest_items";
    private static final String JOIN_VIEW = "bench_join_output";

    private static final List<Workload> ALL = List.of(copy(), stateless(), stateful());

    private BenchmarkWorkloadDefinitions() {
    }

    static List<Workload> all() {
        return ALL;
    }

    static Workload byId(String id) {
        return ALL.stream().filter(workload -> workload.id().equals(id)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown benchmark workload: " + id));
    }

    enum Database {
        MYSQL,
        POSTGRES
    }

    enum Stage {
        SNAPSHOT,
        WARM_UP,
        HOT_STATE,
        COLD_READ,
        CDC_UPDATE,
        TERMINAL
    }

    enum TargetLocation {
        EXTERNAL_MONGO,
        MANAGED_VIEW
    }

    enum Projection {
        COPY,
        STATELESS,
        JOIN,
        NEST
    }

    /** A distinct source-to-pipeline chain, including its own final physical source row. */
    record SourceChain(
            String id,
            String pipelineId,
            String sourceId,
            String table,
            String terminalLogicalId,
            long terminalRowId) {
    }

    /** Incremental source evidence; the target expectation describes the state after this phase. */
    record Phase(
            String id,
            Stage stage,
            boolean measured,
            long expectedLogicalOutputChanges,
            List<String> sql,
            int statementsPerBatch,
            Duration batchInterval,
            Map<String, Long> expectedLogicalCoverage,
            List<TargetExpectation> targets) {
        Phase {
            sql = List.copyOf(sql);
            if (statementsPerBatch < 1 || sql.size() % statementsPerBatch != 0) {
                throw new IllegalArgumentException("SQL statements must form complete benchmark batches");
            }
            if (batchInterval.isNegative()) {
                throw new IllegalArgumentException("batch interval must not be negative");
            }
            expectedLogicalCoverage = Map.copyOf(expectedLogicalCoverage);
            targets = List.copyOf(targets);
        }

        /**
         * Start each batch no sooner than the fixed interval after the prior batch start. If SQL takes
         * longer, start the next batch when it completes; never catch up by releasing a burst.
         */
        List<List<String>> batches() {
            List<List<String>> batches = new ArrayList<>();
            for (int offset = 0; offset < sql.size(); offset += statementsPerBatch) {
                batches.add(sql.subList(offset, offset + statementsPerBatch));
            }
            return List.copyOf(batches);
        }
    }

    /** The row count and checksum must both match before the next phase can be issued. */
    record TargetExpectation(
            String pipelineId,
            TargetLocation location,
            String table,
            Projection projection,
            int rows,
            String checksum) {
    }

    /** One fixed workload. SQL is executed by the harness, never by the product. */
    record Workload(
            String id,
            long seed,
            Database database,
            List<String> pipelineIds,
            List<SourceChain> sourceChains,
            List<String> setupSql,
            List<Phase> phases) {
        Workload {
            pipelineIds = List.copyOf(pipelineIds);
            sourceChains = List.copyOf(sourceChains);
            setupSql = List.copyOf(setupSql);
            phases = List.copyOf(phases);
        }

        Map<String, String> resources(Map<String, Object> sourceConfig, String externalTargetUri) {
            Map<String, Object> connectorConfig = connectorConfig(sourceConfig);
            return switch (id) {
                case COPY -> copyResources(connectorConfig, externalTargetUri);
                case STATELESS -> statelessResources(connectorConfig, externalTargetUri);
                case STATEFUL -> statefulResources(connectorConfig, externalTargetUri);
                default -> throw new IllegalStateException("unknown benchmark fixture: " + id);
            };
        }

        /** The terminal-position sidecar opens the same connector mode as the product source. */
        Map<String, Object> connectorConfig(Map<String, Object> sourceSettings) {
            Map<String, Object> config = new LinkedHashMap<>(sourceSettings);
            if (database == Database.POSTGRES) {
                config.put("user", config.remove("username"));
                config.put("schema", "public");
                config.put("logPluginName", "pgoutput");
            } else {
                config.put("highPerformance", false);
            }
            return Map.copyOf(config);
        }

        Phase phase(String phaseId) {
            return phases.stream().filter(phase -> phase.id().equals(phaseId)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("unknown phase " + phaseId + " for " + id));
        }

        List<TargetExpectation> targetResets() {
            return phases.getLast().targets();
        }

        /**
         * A fresh source database is not enough: view rows live in a deployment-wide Mongo database.
         * The prior server must already be stopped before this method runs.
         */
        void resetTargets(String externalTargetUri, String managedViewsUri, String operatorStateUri) {
            for (TargetExpectation target : targetResets()) {
                String uri = target.location() == TargetLocation.MANAGED_VIEW
                        ? managedViewsUri : externalTargetUri;
                ConnectionString address = new ConnectionString(uri);
                String database = Objects.requireNonNull(address.getDatabase(), "target URI has no database");
                try (MongoClient client = MongoClients.create(address)) {
                    client.getDatabase(database).getCollection(target.table()).drop();
                }
            }
            if (id.equals(STATEFUL)) {
                ConnectionString address = new ConnectionString(operatorStateUri);
                String database = Objects.requireNonNull(address.getDatabase(), "state URI has no database");
                try (MongoClient client = MongoClients.create(address)) {
                    client.getDatabase(database).drop();
                }
            }
        }
    }

    /** Hashes only the declared logical output fields, with rows and nested children sorted by id. */
    static String checksumOf(TargetExpectation expectation, List<Document> actual) {
        List<String> lines = new ArrayList<>(actual.size());
        for (Document document : actual) {
            lines.add(switch (expectation.projection()) {
                case COPY -> copyLine(number(document, "id"), number(document, "amount"),
                        value(document, "payload"), value(document, "marker"));
                case STATELESS -> statelessLine(number(document, "id"), number(document, "item_index"),
                        value(document, "items"), number(document, "qty"), number(document, "projected_qty"));
                case JOIN -> joinLine(number(document, "order_id"), number(document, "qty"),
                        value(document, "customer_name"));
                case NEST -> nestLine(number(document, "id"), value(document, "label"),
                        value(document, "marker"), items(document.get("items")));
            });
        }
        return checksum(lines);
    }

    private static Workload copy() {
        SourceChain orders = chain(COPY_PIPELINE, "src_bench_copy", COPY_TABLE, 900_001);
        List<String> setup = new ArrayList<>();
        setup.add("CREATE TABLE " + COPY_TABLE
                + " (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, payload VARCHAR(64) NOT NULL,"
                + " marker VARCHAR(64))");
        setup.addAll(inserts(COPY_TABLE, "id,amount,payload", 1, SNAPSHOT_ROWS,
                id -> "(" + id + "," + copyAmount(id) + ",'payload-" + id + "')"));

        List<Phase> phases = List.of(
                phase("snapshot", Stage.SNAPSHOT, false, SNAPSHOT_ROWS,
                        List.of(), coverage(orders, "snapshot", SNAPSHOT_ROWS),
                        copyTarget(0)),
                phase("warm-up", Stage.WARM_UP, false, 128,
                        List.of("UPDATE " + COPY_TABLE + " SET amount = amount + 1 WHERE id BETWEEN 1 AND 128"),
                        coverage(orders, "warm-up", 128), copyTarget(1)),
                pacedPhase("cdc-update", Stage.CDC_UPDATE, SNAPSHOT_ROWS,
                        updates(COPY_TABLE, "amount = amount + 1", 1, SNAPSHOT_ROWS, 100, ""),
                        1, coverage(orders, "cdc-update", SNAPSHOT_ROWS), copyTarget(2)),
                phase("terminal", Stage.TERMINAL, false, 1,
                        List.of("INSERT INTO " + COPY_TABLE
                                + " (id,amount,payload,marker) VALUES (900001,7,'terminal','copy-orders-terminal')"),
                        Map.of(orders.terminalLogicalId(), 1L), copyTarget(3)));
        return new Workload(COPY, SEED, Database.MYSQL, List.of(COPY_PIPELINE),
                List.of(orders), setup, phases);
    }

    private static Workload stateless() {
        SourceChain orders = chain(STATELESS_PIPELINE, "src_bench_stateless", STATELESS_TABLE, 900_002);
        List<String> setup = new ArrayList<>();
        setup.add("CREATE TABLE " + STATELESS_TABLE
                + " (id BIGINT PRIMARY KEY, qty BIGINT NOT NULL, region TEXT NOT NULL, items TEXT[] NOT NULL)");
        setup.add("ALTER TABLE " + STATELESS_TABLE + " REPLICA IDENTITY FULL");
        setup.addAll(inserts(STATELESS_TABLE, "id,qty,region,items", 1, SNAPSHOT_ROWS,
                id -> "(" + id + "," + statelessQty(id) + ",'"
                        + (id % 2 == 0 ? "keep" : "skip") + "',ARRAY['item-" + id + "-a','item-" + id
                        + "-b'])"));

        List<Phase> phases = List.of(
                phase("snapshot", Stage.SNAPSHOT, false, SNAPSHOT_ROWS,
                        List.of(), coverage(orders, "snapshot", SNAPSHOT_ROWS),
                        statelessTarget(0)),
                phase("warm-up", Stage.WARM_UP, false, 256,
                        List.of("UPDATE " + STATELESS_TABLE
                                + " SET qty = qty + 1 WHERE id BETWEEN 1 AND 256 AND id % 2 = 0"),
                        coverage(orders, "warm-up", 128), statelessTarget(1)),
                pacedPhase("cdc-update", Stage.CDC_UPDATE, SNAPSHOT_ROWS,
                        updates(STATELESS_TABLE, "qty = qty + 1", 1, SNAPSHOT_ROWS, 100,
                                " AND id % 2 = 0"),
                        1, coverage(orders, "cdc-update", SNAPSHOT_ROWS / 2), statelessTarget(2)),
                phase("terminal", Stage.TERMINAL, false, 2,
                        List.of("INSERT INTO " + STATELESS_TABLE
                                + " (id,qty,region,items) VALUES"
                                + " (900002,7,'keep',ARRAY['terminal-left','terminal-right'])"),
                        Map.of(orders.terminalLogicalId(), 1L), statelessTarget(3)));
        return new Workload(STATELESS, SEED, Database.POSTGRES, List.of(STATELESS_PIPELINE),
                List.of(orders), setup, phases);
    }

    private static Workload stateful() {
        SourceChain joinOrders = chain(JOIN_PIPELINE, "src_bench_join_orders", JOIN_ORDERS, 900_011);
        SourceChain joinCustomers = chain(JOIN_PIPELINE, "src_bench_join_customers", JOIN_CUSTOMERS, 900_012);
        SourceChain nestOrders = chain(NEST_PIPELINE, "src_bench_nest_orders", NEST_ORDERS, 900_013);
        SourceChain nestItems = chain(NEST_PIPELINE, "src_bench_nest_items", NEST_ITEMS, 900_014);
        List<String> setup = new ArrayList<>();
        setup.add("CREATE TABLE " + JOIN_CUSTOMERS
                + " (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, marker VARCHAR(64))");
        setup.add("CREATE TABLE " + JOIN_ORDERS
                + " (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, qty BIGINT NOT NULL,"
                + " marker VARCHAR(64))");
        setup.add("CREATE TABLE " + NEST_ORDERS
                + " (id BIGINT PRIMARY KEY, label VARCHAR(64) NOT NULL, marker VARCHAR(64))");
        setup.add("CREATE TABLE " + NEST_ITEMS
                + " (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL, sku VARCHAR(64) NOT NULL,"
                + " marker VARCHAR(64))");
        setup.addAll(inserts(JOIN_CUSTOMERS, "id,name", 1, 32,
                id -> "(" + id + ",'customer-" + id + "')"));
        setup.addAll(inserts(JOIN_ORDERS, "id,customer_id,qty", 1, SNAPSHOT_ROWS,
                id -> "(" + id + "," + customerId(id) + "," + joinQty(id) + ")"));
        setup.addAll(inserts(NEST_ORDERS, "id,label", 1, SNAPSHOT_ROWS,
                id -> "(" + id + ",'root-" + id + "')"));
        setup.addAll(inserts(NEST_ITEMS, "id,order_id,sku", 1, SNAPSHOT_ROWS,
                id -> "(" + id + "," + id + ",'sku-" + id + "')"));

        List<String> warm = new ArrayList<>();
        warm.add("UPDATE " + JOIN_ORDERS + " SET qty = qty + 1 WHERE id BETWEEN 1 AND 256");
        warm.add("UPDATE " + JOIN_CUSTOMERS + " SET name = 'customer-1-warm' WHERE id = 1");
        warm.add("UPDATE " + NEST_ORDERS + " SET label = 'root-1-warm' WHERE id = 1");
        warm.addAll(inserts(NEST_ITEMS, "id,order_id,sku", 1, 256,
                id -> "(" + (100_000 + id) + "," + id + ",'warm-" + id + "')"));
        List<String> hot = inserts(NEST_ITEMS, "id,order_id,sku", 1, 64,
                id -> "(" + (200_000 + id) + ",1,'hot-" + id + "')");
        List<String> cold = inserts(NEST_ITEMS, "id,order_id,sku", 1, SNAPSHOT_ROWS, 100,
                id -> "(" + (300_000 + id) + "," + id + ",'cold-" + id + "')");
        List<String> update = new ArrayList<>();
        List<String> joinUpdates = updates(JOIN_ORDERS, "qty = qty + 1", 1, SNAPSHOT_ROWS, 100, "");
        List<String> nestUpdates = updates(NEST_ITEMS, "sku = CONCAT(sku, 'u')",
                1, SNAPSHOT_ROWS, 100, "");
        for (int batch = 0; batch < joinUpdates.size(); batch++) {
            update.add(joinUpdates.get(batch));
            update.add(nestUpdates.get(batch));
        }
        List<String> terminal = List.of(
                "INSERT INTO " + JOIN_CUSTOMERS
                        + " (id,name,marker) VALUES (900012,'terminal-customer','join-customers-terminal')",
                "INSERT INTO " + JOIN_ORDERS
                        + " (id,customer_id,qty,marker) VALUES"
                        + " (900011,900012,7,'join-orders-terminal')",
                "INSERT INTO " + NEST_ORDERS
                        + " (id,label,marker) VALUES (900013,'terminal-root','nest-orders-terminal')",
                "INSERT INTO " + NEST_ITEMS
                        + " (id,order_id,sku,marker) VALUES"
                        + " (900014,900013,'terminal-item','nest-items-terminal')");

        List<Phase> phases = List.of(
                phase("snapshot", Stage.SNAPSHOT, false, 2L * SNAPSHOT_ROWS, List.of(),
                        coverage(Map.of(joinOrders, (long) SNAPSHOT_ROWS, joinCustomers, 32L,
                                nestOrders, (long) SNAPSHOT_ROWS, nestItems, (long) SNAPSHOT_ROWS), "snapshot"),
                        statefulTargets(0)),
                phase("warm-up", Stage.WARM_UP, false, 888, warm,
                        coverage(Map.of(joinOrders, 256L, joinCustomers, 1L,
                                nestOrders, 1L, nestItems, 256L), "warm-up"),
                        statefulTargets(1)),
                phase("hot-state", Stage.HOT_STATE, false, 64, hot,
                        coverage(nestItems, "hot-state", 64), statefulTargets(2)),
                pacedPhase("cold-read", Stage.COLD_READ, SNAPSHOT_ROWS, cold, 1,
                        coverage(nestItems, "cold-read", SNAPSHOT_ROWS), statefulTargets(3)),
                pacedPhase("cdc-update", Stage.CDC_UPDATE, 2L * SNAPSHOT_ROWS, update, 2,
                        coverage(Map.of(joinOrders, (long) SNAPSHOT_ROWS,
                                nestItems, (long) SNAPSHOT_ROWS), "cdc-update"), statefulTargets(4)),
                phase("terminal", Stage.TERMINAL, false, 2, terminal,
                        Map.of(joinOrders.terminalLogicalId(), 1L, joinCustomers.terminalLogicalId(), 1L,
                                nestOrders.terminalLogicalId(), 1L, nestItems.terminalLogicalId(), 1L),
                        statefulTargets(5)));
        return new Workload(STATEFUL, SEED, Database.MYSQL, List.of(JOIN_PIPELINE, NEST_PIPELINE),
                List.of(joinOrders, joinCustomers, nestOrders, nestItems), setup, phases);
    }

    private static Map<String, String> copyResources(Map<String, Object> source, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_bench_copy.tap.yml", mysqlSource("src_bench_copy", COPY_TABLE, source));
        resources.put("tgt_bench_mongo.tap.yml", mongoTarget(targetUri));
        resources.put("bench_copy.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: bench_copy
                source: src_bench_copy
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: bench_copy_orders
                  sync: [ { source: tgt_bench_mongo } ]
                """);
        return Map.copyOf(resources);
    }

    private static Map<String, String> statelessResources(Map<String, Object> source, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_bench_stateless.tap.yml", postgresSource(source));
        resources.put("tgt_bench_mongo.tap.yml", mongoTarget(targetUri));
        resources.put("bench_stateless.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: bench_stateless
                source: src_bench_stateless
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: kept, from: [bench_stateless_orders], type: filter,
                      expr: "int(after.id) % 2 == 0" }
                  - id: projected
                    from: kept
                    type: map
                    fields: { region: false, projected_qty: "=int(after.qty) * 2" }
                  - { id: expanded, from: projected, type: unwind, path: items,
                      include_array_index: item_index }
                serve:
                  from: expanded
                  sync: [ { source: tgt_bench_mongo } ]
                """);
        return Map.copyOf(resources);
    }

    private static Map<String, String> statefulResources(Map<String, Object> source, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_bench_join_orders.tap.yml",
                mysqlSource("src_bench_join_orders", JOIN_ORDERS, source));
        resources.put("src_bench_join_customers.tap.yml",
                mysqlSource("src_bench_join_customers", JOIN_CUSTOMERS, source));
        resources.put("src_bench_nest_orders.tap.yml",
                mysqlSource("src_bench_nest_orders", NEST_ORDERS, source));
        resources.put("src_bench_nest_items.tap.yml",
                mysqlSource("src_bench_nest_items", NEST_ITEMS, source));
        resources.put("tgt_bench_mongo.tap.yml", mongoTarget(targetUri));
        resources.put("bench_stateful_join.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: bench_stateful_join
                source: [ src_bench_join_orders, src_bench_join_customers ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: joined
                    type: join
                    from: { o: bench_join_orders, c: bench_join_customers }
                    engine: builtin
                    sql: |
                      SELECT o.id AS order_id, o.qty AS qty, c.name AS customer_name
                      FROM o JOIN c ON o.customer_id = c.id
                view:
                  id: bench_join_output
                  from: joined
                  primary_key: order_id
                  storage: { warm: { collection: bench_join_output } }
                """);
        resources.put("bench_stateful_nest.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: bench_stateful_nest
                source: [ src_bench_nest_orders, src_bench_nest_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: nested
                    type: nest
                    entries_in_memory: 271
                    from: { o: bench_nest_orders, i: bench_nest_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items,
                            arrayKey: [ id ] }
                serve:
                  from: nested
                  sync: [ { source: tgt_bench_mongo } ]
                """);
        return Map.copyOf(resources);
    }

    private static String mysqlSource(String sourceId, String table, Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s,
                          highPerformance: false }
                mode: cdc
                tables: [ %s ]
                """.formatted(sourceId, config.get("host"), config.get("port"), config.get("database"),
                config.get("username"), config.get("password"), table);
    }

    private static String postgresSource(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: src_bench_stateless
                connector: postgres
                config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s,
                          logPluginName: pgoutput }
                mode: cdc
                tables: [ bench_stateless_orders ]
                """.formatted(config.get("host"), config.get("port"), config.get("database"),
                config.get("user"), config.get("password"));
    }

    private static String mongoTarget(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_bench_mongo
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(uri);
    }

    private static SourceChain chain(String pipeline, String source, String table, long terminalId) {
        return new SourceChain(pipeline + "/" + source, pipeline, source, table,
                pipeline + "/" + source + "/terminal", terminalId);
    }

    private static Phase phase(String id, Stage stage, boolean measured, long logicalOutputChanges,
                               List<String> sql,
                               Map<String, Long> coverage, TargetExpectation target) {
        return new Phase(id, stage, measured, logicalOutputChanges, sql, 1, Duration.ZERO,
                coverage, List.of(target));
    }

    private static Phase phase(String id, Stage stage, boolean measured, long logicalOutputChanges,
                               List<String> sql,
                               Map<String, Long> coverage, List<TargetExpectation> targets) {
        return new Phase(id, stage, measured, logicalOutputChanges, sql, 1, Duration.ZERO,
                coverage, targets);
    }

    private static Phase pacedPhase(String id, Stage stage, long logicalOutputChanges,
                                   List<String> sql, int statementsPerBatch,
                                   Map<String, Long> coverage, TargetExpectation target) {
        return new Phase(id, stage, true, logicalOutputChanges, sql, statementsPerBatch,
                Duration.ofMillis(250), coverage, List.of(target));
    }

    private static Phase pacedPhase(String id, Stage stage, long logicalOutputChanges,
                                   List<String> sql, int statementsPerBatch,
                                   Map<String, Long> coverage, List<TargetExpectation> targets) {
        return new Phase(id, stage, true, logicalOutputChanges, sql, statementsPerBatch,
                Duration.ofMillis(250), coverage, targets);
    }

    private static Map<String, Long> coverage(SourceChain chain, String phase, long events) {
        return Map.of(chain.id() + "/" + phase, events);
    }

    private static Map<String, Long> coverage(Map<SourceChain, Long> chains, String phase) {
        Map<String, Long> result = new LinkedHashMap<>();
        chains.forEach((chain, events) -> result.put(chain.id() + "/" + phase, events));
        return Map.copyOf(result);
    }

    private static TargetExpectation copyTarget(int phase) {
        List<String> lines = new ArrayList<>(SNAPSHOT_ROWS + 1);
        for (int id = 1; id <= SNAPSHOT_ROWS; id++) {
            long amount = copyAmount(id) + (phase >= 1 && id <= 128 ? 1 : 0) + (phase >= 2 ? 1 : 0);
            lines.add(copyLine(id, amount, "payload-" + id, ""));
        }
        if (phase >= 3) {
            lines.add(copyLine(900_001, 7, "terminal", "copy-orders-terminal"));
        }
        return new TargetExpectation(COPY_PIPELINE, TargetLocation.EXTERNAL_MONGO, COPY_TABLE,
                Projection.COPY, lines.size(), checksum(lines));
    }

    private static TargetExpectation statelessTarget(int phase) {
        List<String> lines = new ArrayList<>(SNAPSHOT_ROWS + 2);
        for (int id = 2; id <= SNAPSHOT_ROWS; id += 2) {
            long qty = statelessQty(id) + (phase >= 1 && id <= 256 ? 1 : 0) + (phase >= 2 ? 1 : 0);
            lines.add(statelessLine(id, 0, "item-" + id + "-a", qty, qty * 2));
            lines.add(statelessLine(id, 1, "item-" + id + "-b", qty, qty * 2));
        }
        if (phase >= 3) {
            lines.add(statelessLine(900_002, 0, "terminal-left", 7, 14));
            lines.add(statelessLine(900_002, 1, "terminal-right", 7, 14));
        }
        return new TargetExpectation(STATELESS_PIPELINE, TargetLocation.EXTERNAL_MONGO, STATELESS_TABLE,
                Projection.STATELESS, lines.size(), checksum(lines));
    }

    private static List<TargetExpectation> statefulTargets(int phase) {
        List<String> join = new ArrayList<>(SNAPSHOT_ROWS + 1);
        List<String> nest = new ArrayList<>(SNAPSHOT_ROWS + 1);
        for (int id = 1; id <= SNAPSHOT_ROWS; id++) {
            long qty = joinQty(id) + (phase >= 1 && id <= 256 ? 1 : 0) + (phase >= 4 ? 1 : 0);
            String customerName = phase >= 1 && customerId(id) == 1
                    ? "customer-1-warm" : "customer-" + customerId(id);
            join.add(joinLine(id, qty, customerName));
            List<String> items = new ArrayList<>();
            items.add(id + ":sku-" + id + (phase >= 4 ? "u" : ""));
            if (phase >= 1 && id <= 256) {
                items.add((100_000 + id) + ":warm-" + id);
            }
            if (phase >= 2 && id == 1) {
                for (int hot = 1; hot <= 64; hot++) {
                    items.add((200_000 + hot) + ":hot-" + hot);
                }
            }
            if (phase >= 3) {
                items.add((300_000 + id) + ":cold-" + id);
            }
            String label = phase >= 1 && id == 1 ? "root-1-warm" : "root-" + id;
            nest.add(nestLine(id, label, "", items));
        }
        if (phase >= 5) {
            join.add(joinLine(900_011, 7, "terminal-customer"));
            nest.add(nestLine(900_013, "terminal-root", "nest-orders-terminal",
                    List.of("900014:terminal-item")));
        }
        return List.of(
                new TargetExpectation(JOIN_PIPELINE, TargetLocation.MANAGED_VIEW, JOIN_VIEW,
                        Projection.JOIN, join.size(), checksum(join)),
                new TargetExpectation(NEST_PIPELINE, TargetLocation.EXTERNAL_MONGO, NEST_ORDERS,
                        Projection.NEST, nest.size(), checksum(nest)));
    }

    private static long copyAmount(long id) {
        return (id * 17 + SEED) % 1_000;
    }

    private static long statelessQty(long id) {
        return (id * 13 + SEED) % 500;
    }

    private static long joinQty(long id) {
        return (id * 7 + SEED) % 1_000;
    }

    private static long customerId(long orderId) {
        return (orderId % 32) + 1;
    }

    private static List<String> inserts(String table, String columns, int first, int last,
                                        java.util.function.IntFunction<String> row) {
        return inserts(table, columns, first, last, 500, row);
    }

    private static List<String> inserts(String table, String columns, int first, int last, int batchSize,
                                        java.util.function.IntFunction<String> row) {
        List<String> result = new ArrayList<>();
        for (int start = first; start <= last; start += batchSize) {
            int end = Math.min(last, start + batchSize - 1);
            List<String> values = new ArrayList<>(end - start + 1);
            for (int id = start; id <= end; id++) {
                values.add(row.apply(id));
            }
            result.add("INSERT INTO " + table + " (" + columns + ") VALUES " + String.join(",", values));
        }
        return result;
    }

    private static List<String> updates(String table, String assignment, int first, int last,
                                        int batchSize, String additionalPredicate) {
        List<String> result = new ArrayList<>();
        for (int start = first; start <= last; start += batchSize) {
            int end = Math.min(last, start + batchSize - 1);
            result.add("UPDATE " + table + " SET " + assignment + " WHERE id BETWEEN " + start
                    + " AND " + end + additionalPredicate);
        }
        return result;
    }

    private static String copyLine(long id, long amount, String payload, String marker) {
        return id + "|" + amount + "|" + payload + "|" + marker;
    }

    private static String statelessLine(long id, long itemIndex, String item, long qty, long projectedQty) {
        return id + "|" + itemIndex + "|" + item + "|" + qty + "|" + projectedQty;
    }

    private static String joinLine(long id, long qty, String customerName) {
        return id + "|" + qty + "|" + customerName;
    }

    private static String nestLine(long id, String label, String marker, List<String> items) {
        List<String> sorted = new ArrayList<>(items);
        sorted.sort(Comparator.comparingLong(value -> Long.parseLong(value.substring(0, value.indexOf(':')))));
        return id + "|" + label + "|" + marker + "|" + String.join(",", sorted);
    }

    private static List<String> items(Object raw) {
        if (!(raw instanceof List<?> values)) {
            throw new AssertionError("nested target has no items array: " + raw);
        }
        List<String> result = new ArrayList<>(values.size());
        for (Object value : values) {
            if (!(value instanceof Map<?, ?> item)) {
                throw new AssertionError("nested item is not a document: " + value);
            }
            result.add(number(item, "id") + ":" + value(item, "sku"));
        }
        return result;
    }

    private static long number(Map<?, ?> document, String field) {
        Object value = document.get(field);
        if (!(value instanceof Number number)) {
            throw new AssertionError("target field " + field + " is not numeric: " + document);
        }
        return number.longValue();
    }

    private static String value(Map<?, ?> document, String field) {
        Object value = document.get(field);
        return value == null ? "" : String.valueOf(value);
    }

    private static String checksum(List<String> lines) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            lines.stream().sorted().forEach(line -> {
                digest.update(line.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) '\n');
            });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JDK", e);
        }
    }
}
