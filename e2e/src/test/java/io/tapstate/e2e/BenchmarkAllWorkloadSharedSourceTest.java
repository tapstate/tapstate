package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure service-boundary controls. No driver/server/container is started. */
class BenchmarkAllWorkloadSharedSourceTest {
    // These immutable original inputs are constructed once for this control invocation, outside
    // every activated component scope. They do not prove a measured fork or method factory cost.
    private static final Map<String, BenchmarkWorkloadDefinitions.Workload> WORKLOADS = Map.of(
            "copy", BenchmarkWorkloadDefinitions.steadyPilot("copy"),
            "stateless", BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
            "stateful", BenchmarkWorkloadDefinitions.steadyPilot("stateful"));

    @Test void all_original_shapes_are_accepted_without_an_extra_factory_or_provider_call() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "three-original-shapes")) {
            scope.bindRoot(17, 23);
            for (String id : List.of("copy", "stateless", "stateful")) {
                var workload = WORKLOADS.get(id); var g = BenchmarkSharedSupportSourceWork.geometry(id);
                BenchmarkSharedSupportSourceWork.originalWorkload(workload);
                assertThat(workload.setupSql()).hasSize(g.setupStatements());
                assertThat(workload.pipelineIds()).hasSize(g.pipelines());
                assertThat(workload.sourceChains()).hasSize(g.chains());
            }
            assertThat(WORKLOADS.get("stateful").phase("cold-read").batches()).hasSize(960);
            assertThat(WORKLOADS.get("stateful").phase("cdc-update").batches()).hasSize(960)
                    .allSatisfy(batch -> assertThat(batch).hasSize(2));
            assertThat(WORKLOADS.get("stateful").phases().stream().filter(BenchmarkWorkloadDefinitions.Phase::measured)
                    .mapToLong(BenchmarkWorkloadDefinitions.Phase::expectedLogicalOutputChanges).sum()).isEqualTo(288000);
            assertThat(BenchmarkSharedSupportSourceWork.geometry("stateful").cohortRows()).isEqualTo(144000);
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void stateful_setup_executes_all_581_statements_in_original_order_and_closes_once() throws Exception {
        var seen = new ArrayList<String>();
        Statement statement = proxy(Statement.class, (name, args) -> {
            if (name.equals("execute")) { seen.add((String) args[0]); return false; }
            if (name.equals("close")) { seen.add("close"); return null; }
            throw new AssertionError("unexpected statement method");
        });
        var creates = new AtomicInteger();
        Connection connection = proxy(Connection.class, (name, args) -> { assertThat(name).isEqualTo("createStatement"); creates.incrementAndGet(); return statement; });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "stateful-setup")) {
            BenchmarkForkEnvironment.execute(connection, WORKLOADS.get("stateful").setupSql());
            assertThat(creates).hasValue(1);
            assertThat(seen.subList(0, 581)).containsExactlyElementsOf(WORKLOADS.get("stateful").setupSql());
            assertThat(seen).hasSize(582).last().isEqualTo("close");
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 582L)
                    .containsEntry("completedRequests", 582L).containsEntry("failedRequests", 0L);
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void sql_prior_roster_and_scalar_refuse_before_traversal_or_statement_creation() {
        var creates = new AtomicInteger();
        Connection connection = proxy(Connection.class, (name, args) -> { creates.incrementAndGet(); throw new AssertionError("provider must not run"); });
        List<String> broad = new AbstractList<>() {
            @Override public int size() { return 582; }
            @Override public String get(int index) { throw new AssertionError("broad roster traversed"); }
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "sql-prior-roster")) {
            assertThatThrownBy(() -> BenchmarkForkEnvironment.execute(connection, broad))
                    .isInstanceOf(AssertionError.class).hasMessage("shared support source shape is unavailable");
            assertThat(creates).hasValue(0);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "sql-prior-scalar")) {
            assertThatThrownBy(() -> BenchmarkForkEnvironment.execute(connection, List.of("x".repeat(65537))))
                    .isInstanceOf(AssertionError.class).hasMessage("shared support source shape is unavailable");
            assertThat(creates).hasValue(0);
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 0L);
        }
    }

    @Test void phase_prior_shape_refuses_incomplete_or_more_than_960_batches_before_layout() {
        var original = WORKLOADS.get("stateful").phase("cdc-update");
        var altered = new BenchmarkWorkloadDefinitions.Phase(original.id(), original.stage(), true,
                original.expectedLogicalOutputChanges(), java.util.Collections.nCopies(1922, "SELECT 1"),
                2, original.batchInterval(), original.expectedLogicalCoverage(), original.targets());
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "phase-prior-layout")) {
            assertThatThrownBy(() -> BenchmarkSharedSupportSourceWork.phaseSql(altered)).isInstanceOf(AssertionError.class);
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void mysql_connect_reads_five_original_values_and_issues_only_one_connection() throws Exception {
        initializeMySql();
        var gets = new AtomicInteger(); var opens = new AtomicInteger();
        var values = new java.util.LinkedHashMap<String, Object>(settings()) {
            @Override public Object get(Object key) { gets.incrementAndGet(); return super.get(key); }
        };
        Connection connection = proxy(Connection.class, (name, args) -> null);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "mysql-connect")) {
            assertThat(SharedMySql.connect(values, (uri, user, password) -> {
                opens.incrementAndGet();
                assertThat(uri).isEqualTo("jdbc:mysql://127.0.0.1:3306/fixture?sslMode=DISABLED&allowPublicKeyRetrieval=true");
                assertThat(user).isEqualTo("fixture"); assertThat(password).isEqualTo("fixture"); return connection;
            })).isSameAs(connection);
            assertThat(gets).hasValue(5); assertThat(opens).hasValue(1);
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 1L)
                    .containsEntry("completedRequests", 1L).containsEntry("failedRequests", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN");
        }
    }

    @Test void mysql_unknown_scalar_refuses_before_conversion_and_keeps_checked_connection_primary() throws Exception {
        initializeMySql(); var strings = new AtomicInteger(); var opens = new AtomicInteger();
        var values = new java.util.LinkedHashMap<String, Object>(settings());
        values.put("host", new Object() { @Override public String toString() { strings.incrementAndGet(); return "opaque"; } });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "mysql-unknown-scalar")) {
            assertThatThrownBy(() -> SharedMySql.connect(values, (uri, user, password) -> { opens.incrementAndGet(); return null; }))
                    .isInstanceOf(AssertionError.class);
            assertThat(strings).hasValue(0); assertThat(opens).hasValue(0);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
        SQLException primary = new SQLException("controlled JDBC open");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "mysql-connection-failure")) {
            assertThatThrownBy(() -> SharedMySql.connect(settings(), (uri, user, password) -> { throw primary; })).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("failedRequests", 1L);
        }
    }

    @Test void mysql_provision_keeps_sql_wrapper_and_twr_primary_suppression_order() throws Exception {
        initializeMySql(); var seen = new ArrayList<String>();
        SQLException primary = new SQLException("controlled CREATE"), statementClose = new SQLException("controlled statement close"),
                rootClose = new SQLException("controlled root close");
        Statement statement = proxy(Statement.class, (name, args) -> {
            if (name.equals("execute")) { seen.add((String) args[0]); throw primary; }
            if (name.equals("close")) { seen.add("statement-close"); throw statementClose; }
            throw new AssertionError("unexpected provision statement");
        });
        Connection connection = proxy(Connection.class, (name, args) -> {
            if (name.equals("createStatement")) { seen.add("create"); return statement; }
            if (name.equals("close")) { seen.add("root-close"); throw rootClose; }
            throw new AssertionError("unexpected provision root");
        });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "mysql-provision-primary")) {
            assertThatThrownBy(() -> SharedMySql.provision(server(), "fixture", (uri, user, secret) -> connection))
                    .isInstanceOf(IllegalStateException.class).hasMessage("cannot provision the database fixture").hasCause(primary);
            assertThat(primary.getSuppressed()).containsExactly(statementClose, rootClose);
            assertThat(seen).containsExactly("create", "CREATE DATABASE IF NOT EXISTS `fixture`", "statement-close", "root-close");
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 2L).containsEntry("failedRequests", 2L);
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void unscoped_and_scoped_same_primary_close_reproduce_twr_self_suppression_and_outer_close() throws Exception {
        initializeMySql();
        for (boolean enabled : List.of(false, true)) {
            SQLException primary = new SQLException("controlled reused primary"); var rootCloses = new AtomicInteger();
            Statement statement = proxy(Statement.class, (name, args) -> {
                if (name.equals("execute") || name.equals("close")) throw primary;
                throw new AssertionError("unexpected self-suppression statement");
            });
            Connection connection = proxy(Connection.class, (name, args) -> {
                if (name.equals("createStatement")) return statement;
                if (name.equals("close")) { rootCloses.incrementAndGet(); return null; }
                throw new AssertionError("unexpected self-suppression connection");
            });
            if (!enabled) {
                assertThatThrownBy(() -> SharedMySql.provision(server(), "fixture", (uri, user, secret) -> connection))
                        .isInstanceOf(IllegalArgumentException.class).hasMessage("Self-suppression not permitted").hasCause(primary);
                assertThat(BenchmarkReturnWorkBudget.current()).isNull();
            } else {
                try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "scoped-self-suppression")) {
                    assertThatThrownBy(() -> SharedMySql.provision(server(), "fixture", (uri, user, secret) -> connection))
                            .isInstanceOf(IllegalArgumentException.class).hasMessage("Self-suppression not permitted").hasCause(primary);
                    assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
                }
            }
            assertThat(rootCloses).hasValue(1);
        }
    }

    @Test void mysql_grant_and_provision_keep_all_original_statement_and_getter_order() throws Exception {
        initializeMySql(); var seen = new ArrayList<String>();
        Statement statement = proxy(Statement.class, (name, args) -> {
            if (name.equals("execute")) { seen.add((String) args[0]); return false; }
            if (name.equals("close")) { seen.add("statement-close"); return null; }
            throw new AssertionError("unexpected grant statement");
        });
        Connection connection = proxy(Connection.class, (name, args) -> {
            if (name.equals("createStatement")) { seen.add("create"); return statement; }
            if (name.equals("close")) { seen.add("root-close"); return null; }
            throw new AssertionError("unexpected grant connection");
        });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "mysql-two-owned-bodies")) {
            SharedMySql.grantReplication(server(), (uri, user, secret) -> { seen.add("open:" + user); return connection; });
            SharedMySql.provision(server(), "fixture", (uri, user, secret) -> { seen.add("open:" + user); return connection; });
            assertThat(seen).containsExactly("open:root", "create",
                    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO 'fixture'@'%'", "FLUSH PRIVILEGES",
                    "statement-close", "root-close", "open:root", "create", "CREATE DATABASE IF NOT EXISTS `fixture`",
                    "GRANT ALL PRIVILEGES ON `fixture`.* TO 'fixture'@'%'", "FLUSH PRIVILEGES", "statement-close", "root-close");
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 4L).containsEntry("completedRequests", 4L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void stateful_reset_drops_two_collections_then_owned_state_database_with_three_real_closes() {
        var seen = new ArrayList<String>();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "stateful-reset")) {
            BenchmarkForkEnvironment.resetTargetsUsing(WORKLOADS.get("stateful"),
                    "mongodb://127.0.0.1:27017/target", "mongodb://127.0.0.1:27017/views", "mongodb://127.0.0.1:27017/state",
                    address -> mongo(address.getDatabase(), seen, null, null));
            assertThat(seen).containsExactly("create:views", "database:views", "collection:bench_join_output", "collection-drop", "close:views",
                    "create:target", "database:target", "collection:bench_nest_orders", "collection-drop", "close:target",
                    "create:state", "database:state", "database-drop", "close:state");
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 17L).containsEntry("completedRequests", 17L);
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 3L).containsEntry("completedRequests", 3L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void reset_failure_keeps_actual_drop_primary_and_close_secondary_and_stops_remaining_resets() {
        var seen = new ArrayList<String>();
        IllegalStateException primary = new IllegalStateException("controlled drop"),
                secondary = new IllegalStateException("controlled client close");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "stateful-reset-primary")) {
            assertThatThrownBy(() -> BenchmarkForkEnvironment.resetTargetsUsing(WORKLOADS.get("stateful"),
                    "mongodb://127.0.0.1:27017/target", "mongodb://127.0.0.1:27017/views", "mongodb://127.0.0.1:27017/state",
                    address -> mongo(address.getDatabase(), seen, primary, secondary))).isSameAs(primary);
            assertThat(primary.getSuppressed()).containsExactly(secondary);
            assertThat(seen).containsExactly("create:views", "database:views", "collection:bench_join_output", "collection-drop", "close:views");
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void boundary_update_has_prior_scalar_and_retains_original_affected_row_assertion() throws Exception {
        var seen = new ArrayList<String>();
        Statement statement = proxy(Statement.class, (name, args) -> {
            if (name.equals("executeUpdate")) { seen.add((String) args[0]); return 0; }
            if (name.equals("close")) { seen.add("close"); return null; }
            throw new AssertionError("unexpected boundary method");
        });
        Connection connection = proxy(Connection.class, (name, args) -> statement);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "boundary-update")) {
            assertThatThrownBy(() -> BenchmarkForkEnvironment.executeOneSourceUpdate(connection, "UPDATE fixture"))
                    .isInstanceOf(AssertionError.class).hasMessage("benchmark boundary update affected 0 rows");
            assertThat(seen).containsExactly("UPDATE fixture", "close");
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void unscoped_mysql_and_sql_preserve_the_original_provider_sequence_without_a_ledger() throws Exception {
        var seen = new ArrayList<String>();
        Statement statement = proxy(Statement.class, (name, args) -> {
            if (name.equals("execute")) { seen.add((String) args[0]); return false; }
            if (name.equals("close")) { seen.add("close"); return null; }
            throw new AssertionError("unexpected unscoped statement");
        });
        Connection connection = proxy(Connection.class, (name, args) -> statement);
        SharedMySql.connect(settings(), (uri, user, secret) -> { seen.add("connect"); return connection; });
        BenchmarkForkEnvironment.execute(connection, List.of("one", "two"));
        assertThat(seen).containsExactly("connect", "one", "two", "close");
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void checked_source_vectors_cover_all_fixed_local_shapes_and_do_not_wrap() {
        var rows = BenchmarkSharedSupportSourceWork.bounds();
        assertThat(rows.keySet()).containsExactlyInAnyOrder(BenchmarkSharedSupportSourceWork.Kind.values());
        assertThat(rows.get(BenchmarkSharedSupportSourceWork.Kind.SQL_PARENT).elements()).isEqualTo(2L * 581);
        assertThat(rows.get(BenchmarkSharedSupportSourceWork.Kind.BATCH_ROW).elements()).isEqualTo(2);
        assertThat(rows.get(BenchmarkSharedSupportSourceWork.Kind.MY_COMMAND).bytes()).isEqualTo(4L * (2 * 65536 + 128));
        assertThat(rows.get(BenchmarkSharedSupportSourceWork.Kind.WORKLOAD_SHAPE).elements()).isEqualTo(3470 + 173);
        assertThatThrownBy(() -> rows.get(BenchmarkSharedSupportSourceWork.Kind.WORKLOAD_SHAPE).times(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    private static Map<String, Object> settings() {
        return Map.of("host", "127.0.0.1", "port", 3306, "database", "fixture", "username", "fixture", "password", "fixture");
    }
    private static void initializeMySql() throws SQLException {
        SharedMySql.connect(settings(), (uri, user, password) -> proxy(Connection.class, (method, args) -> null));
    }
    private static MySQLContainer<?> server() { return new ControlledMySql(); }
    private static final class ControlledMySql extends MySQLContainer<ControlledMySql> {
        ControlledMySql() { super(DockerImageName.parse("mysql:8.0")); }
        @Override public String getJdbcUrl() { return "jdbc:mysql://127.0.0.1:3306/fixture"; }
        @Override public String getUsername() { return "fixture"; }
        @Override public String getPassword() { return "fixture"; }
    }
    private static MongoClient mongo(String name, List<String> seen, RuntimeException drop, RuntimeException close) {
        seen.add("create:" + name);
        MongoCollection<?> collection = proxy(MongoCollection.class, (method, args) -> {
            assertThat(method).isEqualTo("drop"); seen.add("collection-drop"); if (drop != null) throw drop; return null;
        });
        MongoDatabase database = proxy(MongoDatabase.class, (method, args) -> {
            if (method.equals("getCollection")) { seen.add("collection:" + args[0]); return collection; }
            if (method.equals("drop")) { seen.add("database-drop"); if (drop != null) throw drop; return null; }
            throw new AssertionError("unexpected Mongo database method");
        });
        return proxy(MongoClient.class, (method, args) -> {
            if (method.equals("getDatabase")) { seen.add("database:" + args[0]); return database; }
            if (method.equals("close")) { seen.add("close:" + name); if (close != null) throw close; return null; }
            throw new AssertionError("unexpected Mongo client method");
        });
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.put(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.sharedUpper());
        upper.put(BenchmarkReturnWorkBudget.Activity.CLEANUP, BenchmarkSharedSupportSourceWork.cleanupUpper());
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("controlled-three-shared-recipes", "a".repeat(64), Map.of(), upper, excluded);
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> row(Map<String, Object> value, String key) {
        return (Map<String, Object>) ((Map<String, Object>) value.get("activities")).get(key);
    }
    @FunctionalInterface private interface Invocation { Object call(String method, Object[] args) throws Throwable; }
    @SuppressWarnings("unchecked") private static <T> T proxy(Class<T> type, Invocation body) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (ignored, method, args) -> body.call(method.getName(), args));
    }
}
