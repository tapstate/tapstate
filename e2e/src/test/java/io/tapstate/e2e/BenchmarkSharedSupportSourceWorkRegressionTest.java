package io.tapstate.e2e;

import java.sql.SQLException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** These controls invoke the actual owned methods through extraction-only provider seams. */
class BenchmarkSharedSupportSourceWorkRegressionTest {
    @Test void actual_connect_reads_the_five_original_scalars_once_and_issues_one_connection() throws Exception {
        var gets = new AtomicInteger(); var opens = new AtomicInteger();
        var settings = new java.util.LinkedHashMap<String, Object>(Map.of(
                "host", "127.0.0.1", "port", 5432, "database", "controlled", "username", "user", "password", "secret")) {
            @Override public Object get(Object key) { gets.incrementAndGet(); return super.get(key); }
        };
        java.sql.Connection expected = proxy(java.sql.Connection.class, (method, args) -> null);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "actual-connect")) {
            scope.bindRoot(17, 23);
            assertThat(SharedPostgres.connect(settings, (uri, user, secret) -> {
                opens.incrementAndGet(); assertThat(uri).isEqualTo("jdbc:postgresql://127.0.0.1:5432/controlled");
                assertThat(user).isEqualTo("user"); assertThat(secret).isEqualTo("secret"); return expected;
            })).isSameAs(expected);
            assertThat(gets).hasValue(5); assertThat(opens).hasValue(1);
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 1L)
                    .containsEntry("completedRequests", 1L).containsEntry("failedRequests", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void actual_stateless_reset_keeps_the_five_provider_boundaries_without_response_traversal() {
        var seen = new java.util.ArrayList<String>();
        com.mongodb.client.MongoCollection<?> collection = proxy(com.mongodb.client.MongoCollection.class, (method, args) -> {
            assertThat(method).isEqualTo("drop"); seen.add("drop"); return null;
        });
        com.mongodb.client.MongoDatabase database = proxy(com.mongodb.client.MongoDatabase.class, (method, args) -> {
            assertThat(method).isEqualTo("getCollection"); seen.add("collection"); return collection;
        });
        com.mongodb.client.MongoClient client = proxy(com.mongodb.client.MongoClient.class, (method, args) -> {
            if (method.equals("getDatabase")) { seen.add("database"); return database; }
            if (method.equals("close")) { seen.add("close"); return null; }
            throw new AssertionError("unexpected Mongo client call " + method);
        });
        var workload = BenchmarkWorkloadDefinitions.steadyPilot("stateless");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "actual-reset")) {
            scope.bindRoot(17, 23);
            workload.resetTargetsUsing("mongodb://127.0.0.1:27017/controlled", "mongodb://127.0.0.1:27017/views",
                    "mongodb://127.0.0.1:27017/state", address -> { seen.add("create"); return client; });
            assertThat(seen).containsExactly("create", "database", "collection", "drop", "close");
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 4L)
                    .containsEntry("completedRequests", 4L);
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 1L)
                    .containsEntry("completedRequests", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void actual_sql_helper_keeps_order_and_records_its_original_owned_close() throws Exception {
        var seen = new java.util.ArrayList<String>();
        java.sql.Statement statement = proxy(java.sql.Statement.class, (method, args) -> {
            if (method.equals("execute")) { seen.add((String) args[0]); return false; }
            if (method.equals("close")) { seen.add("close"); return null; }
            throw new AssertionError("unexpected Statement call " + method);
        });
        java.sql.Connection connection = proxy(java.sql.Connection.class, (method, args) -> {
            assertThat(method).isEqualTo("createStatement"); seen.add("create"); return statement;
        });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "actual-sql")) {
            scope.bindRoot(17, 23);
            BenchmarkForkEnvironment.execute(connection, java.util.List.of("one", "two"));
            assertThat(seen).containsExactly("create", "one", "two", "close");
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 3L)
                    .containsEntry("completedRequests", 3L).containsEntry("failedRequests", 0L);
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 1L)
                    .containsEntry("completedRequests", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void actual_sql_refusal_preserves_the_primary_and_one_real_close_secondary() {
        SQLException primary = new SQLException("execute primary"), secondary = new SQLException("close secondary");
        java.sql.Statement statement = proxy(java.sql.Statement.class, (method, args) -> {
            if (method.equals("execute")) throw primary;
            if (method.equals("close")) throw secondary;
            throw new AssertionError("unexpected Statement call");
        });
        java.sql.Connection connection = proxy(java.sql.Connection.class, (method, args) -> statement);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "actual-sql-failure")) {
            assertThatThrownBy(() -> BenchmarkForkEnvironment.execute(connection, java.util.List.of("one"))).isSameAs(primary);
            assertThat(primary.getSuppressed()).containsExactly(secondary);
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 1L)
                    .containsEntry("failedRequests", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.put(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkSharedSupportSourceWork.sharedUpper());
        upper.put(BenchmarkReturnWorkBudget.Activity.CLEANUP, BenchmarkSharedSupportSourceWork.cleanupUpper());
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class);
        excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("controlled-owned-support", "a".repeat(64), Map.of(), upper, excluded);
    }
    @FunctionalInterface interface Invocation { Object call(String method, Object[] args) throws Throwable; }
    @SuppressWarnings("unchecked") private static <T> T proxy(Class<T> type, Invocation invocation) {
        return (T) java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (instance, method, args) -> invocation.call(method.getName(), args));
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> row(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) value = ((Map<String, Object>) map.get("activities")).get(key);
        return (Map<String, Object>) value;
    }
}
