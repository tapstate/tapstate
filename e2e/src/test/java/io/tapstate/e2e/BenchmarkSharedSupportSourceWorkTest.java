package io.tapstate.e2e;

import java.sql.SQLException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkSharedSupportSourceWorkTest {
    @Test void an_unregistered_owned_close_body_is_still_closed_and_cannot_receive_source_credit() throws Exception {
        var closed = new AtomicInteger(); var terminal = new AtomicInteger();
        var resource = new SharedPostgres.Closing("controlled close", closed::incrementAndGet,
                () -> { terminal.incrementAndGet(); return true; });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "unregistered-close-body")) {
            resource.close(); resource.close();
            assertThat(closed).hasValue(1); assertThat(terminal).hasValue(1);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void actual_closing_failure_keeps_original_and_does_not_fabricate_terminal_confirmation() {
        SQLException primary = new SQLException("owned close primary"); var terminal = new AtomicInteger();
        var resource = new SharedPostgres.Closing("controlled close", () -> { throw primary; },
                () -> { terminal.incrementAndGet(); return true; });
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "closing-failure")) {
            assertThatThrownBy(resource::close).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary); assertThat(terminal).hasValue(0);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }
    @Test void producer_writer_output_catalog_keeps_b_prior_and_adds_only_the_fixed_three_a_projections() {
        var workload = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless");
        var b = BenchmarkReturnWorkBudgetReportPlan.prepare(workload);
        assertThat(b.plan()).isEqualTo(BenchmarkReturnWorkBudgetReportPlan.prepare(workload, false).plan());
        var a = BenchmarkReturnWorkBudgetReportPlan.prepare(workload, true);
        var writer = a.components().stream().filter(part -> part.id().equals("OPTIONAL_PRODUCER_WRITER_SCALAR_PROJECTIONS"))
                .findFirst().orElseThrow();
        long strings = 32L * 128 + 2L * 4096 + 512 + 256 + 2L * 64 + 2L * 20 + 128;
        assertThat(writer.rows()).isEqualTo(3);
        assertThat(a.plan().nodeUpper()).isEqualTo(Math.addExact(b.plan().nodeUpper(), 3L * 65));
        assertThat(a.plan().stringUtf8Upper()).isEqualTo(Math.addExact(b.plan().stringUtf8Upper(), 3 * strings));
        assertThat(a.plan().encodedUpperBytes()).isEqualTo(Math.addExact(b.plan().encodedUpperBytes(),
                Math.addExact(3L * 65 * (8 + b.plan().numberTextUpper()), 6 * 3 * strings)));
    }

    @Test void unknown_cleanup_material_refuses_credit_without_skipping_an_owned_close() {
        var closed = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "cleanup-material")) {
            BenchmarkSharedSupportSourceWork.cleanupMaterial("x".repeat(65537));
            closed.incrementAndGet();
            assertThat(closed).hasValue(1);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void actual_await_success_counts_its_final_iteration_without_an_extra_condition_read() {
        var calls = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "actual-await")) {
            scope.bindRoot(17, 23);
            Await.until("controlled immediate condition", java.time.Duration.ofSeconds(1),
                    () -> { calls.incrementAndGet(); return true; }, () -> { throw new AssertionError("unused failure reading"); });
            assertThat(calls).hasValue(1);
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("attempts", 1L).containsEntry("completedAttempts", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }
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
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 1L).containsEntry("completedRequests", 1L);
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
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 1L).containsEntry("failedRequests", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
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
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("issuedRequests", 4L).containsEntry("completedRequests", 4L);
            assertThat(row(scope.snapshot(), "CLEANUP")).containsEntry("issuedRequests", 1L).containsEntry("completedRequests", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void configured_mongo_command_parts_are_bounded_before_list_copy() {
        var copies = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "mongo-parts")) {
            BenchmarkSharedSupportSourceWork.commandParts(new String[]{"--replSet", "docker-rs"}); copies.incrementAndGet();
            assertThatThrownBy(() -> { BenchmarkSharedSupportSourceWork.commandParts(new String[513]); copies.incrementAndGet(); })
                    .isInstanceOf(AssertionError.class);
            assertThat(copies).hasValue(1); assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }
    @Test void source_bound_keeps_records_one_and_has_explicit_local_api_allowances() {
        var rows = BenchmarkSharedSupportSourceWork.bounds();
        assertThat(rows.get(BenchmarkSharedSupportSourceWork.Kind.PG_CONNECT).requests()).isGreaterThanOrEqualTo(9);
        assertThat(rows.get(BenchmarkSharedSupportSourceWork.Kind.TARGET_RESET).requests()).isGreaterThanOrEqualTo(5);
        // Two element slots per statement over the largest original setup roster (581 statements).
        assertThat(rows.get(BenchmarkSharedSupportSourceWork.Kind.SQL_PARENT).elements()).isEqualTo(1162);
        assertThat(BenchmarkSharedSupportSourceWork.sharedUpper().records()).isEqualTo(1);
        assertThat(BenchmarkSharedSupportSourceWork.cleanupUpper().records()).isEqualTo(1);
    }

    @Test void completed_entry_is_counted_without_reporting_static_allowance_as_actual_work() throws Exception {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "owned-helper")) {
            scope.bindRoot(17, 23);
            var issued = new AtomicInteger();
            BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    BenchmarkSharedSupportSourceWork.Kind.PG_CONNECT, () -> {
                        return BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                                () -> { issued.incrementAndGet(); return "result"; });
                    });
            var row = row(scope.snapshot(), "SHARED_SUPPORT");
            assertThat(issued).hasValue(1);
            assertThat(row).containsEntry("attempts", 2L).containsEntry("completedAttempts", 2L)
                    .containsEntry("issuedRequests", 1L).containsEntry("completedRequests", 1L);
            assertThat(row(row, "actual")).containsEntry("requests", 1L).containsEntry("records", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void exact_checked_primary_and_cleanup_secondary_remain_distinct() {
        SQLException primary = new SQLException("controlled SQL primary");
        SQLException secondary = new SQLException("controlled SQL close");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "owned-helper-failure")) {
            assertThatThrownBy(() -> BenchmarkSharedSupportSourceWork.entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    BenchmarkSharedSupportSourceWork.Kind.SQL_PARENT, () -> {
                        try { throw primary; }
                        finally { primary.addSuppressed(secondary); }
                    })).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(primary.getSuppressed()).containsExactly(secondary);
            assertThat(row(scope.snapshot(), "SHARED_SUPPORT")).containsEntry("attempts", 1L)
                    .containsEntry("completedAttempts", 1L).containsEntry("failedAttempts", 1L);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void unknown_material_refuses_before_the_owned_consumer_runs() {
        var consumed = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "owned-material-refusal")) {
            assertThatThrownBy(() -> {
                BenchmarkSharedSupportSourceWork.uri("x".repeat(4097));
                consumed.incrementAndGet();
            }).isInstanceOf(AssertionError.class);
            assertThat(consumed).hasValue(0);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void checked_vectors_do_not_wrap_a_source_upper() {
        assertThatThrownBy(() -> new BenchmarkReturnWorkBudget.Work(1, 1, 1, 1, Long.MAX_VALUE, 1, 1, 1).times(2))
                .isInstanceOf(ArithmeticException.class);
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
