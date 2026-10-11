package io.tapstate.e2e;

import java.sql.SQLException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnBridgeSourceWorkTest {
    @Test void unscoped_action_keeps_one_original_invocation_and_exact_checked_primary() {
        var calls = new AtomicInteger(); SQLException primary = new SQLException("controlled primary");
        assertThatThrownBy(() -> BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.OPEN,
                () -> { calls.incrementAndGet(); throw primary; })).isSameAs(primary);
        assertThat(calls).hasValue(1); assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }
    @Test void scoped_entry_counts_success_failure_and_no_unissued_provider_request() {
        var rows = BenchmarkReturnBridgeSourceWork.additions();
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(rows.keySet());
        var definition = new BenchmarkReturnWorkBudget.Definition("bridge-source-control", "a".repeat(64), Map.of(), rows, excluded);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, "bridge-source-entry")) {
            assertThat(BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.EVIDENCE, () -> 7)).isEqualTo(7);
            AssertionError primary = new AssertionError("controlled guard refusal");
            assertThatThrownBy(() -> BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.EVIDENCE,
                    () -> { throw primary; })).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary);
            @SuppressWarnings("unchecked") var activity = (Map<String, Object>) ((Map<?, ?>) scope.snapshot().get("activities")).get("INSTALL_BINDINGS");
            assertThat(activity).containsEntry("attempts", 2L).containsEntry("completedAttempts", 2L)
                    .containsEntry("failedAttempts", 1L).containsEntry("issuedRequests", 0L);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }
    @Test void every_source_kind_has_explicit_requests_and_factory_multiplication_is_checked() {
        for (var kind : BenchmarkReturnBridgeSourceWork.Kind.values()) {
            assertThat(BenchmarkReturnBridgeSourceWork.upper(kind).requests()).isPositive();
        }
        assertThatThrownBy(() -> BenchmarkReturnBridgeSourceWork.factoryUpper(Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
        assertThat(BenchmarkReturnBridgeSourceWork.harnessUpper().frames()).isEqualTo(30);
    }
    @Test void exact_appended_root_source_roles_do_not_replace_any_producer_helper() {
        for (var role : List.of(BenchmarkReturnWorkBudgetBindings.SourceRole.RETURN_FORK_BRIDGE,
                BenchmarkReturnWorkBudgetBindings.SourceRole.RETURN_HARNESS,
                BenchmarkReturnWorkBudgetBindings.SourceRole.RETURN_BRIDGE_SOURCE_WORK, BenchmarkReturnWorkBudgetBindings.SourceRole.TABLE_ACK_GATE)) {
            assertThat(role.owners()).containsExactly(BenchmarkReturnWorkBudgetBindings.Owner.ROOT);
            assertThat(role.module()).isEqualTo("e2e");
        }
    }

    @Test void full_fork_source_coverage_includes_required_optional_targets_and_chain_keys() {
        assertThat(BenchmarkReturnBridgeSourceWork.coverageUpper(BenchmarkWorkloadDefinitions.steadyPilot("copy"))).isEqualTo(96002);
        assertThat(BenchmarkReturnBridgeSourceWork.coverageUpper(BenchmarkWorkloadDefinitions.steadyPilot("stateless"))).isEqualTo(96003);
        assertThat(BenchmarkReturnBridgeSourceWork.coverageUpper(BenchmarkWorkloadDefinitions.steadyPilot("stateful"))).isEqualTo(288007);
        String checksum = "bench_stateful_join/bench_join_output=" + "a".repeat(64)
                + ";bench_stateful_nest/bench_nest_orders=" + "b".repeat(64);
        assertThat(checksum).hasSize(205);
        BenchmarkReturnBridgeSourceWork.text(checksum, BenchmarkReturnBridgeSourceWork.CHECKSUM);
    }
    @Test void known_table_ack_body_counts_actual_parse_and_cover_without_an_unconditional_gap() {
        var proof = tableProof();
        var fork = new BenchmarkAckOracle.Fork("copy-A-1", List.of(new BenchmarkAckOracle.SourceChain(
                "pipeline/source", List.of(new BenchmarkAckOracle.TerminalEvent("terminal", "source-terminal")),
                null, null, proof)), Map.of("terminal", 1L), "a".repeat(64), 0);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "known-table-source")) {
            BenchmarkAckOracle.verify(List.of(fork), Map.of("pipeline/source", "terminal"));
            var snapshot = scope.snapshot();
            assertThat(snapshot).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
            assertThat(activity(snapshot, "ASSOCIATION")).containsEntry("attempts", 3L).containsEntry("completedAttempts", 3L);
            assertThat(activity(snapshot, "DECODE")).containsEntry("issuedRequests", 1L).containsEntry("completedRequests", 1L);
            assertThat(activity(snapshot, "ASSOCIATION")).doesNotContainKey("gap");
        }
    }
    @Test void oversized_json_refuses_before_any_parse_request_is_issued() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "parse-prior-refusal")) {
            assertThatThrownBy(() -> BenchmarkReturnBridgeSourceWork.entered(BenchmarkReturnBridgeSourceWork.Kind.TABLE_CONFIRMATION,
                    () -> BenchmarkReturnBridgeSourceWork.parsedCursor(" ".repeat(65537))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("prior character bound");
            assertThat(activity(scope.snapshot(), "DECODE")).containsEntry("issuedRequests", 0L);
            assertThat(activity(scope.snapshot(), "ASSOCIATION")).containsEntry("failedAttempts", 1L);
        }
    }
    @Test void fence_format_failure_keeps_exact_primary_and_issued_failed_request() {
        RuntimeException primary = new IllegalArgumentException("controlled BSON format failure");
        org.bson.Document fence = new org.bson.Document() {
            @Override public String toJson(org.bson.json.JsonWriterSettings settings) { throw primary; }
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "fence-format-refusal")) {
            assertThatThrownBy(() -> BenchmarkTableAckGate.copy(fence)).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(activity(scope.snapshot(), "COPY")).containsEntry("issuedRequests", 1L)
                    .containsEntry("failedRequests", 1L).containsEntry("failedAttempts", 2L);
            assertThat(activity(scope.snapshot(), "DECODE")).containsEntry("issuedRequests", 0L);
        }
    }
    @Test void unknown_position_callback_retains_correctness_but_refuses_source_credit() {
        var chain = new BenchmarkAckOracle.SourceChain("pipeline/source",
                List.of(new BenchmarkAckOracle.TerminalEvent("terminal", "source-terminal")), "ack", (ack, terminal) -> true);
        var fork = new BenchmarkAckOracle.Fork("copy-A-1", List.of(chain), Map.of("terminal", 1L), "a".repeat(64), 0);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "unknown-callback-source")) {
            BenchmarkAckOracle.verify(List.of(fork));
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
            assertThat(activity(scope.snapshot(), "ASSOCIATION")).containsEntry("gap", "UNINSPECTED_POSITION_CALLBACK_SOURCE");
        }
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var rows = BenchmarkReturnBridgeSourceWork.additions();
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(rows.keySet());
        return new BenchmarkReturnWorkBudget.Definition("bridge-ack-source-control", "a".repeat(64), Map.of(), rows, excluded);
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> activity(Map<String, Object> snapshot, String name) {
        return (Map<String, Object>) ((Map<?, ?>) snapshot.get("activities")).get(name);
    }
    private static BenchmarkAckOracle.TableConfirmationProof tableProof() {
        String consumer = io.tapstate.spi.store.SrsConsumerId.of("pipeline", "source").value();
        var confirmed = new org.bson.Document("sinkAckedEpoch", 7L).append("sinkAckedSeq", 41L).append("ringDone", 41L);
        var writers = new org.bson.Document("first", new org.bson.Document("orders", confirmed))
                .append("second", new org.bson.Document("orders", new org.bson.Document(confirmed)));
        var cursor = new org.bson.Document("_id", new org.bson.Document("chain", "chain").append("pipeline", consumer))
                .append("miningChainId", "chain").append("pipelineId", consumer).append("ownerPipelineId", "pipeline")
                .append("sourceNodeId", "source").append("expectedSinkWriters", new org.bson.Document("orders", List.of("first", "second")))
                .append("sinkWriterProgress", writers).append("sinkAckedByTable", new org.bson.Document("orders", confirmed))
                .append("perTableRingDone", new org.bson.Document("orders", 41L));
        var marker = new BenchmarkTableTerminalObserver.Point("terminal", io.tapstate.runtime.srs.SrsRingbuffer.ringName("chain", "orders"), 7, 41, null);
        var binding = BenchmarkTableAckGate.bind("chain", "pipeline", "source", "orders", cursor);
        return BenchmarkAckOracle.TableConfirmationProof.from("terminal", "source-terminal", marker, binding, cursor);
    }
}
