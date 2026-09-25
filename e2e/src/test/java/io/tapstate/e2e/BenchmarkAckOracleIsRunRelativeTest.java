package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A fork's ACK is checked against its own source terminal, never another run's opaque position. */
class BenchmarkAckOracleIsRunRelativeTest {

    private static final Map<String, Long> COVERAGE = Map.of("order-1", 1L, "order-2", 2L, "terminal", 1L);

    @Test
    void equalLogicalResultsWithDifferentOpaquePositionsPass() {
        BenchmarkAckOracle.verify(List.of(
                fork("baseline", "mysql-tail-a", "mysql-ack-a", Map.of("mysql-ack-a", "mysql-tail-a")),
                fork("candidate", "pg-tail-b", "pg-ack-b", Map.of("pg-ack-b", "pg-tail-b"))));
    }

    @Test
    void missingTerminalEventOrAnAckBehindItFails() {
        BenchmarkAckOracle.Fork good = fork(
                "baseline", "mysql-tail-a", "mysql-ack-a", Map.of("mysql-ack-a", "mysql-tail-a"));
        BenchmarkAckOracle.Fork noTerminal = new BenchmarkAckOracle.Fork(
                "candidate", List.of(), "pg-ack-b", (ack, terminal) -> true, COVERAGE, "same-checksum", 0);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, noTerminal)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("exactly one source terminal event");

        BenchmarkAckOracle.Fork noAck = fork("candidate", "pg-tail-b", null, Map.of());
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, noAck)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("no target ACK covering");

        BenchmarkAckOracle.Fork behind = fork(
                "candidate", "pg-tail-b", "pg-ack-before-terminal", Map.of("pg-ack-b", "pg-tail-b"));
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, behind)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("no target ACK covering");
    }

    @Test
    void missingDeliveredTerminalAndDivergentLogicalResultsFail() {
        BenchmarkAckOracle.Fork good = fork(
                "baseline", "mysql-tail-a", "mysql-ack-a", Map.of("mysql-ack-a", "mysql-tail-a"));
        BenchmarkAckOracle.Fork noDelivery = new BenchmarkAckOracle.Fork(
                "candidate", List.of(new BenchmarkAckOracle.TerminalEvent("terminal", "pg-tail-b")),
                "pg-ack-b", (ack, terminal) -> true, Map.of("order-1", 1L), "same-checksum", 0);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, noDelivery)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("did not deliver its terminal event");

        BenchmarkAckOracle.Fork differentCoverage = new BenchmarkAckOracle.Fork(
                "candidate", List.of(new BenchmarkAckOracle.TerminalEvent("terminal", "pg-tail-b")),
                "pg-ack-b", (ack, terminal) -> true, Map.of("order-1", 1L, "order-2", 1L, "terminal", 1L),
                "same-checksum", 0);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, differentCoverage)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("logical event coverage differs");

        BenchmarkAckOracle.Fork differentChecksum = new BenchmarkAckOracle.Fork(
                "candidate", List.of(new BenchmarkAckOracle.TerminalEvent("terminal", "pg-tail-b")),
                "pg-ack-b", (ack, terminal) -> true, COVERAGE, "different-checksum", 0);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, differentChecksum)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("output checksum differs");

        BenchmarkAckOracle.Fork differentErrors = new BenchmarkAckOracle.Fork(
                "candidate", List.of(new BenchmarkAckOracle.TerminalEvent("terminal", "pg-tail-b")),
                "pg-ack-b", (ack, terminal) -> true, COVERAGE, "same-checksum", 1);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, differentErrors)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("error total differs");
    }

    private static BenchmarkAckOracle.Fork fork(
            String id, String terminalPosition, String targetAck, Map<String, String> coveredByAck) {
        return new BenchmarkAckOracle.Fork(
                id, List.of(new BenchmarkAckOracle.TerminalEvent("terminal", terminalPosition)),
                targetAck, (ack, terminal) -> terminal.equals(coveredByAck.get(ack)),
                COVERAGE, "same-checksum", 0);
    }
}
