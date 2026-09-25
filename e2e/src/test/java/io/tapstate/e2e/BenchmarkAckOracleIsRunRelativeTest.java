package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Each source chain's ACK is checked against its own fork's opaque terminal position. */
class BenchmarkAckOracleIsRunRelativeTest {

    private static final Map<String, Long> COVERAGE = Map.of(
            "order-1", 1L, "terminal-orders", 1L, "terminal-customers", 1L);

    @Test
    void twoChainsWithEqualLogicalResultsAndDifferentOpaquePositionsPass() {
        BenchmarkAckOracle.verify(List.of(fork("baseline", "first"), fork("candidate", "second")));
    }

    @Test
    void aMissingTerminalOrAckOnEitherChainFails() {
        BenchmarkAckOracle.Fork good = fork("baseline", "first");
        BenchmarkAckOracle.Fork candidate = fork("candidate", "second");
        BenchmarkAckOracle.SourceChain orders = candidate.chains().getFirst();
        BenchmarkAckOracle.SourceChain customers = candidate.chains().get(1);

        BenchmarkAckOracle.SourceChain noTerminal = new BenchmarkAckOracle.SourceChain(
                "customers", List.of(), customers.authoritativeTargetAck(), customers.positionCoverage());
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good,
                        withChains(candidate, List.of(orders, noTerminal)))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("chain customers must have exactly one source terminal event");

        BenchmarkAckOracle.SourceChain noAck = new BenchmarkAckOracle.SourceChain(
                "customers", customers.sourceTerminals(), null, customers.positionCoverage());
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good,
                        withChains(candidate, List.of(orders, noAck)))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("chain customers has no authoritative target ACK covering");

        BenchmarkAckOracle.SourceChain behind = chain(
                "customers", "terminal-customers", "second", "ack-before-terminal", Map.of());
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good,
                        withChains(candidate, List.of(orders, behind)))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("chain customers has no authoritative target ACK covering");
    }

    @Test
    void duplicateOrOmittedChainIdentityCannotPassWithTheSameOutput() {
        BenchmarkAckOracle.Fork good = fork("baseline", "first");
        BenchmarkAckOracle.Fork candidate = fork("candidate", "second");
        BenchmarkAckOracle.SourceChain orders = candidate.chains().getFirst();
        BenchmarkAckOracle.SourceChain duplicateTerminal = chain(
                "customers", "terminal-orders", "second", "customers-ack-second",
                Map.of("customers-ack-second", "customers-tail-second"));
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good,
                        withChains(candidate, List.of(orders, duplicateTerminal)))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("duplicate source terminal event");

        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good,
                        withChains(candidate, List.of(orders)))))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("source chains or terminal identities differ");
    }

    @Test
    void deliveredTerminalAndCrossForkLogicalResultsMustStillAgree() {
        BenchmarkAckOracle.Fork good = fork("baseline", "first");
        BenchmarkAckOracle.Fork candidate = fork("candidate", "second");
        BenchmarkAckOracle.Fork noDelivery = new BenchmarkAckOracle.Fork(
                candidate.id(), candidate.chains(), Map.of("order-1", 1L, "terminal-orders", 1L),
                candidate.checksum(), candidate.errorTotal());
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, noDelivery)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("chain customers did not deliver its terminal event");

        BenchmarkAckOracle.Fork differentCoverage = new BenchmarkAckOracle.Fork(
                candidate.id(), candidate.chains(),
                Map.of("order-1", 2L, "terminal-orders", 1L, "terminal-customers", 1L),
                candidate.checksum(), candidate.errorTotal());
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, differentCoverage)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("logical event coverage differs");

        BenchmarkAckOracle.Fork differentChecksum = new BenchmarkAckOracle.Fork(
                candidate.id(), candidate.chains(), COVERAGE, "different-checksum", candidate.errorTotal());
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, differentChecksum)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("output checksum differs");

        BenchmarkAckOracle.Fork differentErrors = new BenchmarkAckOracle.Fork(
                candidate.id(), candidate.chains(), COVERAGE, candidate.checksum(), 1);
        assertThatThrownBy(() -> BenchmarkAckOracle.verify(List.of(good, differentErrors)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("error total differs");
    }

    private static BenchmarkAckOracle.Fork fork(String id, String run) {
        return new BenchmarkAckOracle.Fork(id, List.of(
                chain("orders", "terminal-orders", run, "orders-ack-" + run,
                        Map.of("orders-ack-" + run, "orders-tail-" + run)),
                chain("customers", "terminal-customers", run, "customers-ack-" + run,
                        Map.of("customers-ack-" + run, "customers-tail-" + run))),
                COVERAGE, "same-checksum", 0);
    }

    private static BenchmarkAckOracle.SourceChain chain(
            String chainId, String terminalId, String run, String ack, Map<String, String> coveredByAck) {
        return new BenchmarkAckOracle.SourceChain(chainId,
                List.of(new BenchmarkAckOracle.TerminalEvent(terminalId, chainId + "-tail-" + run)),
                ack, (observed, terminal) -> terminal.equals(coveredByAck.get(observed)));
    }

    private static BenchmarkAckOracle.Fork withChains(
            BenchmarkAckOracle.Fork fork, List<BenchmarkAckOracle.SourceChain> chains) {
        return new BenchmarkAckOracle.Fork(
                fork.id(), chains, fork.logicalCoverage(), fork.checksum(), fork.errorTotal());
    }
}
