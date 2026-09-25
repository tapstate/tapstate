package io.tapstate.e2e;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Checks delivery evidence without assuming that positions from separate source runs are comparable. */
final class BenchmarkAckOracle {

    private BenchmarkAckOracle() {
    }

    /** The source connector's rule for whether one chain's target ACK includes its terminal event. */
    @FunctionalInterface
    interface PositionCoverage {
        boolean covers(String targetAck, String terminalPosition);
    }

    record TerminalEvent(String logicalId, String sourcePosition) {}

    record SourceChain(String id, List<TerminalEvent> sourceTerminals, String authoritativeTargetAck,
                       PositionCoverage positionCoverage) {
        SourceChain {
            sourceTerminals = List.copyOf(sourceTerminals);
        }
    }

    /** Logical coverage includes occurrence counts, so duplicate deliveries cannot hide in a set. */
    record Fork(
            String id,
            List<SourceChain> chains,
            Map<String, Long> logicalCoverage,
            String checksum,
            long errorTotal) {
        Fork {
            chains = List.copyOf(chains);
            logicalCoverage = Map.copyOf(logicalCoverage);
        }
    }

    static void verify(List<Fork> forks) {
        if (forks == null || forks.isEmpty()) {
            throw new AssertionError("no benchmark forks to verify");
        }
        Set<String> seen = new HashSet<>();
        Map<String, String> expectedTerminals = null;
        for (Fork fork : forks) {
            Map<String, String> terminals = verifyFork(fork);
            if (expectedTerminals == null) {
                expectedTerminals = terminals;
            } else if (!expectedTerminals.equals(terminals)) {
                throw new AssertionError("source chains or terminal identities differ in fork " + fork.id());
            }
            if (!seen.add(fork.id())) {
                throw new AssertionError("duplicate benchmark fork id: " + fork.id());
            }
        }

        Fork reference = forks.getFirst();
        for (Fork fork : forks.subList(1, forks.size())) {
            if (!reference.logicalCoverage().equals(fork.logicalCoverage())) {
                throw new AssertionError("logical event coverage differs in fork " + fork.id());
            }
            if (!reference.checksum().equals(fork.checksum())) {
                throw new AssertionError("output checksum differs in fork " + fork.id());
            }
            if (reference.errorTotal() != fork.errorTotal()) {
                throw new AssertionError("error total differs in fork " + fork.id());
            }
        }
    }

    private static Map<String, String> verifyFork(Fork fork) {
        if (fork.id() == null || fork.id().isBlank()) {
            throw new AssertionError("benchmark fork has no id");
        }
        if (fork.chains().isEmpty()) {
            throw new AssertionError("fork " + fork.id() + " declares no source chains");
        }
        Map<String, String> terminalsByChain = new HashMap<>();
        Set<String> terminalIds = new HashSet<>();
        for (SourceChain chain : fork.chains()) {
            if (chain.id() == null || chain.id().isBlank()
                    || terminalsByChain.containsKey(chain.id())) {
                throw new AssertionError("fork " + fork.id() + " has a missing or duplicate source chain id");
            }
            if (chain.sourceTerminals().size() != 1) {
                throw new AssertionError("fork " + fork.id() + " chain " + chain.id()
                        + " must have exactly one source terminal event");
            }
            TerminalEvent terminal = chain.sourceTerminals().getFirst();
            if (terminal.logicalId() == null || terminal.logicalId().isBlank()
                    || terminal.sourcePosition() == null || terminal.sourcePosition().isBlank()) {
                throw new AssertionError("fork " + fork.id() + " chain " + chain.id()
                        + " has an incomplete source terminal event");
            }
            if (!terminalIds.add(terminal.logicalId())) {
                throw new AssertionError("fork " + fork.id() + " has a duplicate source terminal event");
            }
            terminalsByChain.put(chain.id(), terminal.logicalId());
            if (chain.authoritativeTargetAck() == null || chain.authoritativeTargetAck().isBlank()
                    || chain.positionCoverage() == null
                    || !chain.positionCoverage().covers(chain.authoritativeTargetAck(), terminal.sourcePosition())) {
                throw new AssertionError("fork " + fork.id() + " chain " + chain.id()
                        + " has no authoritative target ACK covering its source terminal event");
            }
            if (fork.logicalCoverage().getOrDefault(terminal.logicalId(), 0L) < 1) {
                throw new AssertionError("fork " + fork.id() + " chain " + chain.id()
                        + " did not deliver its terminal event");
            }
        }
        if (fork.logicalCoverage().isEmpty() || fork.logicalCoverage().values().stream().anyMatch(count -> count < 1)) {
            throw new AssertionError("fork " + fork.id() + " has invalid logical event coverage");
        }
        if (fork.checksum() == null || fork.checksum().isBlank() || fork.errorTotal() < 0) {
            throw new AssertionError("fork " + fork.id() + " has incomplete output evidence");
        }
        return Map.copyOf(terminalsByChain);
    }
}
