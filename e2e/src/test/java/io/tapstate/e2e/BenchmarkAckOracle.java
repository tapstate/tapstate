package io.tapstate.e2e;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Checks delivery evidence without assuming that positions from separate source runs are comparable. */
final class BenchmarkAckOracle {

    private BenchmarkAckOracle() {
    }

    /** The source connector's rule for whether this fork's target ACK includes its terminal event. */
    @FunctionalInterface
    interface PositionCoverage {
        boolean covers(String targetAck, String terminalPosition);
    }

    record TerminalEvent(String logicalId, String sourcePosition) {}

    /** Logical coverage includes occurrence counts, so duplicate deliveries cannot hide in a set. */
    record Fork(
            String id,
            List<TerminalEvent> sourceTerminals,
            String targetAck,
            PositionCoverage positionCoverage,
            Map<String, Long> logicalCoverage,
            String checksum,
            long errorTotal) {
        Fork {
            sourceTerminals = List.copyOf(sourceTerminals);
            logicalCoverage = Map.copyOf(logicalCoverage);
        }
    }

    static void verify(List<Fork> forks) {
        if (forks == null || forks.isEmpty()) {
            throw new AssertionError("no benchmark forks to verify");
        }
        Set<String> seen = new HashSet<>();
        for (Fork fork : forks) {
            verifyFork(fork);
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

    private static void verifyFork(Fork fork) {
        if (fork.id() == null || fork.id().isBlank()) {
            throw new AssertionError("benchmark fork has no id");
        }
        if (fork.sourceTerminals().size() != 1) {
            throw new AssertionError("fork " + fork.id() + " must have exactly one source terminal event");
        }
        TerminalEvent terminal = fork.sourceTerminals().getFirst();
        if (terminal.logicalId() == null || terminal.logicalId().isBlank()
                || terminal.sourcePosition() == null || terminal.sourcePosition().isBlank()) {
            throw new AssertionError("fork " + fork.id() + " has an incomplete source terminal event");
        }
        if (fork.targetAck() == null || fork.targetAck().isBlank() || fork.positionCoverage() == null
                || !fork.positionCoverage().covers(fork.targetAck(), terminal.sourcePosition())) {
            throw new AssertionError("fork " + fork.id() + " has no target ACK covering its source terminal event");
        }
        if (fork.logicalCoverage().getOrDefault(terminal.logicalId(), 0L) < 1) {
            throw new AssertionError("fork " + fork.id() + " did not deliver its terminal event");
        }
        if (fork.logicalCoverage().isEmpty() || fork.logicalCoverage().values().stream().anyMatch(count -> count < 1)) {
            throw new AssertionError("fork " + fork.id() + " has invalid logical event coverage");
        }
        if (fork.checksum() == null || fork.checksum().isBlank() || fork.errorTotal() < 0) {
            throw new AssertionError("fork " + fork.id() + " has incomplete output evidence");
        }
    }
}
