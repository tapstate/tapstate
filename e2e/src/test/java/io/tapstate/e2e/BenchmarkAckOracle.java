package io.tapstate.e2e;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

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

    /** A separate table-order proof; it never fabricates a connector token for a tokenless log row. */
    record TableConfirmationProof(String terminalLogicalId, String sourceTerminalToken,
            BenchmarkTableTerminalObserver.Point marker, BenchmarkTableAckGate.Binding binding,
            String confirmedConsumerCanonicalJson) {
        TableConfirmationProof {
            if (terminalLogicalId == null || terminalLogicalId.isBlank() || terminalLogicalId.length() > 512
                    || sourceTerminalToken == null || sourceTerminalToken.isBlank() || sourceTerminalToken.length() > 65_536
                    || marker == null || binding == null || !terminalLogicalId.equals(marker.markerId())
                    || confirmedConsumerCanonicalJson == null
                    || confirmedConsumerCanonicalJson.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 65_536) {
                throw new AssertionError("table-order confirmation proof is incomplete or exceeds its byte budget");
            }
        }
        static TableConfirmationProof from(String logicalId, String sourceToken,
                BenchmarkTableTerminalObserver.Point marker, BenchmarkTableAckGate.Binding binding, Document cursor) {
            if (!logicalId.equals(marker.markerId()) || sourceToken == null || sourceToken.isBlank()) {
                throw new AssertionError("table-order proof names a different source terminal event");
            }
            if (!BenchmarkTableAckGate.covers(binding, marker, cursor)) {
                throw new AssertionError("table-order terminal has not been confirmed by every target writer");
            }
            String json = cursor.toJson(JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build());
            if (json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 65_536) {
                throw new AssertionError("table-order confirmation proof exceeded its byte budget");
            }
            return new TableConfirmationProof(logicalId, sourceToken, marker, binding, json);
        }

        boolean covers(TerminalEvent terminal) {
            return terminal != null && terminal.logicalId().equals(terminalLogicalId)
                    && marker.markerId().equals(terminalLogicalId)
                    && terminal.sourcePosition().equals(sourceTerminalToken)
                    && BenchmarkTableAckGate.covers(binding, marker, Document.parse(confirmedConsumerCanonicalJson));
        }
    }

    record SourceChain(String id, List<TerminalEvent> sourceTerminals, String authoritativeTargetAck,
                       PositionCoverage positionCoverage, TableConfirmationProof tableConfirmation) {
        SourceChain {
            sourceTerminals = List.copyOf(sourceTerminals);
        }
        SourceChain(String id, List<TerminalEvent> sourceTerminals, String authoritativeTargetAck,
                PositionCoverage positionCoverage) {
            this(id, sourceTerminals, authoritativeTargetAck, positionCoverage, null);
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
        verifyAgainst(forks, null);
    }

    static void verify(List<Fork> forks, Map<String, String> frozenTerminals) {
        verifyAgainst(forks, Map.copyOf(frozenTerminals));
    }

    private static void verifyAgainst(List<Fork> forks, Map<String, String> frozenTerminals) {
        if (forks == null || forks.isEmpty()) {
            throw new AssertionError("no benchmark forks to verify");
        }
        Set<String> seen = new HashSet<>();
        Map<String, String> expectedTerminals = null;
        for (Fork fork : forks) {
            Map<String, String> terminals = verifyFork(fork);
            if (frozenTerminals != null && !frozenTerminals.equals(terminals)) {
                throw new AssertionError("source chains or terminal identities differ from frozen workload in fork "
                        + fork.id());
            }
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
            boolean coveredByTable = chain.tableConfirmation() != null && chain.tableConfirmation().covers(terminal);
            if (chain.tableConfirmation() != null && !coveredByTable) {
                throw new AssertionError("table-order confirmation does not cover its own source terminal event");
            }
            if (chain.tableConfirmation() != null && !chain.id().equals(
                    chain.tableConfirmation().binding().pipeline() + "/" + chain.tableConfirmation().binding().source())) {
                throw new AssertionError("table-order confirmation belongs to a different source chain");
            }
            if (!coveredByTable && (chain.authoritativeTargetAck() == null || chain.authoritativeTargetAck().isBlank()
                    || chain.positionCoverage() == null
                    || !chain.positionCoverage().covers(chain.authoritativeTargetAck(), terminal.sourcePosition()))) {
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
