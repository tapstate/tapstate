package io.tapstate.spi.store;

import java.util.Objects;

/** The latest bounded diagnostic attempt for one immutable managed Source, never execution authority. */
public record DefaultSourceTestAttempt(String connectionId, String connectorId, String sourceHash, String attemptId,
        WorkloadOwner owner, long sessionGeneration, long acceptedAt, long deadlineAt, State state,
        Long finishedAt, ConnectionTestResult report) {
    public enum State { CHECKING, PASSED, FAILED, UNKNOWN }
    public DefaultSourceTestAttempt {
        Objects.requireNonNull(connectionId); Objects.requireNonNull(connectorId); Objects.requireNonNull(sourceHash);
        Objects.requireNonNull(attemptId); Objects.requireNonNull(owner); Objects.requireNonNull(state);
        if (connectionId.isBlank() || connectorId.isBlank() || !sourceHash.matches("[0-9a-f]{64}")
                || !attemptId.matches("[0-9a-f-]{36}") || sessionGeneration < 1 || acceptedAt < 1 || deadlineAt <= acceptedAt || deadlineAt - acceptedAt > 60_000
                || state == State.CHECKING && (finishedAt != null || report != null)
                || state != State.CHECKING && (finishedAt == null || finishedAt < acceptedAt))
            throw new IllegalArgumentException("invalid default Source test attempt");
        boolean completed = state == State.PASSED || state == State.FAILED;
        if (completed != (report != null) || report != null && (!report.connectionId().equals(connectionId)
                || !report.connectorId().equals(connectorId) || !report.diagnosticsRedacted()
                || report.testedAt() < acceptedAt || report.testedAt() > finishedAt
                || state == State.PASSED && report.testedAt() >= deadlineAt
                || (state == State.PASSED) != (report.outcome() == ConnectionTestResult.Outcome.PASSED)))
            throw new IllegalArgumentException("invalid default Source terminal report");
    }
    public DefaultSourceTestAttempt finish(State terminal, long now, ConnectionTestResult result) {
        if (state != State.CHECKING || terminal == State.CHECKING) throw new IllegalStateException("attempt already terminal");
        return new DefaultSourceTestAttempt(connectionId, connectorId, sourceHash, attemptId, owner, sessionGeneration,
                acceptedAt, deadlineAt, terminal, now, result);
    }
}
