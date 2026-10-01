package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.Optional;

/** Cold-path writes for one stop marker in the same document as the actual checkpoint. */
public interface StopReservationStore {

    /** True only when this binding can atomically guard intent, authority, and checkpoint writes. */
    default boolean supportsStopReservations() {
        return false;
    }

    /** Reads the current marker, if any; malformed stored fields are a coded storage failure. */
    default Optional<StopReservation> readStopReservation(String pipelineId) {
        throw new UnsupportedOperationException("durable stop reservations are unavailable");
    }

    /** Keeps actual state unchanged while atomically reserving its next epoch under current authority. */
    default Optional<StopReservation> reserveStop(
            CheckpointDoc expected, StopReservation proposal, Instant touchTime) {
        throw new UnsupportedOperationException("durable stop reservations are unavailable");
    }

    /** Transfers a pending stop to a current owner without changing its original job or intent. */
    default Optional<StopReservation> rebindStop(
            StopReservation expected, StopAuthority successor, Instant touchTime) {
        throw new UnsupportedOperationException("durable stop reservations are unavailable");
    }

    /** Replaces the exact old marker with a fresh intent while preserving actual state and fencing old work. */
    default Optional<StopReservation> replaceStop(
            StopReservation expected, StopReservation successor, Instant touchTime) {
        throw new UnsupportedOperationException("durable stop reservations are unavailable");
    }

    default Optional<StopReservation> promoteStopReservation(StopReservation expectedLegacy,
            DesiredState currentIntent, StopAuthority currentWriter, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    default Optional<StopReservation> markReplacementPending(StopReservation expectedStopping, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    default Optional<SuccessorAdmission> admitSuccessor(StopReservation expectedPending,
            String pipelineIncarnationId, String submissionBootId, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    default Optional<StopReservation> bindSuccessor(StopReservation expectedAdmitted,
            ObservationStore.Scope observedScope, StopReservation.JobIdentity observedJob, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    default Optional<StopReservation> retireSuccessor(StopReservation expectedSlot,
            SuccessorEnd observedEnd, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    default Optional<CheckpointDoc> completeHandoff(StopReservation expectedBound,
            HandoffIdentity durableReady, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    default Optional<StopReservation> recordSuccessorTerminal(StopReservation expectedBound,
            SuccessorEnd.Terminal exactEnd, PipelineState terminal, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    /**
     * Records one coded replacement refusal under its exact pending or admitted marker. The caller
     * proves native absence before supplying an empty job; a present job is the factual matching
     * admitted execution. Native lookup and cleanup happen outside the checkpoint transaction.
     * An absent execution clears the marker; an actual execution keeps its bound handoff while failed.
     */
    default Optional<CheckpointDoc> failReplacement(StopReservation expected,
            Optional<StopReservation.JobIdentity> factualSuccessorJob, Instant at) {
        throw new UnsupportedOperationException("phased stop reservations are unavailable");
    }

    /** Completes only the exact pending stop while its original intent and current authority still hold. */
    default Optional<CheckpointDoc> completeStop(StopReservation expected, Instant touchTime) {
        throw new UnsupportedOperationException("durable stop reservations are unavailable");
    }

    /** Retires an old marker only under the exact newer intent and current authority. */
    default Optional<CheckpointDoc> retireStop(
            StopReservation expected, DesiredState successor, StopAuthority authority, Instant touchTime) {
        throw new UnsupportedOperationException("durable stop reservations are unavailable");
    }
}
