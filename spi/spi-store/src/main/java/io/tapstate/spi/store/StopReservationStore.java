package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;

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
