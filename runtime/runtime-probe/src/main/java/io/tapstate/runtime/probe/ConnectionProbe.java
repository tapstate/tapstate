package io.tapstate.runtime.probe;

import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;

/**
 * The first synchronous call the control plane is permitted to make into the runtime: a connection
 * test. Every other control operation writes desired state the runtime converges toward, so the two
 * rings decouple through the store and hold no reference to each other; the exceptions are the one-shot
 * request/response calls that cannot be expressed as desired state. This interface is one of those
 * narrow seams.
 *
 * <p>The whitelist of such calls is a closed set of seven probes, pinned by the ring-dependency gate.
 * Any further synchronous control-to-runtime call is a deliberate widening of the seam that must change the gate and the sync-whitelist decision, not
 * something a later slice adds in passing.
 *
 * <p>The probe drives the target connector's own connection test and reports the normalized
 * {@link ConnectionTestResult}: the overall outcome plus the per-item checks the connector reported.
 * In the first landing the control and runtime rings share one process and this is a direct in-process
 * call; when the roles split, this seam is where the call becomes a request across instances — which is
 * why the result it returns carries no connector-framework types and is addressed by a stored
 * {@link ConnectionConfig} rather than a live connector handle.
 */
public interface ConnectionProbe {

    /** Probes the target connection synchronously and reports the normalized result. */
    ConnectionTestResult probe(ConnectionConfig config);
}
