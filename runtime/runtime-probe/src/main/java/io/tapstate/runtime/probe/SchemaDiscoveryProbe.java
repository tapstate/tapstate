package io.tapstate.runtime.probe;

import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.SourceModel;

/**
 * The second synchronous call the control plane is permitted to make into the runtime: a schema
 * discovery. Discovery runs where the connectors run — the runtime side — because it drives the target
 * connector against the live source; like the connection test it is a one-shot request/response that
 * cannot be expressed as desired state, so it crosses the same narrow seam rather than decoupling
 * through the store.
 *
 * <p>The whitelist of such calls is a closed set of seven probes, pinned by the ring-dependency gate.
 * Any further synchronous call is a deliberate widening of the seam that must change the gate and the sync-whitelist decision, not slip in beside it.
 *
 * <p>The probe drives the target connector's own schema discovery and reports the normalized
 * {@link SourceModel}: the tables the source exposes, each with its fields, primary key and indexes.
 * In the first landing the control and runtime rings share one process and this is a direct in-process
 * call; when the roles split, this seam is where the call becomes a request across instances — which is
 * why the model it returns carries no connector-framework types and is addressed by a stored
 * {@link ConnectionConfig} rather than a live connector handle.
 */
public interface SchemaDiscoveryProbe {

    /** Discovers the target connection's source model synchronously and reports it normalized. */
    SourceModel discover(ConnectionConfig config);
}
