package io.tapstate.core.lifecycle;

import java.util.Map;
import java.util.Objects;

/**
 * Why a pipeline's run died, as the read faces carry it: the canonical error code plus the named
 * arguments that code's message template consumes. Without it a dead pipeline is observable only as a
 * state and a count — the reason would live in a log line, where neither a frontend nor a test can read
 * it as data.
 *
 * <ul>
 *   <li>{@code code} — the canonical code in its String form ({@code <domain>.<symbol>}), never the enum
 *       object: this projection is written to the store and served over the wire, and reconstructing an
 *       enum on the far side would need reflection.</li>
 *   <li>{@code params} — the code's named arguments as strings ({@code name -> value}), the single
 *       source of variable data for rendering the message; empty when the code takes none.</li>
 * </ul>
 *
 * <p>The failure is current-state, like the observation carrying it: a recovered pipeline publishes an
 * observation with no failure rather than keeping the one that killed its previous run.
 */
public record ObservationFailure(String code, Map<String, String> params) {

    public ObservationFailure {
        Objects.requireNonNull(code, "code");
        // A code with no arguments is normal; null reads as empty, and the copy makes the stored
        // projection immutable.
        params = ObservationMaps.copyOf(params);
    }
}
