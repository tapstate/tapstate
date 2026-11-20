package io.tapstate.adapters.pdk;

import java.util.HashMap;
import java.util.Map;

/**
 * The runtime registry for connector-declared error codes. A connector's own jar can declare codes,
 * but the build never sees those jars, so their uniqueness cannot be a build-time gate like first-party
 * codes; it is enforced here at runtime instead.
 *
 * <p>Every connector code is namespaced {@code connector.<connector-id>.<symbol>} — three segments,
 * carrying the connector id. That makes it structurally disjoint from an allocated first-party
 * numeric product code: a connector cannot mint a first-party code no matter
 * what symbol it chooses. Registering the same connector code twice is rejected rather than silently
 * overwritten (the legacy last-writer-wins that let two connectors clobber each other's codes
 * unnoticed); two different connectors may reuse a symbol, since their ids keep the codes apart.
 */
public final class ConnectorCodeRegistry {

    /** The reserved namespace segment connector codes live under. */
    static final String NAMESPACE = "connector";

    private final Map<String, String> owners = new HashMap<>();

    /**
     * Registers {@code symbol} declared by {@code connectorId}, returning its namespaced canonical code.
     *
     * @throws IllegalStateException if this connector already registered this symbol (a collision the
     *     registry refuses rather than silently overwrites)
     * @throws IllegalArgumentException if the id or symbol is blank or carries a dot
     */
    public String register(String connectorId, String symbol) {
        String code = namespaced(connectorId, symbol);
        String prior = owners.putIfAbsent(code, connectorId);
        if (prior != null) {
            throw new IllegalStateException("duplicate connector code " + code + " (already registered)");
        }
        return code;
    }

    /**
     * The reserved namespaced form {@code connector.<connectorId>.<symbol>} — always three segments, so
     * it can never equal an allocated numeric product code.
     *
     * @throws IllegalArgumentException if the id or symbol is blank or carries a dot (which would break
     *     the three-segment structure the isolation guarantee rests on)
     */
    public static String namespaced(String connectorId, String symbol) {
        requireSegment(connectorId, "connector id");
        requireSegment(symbol, "connector code symbol");
        return NAMESPACE + "." + connectorId + "." + symbol;
    }

    private static void requireSegment(String value, String what) {
        if (value == null || value.isBlank() || value.indexOf('.') >= 0) {
            throw new IllegalArgumentException(what + " must be non-blank and contain no dot: " + value);
        }
    }
}
