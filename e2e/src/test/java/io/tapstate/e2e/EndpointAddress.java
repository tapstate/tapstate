package io.tapstate.e2e;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Where one endpoint answers, spelled the way the resource declaring it spelled it: the whole settings
 * mapping, not one setting lifted out of it.
 *
 * <p>Which setting carries the address is a fact about a store, not about endpoints. A directory of files
 * and a Mongo replica set each answer on a single {@code uri}; a JDBC store answers on a host, a port, a
 * database and credentials, and no one of them is the address. Handing a driver the whole mapping lets
 * each read what its own store is addressed by, and spares the harness from having to know - it would
 * otherwise have to grow a rule per store, in the one place that is supposed to be store-agnostic.
 *
 * <p>The mapping is passed through unread. That is what keeps the address the harness dials identical to
 * the one the product was given, which is the property every count in every specification rests on: a
 * harness free to compose its own address could reach a store the product never wrote to and count rows
 * there, green and meaningless.
 */
record EndpointAddress(String resourceId, Map<String, Object> settings) {

    /** Stands in for a resource id when a test dials an address it holds directly, owning no resource. */
    private static final String DIRECT = "an address dialled directly";

    EndpointAddress {
        // Not Map.copyOf: a setting written with no value parses to null, and copyOf answers that with a
        // bare NullPointerException. Kept here, it reaches text() and comes back naming the setting.
        settings = Collections.unmodifiableMap(new LinkedHashMap<>(settings));
    }

    /** The address of a store a test reaches by a uri it already holds, belonging to no applied resource. */
    static EndpointAddress uri(String uri) {
        return new EndpointAddress(DIRECT, Map.of("uri", uri));
    }

    /**
     * The named setting as text, refusing an absent one by naming what should have carried it.
     *
     * <p>Text rather than a type per setting: every store addresses itself in strings on the wire - a port
     * is as much text in a JDBC url as a host is - and the resource carries whatever YAML made of it.
     */
    String text(String setting) {
        Object value = settings.get(setting);
        if (value == null) {
            throw new EnvelopeException(
                    resourceId + " carries no " + setting + " setting, so it cannot be reached from outside "
                            + "the product; it carries " + settings.keySet());
        }
        return value.toString();
    }
}
