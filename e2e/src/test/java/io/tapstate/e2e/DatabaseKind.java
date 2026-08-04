package io.tapstate.e2e;

import java.util.Locale;

/**
 * A kind of store a specification can ask the harness to provide.
 *
 * <p>The kind decides three things at once, which is why it is one word and not three: how the store is
 * provisioned, which settings its address is spelled with, and which driver reads it independently. A
 * specification says {@code kind: mysql} and gets all three; it never names a driver or a container.
 *
 * <p>Deliberately not the connector id. A connector is the product's way to reach a store; this is the
 * harness's own, and the two must stay separable - a specification that named the connector twice would
 * make "read it back without the product" impossible to express.
 */
enum DatabaseKind {

    MYSQL("mysql"),
    MONGO("mongodb");

    private final String connectorId;

    DatabaseKind(String connectorId) {
        this.connectorId = connectorId;
    }

    /** The word a specification writes, which is the constant in lower case. */
    String word() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * The connector a resource reaches this store with, which is what keys the driver the harness reads
     * it back through.
     *
     * <p>Knowing this is not the same as a specification naming it: the harness needs the pairing to hand
     * out the right independent reader, while the specification still says only what kind of store it
     * wants. One connector per kind is enough today; a store reachable by two (mysql and a fork of it)
     * would make this a set, and the specification would then have to say which.
     */
    String connectorId() {
        return connectorId;
    }
}
