package io.tapstate.e2e;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The endpoints a specification lays data on and reads data from, reached with a driver of the
 * harness's own rather than through the product.
 *
 * <p>This is the one thing the harness must not delegate: a count taken from the product's own record
 * of what it wrote would agree with the product by construction, and would keep agreeing while the
 * target stayed empty. Every implementation of this interface is therefore a second, independent
 * reader of a store the product also reaches - and owes that store's format nothing but agreement.
 *
 * <p>One implementation per kind of store, chosen by the connector the resource names: a specification
 * addresses a table as {@code <resourceId>.<table>}, the resource says which connector reaches it, and
 * that is what decides which driver reads it. The settings the resource carries are passed through as
 * written, so what the harness dials is what the product was given.
 *
 * <p>The address arrives whole rather than as one string, because which setting names a store is the
 * store's business: a directory and a replica set answer on a {@code uri}, a JDBC endpoint on a host,
 * a port and a database. Each implementation reads its own out of {@link EndpointAddress}.
 */
interface Endpoints extends AutoCloseable {

    /**
     * Lays the given rows down, replacing whatever the table held. Rows arrive explicit - columns and
     * values, every row carrying {@code id} - and the driver spells them its store's way; no driver
     * decides what a row looks like.
     */
    void seed(EndpointAddress address, String table, List<Map<String, Object>> rows);

    /**
     * Produces {@code rows} changes of one kind against a table that is already seeded. The change
     * generators assume the generated row shape (an id and a sequence); driving them against a table
     * seeded with other columns fails loudly rather than inventing a change.
     */
    void cdc(EndpointAddress address, String table, CdcOp op, long rows);

    /**
     * The one document the equality settings locate, or empty when none matches. Identity is spelled
     * {@code id} in the settings and in the returned document whatever the store calls it; the
     * translation is the driver's. More than one match is an error - a matcher that silently read the
     * first of many would hold or fail by insertion order.
     */
    Optional<Map<String, Object>> fetch(EndpointAddress address, String table, Map<String, Object> where);

    /**
     * Re-emits the table's current rows as fresh change events under their existing keys, for a change
     * stream that was not yet positioned when the rows were first written. The default does nothing:
     * a store whose stream replays from the origin cannot lose an early change, so there is nothing to
     * re-emit. A store with a positional stream overrides this with a real re-emission.
     */
    default void redeliver(EndpointAddress address, String table) {
    }

    /** The rows the table holds now; zero when the product has not created it yet. */
    long count(EndpointAddress address, String table);

    /** Releases whatever the driver holds open. Overridden without a checked exception. */
    @Override
    void close();
}
