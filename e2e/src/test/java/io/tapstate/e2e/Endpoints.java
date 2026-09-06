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
     * Stops a seeded table's change stream carrying the row an update replaces.
     *
     * <p>Refused by default. Only a store that arranges them in the first place can take them away
     * again, and a driver that quietly did nothing here would leave a case asserting on their absence
     * running against a table that still sends them.
     */
    default void withoutBeforeImages(EndpointAddress address, String table) {
        throw new EnvelopeException(
                "this store has no way to stop " + table + " sending the row an update replaces");
    }

    /**
     * Produces {@code rows} changes of one kind against a table that is already seeded. The change
     * generators assume the generated row shape (an id and a sequence); driving them against a table
     * seeded with other columns fails loudly rather than inventing a change.
     */
    void cdc(EndpointAddress address, String table, CdcOp op, long rows);

    /**
     * Sets the given columns on the one row the equality settings locate, leaving its other columns
     * alone. Locating is spelled the way {@link #fetch} spells it - identity is {@code id} whatever the
     * store calls it, and the translation is the driver's.
     *
     * <p>Matching no row is an error rather than a no-op. A case that updates a row it has already
     * seeded and then waits for the new value to arrive downstream would otherwise fail much later, at
     * the await, reading as though the product never propagated a change nobody actually made.
     */
    void update(EndpointAddress address, String table, Map<String, Object> where, Map<String, Object> set);

    /** Removes the one row the settings locate. Matching no row is an error, for the same reason. */
    void delete(EndpointAddress address, String table, Map<String, Object> where);

    /**
     * Adds the given rows to a table that is already seeded, leaving what it held alone - unlike
     * {@link #seed}, which replaces. Rows arrive explicit, the same shape a seed lays down.
     */
    void insert(EndpointAddress address, String table, List<Map<String, Object>> rows);

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
