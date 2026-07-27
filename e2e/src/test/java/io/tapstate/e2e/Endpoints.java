package io.tapstate.e2e;

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

    /** Lays {@code rows} rows down, numbered from one, replacing whatever the table held. */
    void seed(EndpointAddress address, String table, long rows);

    /** Produces {@code rows} changes of one kind against a table that is already seeded. */
    void cdc(EndpointAddress address, String table, CdcOp op, long rows);

    /** The rows the table holds now; zero when the product has not created it yet. */
    long count(EndpointAddress address, String table);

    /** Releases whatever the driver holds open. Overridden without a checked exception. */
    @Override
    void close();
}
