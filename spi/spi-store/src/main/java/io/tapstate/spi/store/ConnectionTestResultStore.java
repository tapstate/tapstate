package io.tapstate.spi.store;

import java.util.Optional;

/**
 * Persists the latest connection-test result per connection, keyed by the tested connection's id: the
 * truth-layer store counterpart of {@link ConnectionTester}. A pure interface (rule R2); it carries no
 * connector-framework or store-driver types.
 *
 * <p>Identity is the connection id (carried inside the result). {@link #save} upserts the result
 * latest-only, so a re-test overwrites the previous result in place rather than accumulating history;
 * {@link #find} returns the stored result for a connection, or empty when it has never been tested.
 */
public interface ConnectionTestResultStore {

    /** Upserts the latest test result for its connection id; latest-only, a re-test overwrites. */
    void save(ConnectionTestResult result);

    /** Publishes a newly accepted managed default Source attempt in its existing latest-only row. */
    default void beginDefaultAttempt(DefaultSourceTestAttempt attempt) { throw new UnsupportedOperationException("default Source attempts unsupported"); }

    /** Publishes only the still-current CHECKING attempt. Late success must be rejected at the write. */
    default boolean finishDefaultAttempt(DefaultSourceTestAttempt terminal, boolean controlledTimeout) {
        throw new UnsupportedOperationException("default Source attempts unsupported");
    }

    /** Reads diagnostics only; an absent or legacy report cannot authorize a new attempt. */
    default Optional<DefaultSourceTestAttempt> findDefaultAttempt(String connectionId) { return Optional.empty(); }

    /** Returns the stored latest result for the connection id, or empty if it has never been tested. */
    Optional<ConnectionTestResult> find(String connectionId);
}
