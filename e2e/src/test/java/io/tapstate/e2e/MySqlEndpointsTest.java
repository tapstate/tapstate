package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The parts of the SQL driver that need no database.
 *
 * <p>Giving the transaction back is one of them, and it is the part a database cannot easily be asked
 * about: the case that matters is a connection broken enough that both the work and the restoration
 * fail, and a real server has no way to be asked for that at a chosen moment. A stub connection does,
 * so the composition of the two failures is pinned here rather than left to a comment.
 */
class MySqlEndpointsTest {

    /**
     * A re-emission fails on a connection that is already broken, and giving the transaction back fails
     * for the same reason. The reading an author needs is the first one. Restoring in a {@code finally}
     * threw the second over it, so what reached the specification was a message about auto-commit where
     * the account of what actually went wrong should have been.
     */
    @Test
    void aFailedRestoreIsAttachedToTheFailureRatherThanReplacingIt() {
        EnvelopeException original = new EnvelopeException("cannot re-emit the rows of orders");
        SQLException broken = new SQLException("no operations allowed after connection closed");

        EnvelopeException thrown = MySqlEndpoints.withAutoCommitRestored(refusing(broken), original);

        assertThat(thrown).isSameAs(original);
        assertThat(thrown.getSuppressed()).containsExactly(broken);
    }

    /** A connection that does give the transaction back is not heard from at all. */
    @Test
    void aRestoreThatWorksAddsNothingToTheFailure() {
        EnvelopeException original = new EnvelopeException("cannot re-emit the rows of orders");

        EnvelopeException thrown = MySqlEndpoints.withAutoCommitRestored(accepting(), original);

        assertThat(thrown).isSameAs(original);
        assertThat(thrown.getSuppressed()).isEmpty();
    }

    private static Connection refusing(SQLException onRestore) {
        return stub(onRestore);
    }

    private static Connection accepting() {
        return stub(null);
    }

    /** A connection that answers nothing, and fails the restoration when asked to. */
    private static Connection stub(SQLException onRestore) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setAutoCommit" -> {
                        if (onRestore != null) {
                            throw onRestore;
                        }
                        yield null;
                    }
                    case "toString" -> "a stub connection";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == args[0];
                    default -> null;
                });
    }
}
