package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class SqlServerEndpointsTest {
    private static final EndpointAddress ADDRESS = new EndpointAddress("src", Map.of("schema", "dbo"));

    @Test
    void retriesTheNestedAgentStartupFailureBeforeEnablingCdc() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        SQLException starting = new SQLException("Could not update CDC metadata. The error returned was 22836: "
                + "'The error returned was 14258: 'Cannot perform this operation while SQLServerAgent is starting. "
                + "Try again later.'.'", "S0001", 22832);
        new SqlServerEndpoints().enableChanges(connection(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw starting;
            }
        }), ADDRESS, "orders");
        assertThat(attempts).hasValue(2);
    }

    @Test
    void retriesAnAgentStartupErrorInTheJdbcExceptionChain() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        SQLException wrapper = new SQLException("CDC enable failed", "S0001", 22832);
        wrapper.setNextException(new SQLException("Agent is starting", "S0001", 14258));
        new SqlServerEndpoints().enableChanges(connection(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw wrapper;
            }
        }), ADDRESS, "orders");
        assertThat(attempts).hasValue(2);
    }

    @Test
    void doesNotRetryUnrelatedCdcMetadataFailures() {
        AtomicInteger attempts = new AtomicInteger();
        SQLException denied = new SQLException("CDC metadata update failed: permission denied", "S0001", 22832);
        assertThatThrownBy(() -> new SqlServerEndpoints().enableChanges(connection(() -> {
            attempts.incrementAndGet();
            throw denied;
        }), ADDRESS, "orders")).isSameAs(denied);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void propagatesAnUnexpectedSqlFailureAfterAgentStartup() {
        AtomicInteger attempts = new AtomicInteger();
        SQLException denied = new SQLException("permission denied", "S0001", 229);
        assertThatThrownBy(() -> new SqlServerEndpoints().enableChanges(connection(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new SQLException("Agent is starting", "S0001", 14258);
            }
            throw denied;
        }), ADDRESS, "orders")).isSameAs(denied);
        assertThat(attempts).hasValue(2);
    }

    @Test
    void stopsAtTheDeadlineAndPreservesTheStartupError() {
        AtomicInteger attempts = new AtomicInteger();
        SQLException starting = new SQLException("Agent is starting", "S0001", 14258);
        assertThatThrownBy(() -> new SqlServerEndpoints().enableChanges(connection(() -> {
            attempts.incrementAndGet();
            throw starting;
        }), ADDRESS, "orders", Duration.ZERO)).isSameAs(starting);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void persistentStartupFailureCannotWaitBeyondItsBound() {
        SQLException starting = new SQLException("Agent is starting", "S0001", 14258);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThatThrownBy(() -> new SqlServerEndpoints().enableChanges(connection(() -> {
                    throw starting;
                }), ADDRESS, "orders", Duration.ofMillis(150))).isSameAs(starting));
    }

    @Test
    void preservesInterruptionInsteadOfRetrying() {
        AtomicInteger attempts = new AtomicInteger();
        try {
            assertThatThrownBy(() -> new SqlServerEndpoints().enableChanges(connection(() -> {
                attempts.incrementAndGet();
                Thread.currentThread().interrupt();
                throw new SQLException("Agent is starting", "S0001", 14258);
            }), ADDRESS, "orders"))
                    .isInstanceOf(EnvelopeException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(attempts).hasValue(1);
        } finally {
            Thread.interrupted();
        }
    }

    @FunctionalInterface
    private interface Execute {
        void run() throws SQLException;
    }

    private static Connection connection(Execute execute) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("execute")) {
                        execute.run();
                        return false;
                    }
                    return null;
                });
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> method.getName().equals("prepareStatement") ? statement : null);
    }
}
