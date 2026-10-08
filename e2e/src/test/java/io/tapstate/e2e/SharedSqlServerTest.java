package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class SharedSqlServerTest {
    @Test
    void retriesTheDatabaseMetadataDeadlockReportedBySqlServer() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        SQLException deadlock = new SQLException("Could not update the metadata that indicates database fixture "
                + "is enabled for Change Data Capture. The failure occurred when executing the command "
                + "'sp_addrolemember 'db_owner', 'cdc''. The error returned was 1205: 'Transaction (Process ID 51) "
                + "was deadlocked on lock resources with another process and has been chosen as the deadlock victim. "
                + "Rerun the transaction.'. Use the action and error to determine the cause of the failure "
                + "and resubmit the request.", "S0001", 22830);
        SharedSqlServer.enableCdc(statement(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw deadlock;
            }
        }), Duration.ofSeconds(2));
        assertThat(attempts).hasValue(2);
    }

    @Test
    void retriesADirectDeadlock() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        SharedSqlServer.enableCdc(statement(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new SQLException("deadlock victim", "40001", 1205);
            }
        }), Duration.ofSeconds(2));
        assertThat(attempts).hasValue(2);
    }

    @Test
    void retriesADeadlockInTheJdbcExceptionChain() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        SQLException wrapper = new SQLException("CDC enable failed", "S0001", 22830);
        wrapper.setNextException(new SQLException("deadlock victim", "40001", 1205));
        SharedSqlServer.enableCdc(statement(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw wrapper;
            }
        }), Duration.ofSeconds(2));
        assertThat(attempts).hasValue(2);
    }

    @Test
    void doesNotRetryOtherDatabaseMetadataErrors() {
        AtomicInteger attempts = new AtomicInteger();
        SQLException denied = new SQLException("Could not update CDC metadata. "
                + "The error returned was 229: 'Permission denied.'", "S0001", 22830);
        assertThatThrownBy(() -> SharedSqlServer.enableCdc(statement(() -> {
            attempts.incrementAndGet();
            throw denied;
        }), Duration.ofSeconds(2))).isSameAs(denied);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void propagatesAPermanentFailureAfterADeadlock() {
        AtomicInteger attempts = new AtomicInteger();
        SQLException denied = new SQLException("permission denied", "S0001", 229);
        assertThatThrownBy(() -> SharedSqlServer.enableCdc(statement(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new SQLException("deadlock victim", "40001", 1205);
            }
            throw denied;
        }), Duration.ofSeconds(2))).isSameAs(denied);
        assertThat(attempts).hasValue(2);
    }

    @Test
    void preservesTheDeadlockWhenTheDeadlineHasExpired() {
        AtomicInteger attempts = new AtomicInteger();
        SQLException deadlock = new SQLException("deadlock victim", "40001", 1205);
        assertThatThrownBy(() -> SharedSqlServer.enableCdc(statement(() -> {
            attempts.incrementAndGet();
            throw deadlock;
        }), Duration.ZERO)).isSameAs(deadlock);
        assertThat(attempts).hasValue(1);
    }

    @Test
    void persistentDeadlocksCannotWaitBeyondTheBound() {
        SQLException deadlock = new SQLException("deadlock victim", "40001", 1205);
        AtomicInteger attempts = new AtomicInteger();
        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThatThrownBy(() -> SharedSqlServer.enableCdc(statement(() -> {
                    attempts.incrementAndGet();
                    throw deadlock;
                }), Duration.ofMillis(150))).isSameAs(deadlock));
        assertThat(attempts.get()).isGreaterThan(1);
    }

    @Test
    void preservesInterruptionInsteadOfRetrying() {
        AtomicInteger attempts = new AtomicInteger();
        try {
            assertThatThrownBy(() -> SharedSqlServer.enableCdc(statement(() -> {
                attempts.incrementAndGet();
                Thread.currentThread().interrupt();
                throw new SQLException("deadlock victim", "40001", 1205);
            }), Duration.ofSeconds(2))).isInstanceOf(EnvelopeException.class)
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

    private static Statement statement(Execute execute) {
        return (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class}, (proxy, method, args) -> {
                    if (method.getName().equals("execute")) {
                        assertThat(args[0]).isEqualTo("EXEC sys.sp_cdc_enable_db");
                        execute.run();
                        return false;
                    }
                    return null;
                });
    }
}
