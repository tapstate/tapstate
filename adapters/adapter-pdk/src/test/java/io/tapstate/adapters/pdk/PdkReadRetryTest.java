package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.debezium.DebeziumException;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import java.io.EOFException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PdkReadRetryTest {
    @Test
    void rejectsWrappedMysqlRowImageConfigurationFailureBeforeCallingTheConnectorHook() {
        var hookCalls = new AtomicInteger();
        var connector = mysqlWithRetryHook(hookCalls);
        var cause = new DebeziumException(
                "The MySQL server is not configured to use a FULL binlog_row_image, which is required for this "
                        + "connector to work properly. Change the MySQL configuration to use a "
                        + "binlog_row_image=FULL and restart the connector.");
        var failure = new RuntimeException(cause);

        assertThat(PdkReadRetry.prepare(connector, PDKMethod.SOURCE_STREAM_READ, failure)).isEmpty();
        assertThat(hookCalls).hasValue(0);
        assertThat(failure.getCause()).isSameAs(cause);
    }

    @Test
    void stillRetriesAWrappedMysqlStreamEof() {
        var hookCalls = new AtomicInteger();
        var connector = mysqlWithRetryHook(hookCalls);
        var failure = new RuntimeException(new EOFException("Failed to read next byte"));

        assertThat(PdkReadRetry.prepare(connector, PDKMethod.SOURCE_STREAM_READ, failure)).isPresent();
        assertThat(hookCalls).hasValue(1);
    }

    private static PdkConnector mysqlWithRetryHook(AtomicInteger hookCalls) {
        var connector = mock(PdkConnector.class);
        when(connector.connectorId()).thenReturn("mysql");
        var functions = mock(ConnectorFunctions.class);
        when(connector.functions()).thenReturn(functions);
        when(functions.getErrorHandleFunction()).thenReturn((context, method, failure) -> {
            hookCalls.incrementAndGet();
            return RetryOptions.create().needRetry(true);
        });
        return connector;
    }
}
