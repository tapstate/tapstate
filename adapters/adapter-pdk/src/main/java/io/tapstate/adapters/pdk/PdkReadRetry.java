package io.tapstate.adapters.pdk;

import io.tapdata.ErrorCodeConfig;
import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/** Adapts the PDK method retry contract while the original connector is still live. */
final class PdkReadRetry {
    private PdkReadRetry() {}

    static Optional<Runnable> prepare(PdkConnector connector, PDKMethod method, Throwable failure) {
        if (failure instanceof Error || failure instanceof InterruptedException || failure instanceof CancellationException) {
            return Optional.empty();
        }
        if (method == PDKMethod.SOURCE_STREAM_READ && "mysql".equals(connector.connectorId())
                && mysqlRequiresFullRowImage(failure)) {
            return Optional.empty();
        }
        var hook = connector.functions().getErrorHandleFunction();
        RetryOptions options;
        try {
            options = hook == null ? null : hook.needRetry(connector.context(), method, failure);
        } catch (Error fatal) {
            throw fatal;
        } catch (Throwable rejected) {
            if (rejected != failure) failure.addSuppressed(rejected);
            return Optional.empty();
        }
        var code = failure instanceof TapCodeException coded
                ? ErrorCodeConfig.getInstance().getErrorCode(coded.getCode()) : null;
        boolean requested = code != null && !"UNKNOWN_ERROR".equals(code.getName())
                ? code.isRecoverable() : options != null && options.isNeedRetry();
        if (!requested && !defaultRetry(failure)) return Optional.empty();
        var beforeRetry = options == null ? null : options.getBeforeRetryMethod();
        return Optional.of(beforeRetry == null ? () -> {} : () -> prepareBestEffort(beforeRetry, failure));
    }

    /** The PDK preparation hook is advisory. The read still owns the retry decision and its failure. */
    private static void prepareBestEffort(Runnable beforeRetry, Throwable failure) {
        try {
            beforeRetry.run();
        } catch (CancellationException cancelled) {
            throw cancelled;
        } catch (RuntimeException rejected) {
            if (rejected != failure) failure.addSuppressed(rejected);
        }
    }

    private static boolean mysqlRequiresFullRowImage(Throwable failure) {
        var visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
            // Debezium is loaded by the connector. The PDK wrapper carries no structured error code here.
            if ("io.debezium.DebeziumException".equals(cause.getClass().getName())
                    && cause.getMessage() != null
                    && cause.getMessage().startsWith("The MySQL server is not configured to use a FULL binlog_row_image,")
                    && cause.getMessage().contains("binlog_row_image=FULL")) {
                return true;
            }
        }
        return false;
    }

    private static boolean defaultRetry(Throwable failure) {
        var visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
            if (cause instanceof IOException || cause instanceof SQLRecoverableException || cause instanceof SQLTimeoutException) {
                return true;
            }
        }
        return false;
    }
}
