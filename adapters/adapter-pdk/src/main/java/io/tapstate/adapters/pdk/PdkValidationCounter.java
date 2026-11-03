package io.tapstate.adapters.pdk;

import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import java.math.BigInteger;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Receives one native exact count, never rows or codec values. The caller must establish exact count
 * semantics, predicate equivalence and table existence, and supply an independent context/loader and
 * admitted cancellable I/O call. An optional schema-discovery estimate is not an admissible input.
 * Authorization guards must be nonblocking; cancellation belongs on separate admitted I/O. These
 * guards cannot make an uninterruptible connector call stop before it returns.
 */
final class PdkValidationCounter {
    @FunctionalInterface interface DirectCall { long run() throws Throwable; }
    @FunctionalInterface interface CallbackCall { void run(Consumer<ExecuteResult<?>> consumer) throws Throwable; }

    private PdkValidationCounter() {}

    static long direct(String table, DirectCall call, Runnable requireAuthorized) {
        Objects.requireNonNull(call, "call");
        check(table, requireAuthorized);
        try {
            long result = call.run();
            check(table, requireAuthorized);
            if (result < 0) throw protocol(table);
            return result;
        } catch (Throwable failure) { throw propagate(table, failure); }
    }

    static long callback(String table, CallbackCall call, Runnable requireAuthorized) {
        Objects.requireNonNull(call, "call");
        check(table, requireAuthorized);
        var result = new CountResult(table, requireAuthorized);
        Throwable reported = null;
        try { call.run(result::accept); }
        catch (Throwable failure) { reported = failure; result.fail(failure); }
        finally { result.seal(); }
        result.throwFailure(reported);
        check(table, requireAuthorized);
        return result.value();
    }

    private static void check(String table, Runnable requireAuthorized) {
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        if (Thread.currentThread().isInterrupted()) throw cancelled(table, null);
        requireAuthorized.run();
    }

    private static RuntimeException propagate(String table, Throwable failure) {
        if (failure instanceof Error error) throw error;
        if (failure instanceof TapstateException coded) return coded;
        if (failure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return cancelled(table, failure);
        }
        return new TapstateException(ValidationError.READ_FAILED, Map.of("table", table), failure);
    }

    private static TapstateException protocol(String table) {
        return new TapstateException(ValidationError.READ_PROTOCOL_VIOLATION, Map.of("table", table), null);
    }

    private static TapstateException cancelled(String table, Throwable cause) {
        return new TapstateException(ValidationError.READ_CANCELLED, Map.of("table", table), cause);
    }

    private static long exactCount(String table, Object value) {
        long count;
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            count = ((Number)value).longValue();
        } else if (value != null && value.getClass() == BigInteger.class) {
            BigInteger integer = (BigInteger)value;
            if (integer.signum() < 0 || integer.bitLength() > 63) throw protocol(table);
            count = integer.longValue();
        } else {
            throw protocol(table);
        }
        if (count < 0) throw protocol(table);
        return count;
    }

    private static final class CountResult {
        private final String table;
        private final Runnable requireAuthorized;
        private boolean sealed;
        private boolean received;
        private long count;
        private final ValidationFailureLatch failure = new ValidationFailureLatch();

        private CountResult(String table, Runnable requireAuthorized) {
            this.table = table;
            this.requireAuthorized = requireAuthorized;
        }

        private synchronized void accept(ExecuteResult<?> result) {
            try {
                if (sealed || received) throw protocol(table);
                check(table, requireAuthorized);
                if (result == null) throw protocol(table);
                if (result.getError() != null) throw propagate(table, result.getError());
                count = exactCount(table, result.getResult());
                received = true;
            } catch (Throwable rejected) {
                fail(rejected);
                throw propagate(table, rejected);
            }
        }

        private synchronized void fail(Throwable cause) { failure.record(cause); }
        private synchronized void seal() { sealed = true; }

        private synchronized void throwFailure(Throwable reported) {
            Throwable first = failure.get();
            if (first != null) {
                ValidationFailureLatch.addContext(first, reported);
                throw propagate(table, first);
            }
        }

        private synchronized long value() {
            throwFailure(null);
            if (!received) throw protocol(table);
            return count;
        }
    }
}
