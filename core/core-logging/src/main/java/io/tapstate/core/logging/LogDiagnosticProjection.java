package io.tapstate.core.logging;

import io.tapstate.core.common.RuntimeDiagnosticProjection;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/** A conservative encoded-size budget for optional node-local log attachments. */
public final class LogDiagnosticProjection {
    public static final int MAX_BYTES = 32 * 1024;

    private LogDiagnosticProjection() {}

    public static Map<String, Object> fit(Map<String, Object> input) {
        try {
            var checked = RuntimeDiagnosticProjection.fit(input);
            return new Budget().accept(checked) ? checked : Map.of("truncated", true);
        } catch (RuntimeException | LinkageError unavailable) {
            return Map.of("truncated", true);
        }
    }

    private static final class Budget {
        private int remaining = MAX_BYTES;

        private boolean charge(long amount) {
            if (amount > remaining) return false;
            remaining -= (int) amount;
            return true;
        }

        boolean accept(Object value) {
            if (value instanceof String text) {
                if (!charge(2)) return false;
                for (int i = 0; i < text.length(); i++) {
                    char c = text.charAt(i);
                    int bytes = c < 0x20 || Character.isSurrogate(c) ? 6
                            : c == '"' || c == '\\' ? 2 : c < 0x80 ? 1 : c < 0x800 ? 2 : 3;
                    if (!charge(bytes)) return false;
                }
                return true;
            }
            if (value instanceof Map<?, ?> fields) {
                if (!charge(2)) return false;
                for (var entry : fields.entrySet()) {
                    if (!charge(2) || !accept(entry.getKey()) || !accept(entry.getValue())) return false;
                }
                return true;
            }
            if (value instanceof List<?> items) {
                if (!charge(2)) return false;
                for (Object item : items) if (!charge(1) || !accept(item)) return false;
                return true;
            }
            if (value instanceof BigInteger integer && value.getClass() == BigInteger.class) return charge(2L + integer.bitLength());
            if (value instanceof BigDecimal decimal && value.getClass() == BigDecimal.class) {
                return charge(32L + decimal.unscaledValue().bitLength());
            }
            if (value == null || value instanceof Boolean) return charge(5);
            if (value instanceof Byte || value instanceof Short || value instanceof Integer
                    || value instanceof Long || value instanceof Float || value instanceof Double) return charge(32);
            // Never invoke an arbitrary Number's formatter in a logging failure path.
            return false;
        }
    }
}
