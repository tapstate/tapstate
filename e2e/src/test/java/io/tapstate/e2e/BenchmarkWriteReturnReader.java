package io.tapstate.e2e;

import javax.management.Attribute;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Reads the bounded return probe only through the existing, exact owned JVM resource connection. */
final class BenchmarkWriteReturnReader {
    private static final String NAME = "io.tapstate.benchmark:type=WriteReturn";
    private static final int MAX_PAGE_BYTES = 64 * 1024;
    private final BenchmarkCausalClock.Identity identity;
    private final MBeanServerConnection connection;
    private final BooleanSupplier alive;
    private final LongSupplier clock;
    private final ObjectName name;

    BenchmarkWriteReturnReader(BenchmarkCausalClock.Identity identity, MBeanServerConnection connection,
                              BooleanSupplier alive, LongSupplier clock) {
        this.identity = java.util.Objects.requireNonNull(identity);
        this.connection = java.util.Objects.requireNonNull(connection);
        this.alive = java.util.Objects.requireNonNull(alive);
        this.clock = java.util.Objects.requireNonNull(clock);
        try { name = new ObjectName(NAME); }
        catch (javax.management.MalformedObjectNameException impossible) { throw new AssertionError(impossible); }
    }

    BenchmarkCausalClock.Sample clockSample(long sequence) {
        requireAlive();
        long before = clock.getAsLong();
        try {
            var attributes = connection.getAttributes(name, new String[]{"Pid", "JvmStartTimeMillis", "NanoTime"});
            long after = clock.getAsLong();
            requireAlive();
            Map<String, Object> values = new HashMap<>();
            for (Object entry : attributes) {
                if (!(entry instanceof Attribute attribute) || values.containsKey(attribute.getName())) {
                    throw new AssertionError("return clock attributes are malformed or duplicate");
                }
                values.put(attribute.getName(), attribute.getValue());
            }
            if (!values.keySet().equals(java.util.Set.of("Pid", "JvmStartTimeMillis", "NanoTime"))) {
                throw new AssertionError("return clock attributes are incomplete");
            }
            var actual = new BenchmarkCausalClock.Identity(number(values, "Pid"), number(values, "JvmStartTimeMillis"));
            if (!identity.equals(actual)) { throw new AssertionError("return clock has another owned runtime identity"); }
            return new BenchmarkCausalClock.Sample(sequence, actual, before, after, number(values, "NanoTime"));
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return clock is unavailable", unavailable);
        }
    }

    boolean start(String window) { return control("start", new Object[]{window}, new String[]{String.class.getName()}); }
    boolean stop() { return control("stop", new Object[0], new String[0]); }

    byte[] page(long completionCursor) {
        clockSample(0);
        try {
            Object result = connection.invoke(name, "read", new Object[]{completionCursor}, new String[]{"long"});
            requireAlive();
            if (!(result instanceof byte[] bytes) || bytes.length < 8 || bytes.length > MAX_PAGE_BYTES) {
                throw new AssertionError("return receipt page is missing or exceeds its bound");
            }
            return bytes;
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return receipt page is unavailable", unavailable);
        }
    }

    private boolean control(String operation, Object[] arguments, String[] signature) {
        clockSample(0);
        try {
            Object result = connection.invoke(name, operation, arguments, signature);
            requireAlive();
            if (!(result instanceof Boolean accepted)) { throw new AssertionError("return probe control has no typed result"); }
            return accepted;
        } catch (java.io.IOException | javax.management.JMException unavailable) {
            throw new AssertionError("owned return probe control is unavailable", unavailable);
        }
    }

    private void requireAlive() {
        if (!alive.getAsBoolean()) { throw new AssertionError("owned return probe JVM has exited"); }
    }
    private static long number(Map<String, Object> values, String field) {
        if (!(values.get(field) instanceof Long value)) { throw new AssertionError("return clock counter is not an exact long"); }
        return value;
    }
}
