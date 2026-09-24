package io.tapstate.e2e;

import java.time.Duration;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Bounded, condition-driven waiting for the cases that drive the product directly rather than through a
 * declarative specification.
 *
 * <p>The same rule the specification runner follows: poll until the condition holds or the bound expires,
 * and never sleep a fixed duration. A sleep long enough to be reliable wastes that long on every green
 * run and is not quite reliable anyway. An expired bound reports what was last read, because "timed out"
 * on its own sends an author looking at the wait instead of at the product.
 */
final class Await {

    private static final Duration BOUND = Duration.ofSeconds(60);
    private static final Duration POLL = Duration.ofMillis(250);

    private Await() {
    }

    /** Waits for {@code condition}, failing with {@code what} and the last reading when the bound expires. */
    static void until(String what, BooleanSupplier condition, Supplier<String> lastReading) {
        until(what, BOUND, condition, lastReading);
    }

    /**
     * The same wait with a bound of the caller's own, for a condition that is legitimately slower than the
     * default allows - a run driving two real database engines through a snapshot and a change stream is
     * the case this exists for.
     *
     * <p>A bound only decides how long a failing case takes to say so; it can never make a broken run
     * pass. Widening the shared default would have slowed every other case's failure instead, which is
     * why this is a parameter rather than a larger constant.
     */
    static void until(String what, Duration bound, BooleanSupplier condition, Supplier<String> lastReading) {
        long start = System.nanoTime();
        long deadline = start + bound.toNanos();
        while (true) {
            if (condition.getAsBoolean()) {
                return;
            }
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("timed out after " + Duration.ofNanos(System.nanoTime() - start)
                        + " (bound " + bound + ") waiting for " + what + "; last read " + lastReading.get());
            }
            sleep();
        }
    }

    /**
     * Waits for a reading to be there at all and answers it, for the cases that need the value rather
     * than only the fact.
     *
     * <p>Separate from {@link #until} because asking twice - once to see whether it is present and once
     * to take it - reads a moving answer twice, and the second read is the one that gets used.
     */
    static <T> T answered(String what, Supplier<Optional<T>> reading) {
        return answered(what, BOUND, reading);
    }

    /** The same with a bound of the caller's own. See {@link #until(String, Duration, BooleanSupplier, Supplier)}. */
    static <T> T answered(String what, Duration bound, Supplier<Optional<T>> reading) {
        long start = System.nanoTime();
        long deadline = start + bound.toNanos();
        while (true) {
            Optional<T> answer = reading.get();
            if (answer.isPresent()) {
                return answer.get();
            }
            if (System.nanoTime() - deadline >= 0) {
                throw new AssertionError("timed out after " + Duration.ofNanos(System.nanoTime() - start)
                        + " (bound " + bound + ") waiting for " + what + "; nothing was ever answered");
            }
            sleep();
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for a condition", e);
        }
    }
}
