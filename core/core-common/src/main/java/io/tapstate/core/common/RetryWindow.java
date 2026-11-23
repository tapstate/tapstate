package io.tapstate.core.common;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One failure episode shared by blocking callers and callers which drive attempts on a tick. */
public final class RetryWindow {
    @FunctionalInterface
    public interface Operation<T> {
        T attempt() throws Throwable;
    }

    private final LongSupplier clock;
    private boolean failed;
    private long firstFailure;
    private long lastFailure;

    public RetryWindow() {
        this(System::nanoTime);
    }

    public RetryWindow(LongSupplier clock) {
        this.clock = Objects.requireNonNull(clock, "monotonic clock");
    }

    public void failed() {
        lastFailure = clock.getAsLong();
        if (!failed) {
            firstFailure = lastFailure;
            failed = true;
        }
    }

    public void recovered() {
        failed = false;
    }

    /**
     * Empty when there is no allowed next attempt, zero when the attempt is due.
     * A changed policy still measures from the original failure.
     */
    public Optional<Duration> delay(RetryPolicy policy) {
        if (!failed || !policy.enabled()) return Optional.empty();
        long now = clock.getAsLong();
        long delay = Math.max(0, policy.interval().toNanos() - (now - lastFailure));
        long remaining = policy.maxDuration().toNanos() - (now - firstFailure);
        return delay < remaining ? Optional.of(Duration.ofNanos(delay)) : Optional.empty();
    }

    /**
     * Wrap a single operation. Empty from the classifier propagates the cause unchanged.
     * The caller's guard verifies that it still owns the operation before every attempt.
     */
    public static <T> T execute(Supplier<RetryPolicy> policies,
            Function<Throwable, Optional<Runnable>> prepareRetry, Runnable guard, Operation<T> operation)
            throws Throwable {
        var window = new RetryWindow();
        while (true) {
            guard.run();
            try {
                return operation.attempt();
            } catch (Exception failure) {
                window.failed();
                if (!policies.get().enabled()) throw failure;
                var prepare = prepareRetry.apply(failure);
                if (prepare.isEmpty()) throw failure;
                while (true) {
                    guard.run();
                    var delay = window.delay(policies.get()).orElseThrow(() -> failure);
                    if (delay.isZero()) break;
                    TimeUnit.NANOSECONDS.sleep(Math.min(delay.toNanos(), TimeUnit.SECONDS.toNanos(1)));
                }
                guard.run();
                prepare.orElseThrow().run();
            }
        }
    }
}
