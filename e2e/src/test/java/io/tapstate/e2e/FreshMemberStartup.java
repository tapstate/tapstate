package io.tapstate.e2e;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One fresh fixture member, with at most one re-selection after an ended HTTP bind failure. */
final class FreshMemberStartup {
    static final Duration BUDGET = Duration.ofSeconds(120);

    record Freshness(boolean noPipeline, boolean noDesired, boolean noActual, boolean noWorkload) {
        boolean fresh() { return noPipeline && noDesired && noActual && noWorkload; }
    }

    record Failure(int port, boolean ended, int exitCode, String rawLog) {
        boolean httpBindInUse() {
            String exact = "Web server failed to start. Port " + port + " was already in use.";
            return port > 0 && port <= 65535 && ended && exitCode > 0
                    && rawLog.contains("APPLICATION FAILED TO START")
                    && rawLog.lines().map(String::trim).anyMatch(exact::equals);
        }
    }

    interface Attempt<T> {
        T await(Duration remaining);
        Failure failure();
        void retain(int ordinal, Freshness freshness, Throwable cause);
        void close();
    }

    static <T> T start(Supplier<Freshness> fresh, Supplier<Attempt<T>> launch) {
        return start(fresh, launch, System::nanoTime);
    }

    static <T> T start(Supplier<Freshness> fresh, Supplier<Attempt<T>> launch, LongSupplier clock) {
        long deadline = clock.getAsLong() + BUDGET.toNanos();
        Throwable previousFailure = null;
        for (int ordinal = 1; ordinal <= 2; ordinal++) {
            Freshness before;
            try { before = Objects.requireNonNull(fresh.get()); }
            catch (RuntimeException | Error failure) {
                preservePrevious(failure, previousFailure);
                throw failure;
            }
            if (!before.fresh()) {
                throw new AssertionError("member re-selection requires an empty pipeline workload", previousFailure);
            }
            long remaining = deadline - clock.getAsLong();
            if (remaining <= 0) {
                throw new AssertionError("fresh member startup exhausted its shared budget", previousFailure);
            }
            Attempt<T> attempt;
            try { attempt = launch.get(); }
            catch (RuntimeException | Error failure) {
                preservePrevious(failure, previousFailure);
                throw failure;
            }
            try {
                long waitBudget = deadline - clock.getAsLong();
                if (waitBudget <= 0) {
                    throw new AssertionError("fresh member startup exhausted its shared budget", previousFailure);
                }
                T result = attempt.await(Duration.ofNanos(waitBudget));
                if (deadline - clock.getAsLong() <= 0) {
                    throw new AssertionError("fresh member startup exhausted its shared budget", previousFailure);
                }
                attempt.retain(ordinal, before, null);
                return result;
            } catch (RuntimeException | Error failure) {
                preservePrevious(failure, previousFailure);
                Failure actual = null;
                boolean retained = false;
                try {
                    actual = attempt.failure();
                    attempt.retain(ordinal, before, failure);
                    retained = true;
                } catch (RuntimeException | Error unavailable) {
                    if (unavailable != failure) { failure.addSuppressed(unavailable); }
                }
                try { attempt.close(); }
                catch (RuntimeException | Error cleanup) {
                    if (cleanup != failure) { failure.addSuppressed(cleanup); }
                    throw failure;
                }
                if (ordinal != 1 || !retained || actual == null || !actual.httpBindInUse()
                        || deadline - clock.getAsLong() <= 0) { throw failure; }
                previousFailure = failure;
                // Repeat the actual store guard before the next launch; never clear it to make it empty.
            }
        }
        throw new IllegalStateException("a fresh member startup cannot fall through its two attempts");
    }

    private static void preservePrevious(Throwable failure, Throwable previous) {
        if (previous != null && previous != failure && failure.getCause() != previous) {
            failure.addSuppressed(previous);
        }
    }
}
