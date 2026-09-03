package io.tapstate.runtime.engine;

import com.hazelcast.jet.Job;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Waits for something a running job is expected to produce, and gives up the moment the job itself ends.
 *
 * <p>Test-only. Every case that watches a job watches it from outside, through a collection the job writes
 * into, and a job that dies stops writing without disturbing anything the case can see. The collection
 * simply stops changing - which is indistinguishable from a mechanism that is merely slow, and is what a
 * plain deadline reports it as. So the case spends its whole budget, then names the mechanism it was
 * watching rather than the job that died under it, and whoever reads that goes looking where nothing is
 * wrong.
 */
public final class JobWatch {

    private JobWatch() {
    }

    /**
     * Returns once {@code reached} holds. Fails if the budget runs out, and {@code whatDid} is what the
     * failure reports as having arrived instead - timing out silently would leave a later assertion
     * reading an empty collection as agreement.
     */
    public static void until(Job job, Duration budget, BooleanSupplier reached, Supplier<String> whatDid) {
        long deadline = System.nanoTime() + budget.toNanos();
        while (System.nanoTime() < deadline) {
            // Read the status first and ask the condition after. A job that ends between the two reads has
            // already done everything it was ever going to, so the condition below sees all of it; the
            // other order would call a delivered result a death.
            boolean ended = job.getStatus().isTerminal();
            if (reached.getAsBoolean()) {
                return;
            }
            if (ended) {
                throw new AssertionError("the job ended as " + job.getStatus() + " before what was waited"
                        + " for arrived; what did: " + whatDid.get(), howItEnded(job));
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        throw new AssertionError("what was waited for never arrived; what did: " + whatDid.get());
    }

    /**
     * The job's own account of how it ended, or null if it ended without one. Carried rather than looked
     * up later: it is the only record of why, it is already in hand here, and finding it again costs
     * whoever reads the failure another run.
     */
    private static Throwable howItEnded(Job job) {
        try {
            job.join();
            return null;
        } catch (RuntimeException end) {
            return end;
        }
    }
}
