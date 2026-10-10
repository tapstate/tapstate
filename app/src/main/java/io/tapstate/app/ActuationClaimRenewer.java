package io.tapstate.app;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Renews the actuation claims this member holds on a thread of its own, apart from the convergence pass.
 *
 * <p>The pass is one thread over every pipeline the member drives, and a start holds it from the moment
 * the run takes its generation until the job is submitted -- longer than a lease on an overloaded host,
 * behind a slow source, or over many tables. Renewed only from the pass, every claim the member held ran
 * out behind one such start: other members took them over from a member that was still there, the runs
 * under them were refused at their next write, and each takeover was then counted as a member leaving.
 *
 * <p>It looks more often than a claim falls due, so a renewal lands within one look of its due moment;
 * only claims that are due cost a round trip.
 */
final class ActuationClaimRenewer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ActuationClaimRenewer.class);

    /** The longest a due renewal waits for a look, when the renewal interval does not look sooner. */
    private static final Duration LOOK = Duration.ofSeconds(1);

    private final ScheduledExecutorService renewer;

    private ActuationClaimRenewer() {
        this.renewer = null;
    }

    /** A single-node run holds no claims, so nothing is renewed. */
    static ActuationClaimRenewer inactive() {
        return new ActuationClaimRenewer();
    }

    ActuationClaimRenewer(PipelineActuationOwnership ownership, Duration renewInterval) {
        Objects.requireNonNull(ownership, "ownership");
        Objects.requireNonNull(renewInterval, "renewInterval");
        this.renewer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-actuation-claim-renewer");
            thread.setDaemon(true);
            return thread;
        });
        long look = Math.max(1L, Math.min(renewInterval.toMillis(), LOOK.toMillis()));
        renewer.scheduleWithFixedDelay(() -> renew(ownership), look, look, TimeUnit.MILLISECONDS);
    }

    /**
     * One look. Whatever it throws is logged and the next look still comes: a scheduled task that throws is
     * never run again, and a renewer that stopped would leave every claim to the pass again, silently.
     */
    private static void renew(PipelineActuationOwnership ownership) {
        try {
            ownership.renewDue();
        } catch (RuntimeException unexpected) {
            LOG.warn("Could not renew this member's actuation claims; looking again shortly", unexpected);
        }
    }

    @Override
    public void close() {
        if (renewer != null) {
            renewer.shutdownNow();
        }
    }
}
