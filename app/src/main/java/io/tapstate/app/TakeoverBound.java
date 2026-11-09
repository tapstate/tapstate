package io.tapstate.app;

import io.tapstate.spi.store.StorePort;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Says at startup when the store would hold a takeover far longer than a lease.
 *
 * <p>A member that takes over an expired pipeline claim first ends what the previous owner's process left open
 * at the store, so the takeover is bounded by the lease. Where that cannot be done, because the store refuses
 * it or cannot be asked, the takeover waits for the store to end the old transaction by itself, which takes as
 * long as the store's transaction lifetime. A lifetime well above the lease is then the real bound, and
 * nothing else would tell the operator so.
 */
final class TakeoverBound {

    private static final Logger LOG = LoggerFactory.getLogger(TakeoverBound.class);

    /** How many leases the store's transaction lifetime may span before it is worth saying. */
    static final long LEASES_BEFORE_WARNING = 2;

    private TakeoverBound() {
    }

    /** Logs a warning when {@code store} lets a transaction outlive {@code lease} by more than it should. */
    static void warnIfStoreOutlastsLease(StorePort store, Duration lease) {
        Optional<Duration> lifetime;
        try {
            lifetime = store.transactionLifetimeLimit();
        } catch (RuntimeException unavailable) {
            return;
        }
        warning(lifetime, lease).ifPresent(LOG::warn);
    }

    /** The warning to log for a store transaction lifetime against a pipeline claim lease, if any. */
    static Optional<String> warning(Optional<Duration> lifetime, Duration lease) {
        Objects.requireNonNull(lifetime, "lifetime");
        Objects.requireNonNull(lease, "lease");
        if (lifetime.isEmpty() || lifetime.get().compareTo(lease.multipliedBy(LEASES_BEFORE_WARNING)) <= 0) {
            return Optional.empty();
        }
        return Optional.of("The store lets a transaction stay open for " + lifetime.get().toSeconds()
                + " s, more than " + LEASES_BEFORE_WARNING + " pipeline claim leases of " + lease.toSeconds()
                + " s. A member taking over from an owner that stopped inside a write ends that owner's open"
                + " transactions first; where it cannot, the takeover waits up to the store's limit. Lower"
                + " transactionLifetimeLimitSeconds on the store, or raise the claim lease, to bound it.");
    }
}
