package io.tapstate.runtime.srs;

import com.hazelcast.jet.pipeline.StreamSource;
import io.tapstate.spi.capture.Subscription;

import java.util.Objects;
import java.util.Optional;
import java.util.Map;

/**
 * The handle a {@link CaptureRunUnit#start started} source run hands back — what the assembly did and the
 * live pieces the downstream pipeline wires onto.
 *
 * <ul>
 *   <li>{@code chainId} — the mining chain provisioned for this run, present only on the shared-ring path;
 *       a snapshot-only or srs-disabled run opens no shared chain.</li>
 *   <li>{@code merged} — whether provisioning force-merged onto an already-open chain (a config coinciding
 *       with a running capture) rather than opening a fresh one; false when no chain was provisioned.</li>
 *   <li>{@code snapshotCount} / {@code snapshotCounts} — rows read by the time this handle was created;
 *       an asynchronously activated snapshot begins at zero and reports live counts through its health
 *       and the coordinator's pass-through callback.</li>
 *   <li>{@code ringSource} — the self-built Jet source over the change ring, present only on the shared-ring
 *       path; the downstream reads the cdc tail from it.</li>
 *   <li>{@code cdcSubscription} — the close handle for a running tail or reserved snapshot worker;
 *       a snapshot-only run may carry the latter until its bounded read ends.</li>
 * </ul>
 *
 * <p>The handle is itself {@link AutoCloseable}: closing it tears the running capture down by closing the
 * cdc subscription or snapshot reservation it carries. A run that opened neither has no subscription, so
 * closing it is a safe no-op. The subscription contract stops the capture with no checked
 * exception and is idempotent, so this close needs neither a throws clause nor its own guard.
 */
public record CaptureRun(
        Optional<MiningChainId> chainId,
        boolean merged,
        long snapshotCount,
        Map<String, Long> snapshotCounts,
        Optional<StreamSource<SrsItem>> ringSource,
        Optional<Subscription> cdcSubscription,
        CaptureHealth health) implements AutoCloseable {

    public CaptureRun(
            Optional<MiningChainId> chainId,
            boolean merged,
            long snapshotCount,
            Optional<StreamSource<SrsItem>> ringSource,
            Optional<Subscription> cdcSubscription,
            CaptureHealth health) {
        this(chainId, merged, snapshotCount, Map.of(), ringSource, cdcSubscription, health);
    }

    public CaptureRun {
        Objects.requireNonNull(chainId, "chainId");
        Objects.requireNonNull(snapshotCounts, "snapshotCounts");
        Objects.requireNonNull(ringSource, "ringSource");
        Objects.requireNonNull(cdcSubscription, "cdcSubscription");
        Objects.requireNonNull(health, "health");
        snapshotCounts = Map.copyOf(snapshotCounts);
    }

    /**
     * The failure the run's cdc stream died with, or empty while it is healthy or opened no tail. The
     * stream reports a failure on its own thread, so this is how a caller learns a tail died rather than
     * merely going quiet.
     */
    public Optional<Throwable> failure() {
        return health.failure();
    }

    /** Starts a reserved data-plane snapshot after the downstream job is ready to drain it. */
    public void activateSnapshot() {
        cdcSubscription.ifPresent(subscription -> {
            if (subscription instanceof SnapshotActivation activation) {
                activation.activateSnapshot();
            }
        });
    }

    /** Stops the capture by closing its cdc subscription, or does nothing when the run opened no tail. */
    @Override
    public void close() {
        cdcSubscription.ifPresent(Subscription::close);
    }
}
