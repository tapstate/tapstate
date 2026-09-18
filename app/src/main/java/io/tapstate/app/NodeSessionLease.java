package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Keeps a pre-join node-session claim live and removes the member when that proof is lost. */
final class NodeSessionLease implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(NodeSessionLease.class);

    private final WorkloadClaimStore store;
    private final AtomicReference<WorkloadClaim> current;
    private final Duration ttl;
    private final Runnable lost;
    private final ScheduledExecutorService renewer;
    private final AtomicBoolean closed = new AtomicBoolean();

    private NodeSessionLease() {
        this.store = null;
        this.current = null;
        this.ttl = Duration.ZERO;
        this.lost = () -> { };
        this.renewer = null;
        this.closed.set(true);
    }

    static NodeSessionLease inactive() {
        return new NodeSessionLease();
    }

    NodeSessionLease(
            WorkloadClaimStore store,
            WorkloadClaim initial,
            Duration ttl,
            Duration renewInterval,
            Runnable lost) {
        this.store = Objects.requireNonNull(store, "store");
        this.current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.lost = Objects.requireNonNull(lost, "lost");
        Objects.requireNonNull(renewInterval, "renewInterval");
        this.renewer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "tapstate-node-session-renewer");
            thread.setDaemon(true);
            return thread;
        });
        renewer.scheduleWithFixedDelay(
                this::renew, renewInterval.toMillis(), renewInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void renew() {
        if (closed.get()) {
            return;
        }
        WorkloadClaim expected = current.get();
        try {
            Optional<WorkloadClaim> renewed = store.renew(expected, ttl);
            if (renewed.isPresent()) {
                current.set(renewed.get());
                return;
            }
            lose("the stored owner or generation no longer matches", null);
        } catch (RuntimeException unavailable) {
            lose("the coordination store could not renew the node session", unavailable);
        }
    }

    private void lose(String reason, RuntimeException cause) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (cause == null) {
            LOG.error("Node session lost: {}. Shutting down the Hazelcast member.", reason);
        } else {
            LOG.error("Node session lost: {}. Shutting down the Hazelcast member.", reason, cause);
        }
        renewer.shutdownNow();
        lost.run();
    }

    @Override
    public void close() {
        if (store == null) {
            return;
        }
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        renewer.shutdownNow();
        try {
            store.release(current.get());
        } catch (RuntimeException unavailable) {
            LOG.warn("Could not release the node session during shutdown; it will expire by lease.", unavailable);
        }
    }
}
