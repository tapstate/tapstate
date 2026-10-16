package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimStore;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/** Keeps the reserved session live across member construction and fences a lost startup. */
final class NodeSessionStartup implements HazelcastInstanceAware, LifecycleListener {

    private final NodeSessionLease lease;
    private volatile boolean lost;
    private HazelcastInstance member;
    private boolean handedOff;
    private boolean stopped;

    NodeSessionStartup(WorkloadClaimStore store, WorkloadClaim claim, long askedAt,
            Duration ttl, Duration interval, SourceConfigKeyringSession keyring) {
        lease = new NodeSessionLease(store, claim, askedAt, ttl, interval,
                Objects.requireNonNull(keyring, "keyring"), this::lose);
    }

    NodeSessionLease lease() {
        return lease;
    }

    boolean isCurrent() {
        return !lost && lease.isCurrent();
    }

    void requireCurrent() {
        if (!isCurrent()) throw lostStartup();
    }

    /** The library supplies this handle before it starts network join. */
    @Override
    public void setHazelcastInstance(HazelcastInstance instance) {
        Stop stop;
        synchronized (this) {
            bind(instance);
            if (!isCurrent()) lost = true;
            stop = lost ? stop(true) : null;
        }
        if (stop != null) {
            lease.stopAndClose(stop::run);
        }
    }

    /** A returned handle is accepted only while the same pre-start session still proves ownership. */
    void handOff(HazelcastInstance instance) {
        Stop stop;
        boolean accepted;
        synchronized (this) {
            bind(instance);
            accepted = isCurrent();
            if (accepted) {
                handedOff = true;
                stop = null;
            } else {
                lost = true;
                // A factory can register a handle again after its in-construction member was stopped.
                stopped = true;
                stop = new Stop(instance, true);
            }
        }
        if (stop != null) lease.stopAndClose(stop::run);
        if (!accepted) throw lostStartup();
    }

    /** A failed factory must not leave either an initialized member or its renewer alive. */
    void abort() {
        Stop stop;
        synchronized (this) {
            lost = true;
            stop = stop(true);
        }
        lease.stopAndClose(() -> {
            if (stop != null) stop.run();
        });
    }

    @Override
    public void stateChanged(LifecycleEvent event) {
        if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN
                || event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
            synchronized (this) {
                stopped = true;
            }
            // Startup may fail before Spring obtains the destroy bean that owns this lease.
            lease.closeAfterMemberStops();
        }
    }

    private void lose() {
        Stop stop;
        synchronized (this) {
            lost = true;
            stop = stop(!handedOff);
        }
        // No application monitor is held while the library stops services or calls listeners.
        if (stop != null) stop.run();
    }

    private void bind(HazelcastInstance instance) {
        Objects.requireNonNull(instance, "instance");
        if (member != null && !member.getName().equals(instance.getName())) {
            throw new IllegalStateException("node-session startup received a different member");
        }
        member = instance;
    }

    /** Called only while this object's monitor is held. */
    private Stop stop(boolean force) {
        if (member == null || stopped) return null;
        stopped = true;
        return new Stop(member, force);
    }

    private static TapstateException lostStartup() {
        return new TapstateException(BootError.NODE_SESSION_LOST, Map.of(), null);
    }

    private record Stop(HazelcastInstance member, boolean force) {
        void run() {
            if (force) member.getLifecycleService().terminate();
            else member.shutdown();
        }
    }
}
