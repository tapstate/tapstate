package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import io.tapstate.spi.store.StorePort;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ends what a member left open at the store, once it has left the cluster.
 *
 * <p>A member killed halfway through a write to the store leaves the write's transaction open, and the store
 * goes on holding whatever that transaction wrote until it gives up on it by itself - a minute later by default.
 * Everything else that writes the same records waits behind it for all of that time: the sinks reporting their
 * progress on the chain it was writing, the capture whose claim it was proving as it appended a change, the runs
 * that replace the ones it took part in. The member cannot end the transaction, having gone, so the members left
 * do, as soon as the cluster tells them it has gone: its store client was named for the start of the process it
 * belonged to, which is the boot id the member carried, and the store ends every transaction a client of that
 * name left open.
 *
 * <p>Every member left asks, and asking again ends nothing twice. The asking runs off the thread the cluster tells
 * its listeners on, which is no thread to wait on a store from.
 *
 * <p>A member cut off from the others sees them leave as well, and ends what they have open at that moment. That
 * costs them no more than a conflict does: the write is thrown away, and a write made in a transaction the store
 * retries is made again. Asking only from the side of the cluster that carries on was weighed and not done: which
 * side that is settles a moment after a member leaves, and the moment right after is when this has to act.
 */
final class DepartedMemberTransactions implements MembershipListener {

    private static final Logger LOG = LoggerFactory.getLogger(DepartedMemberTransactions.class);

    private final StorePort store;
    private final Executor ending;

    DepartedMemberTransactions(StorePort store) {
        this(store, anEndingThreadWhileThereIsWork());
    }

    DepartedMemberTransactions(StorePort store, Executor ending) {
        this.store = Objects.requireNonNull(store, "store");
        this.ending = Objects.requireNonNull(ending, "ending");
    }

    @Override
    public void memberAdded(MembershipEvent event) {
        // A member joining leaves nothing open that anybody else is waiting on.
    }

    @Override
    public void memberRemoved(MembershipEvent event) {
        Member departed = event.getMember();
        String bootId = departed.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE);
        if (bootId == null) {
            // A member this product did not start names no process whose store client could be found.
            return;
        }
        String nodeId = ClusterMembershipGate.stableIdOf(departed);
        ending.execute(() -> end(nodeId, bootId));
    }

    private void end(String nodeId, String bootId) {
        try {
            int ended = store.endTransactionsLeftOpenBy(bootId);
            if (ended > 0) {
                LOG.warn("Ended {} store transaction(s) that member {} left open when it left the cluster",
                        ended, nodeId);
            }
        } catch (RuntimeException failure) {
            LOG.warn("Could not end the store transactions member {} may have left open when it left the cluster;"
                    + " the store ends them itself once they have been open for as long as it allows", nodeId,
                    failure);
        }
    }

    /** One thread, started when there is something to end and gone again once there has been nothing for a while. */
    private static Executor anEndingThreadWhileThereIsWork() {
        return new ThreadPoolExecutor(0, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), runnable -> {
            Thread thread = new Thread(runnable, "tapstate-departed-member-transactions");
            thread.setDaemon(true);
            return thread;
        });
    }
}
