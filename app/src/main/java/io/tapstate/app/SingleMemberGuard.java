package io.tapstate.app;

import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/** A SINGLE member that has seen a peer cannot resume work when that peer disappears. */
final class SingleMemberGuard implements InitialMembershipListener {
    private static final Logger LOG = LoggerFactory.getLogger(SingleMemberGuard.class);
    interface ProcessExit { void exit(); }
    private final ProcessExit processExit;
    private final String cluster;
    private HazelcastInstance running;
    private volatile String mismatch;

    SingleMemberGuard(String cluster, ProcessExit processExit) { this.cluster = cluster; this.processExit = processExit; }

    @Override public void init(InitialMembershipEvent event) { observe(event.getMembers()); }
    @Override public void memberAdded(MembershipEvent event) { observe(event.getMembers()); }
    @Override public void memberRemoved(MembershipEvent event) { }

    synchronized void observe(Set<Member> members) {
        if (members.size() <= 1 || mismatch != null) return;
        var identities = members.stream().map(member -> member.getUuid() + "@" + member.getAddress()).sorted().toList();
        mismatch = "tapstate.cluster.profile=single observed " + members.size() + " Hazelcast members in cluster "
                + cluster + ": " + identities + ". The local data member cannot run. For single-node OP, isolate the member"
                + " on loopback with discovery NONE. For a cluster, configure every member with the appropriate"
                + " multi-member profile and discovery settings, then restart.";
        LOG.error("{}", mismatch);
        var local = running;
        if (local != null || processExit != null) {
            // Hazelcast delivers membership events on its event thread. Stop the local member off that thread.
            Thread.ofPlatform().daemon().name("tapstate-single-member-stop")
                    .start(() -> {
                        try {
                            if (local != null) local.getLifecycleService().terminate();
                        } finally {
                            if (processExit != null) processExit.exit();
                        }
                    });
        }
    }

    boolean intact() { return mismatch == null; }

    void requireSingle() {
        if (!intact()) throw new TapstateException(BootError.DISCOVERY_CONFIG_INVALID, Map.of("detail", mismatch), null);
    }

    synchronized void started(HazelcastInstance member) {
        requireSingle();
        running = member;
    }
}
