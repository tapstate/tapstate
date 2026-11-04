package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.impl.JetServiceBackend;
import java.util.OptionalLong;

/**
 * Exact local live execution identity from the pinned Jet implementation. No metrics, arrival order
 * or numeric ID ordering is used. This implementation seam must be revalidated on a Jet upgrade.
 * Absence is normal before execution/on nonparticipants/after completion, never an exit receipt.
 */
public final class JetLiveExecution {
    private JetLiveExecution() {}
    public static OptionalLong current(HazelcastInstance member, long jobId) {
        HazelcastInstanceImpl actual;
        if (member instanceof HazelcastInstanceProxy proxy) actual = proxy.getOriginal();
        else if (member instanceof HazelcastInstanceImpl embedded) actual = embedded;
        else return OptionalLong.empty();
        JetServiceBackend service = actual.node.getNodeEngine().getService(JetServiceBackend.SERVICE_NAME);
        Long execution = service.getJobExecutionService().getExecutionIdForJobId(jobId);
        return execution == null ? OptionalLong.empty() : OptionalLong.of(execution);
    }
}
