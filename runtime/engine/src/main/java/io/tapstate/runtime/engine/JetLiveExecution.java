package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.JetServiceBackend;
import java.io.Serializable;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Exact local live execution identity from the pinned Jet implementation. No metrics, arrival order
 * or numeric ID ordering is used. This implementation seam must be revalidated on a Jet upgrade.
 * Absence is normal before execution/on nonparticipants/after completion, never an exit receipt.
 */
public final class JetLiveExecution {
    private JetLiveExecution() {}

    public static OptionalLong current(HazelcastInstance member, long jobId) {
        var actual = localMember(member);
        if (actual == null) return OptionalLong.empty();
        Long execution = jetService(actual).getJobExecutionService().getExecutionIdForJobId(jobId);
        return execution == null ? OptionalLong.empty() : OptionalLong.of(execution);
    }

    /** A coordinator has an exact attempt before the local writer execution context has opened. */
    static OptionalLong forHistory(HazelcastInstance member, long jobId) {
        var local = current(member, jobId);
        if (local.isPresent()) return local;
        var actual = localMember(member);
        if (actual == null) return OptionalLong.empty();
        var context = jetService(actual).getJobCoordinationService().getMasterContext(jobId);
        if (context == null) return OptionalLong.empty();
        var status = context.jobStatus();
        long execution = context.executionId();
        return execution != 0 && (status == JobStatus.STARTING || status == JobStatus.RUNNING)
                ? OptionalLong.of(execution) : OptionalLong.empty();
    }

    static Set<String> historyParticipantAddresses(HazelcastInstance member, long jobId, long executionId) {
        var actual = localMember(member);
        if (actual == null) return Set.of();
        var context = jetService(actual).getJobExecutionService().getExecutionContext(executionId);
        if (context == null || context.jobId() != jobId || context.participants() == null) return Set.of();
        return context.participants().stream().map(Object::toString).collect(Collectors.toSet());
    }

    enum StartupPhase { PROCESSOR, PLANNING }

    record Startup(String pipelineId, long jobId, long executionId, Serializable identity, StartupPhase phase) {
        boolean isLocalTo(HazelcastInstance member) {
            HazelcastInstanceImpl actual;
            try {
                actual = localMember(member);
            } catch (HazelcastInstanceNotActiveException stopped) {
                return false;
            }
            if (actual == null || !actual.isRunning()) return false;
            var service = jetService(actual);
            if (phase == StartupPhase.PLANNING) {
                // A meta-supplier can reject the plan before any member has an execution context.
                var context = service.getJobCoordinationService().getMasterContext(jobId);
                return context != null && context.executionId() == executionId
                        && pipelineId.equals(context.jobConfig().getName())
                        && identity.equals(context.jobConfig().getArgument(Engine.RUN_IDENTITY_ARGUMENT));
            }
            var context = service.getJobExecutionService().getExecutionContext(executionId);
            // The run identity comes from the immutable JobConfig given to this exact processor.
            return context != null && context.jobId() == jobId && pipelineId.equals(context.jobName())
                    && context.coordinator().equals(actual.node.getNodeEngine().getMasterAddress());
        }
    }

    private static HazelcastInstanceImpl localMember(HazelcastInstance member) {
        if (member instanceof HazelcastInstanceProxy proxy) return proxy.getOriginal();
        return member instanceof HazelcastInstanceImpl embedded ? embedded : null;
    }

    private static JetServiceBackend jetService(HazelcastInstanceImpl member) {
        return member.node.getNodeEngine().getService(JetServiceBackend.SERVICE_NAME);
    }
}
