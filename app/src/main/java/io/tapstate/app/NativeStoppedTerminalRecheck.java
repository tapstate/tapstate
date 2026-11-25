package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.Objects;

/** Revalidates a locally completed Stop without taking or mutating any ownership. */
final class NativeStoppedTerminalRecheck implements StoppedTerminalRecheck {
    private final MongoConfigurationDomain configuration;
    private final MongoCacheDomain cache;
    private final Engine engine;
    private final HazelcastExecutionPlans plans;
    private final HazelcastOperatorReadiness operators;
    private final Runnable active;

    NativeStoppedTerminalRecheck(MongoConfigurationDomain configuration, MongoCacheDomain cache, Engine engine,
            HazelcastExecutionPlans plans, HazelcastOperatorReadiness operators, Runnable active) {
        this.configuration = Objects.requireNonNull(configuration); this.cache = Objects.requireNonNull(cache);
        this.engine = Objects.requireNonNull(engine); this.plans = Objects.requireNonNull(plans);
        this.operators = Objects.requireNonNull(operators); this.active = Objects.requireNonNull(active);
    }

    static boolean localEvidence(com.hazelcast.core.HazelcastInstance member, ClusterMembershipGate membership,
            io.tapstate.spi.store.WorkloadClaimFence original) {
        var committed = membership.committed();
        if (committed == null || !committed.clusterId().equals(original.key().clusterId()) || committed.revision() != original.topologyRevision()
                || !committed.activeNodeIds().equals(java.util.Set.of(original.owner().nodeId()))) return false;
        var data = member.getCluster().getMembers().stream().filter(candidate -> !candidate.isLiteMember()).toList();
        return data.size() == 1 && data.getFirst().localMember()
                && original.owner().nodeId().equals(ClusterMembershipGate.stableIdOf(data.getFirst()));
    }

    @Override public boolean unchanged(PipelineIntentSnapshot processed, CheckpointDoc checkpoint,
            WorkloadClaimFence original, Tenure tenure) {
        Objects.requireNonNull(processed); Objects.requireNonNull(checkpoint); Objects.requireNonNull(original); Objects.requireNonNull(tenure);
        var desired = processed.desired();
        if (desired == null || desired.targetState() != PipelineState.STOPPED || desired.purgeState()
                || desired.reassemble() || desired.rebuildOwedAt(checkpoint)) return false;
        active.run();
        if (configuration.intents().loadForActuation(processed.scope().pipelineId()).filter(processed::equals).isEmpty()
                || !cache.stoppedCheckpointUnchanged(processed.scope(), checkpoint, original, tenure == Tenure.ENDED, active)) return false;
        if (engine.isLost()) return false;
        var run = engine.captureRun(processed.scope().pipelineId());
        if (run.isPresent()) {
            if (!run.get().status().isTerminal()) return false;
            var identity = run.get().operatorManifest().executionIdentity();
            if (!(identity instanceof ExecutionFence fence)
                    || !HazelcastExecutionPlans.originalOrPrior(fence, processed.scope(), original)) return false;
        }
        if (!plans.terminalAt(processed.scope(), original)) return false;
        operators.requireTerminal(processed.scope(), original, active);
        active.run();
        return !engine.isLost()
                && cache.stoppedCheckpointUnchanged(processed.scope(), checkpoint, original, tenure == Tenure.ENDED, active)
                && configuration.intents().loadForActuation(processed.scope().pipelineId()).filter(processed::equals).isPresent();
    }
}
