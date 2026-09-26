package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.store.ConnectionTester;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.OperatorStateStores;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimStore;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;

/**
 * Wires the data-plane actuation binding into the assembly root: the Jet {@link Engine} over the embedded
 * member, the topology source, the source-side capture plane (the mining-chain coordinator, the PDK capture
 * port, and the run unit that assembles a capture), and the {@link LifecycleActuator} that joins the converge
 * loop to both the engine and the capture. Split from the convergence loop's wiring so the loop can be
 * brought up in isolation over a stand-in actuator, and gated on the same store switch as the convergence it
 * serves - a run with no store drives no pipeline, so it needs neither the loop, the engine binding, nor the
 * capture plane. The topology source and the capture plane read pipelines and sources from the same store, so
 * they are gated with the rest of the binding.
 */
@Configuration
@ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
class DataPlaneActuationConfiguration {

    @Bean
    Engine engine(HazelcastInstance hazelcastMember, @Nullable OperatorStateStores operatorStateStores) {
        return new Engine(hazelcastMember, operatorStateStores);
    }

    /**
     * The store probe's deadline. Short on purpose: this runs while a person waits for `start` to answer,
     * and a store that has not replied in this long is not one the pipeline was about to write to either.
     */
    private static final Duration STORE_PROBE_TIMEOUT = Duration.ofSeconds(10);

    /**
     * How long a start waits for every member to say whether it can load the pipeline's sink connectors. A member
     * seeing an artifact for the first time downloads and stages it, so this allows for that; a member that has
     * not answered by then refuses the start rather than holding the pass that is starting it.
     */
    private static final Duration CONNECTOR_READINESS_TIMEOUT = Duration.ofSeconds(60);

    /**
     * The topology builder, with every source vertex it builds held to this member. A start runs its capture
     * here before it builds, so this member's hand-off is the one the sources have to drain; left to the
     * engine, a source lands on another member as often as not on a cluster and reads nothing, healthily.
     */
    @Bean
    DagSource dagSource(StorePort storePort, NestSettings nestSettings, ConnectionTester connectionTester,
            HazelcastInstance hazelcastMember, ParallelismBudget parallelismBudget) {
        return new StoreBackedDagSource(storePort, nestSettings,
                StoreReachability.probing(connectionTester, STORE_PROBE_TIMEOUT),
                SourcePlacement.on(hazelcastMember.getCluster().getLocalMember().getAddress()),
                () -> dataMembers(hazelcastMember), parallelismBudget);
    }

    /**
     * The members a run submitted now would take part on, by stable id: every member that holds data, which in
     * this product is every member - none joins as a lite member. A member that names no stable id is named by
     * its engine identity instead.
     */
    private static List<String> dataMembers(HazelcastInstance member) {
        return ClusterMembershipGate.dataMembers(member);
    }

    /** The plans of the runs the cluster is executing, written by whichever member submits each. */
    @Bean
    HazelcastExecutionPlans executionPlans(HazelcastInstance hazelcastMember) {
        return new HazelcastExecutionPlans(hazelcastMember);
    }

    /**
     * The limits a node's per-member width is held to. Every one is countable before anything is opened, so
     * the same configuration is judged the same way on every connector version.
     */
    @Bean
    ParallelismBudget parallelismBudget(
            @Value("${tapstate.execution.max-local-parallelism:16}") int maxLocalParallelism,
            @Value("${tapstate.execution.max-connector-instances-per-member:8}") int maxConnectorInstances,
            @Value("${tapstate.execution.max-buffered-records-per-member:262144}") long maxBufferedRecords,
            @Value("${tapstate.execution.max-blocking-processors-per-member:128}") int maxBlockingProcessors) {
        return new ParallelismBudget(maxLocalParallelism, maxConnectorInstances, maxBufferedRecords,
                maxBlockingProcessors);
    }

    @Bean
    SrsCoordinator srsCoordinator(SrsMetaStore srsMetaStore) {
        return new SrsCoordinator(srsMetaStore);
    }

    @Bean
    CapturePort capturePort(ConnectorProvisioner connectorProvisioner,
            @Nullable KeyedStateStore keyedStateStore,
            @Value("${tapstate.capture.log-miner-preflight-timeout:30s}") Duration preflightTimeout) {
        return new PdkCapturePort(connectorProvisioner, keyedStateStore, preflightTimeout);
    }

    @Bean
    SnapshotBuffer snapshotBuffer() {
        // The one member-local buffer that carries snapshot rows from the capture coordinator (the writer) to
        // the source vertices (the readers). It is bound into the member user context so a source resolves it
        // member-side, and injected into the coordinator so the snapshot pass-through fills it -- the same
        // instance on both sides, so what the coordinator writes is exactly what a source drains.
        return new SnapshotBuffer();
    }

    @Bean
    CaptureRunUnit captureRunUnit(CapturePort capturePort, SrsCoordinator srsCoordinator,
            SrsMetaStore srsMetaStore, HazelcastInstance hazelcastMember) {
        return new CaptureRunUnit(capturePort, srsCoordinator, srsMetaStore, hazelcastMember);
    }

    @Bean
    CaptureOwnership captureOwnership(
            HazelcastInstance hazelcastMember,
            ClusterProperties clusterProperties,
            ClusterMembershipGate membershipGate,
            ClusterWorkloadClaims workloadClaims) {
        if (clusterProperties.getProfile() == ClusterProperties.Profile.SINGLE) {
            return CaptureOwnership.single();
        }
        Object stored = hazelcastMember.getUserContext().get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY);
        if (!(stored instanceof WorkloadClaim nodeSession)) {
            throw new IllegalStateException("cluster member started without its node-session identity");
        }
        return new CaptureOwnership(
                clusterProperties.getId(), nodeSession.owner(), membershipGate,
                workloadClaims, clusterProperties.getWorkloadClaimTtl());
    }

    /**
     * This member's own answer to whether a run may still touch anything outside the cluster, bound onto
     * the member so a sink vertex that lands here -- carrying the generations of the run that submitted
     * it -- can ask without a store round trip per batch. A bean rather than something the member factory
     * makes, because it keeps a refresh thread and the container is what knows when to stop it.
     *
     * <p>A single-node run has one member and one run of anything, so it binds nothing and every resolve
     * answers with a guard that allows everything -- which leaves that path exactly as it was.
     */
    @Bean(destroyMethod = "close")
    ExecutionAuthorization executionAuthorization(
            HazelcastInstance hazelcastMember,
            ClusterProperties clusterProperties,
            WorkloadClaimStore workloadClaimStore) {
        if (clusterProperties.getProfile() == ClusterProperties.Profile.SINGLE) {
            return ExecutionAuthorization.unfenced();
        }
        ExecutionAuthorization authorization = new ExecutionAuthorization(
                clusterProperties.getId(), workloadClaimStore,
                clusterProperties.getWorkloadClaimRenewInterval());
        hazelcastMember.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY, authorization);
        return authorization;
    }

    /**
     * The one way a failed run is replaced with nobody asking. It is here rather than beside the converge
     * loop because the question it answers is about members and claims, which is what this configuration
     * knows: the loop is only told yes or no.
     *
     * <p>A single-node run never rebuilds anything. There is no member whose leaving could have ended the
     * run, so every death there is the pipeline's own and stays recorded as one.
     *
     * <p>The spacing between attempts is one claim lease. That is already this cluster's own answer to
     * "how long before ownership has settled", so a rebuild that waits it out is rebuilding into a
     * membership that has stopped moving rather than into the middle of a handover.
     */
    @Bean
    RebuildAdmission rebuildAdmission(
            ClusterProperties clusterProperties, PipelineActuationOwnership pipelineActuationOwnership,
            Engine engine) {
        if (clusterProperties.getProfile() == ClusterProperties.Profile.SINGLE) {
            return RebuildAdmission.never();
        }
        return new ClusterRebuildAdmission(
                pipelineActuationOwnership,
                pipelineId -> engine.failureOf(pipelineId)
                        .map(ClusterRebuildAdmission::isMembershipChangedBeforeStart).orElse(false),
                clusterProperties.getWorkloadClaimTtl());
    }

    /**
     * Who drives a pipeline's lifecycle. Wired beside the capture owner because the two are the same
     * mechanism over two different resources, and both need the same three things: this member's durable
     * identity, the membership gate that decides whether it may hold business claims at all, and the claim
     * facade that enforces it. A single-node run has one member, so nothing is fenced there.
     */
    @Bean
    PipelineActuationOwnership pipelineActuationOwnership(
            HazelcastInstance hazelcastMember,
            ClusterProperties clusterProperties,
            ClusterMembershipGate membershipGate,
            ClusterWorkloadClaims workloadClaims) {
        if (clusterProperties.getProfile() == ClusterProperties.Profile.SINGLE) {
            return PipelineActuationOwnership.single();
        }
        Object stored = hazelcastMember.getUserContext().get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY);
        if (!(stored instanceof WorkloadClaim nodeSession)) {
            throw new IllegalStateException("cluster member started without its node-session identity");
        }
        return new PipelineActuationOwnership(
                clusterProperties.getId(), nodeSession.owner(), membershipGate, workloadClaims,
                clusterProperties.getWorkloadClaimTtl(), clusterProperties.getWorkloadClaimRenewInterval());
    }

    @Bean
    PipelineCaptureCoordinator pipelineCaptureCoordinator(
            StorePort storePort, CaptureRunUnit captureRunUnit, SrsCoordinator srsCoordinator,
            SnapshotBuffer snapshotBuffer, CaptureOwnership captureOwnership,
            ClusterProperties clusterProperties) {
        // Begun rather than started: a run comes back as soon as its load is open, and the load is read while
        // the pipeline's job takes it. Read to the end first, it would have to fit on the heap whole.
        if (clusterProperties.getProfile() == ClusterProperties.Profile.SINGLE) {
            return new StoreBackedPipelineCaptureCoordinator(
                    storePort, captureRunUnit::begin, srsCoordinator, snapshotBuffer);
        }
        return new StoreBackedPipelineCaptureCoordinator(
                storePort, captureRunUnit::begin, srsCoordinator, snapshotBuffer,
                captureOwnership, clusterProperties.getWorkloadClaimRenewInterval());
    }

    @Bean
    NestStateTeardown nestStateTeardown(
            HazelcastInstance hazelcastMember, OperatorStateStores operatorStateStores) {
        return new NestStateTeardown(hazelcastMember, operatorStateStores);
    }

    @Bean
    LifecycleActuator lifecycleActuator(Engine engine, DagSource dagSource,
            PipelineCaptureCoordinator pipelineCaptureCoordinator, NestStateTeardown nestStateTeardown,
            PipelineActuationOwnership pipelineActuationOwnership, HazelcastExecutionPlans executionPlans,
            HazelcastInstance hazelcastMember) {
        return new EngineLifecycleActuator(engine, dagSource, pipelineCaptureCoordinator, nestStateTeardown,
                pipelineActuationOwnership, executionPlans, Clock.systemUTC(),
                new HazelcastConnectorReadiness(hazelcastMember, CONNECTOR_READINESS_TIMEOUT));
    }
}
