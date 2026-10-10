package io.tapstate.control.restapi;

import io.tapstate.control.core.ClusterPipelineTopologyService;
import io.tapstate.control.core.ClusterTopologyService;
import io.tapstate.control.core.LiveClusterMember;
import io.tapstate.control.core.LiveClusterMembers;
import io.tapstate.control.core.LivePipelineProcessor;
import io.tapstate.control.core.LivePipelineRun;
import io.tapstate.control.core.LivePipelineRuns;
import io.tapstate.control.core.LivePipelineVertex;
import io.tapstate.control.core.PipelineCaptures;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNodeReading;
import io.tapstate.spi.store.ClusterNodeRegistration;
import io.tapstate.spi.store.ClusterNodeReservation;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.net.URI;
import java.util.Map;

/**
 * Two live members, a departed node, and a reserved boot carrying one pipeline, for HTTP contract cases.
 * The engine is faked because
 * what these cases are about is the wire: which fields cross it, and who is allowed to ask. Whether the
 * engine's own member list and its runs are read correctly is a question for the layer that reads them,
 * over real members.
 */
@Configuration
class ClusterTopologyTestConfiguration {

    static final String CLUSTER = "01J5AUTHFIXTURE";
    static final String PIPELINE = "orders";
    static final String CAPTURE = "capture-f00d";

    static final LiveClusterMember FIRST = new LiveClusterMember(
            "node-a", "8b0a1e6e-0000-4000-8000-00000000000a", "boot-a1",
            "[127.0.0.1]:5701", "https://node-a.example:8443");

    static final LiveClusterMember SECOND = new LiveClusterMember(
            "node-b", "8b0a1e6e-0000-4000-8000-00000000000b", "boot-b1",
            "[127.0.0.1]:5702", "https://node-b.example:8443");

    static final Instant MEASURED_AT = Instant.parse("2026-09-19T08:30:00Z");
    static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile(
            CLUSTER, 2, new ExecutionProfile(1, Map.of("build", "fixture")));
    static final LiveClusterMember DEPARTED = new LiveClusterMember(
            "node-c", "uuid-c", "boot-c1", "[127.0.0.1]:5703", "https://node-c.example:8443");
    static final LiveClusterMember RESERVED = new LiveClusterMember(
            "node-d", null, "boot-d1", null, "https://node-d.example:8443");

    @Bean
    ClusterTopologyService clusterTopologyService() {
        LiveClusterMembers members = () -> List.of(SECOND, FIRST);
        return new ClusterTopologyService(members, membership(), pipelines(), CLUSTER, profiles());
    }

    private static ClusterMembershipStore membership() {
        return new ClusterMembershipStore() {
            @Override
            public Optional<ClusterMembership> read(String clusterId) {
                return Optional.of(new ClusterMembership(CLUSTER, 4, Set.of("node-a", "node-b", "node-c")));
            }

            @Override
            public ClusterMembership createIfAbsent(String clusterId, Set<String> activeNodeIds) {
                throw new UnsupportedOperationException("a read face commits nothing");
            }

            @Override
            public Optional<ClusterMembership> compareAndSet(
                    String clusterId, long expectedRevision, Set<String> activeNodeIds) {
                throw new UnsupportedOperationException("a read face commits nothing");
            }
        };
    }

    private static ClusterProfileStore profiles() {
        return new ClusterProfileStore() {
            @Override
            public ClusterNodeReservation reserve(String clusterId, WorkloadOwner owner, URI controlUrl,
                    ExecutionProfile proposed, Duration ttl) {
                throw new UnsupportedOperationException("a read face reserves nothing");
            }

            @Override
            public Optional<ClusterExecutionProfile> profile(String clusterId) {
                return Optional.of(PROFILE);
            }

            @Override
            public List<ClusterNodeReading> nodes(String clusterId) {
                return List.of(node(FIRST, true, 21), node(SECOND, true, 28),
                        node(DEPARTED, true, -7), node(RESERVED, false, 15));
            }

            @Override
            public boolean markJoined(WorkloadClaim expectedSession, String memberUuid, String memberAddress) {
                throw new UnsupportedOperationException("a read face records nothing");
            }
        };
    }

    private static ClusterNodeReading node(LiveClusterMember member, boolean joined, long remainingSeconds) {
        WorkloadClaim session = new WorkloadClaim(
                new WorkloadClaimKey(CLUSTER, WorkloadClaimType.NODE_SESSION, member.nodeId()),
                new WorkloadOwner(member.nodeId(), member.bootId()), 1, 0, 0,
                MEASURED_AT.plusSeconds(remainingSeconds), 0, 0, Set.of(), 0, false, PROFILE.generation());
        return new ClusterNodeReading(new ClusterNodeRegistration(session, PROFILE, URI.create(member.controlUrl()),
                joined, joined ? member.memberUuid() : null, joined ? member.hzAddress() : null,
                joined ? MEASURED_AT.minusSeconds(60) : null), Duration.ofSeconds(remainingSeconds));
    }

    private static ClusterPipelineTopologyService pipelines() {
        return new ClusterPipelineTopologyService(
                runs(), pipelineId -> List.of(CAPTURE), claims(), desired(), CLUSTER);
    }

    /**
     * One vertex pinned to the second member, and one spread over both. The pinned vertex also carries
     * the placeholder instance the engine puts on the member that is not running it, so the wire case
     * shows whether those are filtered before they are published.
     */
    private static LivePipelineRuns runs() {
        return () -> List.of(new LivePipelineRun(
                PIPELINE,
                "1002-8287-ca81-0001",
                MEASURED_AT,
                Set.of(FIRST.memberUuid(), SECOND.memberUuid()),
                List.of(
                        new LivePipelineVertex("source", List.of(
                                new LivePipelineProcessor(0, SECOND.memberUuid(), true),
                                new LivePipelineProcessor(1, FIRST.memberUuid(), false))),
                        new LivePipelineVertex("serve-orders", List.of(
                                new LivePipelineProcessor(0, SECOND.memberUuid(), true),
                                new LivePipelineProcessor(1, FIRST.memberUuid(), true))))));
    }

    private static DesiredStore desired() {
        return new DesiredStore() {
            @Override
            public void save(DesiredState state) {
                throw new UnsupportedOperationException("a read face writes nothing");
            }

            @Override
            public Optional<DesiredState> read(String pipelineId) {
                return Optional.empty();
            }

            @Override
            public List<String> pipelineIds() {
                return List.of(PIPELINE);
            }

            @Override
            public void delete(String pipelineId) {
                throw new UnsupportedOperationException("a read face writes nothing");
            }
        };
    }

    private static WorkloadClaimStore claims() {
        return new WorkloadClaimStore() {
            @Override
            public WorkloadClaimAttempt acquire(
                    WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
                throw new UnsupportedOperationException("a read face takes nothing");
            }

            @Override
            public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
                throw new UnsupportedOperationException("a read face takes nothing");
            }

            @Override
            public boolean release(WorkloadClaim expected) {
                throw new UnsupportedOperationException("a read face takes nothing");
            }

            @Override
            public Optional<WorkloadClaim> advanceExecution(
                    WorkloadClaim expected, long revision, java.util.Set<String> executionNodeIds) {
                throw new UnsupportedOperationException("a read face takes nothing");
            }

            @Override
            public Optional<WorkloadClaim> recordExecutionFailure(
                    WorkloadClaim expected, boolean afterMemberLoss) {
                throw new UnsupportedOperationException("a read face takes nothing");
            }

            @Override
            public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
                if (key.type() == WorkloadClaimType.PIPELINE_ACTUATION) {
                    return Optional.of(new WorkloadClaimReading(
                            new WorkloadClaim(key, new WorkloadOwner("node-b", "boot-b1"), 3, 7, 4,
                                    MEASURED_AT.plusSeconds(30)),
                            Duration.ofSeconds(21)));
                }
                return Optional.of(new WorkloadClaimReading(
                        new WorkloadClaim(key, new WorkloadOwner("node-a", "boot-a1"), 1, 1, 4,
                                MEASURED_AT.plusSeconds(30)),
                        Duration.ofSeconds(28)));
            }
        };
    }
}
