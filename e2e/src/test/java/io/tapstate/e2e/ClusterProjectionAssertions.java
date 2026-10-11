package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ClusterClaimView;
import io.tapstate.control.core.ClusterMemberState;
import io.tapstate.control.core.ClusterRecoveryItemView;
import io.tapstate.control.core.ClusterRecoveryView;
import io.tapstate.spi.store.WorkloadClaimType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;

/** Cross-checks observed cluster facts without deriving membership, placement or recovery policy. */
final class ClusterProjectionAssertions {
    private ClusterProjectionAssertions() { }

    /** Cross-checks settled scenes against native membership and independent Mongo documents. */
    static void assertProjectionFacts(PartitionableCluster cluster, String readerNode, MongoDatabase store,
            Set<String> pipelineIds, List<String> expectedNativeNodes, CommittedTopologyFacts expectedCommitted) {
        assertProjectionFacts(cluster.clusterId(), cluster.member(readerNode), cluster.processCarrying(readerNode),
                cluster::processCarrying, store, pipelineIds, expectedNativeNodes, expectedCommitted, Map.of());
    }

    /** The existing member-add harness supplies its additional owned processes and each job's frozen cohort. */
    static void assertProjectionFacts(TwoMemberCluster cluster, MongoDatabase store, Set<String> pipelineIds,
            List<String> expectedNativeNodes, CommittedTopologyFacts expectedCommitted,
            Map<String, RealProcessServer> additionalOwnedProcesses,
            Map<String, List<NativeMemberWitness.MemberFacts>> expectedExecutionCohorts) {
        assertThat(expectedExecutionCohorts.keySet()).containsExactlyInAnyOrderElementsOf(pipelineIds);
        java.util.function.Function<String, RealProcessServer> owned = node -> additionalOwnedProcesses.containsKey(node)
                ? additionalOwnedProcesses.get(node) : cluster.processCarrying(node);
        ControlPlane reader = cluster.first();
        assertProjectionFacts(reader.clusterId(), reader, cluster.processCarrying(TwoMemberCluster.NODE_A), owned,
                store, pipelineIds, expectedNativeNodes, expectedCommitted, expectedExecutionCohorts);
    }

    private static void assertProjectionFacts(String clusterId, ControlPlane reader, RealProcessServer readerProcess,
            java.util.function.Function<String, RealProcessServer> ownedProcesses, MongoDatabase store,
            Set<String> pipelineIds, List<String> expectedNativeNodes, CommittedTopologyFacts expectedCommitted,
            Map<String, List<NativeMemberWitness.MemberFacts>> expectedExecutionCohorts) {
        try (NativeMemberWitness witness = NativeMemberWitness.connect(clusterId, readerProcess)) {
            var nativeBefore = witness.members();
            assertThat(nativeBefore).hasSize(expectedNativeNodes.size());
            assertThat(nativeBefore.stream().map(NativeMemberWitness.MemberFacts::nodeId))
                    .containsExactlyInAnyOrderElementsOf(expectedNativeNodes);
            assertThat(nativeBefore).allSatisfy(member -> assertThat(member.lite()).isFalse());
            CommittedTopologyFacts committedBefore = committedFacts(store, clusterId);
            assertThat(committedBefore).as("the separately captured committed scene").isEqualTo(expectedCommitted);
            java.time.Instant clockBefore = mongoNow(store, clusterId);
            List<Document> claimsBefore = SystemCollections.WORKLOAD_CLAIMS.on(store)
                    .find(new Document("clusterId", clusterId)).into(new ArrayList<>());
            var view = reader.clusterStatus();
            List<Document> claimsAfter = SystemCollections.WORKLOAD_CLAIMS.on(store)
                    .find(new Document("clusterId", clusterId)).into(new ArrayList<>());
            java.time.Instant clockAfter = mongoNow(store, clusterId);
            assertThat(witness.members()).as("native membership is stable across the projection read").isEqualTo(nativeBefore);
            CommittedTopologyFacts committedAfter = committedFacts(store, clusterId);
            assertThat(committedAfter).as("committed topology is stable across this projection read").isEqualTo(committedBefore);
            Document profile = SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store)
                    .find(new Document("_id", clusterId)).first();
            assertThat(profile).isNotNull();
            assertThat(view.clusterId()).isEqualTo(clusterId);
            assertThat(view.topologyRevision()).isEqualTo(committedBefore.revision());
            // Committed nodes retain the quorum denominator after a loss; native membership is separate.
            assertThat(committedAfter.nodeIds()).containsExactlyInAnyOrderElementsOf(expectedCommitted.nodeIds());
            assertThat(view.profileGeneration()).isEqualTo(number(profile, "generation"));
            assertThat(view.profileHash()).isEqualTo(profile.getString("hash"));
            assertThat(view.recovery()).isNotNull();
            assertReadable(view.recovery());
            assertProfile(view.recovery().currentProfile(), profile);
            List<Document> registry = SystemCollections.CLUSTER_NODE_REGISTRY.on(store)
                    .find(new Document("clusterId", clusterId)).into(new ArrayList<>());
            assertThat(view.members().stream().map(member -> member.nodeId())).doesNotHaveDuplicates()
                    .containsExactlyInAnyOrderElementsOf(registry.stream().map(node -> node.getString("nodeId")).toList());
            for (var member : view.members()) {
                Document registered = registry.stream().filter(node -> member.nodeId().equals(node.getString("nodeId")))
                        .findFirst().orElseThrow();
                Document session = storedClaim(claimsAfter, WorkloadClaimType.NODE_SESSION, member.nodeId());
                Document earlier = storedClaim(claimsBefore, WorkloadClaimType.NODE_SESSION, member.nodeId());
                for (String field : List.of("ownerNodeId", "ownerBootId", "claimGeneration", "profileGeneration")) {
                    assertThat(earlier.get(field)).as("the node-session identity is stable around the projection read: %s", field)
                            .isEqualTo(session.get(field));
                }
                assertThat(member.bootId()).isEqualTo(registered.getString("bootId")).isEqualTo(session.getString("ownerBootId"));
                assertThat(member.sessionBootId()).isEqualTo(session.getString("ownerBootId"));
                assertThat(member.profileGeneration()).isEqualTo(number(registered, "profileGeneration"))
                        .isEqualTo(number(session, "profileGeneration"));
                assertThat(member.profileHash()).isEqualTo(registered.getString("hash"));
                assertThat(member.controlUrl()).isEqualTo(registered.getString("controlUrl"));
                assertThat(member.joined()).isEqualTo(registered.getBoolean("joined"));
                assertThat(member.joinedAt()).isEqualTo(instant(registered, "joinedAt"));
                assertThat(member.joinedMemberUuid()).isEqualTo(registered.getString("memberUuid"));
                assertThat(member.joinedMemberAddress()).isEqualTo(registered.getString("memberAddress"));
                assertLease(member.sessionLeaseUntil(), member.sessionLeaseRemainingMillis(), member.sessionLeased(),
                        earlier, session, clockBefore, clockAfter);
                var nativeMember = nativeBefore.stream().filter(actual -> member.nodeId().equals(actual.nodeId()))
                        .findFirst();
                if (nativeMember.isPresent()) {
                    var actual = nativeMember.orElseThrow();
                    assertThat(member.state()).isEqualTo(ClusterMemberState.ACTIVE);
                    assertThat(member.live()).isTrue();
                    assertThat(member.bootId()).isEqualTo(actual.bootId());
                    assertThat(member.memberUuid()).isEqualTo(actual.uuid());
                    assertThat(member.hzAddress()).isEqualTo(actual.address());
                    assertThat(member.controlUrl()).isEqualTo(actual.controlUrl());
                    assertThat(actual.profileGeneration()).isEqualTo(String.valueOf(member.profileGeneration()));
                    assertThat(actual.profileHash()).isEqualTo(member.profileHash());
                    assertThat(java.net.URI.create(member.controlUrl()).getPort())
                            .isEqualTo(ownedProcesses.apply(member.nodeId()).baseUrl().getPort());
                } else {
                    assertThat(member.live()).isFalse();
                    assertThat(member.joined()).as("these settled registry rows previously joined").isTrue();
                    assertThat(member.state()).as("a departed native member cannot retain ACTIVE").isEqualTo(ClusterMemberState.LOST);
                    assertThat(member.memberUuid()).isEqualTo(registered.getString("memberUuid"));
                    assertThat(member.hzAddress()).isEqualTo(registered.getString("memberAddress"));
                }
            }
            for (String pipelineId : pipelineIds) {
                var pipeline = view.pipelines().stream().filter(row -> row.pipelineId().equals(pipelineId)).findFirst().orElseThrow();
                Document controller = storedClaim(claimsAfter, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
                assertClaim(pipeline.controllerClaim(), storedClaim(claimsBefore, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId),
                        controller, clockBefore, clockAfter);
                for (var capture : pipeline.captureClaims()) {
                    assertClaim(capture, storedClaim(claimsBefore, WorkloadClaimType.CAPTURE, capture.resourceId()),
                            storedClaim(claimsAfter, WorkloadClaimType.CAPTURE, capture.resourceId()), clockBefore, clockAfter);
                }
                assertThat(pipeline.captureClaims()).extracting(capture -> capture.resourceId()).doesNotHaveDuplicates();
                assertThat(pipeline.measuredFrom()).containsAll(pipeline.controllerClaim().executionMembers().stream()
                        .map(ClusterClaimView.Member::memberUuid).toList());
                var compiled = witness.publishedPlan(pipelineId);
                assertThat(compiled.executionGeneration()).isEqualTo(pipeline.controllerClaim().contextExecutionGeneration());
                assertThat(compiled.claimGeneration()).isEqualTo(pipeline.controllerClaim().executionClaimGeneration());
                assertThat(compiled.topologyRevision()).isEqualTo(pipeline.controllerClaim().executionTopologyRevision());
                assertThat(compiled.members()).containsExactlyInAnyOrderElementsOf(pipeline.controllerClaim().executionMembers().stream()
                        .map(ClusterClaimView.Member::nodeId).toList());
                var expectedCohort = expectedExecutionCohorts.get(pipelineId);
                if (expectedCohort != null) {
                    assertThat(expectedCohort).isNotEmpty();
                    assertThat(pipeline.controllerClaim().executionMembers()).containsExactlyInAnyOrderElementsOf(
                            expectedCohort.stream().map(member -> new ClusterClaimView.Member(
                                    member.nodeId(), member.bootId(), member.uuid())).toList());
                    assertThat(compiled.members()).containsExactlyInAnyOrderElementsOf(
                            expectedCohort.stream().map(NativeMemberWitness.MemberFacts::nodeId).toList());
                }
                assertThat(pipeline.vertices()).isNotEmpty();
                var executionClaim = pipeline.controllerClaim();
                assertThat(executionClaim.contextExecutionGeneration()).isNotNull();
                assertThat(executionClaim.executionClaimGeneration()).isNotNull();
                assertThat(executionClaim.executionProfileGeneration()).isNotNull();
                assertThat(executionClaim.executionProfileHash()).isNotBlank();
                assertThat(executionClaim.executionIncarnation()).isNotBlank();
                assertThat(executionClaim.executionTopologyRevision()).isNotNull();
                Document receiptKey = new Document("clusterId", clusterId).append("pipelineId", pipelineId)
                        .append("incarnationId", executionClaim.executionIncarnation())
                        .append("executionGeneration", executionClaim.contextExecutionGeneration())
                        .append("profile.generation", executionClaim.executionProfileGeneration())
                        .append("profile.hash", executionClaim.executionProfileHash())
                        .append("pipelineClaim.clusterId", clusterId)
                        .append("pipelineClaim.resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                        .append("pipelineClaim.resourceId", pipelineId)
                        .append("pipelineClaim.claimGeneration", executionClaim.executionClaimGeneration())
                        .append("pipelineClaim.executionGeneration", executionClaim.contextExecutionGeneration())
                        .append("pipelineClaim.profileGeneration", executionClaim.executionProfileGeneration())
                        .append("pipelineClaim.topologyRevision", executionClaim.executionTopologyRevision());
                if (executionClaim.claimGeneration() == executionClaim.executionClaimGeneration()) {
                    receiptKey.append("pipelineClaim.ownerNodeId", executionClaim.ownerNodeId())
                            .append("pipelineClaim.ownerBootId", executionClaim.ownerBootId());
                }
                List<Document> receipts = SystemCollections.CLUSTER_CAPACITY_OCCUPANCY.on(store)
                        .find(receiptKey).into(new ArrayList<>());
                assertThat(receipts).as("one receipt for the exact immutable executing context").hasSize(1);
                Document occupancy = receipts.getFirst();
                assertThat(occupancy.getString("nativeJobId")).isNotBlank();
                Document receiptProfile = occupancy.get("profile", Document.class);
                Document frozenProfile = controller.get("executionProfile", Document.class);
                assertThat(receiptProfile).isNotNull();
                assertThat(frozenProfile).isNotNull();
                for (String field : List.of("clusterId", "generation", "formatVersion", "hash", "attributes")) {
                    assertThat(receiptProfile.get(field)).as("the receipt carries the complete frozen execution profile: %s", field)
                            .isEqualTo(frozenProfile.get(field));
                }
                Document immutableFence = occupancy.get("pipelineClaim", Document.class);
                assertThat(executionClaim.executionMembers()).anySatisfy(member -> {
                    assertThat(member.nodeId()).isEqualTo(immutableFence.getString("ownerNodeId"));
                    assertThat(member.bootId()).isEqualTo(immutableFence.getString("ownerBootId"));
                });
                Map<String, Integer> observedMemberIndexes = new LinkedHashMap<>();
                for (var vertex : pipeline.vertices()) {
                    var nodes = compiled.nodes().stream().filter(node -> node.vertices().contains(vertex.name())).toList();
                    assertThat(nodes).as("the actual compiler's vertex-to-node association").hasSizeLessThanOrEqualTo(1);
                    if (nodes.isEmpty()) {
                        assertThat(vertex.requested()).isNull();
                        assertThat(vertex.computedLocal()).isNull();
                    } else {
                        var node = nodes.getFirst();
                        assertThat(vertex.requested()).isEqualTo(node.requested());
                        assertThat(vertex.computedLocal()).isEqualTo(node.computedLocal());
                        assertThat(vertex.effective()).isEqualTo(node.effective());
                        assertThat(node.memberCount()).isEqualTo(compiled.members().size());
                    }
                    assertThat(vertex.effective()).isEqualTo(vertex.processors().size());
                    assertThat(vertex.processors()).extracting(processor -> processor.index()).doesNotHaveDuplicates();
                    for (var processor : vertex.processors()) {
                        var context = processor.context();
                        assertThat(context).as("the actual initialized native context, not an inferred local index").isNotNull();
                        assertThat(context.pipelineId()).isEqualTo(pipelineId);
                        assertThat(context.vertex()).isEqualTo(vertex.name());
                        assertThat(context.executionGeneration()).isEqualTo(pipeline.controllerClaim().contextExecutionGeneration());
                        assertThat(context.claimGeneration()).isEqualTo(pipeline.controllerClaim().executionClaimGeneration());
                        assertThat(context.profileGeneration()).isEqualTo(pipeline.controllerClaim().executionProfileGeneration());
                        assertThat(com.hazelcast.jet.Util.idToString(Long.parseUnsignedLong(context.runtimeExecutionId())))
                                .isEqualTo(vertex.executionId());
                        assertThat(context.globalProcessorIndex()).isEqualTo(processor.index());
                        assertThat(processor.localIndex()).isEqualTo(context.localProcessorIndex());
                        assertThat(context.memberUuid()).isEqualTo(processor.memberUuid());
                        assertThat(context.nodeId()).isEqualTo(processor.nodeId());
                        var owner = pipeline.controllerClaim().executionMembers().stream()
                                .filter(row -> row.nodeId().equals(context.nodeId())).findFirst().orElseThrow();
                        assertThat(context.bootId()).isEqualTo(owner.bootId());
                        assertThat(context.memberUuid()).isEqualTo(owner.memberUuid());
                        var nativeOwner = nativeBefore.stream().filter(row -> row.uuid().equals(context.memberUuid())).findFirst().orElseThrow();
                        assertThat(context.memberAddress()).isEqualTo(nativeOwner.address());
                        assertThat(context.memberCount()).isEqualTo(pipeline.controllerClaim().executionMembers().size());
                        if (expectedCohort != null) {
                            var frozenMember = expectedCohort.stream().filter(member -> member.uuid().equals(context.memberUuid()))
                                    .findFirst().orElseThrow();
                            assertThat(context.nodeId()).isEqualTo(frozenMember.nodeId());
                            assertThat(context.bootId()).isEqualTo(frozenMember.bootId());
                            assertThat(context.memberAddress()).isEqualTo(frozenMember.address());
                            assertThat(context.memberCount()).isEqualTo(expectedCohort.size());
                        }

                        Integer earlierIndex = observedMemberIndexes.putIfAbsent(context.memberUuid(), context.memberIndex());
                        if (earlierIndex != null) { assertThat(context.memberIndex()).isEqualTo(earlierIndex); }
                        assertThat(context.initializedAt()).isNotNull();
                        if (vertex.computedLocal() != null) {
                            assertThat(context.localParallelism()).isEqualTo(vertex.computedLocal());
                            assertThat(context.totalParallelism()).isEqualTo(vertex.effective());
                        }
                        assertThat(context.jobId()).isEqualTo(occupancy.getString("nativeJobId"));
                    }
                }
                assertThat(observedMemberIndexes.keySet()).containsExactlyInAnyOrderElementsOf(
                        pipeline.controllerClaim().executionMembers().stream().map(ClusterClaimView.Member::memberUuid).toList());
                assertThat(observedMemberIndexes.values()).containsExactlyInAnyOrderElementsOf(
                        java.util.stream.IntStream.range(0, pipeline.controllerClaim().executionMembers().size()).boxed().toList());
                for (var item : view.recovery().items().stream().filter(row -> row.pipelineId().equals(pipelineId)).toList()) {
                    assertThat(item.currentPipelineClaim()).isNotNull();
                    assertFence(item.currentPipelineClaim().claim(), controller);
                    assertLease(item.currentPipelineClaim().leaseUntil(), item.currentPipelineClaim().leaseRemainingMillis(),
                            item.currentPipelineClaim().leased(), storedClaim(claimsBefore, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId),
                            controller, clockBefore, clockAfter);
                }
            }
            var coordinator = view.recovery().coordinatorClaim();
            var recoveryClaims = claimsAfter.stream().filter(row -> WorkloadClaimType.CLUSTER_RECOVERY.name()
                    .equals(row.getString("resourceType"))).toList();
            if (recoveryClaims.isEmpty()) {
                assertThat(coordinator).isNull();
            } else {
                assertThat(coordinator).isNotNull();
                Document recoveryClaim = storedClaim(claimsAfter, WorkloadClaimType.CLUSTER_RECOVERY, coordinator.claim().resourceId());
                assertFence(coordinator.claim(), recoveryClaim);
                assertLease(coordinator.leaseUntil(), coordinator.leaseRemainingMillis(), coordinator.leased(),
                        storedClaim(claimsBefore, WorkloadClaimType.CLUSTER_RECOVERY, coordinator.claim().resourceId()),
                        recoveryClaim, clockBefore, clockAfter);
            }
        }
    }

    /** Observed Mongo facts, with no liveness, eligibility or quorum calculation. */
    record CommittedTopologyFacts(Long revision, List<String> nodeIds, Long profileGeneration) {
        CommittedTopologyFacts { nodeIds = List.copyOf(nodeIds); }
    }

    static CommittedTopologyFacts committedFacts(MongoDatabase store, String clusterId) {
        Document document = SystemCollections.CLUSTER_MEMBERSHIP.on(store)
                .find(new Document("_id", clusterId)).first();
        assertThat(document).isNotNull();
        List<String> nodes = document.getList("activeNodeIds", String.class);
        assertThat(nodes).doesNotHaveDuplicates();
        return new CommittedTopologyFacts(number(document, "revision"), nodes, number(document, "profileGeneration"));
    }

    private static void assertClaim(ClusterClaimView projected, Document before, Document stored,
            java.time.Instant clockBefore, java.time.Instant clockAfter) {
        assertThat(projected).isNotNull();
        for (String field : List.of("ownerNodeId", "ownerBootId", "claimGeneration", "executionGeneration", "profileGeneration",
                "topologyRevision", "contextExecutionGeneration", "executionClaimGeneration", "executionIncarnation",
                "executionRevision", "executionTopologyRevision", "executionProfileVersion", "executionProfile",
                "executionNodeIds", "executionMembers", "failureClaimGeneration", "failureAfterMemberLoss")) {
            assertThat(before.get(field)).as("the claim identity is stable around the projection read: %s", field).isEqualTo(stored.get(field));
        }
        assertThat(projected.resourceId()).isEqualTo(stored.getString("resourceId"));
        assertThat(projected.ownerNodeId()).isEqualTo(stored.getString("ownerNodeId"));
        assertThat(projected.ownerBootId()).isEqualTo(stored.getString("ownerBootId"));
        assertThat(projected.claimGeneration()).isEqualTo(number(stored, "claimGeneration"));
        assertThat(projected.executionGeneration()).isEqualTo(number(stored, "executionGeneration"));
        assertThat(projected.topologyRevision()).isEqualTo(number(stored, "topologyRevision"));
        assertThat(projected.profileGeneration()).isEqualTo(positive(stored, "profileGeneration"));
        assertThat(projected.contextExecutionGeneration()).isEqualTo(positive(stored, "contextExecutionGeneration"));
        assertThat(projected.executionClaimGeneration()).isEqualTo(positive(stored, "executionClaimGeneration"));
        assertThat(projected.executionIncarnation()).isEqualTo(stored.getString("executionIncarnation"));
        assertThat(projected.executionRevision()).isEqualTo(stored.getString("executionRevision"));
        assertThat(projected.executionTopologyRevision()).isEqualTo(number(stored, "executionTopologyRevision"));
        Document profile = stored.get("executionProfile", Document.class);
        assertThat(projected.executionProfileGeneration()).isEqualTo(profile == null ? null : number(profile, "generation"));
        assertThat(projected.executionProfileHash()).isEqualTo(profile == null ? null : profile.getString("hash"));
        assertThat(projected.executionMembers()).containsExactlyInAnyOrderElementsOf(stored.getList("executionMembers", Document.class, List.of())
                .stream().map(member -> new ClusterClaimView.Member(member.getString("nodeId"), member.getString("bootId"),
                        member.getString("memberUuid"))).toList());
        assertThat(projected.failureClaimGeneration()).isEqualTo(positive(stored, "failureClaimGeneration"));
        assertThat(projected.failureAfterMemberLoss()).isEqualTo(projected.failureClaimGeneration() == null
                ? null : stored.getBoolean("failureAfterMemberLoss"));
        assertThat(projected.executionContextCurrent()).isEqualTo(projected.contextExecutionGeneration() == null
                ? null : projected.contextExecutionGeneration().longValue() == number(stored, "executionGeneration"));
        assertLease(projected.leaseUntil(), projected.leaseRemainingMillis(), projected.leased(), before, stored, clockBefore, clockAfter);
    }

    private static void assertLease(java.time.Instant deadline, Long remaining, Boolean leased, Document before,
            Document after, java.time.Instant clockBefore, java.time.Instant clockAfter) {
        assertThat(deadline).isNotNull();
        assertThat(remaining).isNotNull();
        assertThat(leased).isNotNull();
        assertThat(deadline).isBetween(instant(before, "leaseUntil"), instant(after, "leaseUntil"));
        assertThat(remaining).isBetween(deadline.toEpochMilli() - clockAfter.toEpochMilli(),
                deadline.toEpochMilli() - clockBefore.toEpochMilli());
        assertThat(leased).isEqualTo(remaining > 0);
    }

    private static java.time.Instant mongoNow(MongoDatabase store, String clusterId) {
        Document answer = SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).aggregate(List.of(
                new Document("$match", new Document("_id", clusterId)),
                new Document("$project", new Document("_id", 0).append("now", "$$NOW")))).first();
        assertThat(answer).isNotNull();
        return answer.getDate("now").toInstant();
    }

    private static Document storedClaim(List<Document> claims, WorkloadClaimType type, String resource) {
        List<Document> found = claims.stream().filter(row -> type.name().equals(row.getString("resourceType"))
                && resource.equals(row.getString("resourceId"))).toList();
        assertThat(found).hasSize(1);
        return found.getFirst();
    }

    private static Long number(Document document, String field) {
        return document.get(field) instanceof Number value ? value.longValue() : null;
    }

    private static Long positive(Document document, String field) {
        Long value = number(document, field);
        // A stored zero is this claim's explicit legacy absence; no missing positive proof is synthesized.
        return value == null || value == 0 ? null : value;
    }

    private static java.time.Instant instant(Document document, String field) {
        return document.getDate(field) == null ? null : document.getDate(field).toInstant();
    }

    private static void assertProfile(ClusterRecoveryItemView.Profile projected, Document stored) {
        if (stored == null) { assertThat(projected).isNull(); return; }
        assertThat(projected).isNotNull();
        assertThat(projected.generation()).isEqualTo(number(stored, "generation"));
        assertThat(projected.formatVersion()).isEqualTo(number(stored, "formatVersion").intValue());
        assertThat(projected.hash()).isEqualTo(stored.getString("hash"));
        assertThat(projected.attributes()).isEqualTo(stored.get("attributes", Document.class));
    }

    /** The recovery step is terminal and stable here; transient FIFO position needs its own held step. */
    static void assertQueueProjection(ClusterRecoveryItemView item, Document stored, MongoDatabase store) {
        Document event = stored.get("event", Document.class);
        assertThat(item.pipelineId()).isEqualTo(stored.getString("pipelineId"));
        assertThat(item.incarnation()).isEqualTo(stored.getString("incarnation"));
        Document artifact = SystemCollections.ARTIFACTS.on(store).find(new Document("_id", item.pipelineId())).first();
        assertThat(artifact).isNotNull();
        assertThat(item.incarnation()).isEqualTo(artifact.getString("incarnation"));
        assertThat(item.currentArtifactHash()).isEqualTo(artifact.getString("contentHash"));
        assertThat(item.intentFingerprint()).isEqualTo(event.getString("intentFingerprint"));
        assertThat(item.cause()).isEqualTo(event.getString("cause"));
        assertThat(item.persistedStatus()).isEqualTo(stored.getString("status"));
        assertThat(item.status()).isEqualTo(stored.getString("status"));
        assertThat(item.enqueueSequence()).isEqualTo(number(stored, "enqueueSequence"));
        assertThat(item.enqueuedAt()).isEqualTo(instant(stored, "enqueuedAt"));
        assertThat(item.updatedAt()).isEqualTo(instant(stored, "updatedAt"));
        assertThat(item.queuePosition()).isNull();
        assertThat(item.attempt()).isEqualTo(number(stored, "attempt").intValue());
        assertThat(item.maxAttempts()).isEqualTo(number(stored, "maxAttempts").intValue());
        assertThat(item.nextEligibleAt()).isEqualTo(instant(stored, "nextEligibleAt"));
        assertThat(item.originalExecutionGeneration()).isEqualTo(number(event, "originalExecutionGeneration"));
        assertThat(item.originalExecutionRevision()).isEqualTo(event.getString("originalExecutionRevision"));
        assertThat(item.sourceTopologyRevision()).isEqualTo(number(event, "sourceTopologyRevision"));
        assertThat(item.legacySourceProfile()).isEqualTo(event.getBoolean("legacySourceProfile"));
        assertProfile(item.originalProfile(), event.get("sourceProfile", Document.class));
        assertProfile(item.targetProfile(), stored.get("targetProfile", Document.class));
        assertThat(item.targetTopologyRevision()).isEqualTo(number(stored, "targetTopologyRevision"));
        assertPositions(item.originalPositions(), event.getList("resumePositions", Document.class));
        assertThat(item.permit()).isNull();
        assertThat(stored.get("permit")).isNull();
        assertThat(item.diagnostic()).isNull();
        assertThat(stored.get("diagnostic")).isNull();
        Document successor = stored.get("successor", Document.class);
        var projected = item.successor();
        assertThat(projected).isNotNull();
        assertFence(projected.pipelineClaim(), successor.get("pipelineClaim", Document.class));
        assertThat(item.executionFrontier()).isEqualTo(projected.pipelineClaim().executionGeneration());
        assertProfile(projected.profile(), successor.get("profile", Document.class));
        assertThat(projected.executionNodeIds()).containsExactlyInAnyOrderElementsOf(successor.getList("executionNodeIds", String.class));
        assertThat(projected.requiredSourceIds()).containsExactlyInAnyOrderElementsOf(successor.getList("requiredSourceIds", String.class));
        assertThat(projected.sourceRequirementsRecorded()).isEqualTo(successor.getBoolean("sourceRequirementsRecorded"));
        assertThat(projected.allocatedAt()).isEqualTo(instant(successor, "allocatedAt"));
        assertThat(projected.nativeJobId()).isEqualTo(successor.getString("nativeJobId"));
        assertThat(projected.submittedAt()).isEqualTo(instant(successor, "submittedAt"));
        assertPositions(projected.requestedPositions(), successor.getList("requestedPositions", Document.class));
        Document receipt = successor.get("startupReceipt", Document.class);
        assertThat(receipt).isNotNull();
        assertFence(projected.pipelineClaim(), receipt.get("pipelineClaim", Document.class));
        assertThat(projected.nativeJobId()).isEqualTo(receipt.getString("nativeJobId"));
        assertThat(projected.nativeInitializedAt()).isEqualTo(instant(receipt, "nativeInitializedAt"));
        assertThat(projected.sourcesAcceptedAt()).isEqualTo(instant(receipt, "positionsAcceptedAt"));
        assertThat(projected.executionCompleted()).isEqualTo(receipt.getBoolean("executionCompleted"));
        assertPositions(projected.requestedPositions(), receipt.getList("requestedPositions", Document.class));
        assertPositions(projected.acceptedPositions(), receipt.getList("acceptedPositions", Document.class));
        assertThat(projected.failureNote()).isNull();
        assertThat(successor.get("failureNote")).isNull();
    }

    private static void assertFence(ClusterRecoveryItemView.Claim projected, Document stored) {
        assertThat(projected.clusterId()).isEqualTo(stored.getString("clusterId"));
        assertThat(projected.type()).isEqualTo(stored.getString("resourceType"));
        assertThat(projected.resourceId()).isEqualTo(stored.getString("resourceId"));
        assertThat(projected.ownerNodeId()).isEqualTo(stored.getString("ownerNodeId"));
        assertThat(projected.ownerBootId()).isEqualTo(stored.getString("ownerBootId"));
        assertThat(projected.claimGeneration()).isEqualTo(number(stored, "claimGeneration"));
        assertThat(projected.executionGeneration()).isEqualTo(number(stored, "executionGeneration"));
        assertThat(projected.topologyRevision()).isEqualTo(number(stored, "topologyRevision"));
        assertThat(projected.profileGeneration()).isEqualTo(number(stored, "profileGeneration"));
    }

    private static void assertPositions(Map<String, ClusterRecoveryItemView.Position> projected, List<Document> stored) {
        assertThat(projected.keySet()).containsExactlyInAnyOrderElementsOf(stored.stream().map(row -> row.getString("sourceId")).toList());
        for (Document row : stored) {
            var position = projected.get(row.getString("sourceId"));
            assertThat(position.connectorId()).isEqualTo(row.getString("connectorId"));
            assertThat(position.captureId()).isEqualTo(row.getString("captureId"));
            assertThat(position.kind()).isEqualTo(row.getString("kind"));
            assertThat(position.provenance()).isEqualTo(row.getString("provenance"));
            assertThat(position.reference()).isEqualTo(row.getString("durableStateReference"));
            Document point = row.get("position", Document.class);
            Document order = point == null ? null : point.get("order", Document.class);
            assertThat(position.token()).isEqualTo(point == null ? null : point.getString("token"));
            assertThat(position.epoch()).isEqualTo(order == null ? null : number(order, "epoch"));
            assertThat(position.sequence()).isEqualTo(order == null ? null : number(order, "seq"));
        }
    }

    private static void assertReadable(ClusterRecoveryView recovery) {
        assertThat(recovery.queueUnavailable()).isNull();
        assertThat(recovery.claimUnavailable()).isNull();
        assertThat(recovery.profileUnavailable()).isNull();
        assertThat(recovery.quorumReady()).isTrue();
    }
}
