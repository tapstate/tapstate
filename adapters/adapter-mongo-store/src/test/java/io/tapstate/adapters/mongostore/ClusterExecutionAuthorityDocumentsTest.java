package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterExecutionAuthorityDocumentsTest {
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile("east", 1,
            new ExecutionProfile(1, Map.of("build", "one")));
    private static final ClusterRecoveryKey KEY = new ClusterRecoveryKey("east", "orders", "inc-a");
    private static final WorkloadClaimFence ORIGINAL = new WorkloadClaimFence(
            new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), new WorkloadOwner("a", "boot-a"), 3, 42, 2, 1);

    @Test
    void currentAcquisitionTopologyDoesNotReplaceTheAllocatorsOriginalContext() {
        assertThat(matches(context().append("topologyRevision", 7L))).isTrue();
        assertThat(matches(context().append("executionTopologyRevision", 7L))).isFalse();
    }

    @Test
    void incarnationRevisionAllocatedClaimGenerationAndCohortMustStillMatch() {
        for (var mismatch : List.of(context().append("executionIncarnation", "inc-b"),
                context().append("executionRevision", "changed"), context().append("executionClaimGeneration", 4L),
                context().append("contextExecutionGeneration", 43L), context().append("executionGeneration", 43L),
                context().append("executionNodeIds", List.of("b")).append("executionMembers", List.of(
                        new Document("nodeId", "b").append("bootId", "boot-b").append("memberUuid", "uuid-b"))),
                context().append("executionMembers", List.of()))) {
            assertThat(matches(mismatch)).isFalse();
        }
        var other = new ClusterExecutionProfile("east", 1, new ExecutionProfile(1, Map.of("build", "two")));
        assertThat(matches(context().append("executionProfile", ClusterRecoveryDocuments.profile(other).append("schemaVersion", 1)))).isFalse();
    }

    private static boolean matches(Document context) {
        return MongoClusterCapacityStore.matchesExecutionContext(context, ORIGINAL, PROFILE, KEY, "revision-a", Set.of("a"));
    }

    private static Document context() {
        return new Document("clusterId", "east").append("resourceType", "PIPELINE_ACTUATION").append("resourceId", "orders")
                .append("ownerNodeId", "a").append("ownerBootId", "boot-a").append("claimGeneration", 3L)
                .append("executionGeneration", 42L).append("topologyRevision", 2L).append("profileGeneration", 1L)
                .append("leaseUntil", new Date(Long.MAX_VALUE)).append("contextExecutionGeneration", 42L)
                .append("executionClaimGeneration", 3L).append("executionTopologyRevision", 2L)
                .append("executionIncarnation", "inc-a").append("executionRevision", "revision-a")
                .append("executionProfileVersion", 1).append("executionProfile", ClusterRecoveryDocuments.profile(PROFILE).append("schemaVersion", 1))
                .append("executionNodeIds", List.of("a"))
                .append("executionMembers", List.of(new Document("nodeId", "a").append("bootId", "boot-a").append("memberUuid", "uuid-a")));
    }
}
