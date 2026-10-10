package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;

class AClusterExecutionKeepsItsOriginalMembersTest {
    private static final Map<String, ClusterExecutionMember> ORIGINAL = Map.of(
            "a", new ClusterExecutionMember("a", "boot-a", "uuid-a"),
            "b", new ClusterExecutionMember("b", "boot-b", "uuid-b"));

    @Test
    void sameStableIdsWithANewBootOrUuidDoNotDescribeTheOriginalExecution() {
        WorkloadClaim claim = claim(new WorkloadOwner("a", "boot-a"), 1, ORIGINAL);
        assertThat(claim.originalMembersPresent(ORIGINAL)).contains(true);
        assertThat(claim.originalMembersPresent(Map.of("a", ORIGINAL.get("a"),
                "b", new ClusterExecutionMember("b", "next-boot", "uuid-next")))).contains(false);
        assertThat(claim.originalMembersPresent(Map.of("a", ORIGINAL.get("a"),
                "b", new ClusterExecutionMember("b", "boot-b", "uuid-rejoined")))).contains(false);
    }

    @Test
    void aCompatibleAddedMemberDoesNotRemoveAnOriginalMember() {
        Map<String, ClusterExecutionMember> expanded = Map.of("a", ORIGINAL.get("a"), "b", ORIGINAL.get("b"),
                "c", new ClusterExecutionMember("c", "boot-c", "uuid-c"));
        assertThat(claim(new WorkloadOwner("a", "boot-a"), 1, ORIGINAL).originalMembersPresent(expanded)).contains(true);
    }

    @Test
    void aNewControllerCannotReplaceTheOriginalCohortWithItsOwnBoot() {
        WorkloadClaim inherited = claim(new WorkloadOwner("c", "controller-boot"), 2, ORIGINAL);
        assertThat(inherited.originalMembersPresent(ORIGINAL)).contains(true);
        assertThat(inherited.originalMembersPresent(Map.of("a", ORIGINAL.get("a")))).contains(false);
    }

    @Test
    void stableOnlyContextRemainsUnknownInsteadOfBorrowingCurrentIdentity() {
        WorkloadClaim unknown = claim(new WorkloadOwner("a", "next-boot"), 2, Map.of());
        assertThat(unknown.originalMembersPresent(ORIGINAL)).isEmpty();
    }

    private static WorkloadClaim claim(WorkloadOwner owner, long generation, Map<String, ClusterExecutionMember> members) {
        return new WorkloadClaim(new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                owner, generation, 42, 9, Instant.parse("2026-10-10T01:00:00Z"), 42, 1, Set.of("a", "b"),
                0, false, 1, new ClusterExecutionProfile("east", 1, new ExecutionProfile(1, Map.of("build", "one"))),
                3L, "inc-a", "original-revision", members);
    }
}
