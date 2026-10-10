package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WorkloadClaimFenceTest {
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("a", "boot-a");
    private static final WorkloadClaimFence ORIGINAL = new WorkloadClaimFence(KEY, OWNER, 3, 42, 2, 1);

    @Test
    void aCompatibleAcquisitionTopologyRefreshKeepsAuthorityWithoutMakingTheFencesEqual() {
        var refreshed = new WorkloadClaimFence(KEY, OWNER, 3, 42, 3, 1);
        assertThat(ORIGINAL.sameAuthorityAs(refreshed)).isTrue();
        assertThat(refreshed.sameAuthorityAs(ORIGINAL)).isTrue();
        assertThat(refreshed).isNotEqualTo(ORIGINAL);
    }

    @Test
    void everyOwnerGenerationProfileAndResourceBoundaryReplacesAuthority() {
        for (var changed : List.of(
                new WorkloadClaimFence(KEY, new WorkloadOwner("b", "boot-a"), 3, 42, 2, 1),
                new WorkloadClaimFence(KEY, new WorkloadOwner("a", "boot-b"), 3, 42, 2, 1),
                new WorkloadClaimFence(KEY, OWNER, 4, 42, 2, 1),
                new WorkloadClaimFence(KEY, OWNER, 3, 43, 2, 1),
                new WorkloadClaimFence(KEY, OWNER, 3, 42, 2, 2),
                new WorkloadClaimFence(new WorkloadClaimKey("west", KEY.type(), "orders"), OWNER, 3, 42, 2, 1),
                new WorkloadClaimFence(new WorkloadClaimKey("east", WorkloadClaimType.CAPTURE, "orders"), OWNER, 3, 42, 2, 1),
                new WorkloadClaimFence(new WorkloadClaimKey("east", KEY.type(), "returns"), OWNER, 3, 42, 2, 1))) {
            assertThat(ORIGINAL.sameAuthorityAs(changed)).isFalse();
            assertThat(changed.sameAuthorityAs(ORIGINAL)).isFalse();
        }
        assertThat(ORIGINAL.sameAuthorityAs(null)).isFalse();
    }
}
