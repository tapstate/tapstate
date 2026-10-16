package io.tapstate.spi.store;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExecutionProfileTest {
    @Test
    void profileHashHasAStableVersionedEncodingAndIgnoresMapOrder() {
        Map<String, String> reverse = new LinkedHashMap<>();
        reverse.put("cooperativeThreads", "4");
        reverse.put("buildVersion", "0.6.0");
        ExecutionProfile profile = new ExecutionProfile(1, reverse);
        reverse.put("cooperativeThreads", "100");

        assertThat(profile.hash()).isEqualTo("9242041509a3c9c70563b5a2a36b9e4e56b50314e708104b6b9f0bb9a3ba6abc");
        assertThat(profile).isEqualTo(new ExecutionProfile(1,
                Map.of("buildVersion", "0.6.0", "cooperativeThreads", "4")));
        assertThatThrownBy(() -> profile.attributes().put("extra", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void fieldBoundariesAndFormatVersionCannotAlias() {
        ExecutionProfile one = new ExecutionProfile(1, Map.of("ab", "c"));
        assertThat(one.hash()).isNotEqualTo(new ExecutionProfile(1, Map.of("a", "bc")).hash());
        assertThat(one.hash()).isNotEqualTo(new ExecutionProfile(2, one.attributes()).hash());
        assertThatThrownBy(() -> new ExecutionProfile(1, Map.of("abi", " ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void claimFenceKeepsTheIndependentProfileGeneration() {
        WorkloadClaim claim = new WorkloadClaim(
                new WorkloadClaimKey("east", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("a", "boot"), 8, 12, 3, Instant.EPOCH,
                12, 8, Set.of("a"), 0, false, 7);
        WorkloadClaimFence fence = WorkloadClaimFence.from(claim);
        assertThat(fence.claimGeneration()).isEqualTo(8);
        assertThat(fence.executionGeneration()).isEqualTo(12);
        assertThat(fence.profileGeneration()).isEqualTo(7);
    }
}
