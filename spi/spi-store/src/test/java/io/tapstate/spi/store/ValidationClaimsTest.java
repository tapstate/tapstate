package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationClaimsTest {
    @Test void validationScopeHasItsOwnTypeAndStableUnambiguousPipelineEncoding() {
        var key = ValidationClaims.key("cluster", "orders");
        assertThat(key.type()).isEqualTo(WorkloadClaimType.VALIDATION);
        assertThat(key.resourceId()).isEqualTo("validation:v1:b3JkZXJz");
        assertThat(key).isNotEqualTo(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders"));
        assertThat(ValidationClaims.key("c:a", "b")).isNotEqualTo(ValidationClaims.key("c", "a:b"));
        assertThat(ValidationClaims.key("cluster", "a:b")).isNotEqualTo(ValidationClaims.key("cluster", "a/b"));
        assertThatThrownBy(() -> ValidationClaims.key("cluster", "bad-\ud800"))
                .isInstanceOf(TapstateException.class);
    }

    @Test void ownerRoundTripPreservesTheCompleteAdvancedFence() {
        var claim = claim(WorkloadClaimType.VALIDATION, 4, 9, 9, 4, Set.of("node"));
        var owner = ValidationClaims.owner("orders", claim);
        assertThat(owner.claimGeneration()).isEqualTo(4);
        assertThat(owner.executionGeneration()).isEqualTo(9);
        assertThat(owner.topologyRevision()).isEqualTo(7);
        assertThat(ValidationClaims.fence(owner)).isEqualTo(WorkloadClaimFence.from(claim));
    }

    @Test void ordinaryAcquireOrStaleExecutionContextCannotAuthorizeValidationWork() {
        for (var invalid : Set.of(
                claim(WorkloadClaimType.VALIDATION, 1, 0, 0, 0, Set.of()),
                claim(WorkloadClaimType.VALIDATION, 4, 9, 8, 4, Set.of("node")),
                claim(WorkloadClaimType.VALIDATION, 4, 9, 9, 3, Set.of("node")),
                claim(WorkloadClaimType.VALIDATION, 4, 9, 9, 4, Set.of("node", "other")),
                claim(WorkloadClaimType.PIPELINE_ACTUATION, 4, 9, 9, 4, Set.of("node")))) {
            assertThatThrownBy(() -> ValidationClaims.owner("orders", invalid))
                    .isInstanceOf(TapstateException.class);
        }
        assertThatThrownBy(() -> ValidationClaims.owner("another-pipeline",
                claim(WorkloadClaimType.VALIDATION, 4, 9, 9, 4, Set.of("node"))))
                .isInstanceOf(TapstateException.class);
    }

    private static WorkloadClaim claim(WorkloadClaimType type, long generation, long execution,
            long contextExecution, long contextClaim, Set<String> nodes) {
        var key = ValidationClaims.key("cluster", "orders");
        return new WorkloadClaim(new WorkloadClaimKey(key.clusterId(), type, key.resourceId()),
                new WorkloadOwner("node", "boot"), generation, execution, 7, Instant.EPOCH,
                contextExecution, contextClaim, nodes, 0, false);
    }
}
