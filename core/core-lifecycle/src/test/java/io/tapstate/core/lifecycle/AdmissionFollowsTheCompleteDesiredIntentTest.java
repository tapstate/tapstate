package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AdmissionFollowsTheCompleteDesiredIntentTest {
    @Test
    void keepingTheSameStateAndRevisionDoesNotPreserveAChangedStartInstruction() {
        DesiredState ordinary = new DesiredState("orders", PipelineState.RUNNING, "R7");
        assertThat(DesiredStateFingerprint.of(ordinary)).isNotEqualTo(DesiredStateFingerprint.of(
                new DesiredState("orders", PipelineState.RUNNING, "R7", true)));
        assertThat(DesiredStateFingerprint.of(ordinary)).isNotEqualTo(DesiredStateFingerprint.of(
                new DesiredState("orders", PipelineState.RUNNING, "R7", false, "A8", false)));
        assertThat(DesiredStateFingerprint.of(ordinary)).isNotEqualTo(DesiredStateFingerprint.of(
                new DesiredState("orders", PipelineState.RUNNING, "R7", false, null, true)));
    }

    @Test
    void aDifferentAcceptedRestartEpochInvalidatesTheOriginalReservation() {
        DesiredState first = new DesiredState("orders", PipelineState.RUNNING, "R7", false, "A8", true, 41L);
        DesiredState second = new DesiredState("orders", PipelineState.RUNNING, "R7", false, "A8", true, 42L);
        assertThat(DesiredStateFingerprint.of(first)).isNotEqualTo(DesiredStateFingerprint.of(second));
    }

    @Test
    void absentFieldsCannotCollideWithEmptyOrLiteralNullData() {
        DesiredState absent = new DesiredState("orders", PipelineState.RUNNING, "R7");
        DesiredState empty = new DesiredState("orders", PipelineState.RUNNING, "R7", false, "", false);
        DesiredState literal = new DesiredState("orders", PipelineState.RUNNING, "R7", false, "null", false);
        assertThat(DesiredStateFingerprint.of(absent)).isNotEqualTo(DesiredStateFingerprint.of(empty))
                .isNotEqualTo(DesiredStateFingerprint.of(literal));
    }

    @Test
    void equivalentIntentInstancesHaveTheSameIdentity() {
        DesiredState one = new DesiredState("orders", PipelineState.RUNNING, "R7", false, "A8", true, 41L);
        DesiredState copy = new DesiredState("orders", PipelineState.RUNNING, "R7", false, "A8", true, 41L);
        assertThat(DesiredStateFingerprint.of(one)).isEqualTo(DesiredStateFingerprint.of(copy));
    }
}
