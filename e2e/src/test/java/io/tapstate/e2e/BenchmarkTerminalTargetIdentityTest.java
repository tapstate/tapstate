package io.tapstate.e2e;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTerminalTargetIdentityTest {
    @Test
    void captureEpochCannotTruncateAFractionalOrFloatingIdentity() {
        assertThat(BenchmarkTerminalTargetChangesIT.captureEpoch(new Document("epoch", 1))).isEqualTo(1);
        assertThat(BenchmarkTerminalTargetChangesIT.captureEpoch(new Document("epoch", 2L))).isEqualTo(2);
        assertThatThrownBy(() -> BenchmarkTerminalTargetChangesIT.captureEpoch(new Document("epoch", 1.5)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("exact stored integer");
        assertThatThrownBy(() -> BenchmarkTerminalTargetChangesIT.captureEpoch(new Document("epoch", 1.0)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("exact stored integer");
    }
}
