package io.tapstate.spi.store;

import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterRecoveryDiagnosticTest {
    @Test
    void deeplyNamespacedConnectorCodesCanBeReadWithoutExhaustingTheStack() {
        String code = "connector" + ".position".repeat(10_000) + "-expired";
        ClusterRecoveryDiagnostic diagnostic = new ClusterRecoveryDiagnostic(
                ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                code, Map.of(), Map.of(), "Restore the retained source position");
        assertThat(diagnostic.code()).isEqualTo(code);
    }

    @Test
    void malformedDeeplyNamespacedCodesAreRejectedAsInvalidInsteadOfExhaustingTheStack() {
        String code = "connector" + ".position".repeat(10_000) + "_expired";
        assertThatThrownBy(() -> new ClusterRecoveryDiagnostic(
                ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED,
                code, Map.of(), Map.of(), "Restore the retained source position"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("diagnostic code must be canonical");
    }
}
