package io.tapstate.core.common;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class OperationErrorDiagnosticsTest {
    @Test void requestOwnershipRestoresNestedScopeAndBoundsSupplementaryDetails() {
        OperationErrorDiagnostics.confirmed("ignored outside request");
        assertThat(OperationErrorDiagnostics.active()).isFalse();
        try (var outer = OperationErrorDiagnostics.open()) {
            OperationErrorDiagnostics.confirmed("outer");
            try (var inner = OperationErrorDiagnostics.open()) {
                assertThat(OperationErrorDiagnostics.appendTo("error")).isEqualTo("error");
                OperationErrorDiagnostics.confirmed("x".repeat(3000));
                assertThat(OperationErrorDiagnostics.appendTo("error").length()).isEqualTo(5 + 20 + 2048);
            }
            assertThat(OperationErrorDiagnostics.appendTo("error")).isEqualTo("error Storage structure: outer");
        }
        assertThat(OperationErrorDiagnostics.active()).isFalse();
    }
}
