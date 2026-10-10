package io.tapstate.control.core;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class NoClusterMutationIsRegisteredInBetaTest {
    @Test void everyClusterOperationIsAnExistingUnauditedRead() {
        var operations = ControlOperations.registry().all().stream()
                .filter(operation -> operation.id().startsWith("cluster.")).toList();
        assertThat(operations).extracting(Operation::id).containsExactlyInAnyOrder("cluster.members", "cluster.status");
        assertThat(operations).allSatisfy(operation -> {
            assertThat(operation.scope()).isEqualTo(Scope.READ);
            assertThat(operation.audited()).isFalse();
        });
    }
}
