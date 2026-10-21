package io.tapstate.core.validation;

import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationIdentityTest {
    private static ValidationRunIdentity run(String runId, String boot, long generation,
                                              ValidationMode mode, String spec, long keyRevision) {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", boot,
                generation, 7), "task", runId, mode, spec, keyRevision, "rules-v1");
    }

    @Test void slotIgnoresMethodRevisionAndOwnerButSeparatesModes() {
        var a = run("run-a", "boot-a", 1, ValidationMode.FULL, "spec-a", 1);
        var b = run("run-b", "boot-b", 2, ValidationMode.FULL, "spec-b", 2);
        assertThat(a).isNotEqualTo(b);
        assertThat(a.slot()).isEqualTo(b.slot());
        assertThat(a.slot()).isNotEqualTo(run("run-c", "boot-a", 1,
                ValidationMode.INCREMENTAL, "spec-a", 1).slot());
    }

    @Test void callbacksCannotCrossRunBootOwnerSpecMappingOrObservationRevision() {
        var run = run("run-a", "boot-a", 1, ValidationMode.INCREMENTAL, "spec-a", 1);
        var root = new ValidationRoot("mapping-a", "output", key(1), List.of());
        var current = new ValidationObservation(run, root, 2);
        assertThat(current.isCurrentFor(new ValidationObservation(run, root, 2))).isTrue();
        assertThat(new ValidationObservation(run, root, 1).isCurrentFor(current)).isFalse();
        for (var stale : List.of(
                run("run-b", "boot-a", 1, ValidationMode.INCREMENTAL, "spec-a", 1),
                run("run-a", "boot-b", 1, ValidationMode.INCREMENTAL, "spec-a", 1),
                run("run-a", "boot-a", 2, ValidationMode.INCREMENTAL, "spec-a", 1),
                run("run-a", "boot-a", 1, ValidationMode.INCREMENTAL, "spec-b", 1),
                run("run-a", "boot-a", 1, ValidationMode.INCREMENTAL, "spec-a", 2))) {
            assertThat(new ValidationObservation(stale, root, 2).isCurrentFor(current)).isFalse();
        }
        assertThat(new ValidationObservation(run,
                new ValidationRoot("mapping-b", "output", key(1), List.of()), 2).isCurrentFor(current)).isFalse();
    }

    @Test void siblingPathsAndEveryAncestorDistinguishEqualChildKeys() {
        var a = new ValidationRoot("mapping", "output", key(1), List.of(
                new ValidationRoot.Ancestor(List.of("orders"), key(10)),
                new ValidationRoot.Ancestor(List.of("items"), key(11))));
        var b = new ValidationRoot("mapping", "output", key(1), List.of(
                new ValidationRoot.Ancestor(List.of("orders"), key(20)),
                new ValidationRoot.Ancestor(List.of("items"), key(11))));
        var c = new ValidationRoot("mapping", "output", key(1), List.of(
                new ValidationRoot.Ancestor(List.of("orders.items"), key(11))));
        var d = new ValidationRoot("mapping", "output", key(1), List.of(
                new ValidationRoot.Ancestor(List.of("orders", "items"), key(11))));
        assertThat(a).isNotEqualTo(b);
        assertThat(c).isNotEqualTo(d);
    }

    private static ValidationKey key(long value) {
        return new ValidationKey(List.of(ValidationKey.Part.number(value)));
    }
}
