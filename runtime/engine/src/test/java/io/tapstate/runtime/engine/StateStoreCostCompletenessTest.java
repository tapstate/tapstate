package io.tapstate.runtime.engine;

import io.tapstate.runtime.engine.StateStoreCostMetricNames.Kind;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** A live job's true member-local reading may not be represented as a whole-job cost. */
class StateStoreCostCompletenessTest {

    private static final Set<String> MEMBERS = Set.of("member-a", "member-b");
    private static final Instant STARTED = Instant.parse("2026-09-27T08:00:00Z");

    @Test
    void oneMissingMemberOrMixedExecutionSuppressesEveryCost() {
        Engine.CostMeasurements partial = new Engine.CostMeasurements();
        addMember(partial, "member-a", "exec-one", 1);
        assertThat(partial.complete(MEMBERS, STARTED))
                .as("one member's observed write cannot become the two-member pipeline total")
                .isEmpty();

        addMember(partial, "member-b", "exec-one", 0);
        assertThat(partial.complete(MEMBERS, STARTED)).isPresent()
                .get().satisfies(reading -> assertThat(reading.operations().get("save").completed())
                        .isEqualTo(1));

        Engine.CostMeasurements mixed = new Engine.CostMeasurements();
        addMember(mixed, "member-a", "exec-one", 1);
        addMember(mixed, "member-b", "exec-two", 0);
        assertThat(mixed.complete(MEMBERS, STARTED)).isEmpty();

        Engine.CostMeasurements changedMembership = new Engine.CostMeasurements();
        addMember(changedMembership, "member-a", "exec-one", 1);
        addMember(changedMembership, "member-b", "exec-one", 0);
        assertThat(changedMembership.complete(Set.of("member-a"), STARTED))
                .as("an old two-member execution is absent after membership shrinks")
                .isEmpty();
    }

    private static void addMember(Engine.CostMeasurements measured, String member,
            String execution, long saveCount) {
        for (Kind kind : Kind.values()) {
            long value = switch (kind) {
                case READY -> 1;
                case CLUSTER_SIZE -> 2;
                case SAVE_COMPLETED -> saveCount;
                default -> 0;
            };
            measured.add(kind, member, execution, value);
        }
    }
}
