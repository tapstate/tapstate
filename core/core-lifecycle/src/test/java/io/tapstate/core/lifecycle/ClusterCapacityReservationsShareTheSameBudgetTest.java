package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterCapacityReservationsShareTheSameBudgetTest {

    private final ClusterCapacityLimits limits = new ClusterCapacityLimits(16, 8, 8, 8, 4096, 8192);

    @Test
    void twoIndividuallySafeStartsCannotIgnoreEachOthersConnectorOccupancy() {
        ClusterCapacityDemand first = new ClusterCapacityDemand(6, 0, 5, 5, 1024, 2048);
        ClusterCapacityDemand second = new ClusterCapacityDemand(6, 0, 4, 4, 1024, 2048);
        assertThat(limits.violations(ClusterCapacityDemand.ZERO, first)).isEmpty();
        assertThat(limits.violations(ClusterCapacityDemand.ZERO, second)).isEmpty();
        assertThat(limits.violations(first, second)).containsExactly(
                new ClusterCapacityLimits.Violation("writers", 5, 4, 8),
                new ClusterCapacityLimits.Violation("connector-instances", 5, 4, 8));
    }

    @Test
    void aPendingRebuildUsesBudgetBeforeItsJobExists() {
        ClusterCapacityDemand running = new ClusterCapacityDemand(4, 1, 2, 2, 512, 1024);
        ClusterCapacityDemand recovering = new ClusterCapacityDemand(4, 1, 2, 2, 512, 1024);
        ClusterCapacityDemand following = new ClusterCapacityDemand(4, 1, 5, 5, 512, 1024);
        assertThat(limits.violations(running.plus(recovering), following)).containsExactly(
                new ClusterCapacityLimits.Violation("writers", 4, 5, 8),
                new ClusterCapacityLimits.Violation("connector-instances", 4, 5, 8));
    }

    @Test
    void aLongOverflowCannotMakeARequestFit() {
        ClusterCapacityLimits largest = new ClusterCapacityLimits(
                Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
        ClusterCapacityDemand almostFull = new ClusterCapacityDemand(0, 0, 0, 0, Long.MAX_VALUE - 1, 0);
        ClusterCapacityDemand request = new ClusterCapacityDemand(0, 0, 0, 0, 2, 0);
        assertThat(largest.violations(almostFull, request)).containsExactly(
                new ClusterCapacityLimits.Violation("buffered-records", Long.MAX_VALUE - 1, 2, Long.MAX_VALUE));
        assertThatThrownBy(() -> almostFull.plus(request)).isInstanceOf(ArithmeticException.class);
    }

    @Test
    void everyResourceLimitIsEnforcedIncludingAnAlreadyOverfullMember() {
        ClusterCapacityDemand occupied = new ClusterCapacityDemand(17, 9, 9, 9, 4097, 8193);
        assertThat(limits.violations(occupied, ClusterCapacityDemand.ZERO))
                .extracting(ClusterCapacityLimits.Violation::resource)
                .containsExactly("processors", "blocking-processors", "writers", "connector-instances",
                        "buffered-records", "edge-queue-records");
    }

    @Test
    void exactCeilingsRemainAdmissible() {
        ClusterCapacityDemand occupied = new ClusterCapacityDemand(8, 4, 4, 4, 2048, 4096);
        assertThat(limits.violations(occupied, occupied)).isEmpty();
    }
}
