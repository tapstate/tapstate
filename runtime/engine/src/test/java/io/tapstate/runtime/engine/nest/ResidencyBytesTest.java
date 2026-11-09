package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Residency is bounded by what values weigh, not by how many there are. */
class ResidencyBytesTest {

    private static final long MIB = 1024L * 1024L;

    @Test
    void aNamespaceWithOneMebibyteAndTenKibibyteValuesHoldsAboutAHundred() {
        var resident = new ResidentValues<String>(MIB);
        for (int i = 0; i < 1_000; i++) {
            resident.putClean("key-" + i, new byte[10 * 1024]);
        }
        assertThat(resident.residentCount()).isBetween(95, 102);
        assertThat(resident.residentBytes()).isLessThanOrEqualTo(MIB);
    }

    @Test
    void theSameBudgetHoldsTenTimesAsManyValuesATenthTheSize() {
        var resident = new ResidentValues<String>(MIB);
        for (int i = 0; i < 10_000; i++) {
            resident.putClean("key-" + i, new byte[1024]);
        }
        assertThat(resident.residentCount()).isBetween(950, 1_000);
    }

    @Test
    void theLeastRecentlyUsedValueGoesFirst() {
        var resident = new ResidentValues<String>(3 * (1_000 + ResidentValues.ENTRY_OVERHEAD_BYTES));
        resident.putClean("a", new byte[1_000]);
        resident.putClean("b", new byte[1_000]);
        resident.putClean("c", new byte[1_000]);
        resident.get("a");
        resident.putClean("d", new byte[1_000]);
        assertThat(resident.get("a")).isPresent();
        assertThat(resident.get("b")).isEmpty();
        assertThat(resident.get("c")).isPresent();
        assertThat(resident.get("d")).isPresent();
    }

    @Test
    void stagedValuesStayResidentPastTheBudgetUntilCommitted() {
        var resident = new ResidentValues<String>(10 * 1024);
        for (int i = 0; i < 20; i++) {
            resident.stage("key-" + i, new byte[2 * 1024]);
        }
        for (int i = 0; i < 20; i++) {
            assertThat(resident.get("key-" + i)).hasValueSatisfying(value -> assertThat(value).isPresent());
        }
        assertThat(resident.staged()).hasSize(20);
        resident.committed();
        assertThat(resident.hasStaged()).isFalse();
        assertThat(resident.residentBytes()).isLessThanOrEqualTo(10 * 1024);
        assertThat(resident.get("key-19")).isPresent();
    }

    @Test
    void aStagedRemovalReadsAsAbsentAndAKnownAbsenceIsResident() {
        var resident = new ResidentValues<String>(MIB);
        resident.putClean("gone", new byte[]{1});
        resident.stageRemoval("gone");
        assertThat(resident.get("gone")).contains(Optional.empty());
        assertThat(resident.staged().get("gone")).isEmpty();
        resident.committed();
        assertThat(resident.get("gone")).contains(Optional.empty());
        resident.putClean("never", null);
        assertThat(resident.get("never")).contains(Optional.empty());
    }

    @Test
    void anAbandonedCommitForgetsWhatItWouldHaveWrittenAndWhatWasThere() {
        var resident = new ResidentValues<String>(MIB);
        resident.putClean("key", new byte[]{1});
        resident.stage("key", new byte[]{2});
        resident.abandonStaged();
        assertThat(resident.get("key")).isEmpty();
        assertThat(resident.residentBytes()).isZero();
    }

    @Test
    void aStagedValueIsNotOverwrittenByAStaleRead() {
        var resident = new ResidentValues<String>(MIB);
        resident.stage("key", new byte[]{2});
        resident.putClean("key", new byte[]{1});
        assertThat(resident.get("key")).hasValueSatisfying(value -> assertThat(value).hasValue(new byte[]{2}));
    }

    @Test
    void aStepsBudgetIsSharedEvenlyBetweenItsNamespaces() {
        assertThat(ResidencyBudget.perNamespace(64 * MIB, 4)).isEqualTo(16 * MIB);
        assertThat(ResidencyBudget.perNamespace(MIB, 1_000)).isEqualTo(ResidencyBudget.SMALLEST_NAMESPACE_BYTES);
        assertThat(ResidencyBudget.defaultStepBytes(4L * 1024 * MIB)).isEqualTo(512 * MIB);
        assertThatThrownBy(() -> ResidencyBudget.perNamespace(0, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ResidentValues<String>(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
