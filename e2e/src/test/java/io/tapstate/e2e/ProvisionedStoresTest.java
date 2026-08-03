package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The naming this class does before any store is brought up. */
class ProvisionedStoresTest {

    private static final int NAME_LIMIT = 63;

    @Test
    void aShortEnoughNameIsLeftAlone() {
        assertThat(ProvisionedStores.database("src", "e2e_small_in_process"))
                .isEqualTo("e2e_src_e2e_small_in_process");
    }

    /**
     * The two tiers of one example differ only in a suffix at the very end of the run id, which is
     * exactly what a plain cut to the limit removes first. Cutting alone therefore gave both tiers the
     * same database - and because the tiers run one after the other and every seed drops its table
     * first, the collision would show up as nothing at all until two runs overlapped.
     */
    @Test
    void twoRunsDifferingOnlyPastTheLimitStillGetDifferentNames() {
        String example = "a_specification_with_a_name_long_enough_to_need_trimming_here";

        String inProcess = ProvisionedStores.database("src", "e2e_" + example + "_in_process");
        String realProcess = ProvisionedStores.database("src", "e2e_" + example + "_real_process");

        assertThat(inProcess).hasSizeLessThanOrEqualTo(NAME_LIMIT);
        assertThat(realProcess).hasSizeLessThanOrEqualTo(NAME_LIMIT);
        assertThat(inProcess).isNotEqualTo(realProcess);
    }

    /** Two stores of one run are told apart by the part that is not trimmed at all. */
    @Test
    void twoStoresOfOneRunGetDifferentNames() {
        String runId = "e2e_a_specification_with_a_name_long_enough_to_need_trimming_here_in_process";

        assertThat(ProvisionedStores.database("src", runId))
                .isNotEqualTo(ProvisionedStores.database("tgt", runId));
    }

    /** Same inputs, same name: a run has to be able to find the database it made. */
    @Test
    void theNameIsTheSameEveryTimeItIsAskedFor() {
        String runId = "e2e_a_specification_with_a_name_long_enough_to_need_trimming_here_in_process";

        assertThat(ProvisionedStores.database("src", runId))
                .isEqualTo(ProvisionedStores.database("src", runId));
    }
}
