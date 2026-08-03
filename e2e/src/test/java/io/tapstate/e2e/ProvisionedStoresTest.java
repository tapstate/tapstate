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

    /**
     * Two run ids that a 32-bit hash cannot tell apart still get different databases.
     *
     * <p>The pair is constructed, not stumbled on: {@code aan} and {@code ac0} have the same
     * {@code String.hashCode}, so two ids sharing everything before them and differing only there
     * agree on that hash exactly - and they also share the truncated prefix, because what differs sits
     * past the cut. Under a hashCode digest both halves of the name would therefore be identical and
     * the two runs would share one database, dropping and reseeding each other's tables with nothing
     * reporting it. This is what a digest has to survive to be worth having.
     */
    @Test
    void twoRunsAHashCodeCannotTellApartStillGetDifferentNames() {
        String shared = "e2e_a_specification_named_long_enough_to_be_trimmed_at_the_limit_";

        String first = ProvisionedStores.database("src", shared + "aan");
        String second = ProvisionedStores.database("src", shared + "ac0");

        assertThat(("e2e_src_" + shared + "aan").hashCode())
                .as("the pair has to actually collide, or this test proves nothing")
                .isEqualTo(("e2e_src_" + shared + "ac0").hashCode());
        assertThat(first).isNotEqualTo(second);
        assertThat(first).hasSizeLessThanOrEqualTo(NAME_LIMIT);
    }

    /** Same inputs, same name: a run has to be able to find the database it made. */
    @Test
    void theNameIsTheSameEveryTimeItIsAskedFor() {
        String runId = "e2e_a_specification_with_a_name_long_enough_to_need_trimming_here_in_process";

        assertThat(ProvisionedStores.database("src", runId))
                .isEqualTo(ProvisionedStores.database("src", runId));
    }
}
