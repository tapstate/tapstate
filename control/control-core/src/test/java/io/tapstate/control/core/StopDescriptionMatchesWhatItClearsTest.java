package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.core.lifecycle.PipelineStateInventory;
import org.junit.jupiter.api.Test;

/**
 * What the stop verb says about itself, held to what a stop actually reaches.
 *
 * <p>The description is rendered from the declarations rather than written out, so these cases are
 * pinning that it stays rendered. A sentence maintained by hand agrees with the clearing only for as
 * long as somebody keeps checking it, and the day it stops agreeing every word of it is still true:
 * what a reader cannot notice is the item that is no longer mentioned.
 */
class StopDescriptionMatchesWhatItClearsTest {

    @Test
    void everyKindOfStateAStopReachesIsNamedInItsDescription() {
        String description = ControlOperations.PIPELINE_STOP.description();

        assertThat(PipelineStateInventory.vocabulary()).isNotEmpty();
        for (PipelineStateHolding holding : PipelineStateInventory.vocabulary()) {
            assertThat(description)
                    .as("a kind of state a stop reaches that its own description does not mention")
                    .contains(holding.label());
        }
    }

    @Test
    void bothOutcomesAreDescribed() {
        String description = ControlOperations.PIPELINE_STOP.description();

        // The silent half is the dangerous one. A description covering only the clearing path is harder
        // to catch than one gone stale -- every word of it is true, and the caller who needed to know
        // the other answer exists has nothing to notice.
        assertThat(description).contains("purgeState true");
        assertThat(description).contains("purgeState false");
        assertThat(description).contains(PipelineStateInventory.describe(
                true, PipelineStateInventory.vocabulary()));
        assertThat(description).contains(PipelineStateInventory.describe(
                false, PipelineStateInventory.vocabulary()));
    }

    @Test
    void theDescriptionSaysTheUsersOwnDatabaseIsNotTouched() {
        // The fear a stop actually raises. Nothing a stop does reaches the rows the pipeline wrote, and
        // a description that leaves that unsaid makes the safe answer look like the dangerous one.
        assertThat(ControlOperations.PIPELINE_STOP.description())
                .contains(PipelineStateInventory.TARGET_UNTOUCHED);
    }

    @Test
    void sayingItIsRequiredIsPartOfTheDescription() {
        // The argument has no default, and a caller reading this is the one who has to supply it.
        assertThat(ControlOperations.PIPELINE_STOP.description()).contains("required");
    }
}
