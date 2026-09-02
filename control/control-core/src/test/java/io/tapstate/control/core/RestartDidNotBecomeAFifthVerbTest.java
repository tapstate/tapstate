package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import org.junit.jupiter.api.Test;

/**
 * That restarting a pipeline stayed a composition of the verbs this product has, and did not become a
 * verb of its own.
 *
 * <p>There are four lifecycle verbs, and the decision that there are four is not a matter of taste: a
 * fifth that meant "carry on, or start over, depending on a flag" is the ambiguity the four were chosen
 * to remove. Restarting is a word a terminal offers over the four, and the composition is the terminal's
 * -- everything below it keeps seeing the verbs it already knew.
 *
 * <p>What this guards against is not a design somebody argues for. It is the smaller thing that happens
 * while implementing: adding the verb is the shorter path, every layer is already shaped to carry one,
 * and nothing else in this repository would notice. So the assertion is placed where the verb would
 * have to be declared, in both of the two places it would have to appear.
 */
class RestartDidNotBecomeAFifthVerbTest {

    @Test
    void thereAreStillFourLifecycleVerbs() {
        assertThat(LifecycleVerb.values())
                .as("a fifth verb here is the decision this is meant to stop being made by accident")
                .hasSize(4);
        assertThat(LifecycleVerb.values()).extracting(LifecycleVerb::id)
                .containsExactlyInAnyOrder("start", "pause", "resume", "stop");
    }

    @Test
    void theOperationRegistryCarriesNoRestart() {
        // The second place it would have to be declared. A verb can exist in the registry without a
        // matching enum constant -- that is what every non-lifecycle operation is -- so the enum check
        // above does not cover this one, and a registry entry is what would put it on the wire faces.
        assertThat(ControlOperations.all()).extracting(Operation::id)
                .doesNotContain("pipeline.restart");
    }
}
