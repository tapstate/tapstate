package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The vocabulary has to be one thing, not a set of copies that happen to agree today.
 *
 * <p>Every word an author may write is derived here from what the parser actually dispatches on, or
 * from the product enum that owns it. The failure this prevents is the quiet one: a word list in a
 * generated file, a second list in an error message, and a third in the parser - all correct on the
 * day they were written, and no gate to notice when one of them stops being.
 */
class VocabularyTest {

    @Test
    void lifecycleStepsAreTheProductsOwnVerbs() {
        // Not a hand-kept copy: the product's verb set is the definition of what a lifecycle step is,
        // so a verb the product gains is a step this harness accepts without an edit here.
        assertThat(Vocabulary.LIFECYCLE_STEPS).containsExactly("pause", "resume", "start", "stop");
    }

    @Test
    void cdcOperationsAreTheHarnessesOwnEnum() {
        assertThat(Vocabulary.CDC_OPERATIONS).containsExactly("delete", "insert", "update");
    }

    @Test
    void matcherWordsAreTheOnesTheParserAccepts() {
        assertThat(Vocabulary.MATCHERS).containsExactly("count", "doc", "error_count", "state");
    }

    @Test
    void bodiedStepWordsAreTheOnesTheParserAccepts() {
        assertThat(Vocabulary.BODIED_STEPS).containsExactly("assert", "await", "cdc");
    }

    @Test
    void topLevelKeysAreTheEnvelopesOwnComponents() {
        // The envelope record is the shape the parser must produce, so its components are the keys -
        // a key list written out by hand could disagree with the record the parser fills.
        assertThat(Vocabulary.TOP_LEVEL_KEYS)
                .containsExactly("name", "setup", "pipeline", "seed", "steps");
    }

    /**
     * The order is the dependency order, and it is asserted rather than assumed: {@code databases} is the
     * harness's own and must come first, because a resource cannot be applied before the endpoint whose
     * address it interpolates exists. The remaining three are the product's own verbs, in their order.
     */
    @Test
    void setupKeysAreTheSetupsOwnComponents() {
        assertThat(Vocabulary.SETUP_KEYS)
                .containsExactly("databases", "connectors", "apply", "discover");
    }

    @Test
    void pipelineStatesAreTheProductsOwn() {
        assertThat(Vocabulary.PIPELINE_STATES)
                .containsExactly("COMPLETED", "FAILED", "NEW", "PAUSED", "RUNNING", "STOPPED");
    }
}
