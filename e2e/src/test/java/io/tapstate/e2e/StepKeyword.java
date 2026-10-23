package io.tapstate.e2e;

import java.util.Locale;

/**
 * The keywords that introduce a step carrying a body.
 *
 * <p>A step written on its own is a lifecycle verb and needs no keyword - the product's verb set
 * defines those. These are the steps that take something with them, so they are named, and naming
 * them in an enum makes the parser's dispatch exhaustive: a keyword added here will not compile
 * until the parser and the schema both account for it.
 */
enum StepKeyword {

    /** Produces changes against a seeded table while the pipeline runs. */
    CDC,

    /** Polls a matcher until it holds or the bound expires. */
    AWAIT,

    /** Requires a start admission conflict with the named code. */
    START_REFUSED,

    /** Checks a matcher once, now. */
    ASSERT;

    String word() {
        return name().toLowerCase(Locale.ROOT);
    }
}
