package io.tapstate.e2e;

import java.util.Locale;

/**
 * The words that introduce a matcher.
 *
 * <p>An enum rather than a set of string cases, so the parser's dispatch is an exhaustive switch: a
 * word added here stops the parser and the schema generator from compiling until both say what it
 * means. That is the only version of "the vocabulary is single-sourced" a reader can trust, since it
 * is checked by the compiler rather than by whoever remembers.
 *
 * <p>A word is admitted only once something real answers it. {@code count} reads the target
 * database and {@code state} reads the published observation; a word whose source the runtime does
 * not populate would poll an empty reading until timeout, which is a worse answer than not offering
 * the word.
 */
enum MatcherWord {

    /** Rows present at an endpoint, read from the endpoint itself. */
    COUNT,

    /**
     * One document at an endpoint, located by equality settings and held to scalar values by path and
     * to list lengths by path. This is the word that makes "the right rows crossed" assertable rather
     * than only "rows crossed": a count is satisfied by any rows at all, and every value-level claim -
     * a field surviving the crossing, an embedded array holding exactly its children - needs to read
     * inside one document. Reads through the same independent driver a count does.
     */
    DOC,

    /**
     * The count of observable errors the pipeline has published. Its source is the metrics read face,
     * which the runtime derives from the pipeline's actual state - one while it is FAILED, zero otherwise -
     * so a dead data-plane job is an assertable statistic and not only a log line. The other run
     * statistics have no source wired yet, so no word offers them.
     */
    ERROR_COUNT,

    /** The pipeline's published lifecycle state. */
    STATE;

    String word() {
        return name().toLowerCase(Locale.ROOT);
    }
}
