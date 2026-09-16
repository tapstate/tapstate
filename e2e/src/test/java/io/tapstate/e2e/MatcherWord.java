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

    /**
     * How many changes the pipeline's nests could never place in a document, added up over its namespaces.
     * Its source is the metrics read face, which carries a count per namespace that discarded anything.
     *
     * <p>The only word here whose subject leaves no other trace. A count of rows, a state and a failure code
     * all describe something a specification could corner another way; discarded rows were never going to
     * appear in any document, so a pipeline throwing all of them away and one throwing none produce
     * identical counts, states and codes. Without this word a specification cannot tell those two apart.
     */
    DEAD_LETTERED,

    /**
     * The canonical code of the failure the pipeline has published. Its source is the status read face,
     * which carries the coded reason a run died alongside the state, so what killed a pipeline is assertable
     * rather than only greppable in a log.
     */
    FAILURE_CODE,

    /**
     * How many rows the pipeline has had confirmed by its targets, added up over its tables and source
     * operations. Its source is the metrics read face, which carries one total per direction.
     *
     * <p>Counted where the target confirmed them and nowhere earlier, which is the whole of what this word
     * is for: a pipeline handing rows to a sink that rejects every one of them reads the same as a healthy
     * one on every other word here - the rows left the source, the job runs, nothing was discarded - and
     * differs only in that this total stays at nought.
     *
     * <p><strong>A nought here needs a sibling asserting a real total to mean anything.</strong> The face
     * publishes no entry until something settles, and no entry reads as nought, so this word alone is
     * satisfied by a pipeline that published nothing at all.
     */
    RECORDS_OUT,

    /** The pipeline's published lifecycle state. */
    STATE;

    String word() {
        return name().toLowerCase(Locale.ROOT);
    }
}
