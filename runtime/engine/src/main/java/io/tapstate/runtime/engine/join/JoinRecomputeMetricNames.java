package io.tapstate.runtime.engine.join;

/**
 * What the progress of a large rebuild is called among a run's statistics, how to read one back, and how
 * large a fan-out has to be before one is left at all.
 *
 * <p>Naming and parsing sit together because they are one contract with two ends: a run leaves readings
 * under these names and the engine picks them out again by them. The two drifting apart would show up as
 * a pipeline that simply reports no rebuild - which is also exactly what a pipeline with no large rebuild
 * looks like, so nothing would ever contradict it.
 *
 * <p><b>Two numbers under two names rather than one reading.</b> A rebuild's whole meaning is the
 * distance between them, and a run's statistics carry numbers alone, so the pair has to travel as a pair
 * of numbers. What they are about travels in the name.
 *
 * <p><b>The subject is not taken apart again, and that is what lets both halves of it be arbitrary.</b>
 * The namespace is a map name with separators of its own, and the dimension key is a value out of the
 * user's table, so no split could tell where one ended and the other began. A reader wanting the key
 * reads the tail of the subject; nothing here promises to do it for them.
 */
public final class JoinRecomputeMetricNames {

    /** What the name of "how many rows have gone out" begins with, before the subject it is about. */
    public static final String DONE_PREFIX = "joinRecompute.done.";

    /** What the name of "how many rows there are about" begins with, before the same subject. */
    public static final String EXPECTED_PREFIX = "joinRecompute.expected.";

    /** What joins the namespace to the dimension key, chosen because a map name never holds one. */
    private static final String SEPARATOR = "/";

    /**
     * How large a fan-out has to be before rebuilding it is worth showing anybody.
     *
     * <p><b>Without a threshold this number is noise, and noise is how a number like this comes to be
     * ignored.</b> Every edit to any dimension row rebuilds something; reporting each one puts a constant
     * stream in front of whoever is watching, and the one report that mattered arrives in the middle of
     * it. Below this many rows a rebuild is over in well under a second, which is not a wait anybody has
     * to be told about.
     *
     * <p>Applied here rather than where the number is produced: which rebuilds are worth surfacing is a
     * reporting decision, and every carrier would otherwise write its own copy of it.
     */
    public static final long REPORT_FANOUT_ABOVE = 10_000L;

    private JoinRecomputeMetricNames() {
    }

    /** Whether a rebuild of about {@code rowsExpected} rows is large enough to leave a reading at all. */
    public static boolean worthReporting(long rowsExpected) {
        return rowsExpected >= REPORT_FANOUT_ABOVE;
    }

    /** What a rebuild of {@code dimensionKey}'s fan-out in {@code namespace} is reported as being about. */
    public static String subjectOf(String namespace, String dimensionKey) {
        return namespace + SEPARATOR + dimensionKey;
    }

    /** The name the rows sent so far of that rebuild are left under. */
    public static String doneNameOf(String namespace, String dimensionKey) {
        return DONE_PREFIX + subjectOf(namespace, dimensionKey);
    }

    /** The name the rows that rebuild has altogether are left under. */
    public static String expectedNameOf(String namespace, String dimensionKey) {
        return EXPECTED_PREFIX + subjectOf(namespace, dimensionKey);
    }

    /** The subject a rows-sent reading named {@code metric} is about, or {@code null} when it is not one. */
    public static String doneSubjectOf(String metric) {
        return subjectAfter(metric, DONE_PREFIX);
    }

    /** The subject a rows-altogether reading named {@code metric} is about, or {@code null} when not one. */
    public static String expectedSubjectOf(String metric) {
        return subjectAfter(metric, EXPECTED_PREFIX);
    }

    private static String subjectAfter(String metric, String prefix) {
        return metric.startsWith(prefix) ? metric.substring(prefix.length()) : null;
    }
}
