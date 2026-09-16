package io.tapstate.cli;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * The fixed checklist {@code status} walks so that a run which is not working gives one answer instead of
 * four faces to correlate by hand.
 *
 * <p>Five rules, in order, first match wins, and every one of them is answered out of readings that already
 * exist: how old the reading is, the coded reason a run died, the error count against the state, whether
 * anything has moved at all, and whether a chain's durable position has stood still. Nothing is measured
 * here that was not measured before; what is new is that somebody no longer has to know which face to open.
 *
 * <p><strong>Five, and growing it is not a code change made in passing.</strong> The list is short on
 * purpose: a checklist that accretes a rule per incident becomes a health model, which is precisely the
 * thing this was scoped not to build. The list and its contents are fixed in the design record, which lives
 * in a private repository; a sixth rule is a decision taken there first and reflected here afterwards.
 *
 * <p><strong>No match is an answer, not silence.</strong> When nothing matches, this does not report that
 * all is well — it hands back every reading it went through and names what it could not decide and what is
 * missing to decide it. A product that says "healthy" because its checklist came up empty is claiming to
 * have looked at things it cannot see.
 */
final class StatusDiagnosis {

    /**
     * How old a reading has to be before the answer says the publisher may have stopped.
     *
     * <p><strong>It decides a sentence, never a state.</strong> No lifecycle state is derived from it, none
     * is invented, and the status face returns exactly the states it returned before. A threshold that
     * turned into a state would be this product declaring a run dead on a timer it chose for itself, which
     * is the failure the whole reading exists to avoid; what it chooses between here is two honest
     * sentences.
     *
     * <p>Thirty seconds because the convergence pass republishes every second by default. Thirty is thirty
     * missed passes — past any pause a healthy server takes, and short enough that somebody looking at a
     * run that has stopped gets the answer on their first retry rather than their fifth.
     */
    static final Duration PUBLISHER_SILENCE = Duration.ofSeconds(30);

    private StatusDiagnosis() {
    }

    /**
     * One answer: what it concluded, which face and value it read to conclude it, where to look next, and
     * what it could not decide.
     *
     * @param conclusion what this says happened, in one line
     * @param readings   the face and value behind it, so the conclusion can be checked rather than believed
     * @param next       where to look next, or null when the answer is the readings themselves
     * @param cannotSay  the questions these faces cannot answer, each with what is missing to answer it.
     *                   Never empty on a no-match answer: that is the case in which a reader is most likely
     *                   to take silence for a clean bill of health
     */
    record Answer(String conclusion, List<String> readings, String next, List<String> cannotSay) {

        Answer {
            readings = readings == null ? List.of() : List.copyOf(readings);
            cannotSay = cannotSay == null ? List.of() : List.copyOf(cannotSay);
        }
    }

    /**
     * Rules 1 and 2, which the status face answers on its own.
     *
     * <p>Separated so the caller can stop there: a run whose publisher is gone, or whose job died with a
     * coded reason, is diagnosed without reading three more faces — and in the first of those two, the
     * other faces would only be re-reading the same stale observation anyway.
     */
    static Optional<Answer> fromStatusAlone(
            String pipelineId, String state, String failureCode, Long observedAgeMillis) {
        if (observedAgeMillis != null && observedAgeMillis >= PUBLISHER_SILENCE.toMillis()) {
            return Optional.of(new Answer(
                    "this reading is " + human(observedAgeMillis) + " old, so the publisher may have stopped",
                    List.of("status.observedAt = " + human(observedAgeMillis) + " ago"),
                    "check the server is up and converging -- anything else here was read from that same "
                            + "observation, so it is that old too",
                    List.of("what the pipeline is doing now: this says only what was last published")));
        }
        if (failureCode != null) {
            return Optional.of(new Answer(
                    "the run failed, and said why: " + failureCode,
                    List.of("status.failure = " + failureCode),
                    "tapstate logs " + pipelineId,
                    List.of()));
        }
        return Optional.empty();
    }

    /**
     * The whole checklist.
     *
     * @param metrics       the metrics face's readings, or null when that face could not be read — which is
     *                      carried through to the answer rather than treated as an empty face, because
     *                      "nothing is wrong there" and "nobody looked" are the two readings this exists to
     *                      keep apart
     * @param snapshotRowsLoaded how many rows the snapshot face reports loaded across every table it
     *                      carries, or null when that face could not be read, for the same reason. Rows
     *                      rather than tables, and measured rather than assumed: that face holds an entry
     *                      for every selected table from the moment a run starts and keeps it for the life
     *                      of the run, so counting its entries answers "how many tables were selected",
     *                      which is true of a run that has loaded nothing and of one that finished hours
     *                      ago alike. It reports no total for a table and therefore no completion, so what
     *                      it can be asked is how much it has loaded, never whether it is still loading
     */
    static Answer of(String pipelineId, String state, String failureCode, String failureMessage,
            Long observedAgeMillis, MetricsFacts metrics, Long snapshotRowsLoaded) {
        Optional<Answer> early = fromStatusAlone(pipelineId, state, failureCode, observedAgeMillis);
        if (early.isPresent()) {
            return early.get();
        }
        if (metrics != null && metrics.errorCount() != null && metrics.errorCount() > 0 && converging(state)) {
            // Deliberately not "cannot reach the store": this count rises whenever a convergence pass
            // throws, and a plan that cannot be built throws exactly the same way a store that cannot be
            // reached does. Naming one of the causes would send a reader to check a database that is fine.
            return new Answer(
                    "the server keeps failing to bring this pipeline up: " + metrics.errorCount()
                            + " passes in a row have thrown",
                    List.of("metrics.errorCount = " + metrics.errorCount(),
                            "status.state = " + lower(state)),
                    "read the server's own log -- the reason is printed there once per pass",
                    List.of("whether the job itself is still alive: nothing here has seen it die, so the "
                            + "state stays " + lower(state) + " rather than being guessed into a failure"));
        }
        if (metrics != null && snapshotRowsLoaded != null && converging(state)
                && (metrics.recordCount() == null || metrics.recordCount() == 0) && snapshotRowsLoaded == 0) {
            // Gated on the state for the same reason rule 3 is: both readings this matches on are also
            // true of every pipeline with no live job. No job is asked for a record count, and the
            // snapshot face is dropped when a pipeline stops -- so without the gate a run somebody just
            // stopped, and a bounded run that reached COMPLETED after moving every row, are both told
            // that nothing has moved and sent to read the logs.
            // This rule deliberately says nothing about a source whose schema was never discovered, which is
            // the shape most likely to be added here by whoever reads it next. Measured, not assumed: such a
            // source is refused before it runs, with its own code naming it, so it never reaches this rule --
            // the failure code is read one rule earlier, and repeating it here would send a reader to run a
            // discovery that is not what is wrong. The one shape that starts anyway, a view over literally
            // named tables, does not reach this rule either, and for a worse reason: it moves rows, and the
            // rows are missing every column but the key. No face here can see that, and none pretends to --
            // records were driven and rows were loaded, so the readings this rule matches on are absent.
            // Filed as tapstate/tapstate#407; when it is fixed, that shape joins the refusal above, not this.
            return new Answer(
                    "nothing has moved: no records driven and no rows loaded",
                    List.of("metrics.recordCount = "
                                    + (metrics.recordCount() == null ? "not published" : metrics.recordCount()),
                            "snapshot = no rows loaded"),
                    "tapstate logs " + pipelineId,
                    List.of("whether the source simply has nothing new: how far the source could be read to "
                            + "is not collected, so an idle source and a read that is stuck look the same "
                            + "from here"));
        }
        if (metrics != null && !metrics.stalledChains().isEmpty()) {
            List<String> readings = new ArrayList<>();
            metrics.stalledChains().forEach((chain, millis) ->
                    readings.add("metrics.frontierStalledMillis." + chain + " = " + human(millis)));
            return new Answer(
                    "a chain has stopped advancing: " + String.join(", ", metrics.stalledChains().keySet()),
                    readings,
                    "check the target is accepting writes -- the chain is holding changes it cannot confirm",
                    List.of());
        }
        return nothingMatched(state, observedAgeMillis, metrics, snapshotRowsLoaded);
    }

    /** Every reading the checklist went through, plus what these faces structurally cannot answer. */
    private static Answer nothingMatched(
            String state, Long observedAgeMillis, MetricsFacts metrics, Long snapshotRowsLoaded) {
        List<String> readings = new ArrayList<>();
        readings.add("status.observedAt = "
                + (observedAgeMillis == null ? "not known" : human(observedAgeMillis) + " ago"));
        readings.add("status.failure = none");
        if (metrics == null) {
            readings.add("metrics = could not be read");
        } else {
            readings.add("metrics.errorCount = "
                    + (metrics.errorCount() == null ? "not published" : metrics.errorCount()));
            readings.add("metrics.recordCount = "
                    + (metrics.recordCount() == null ? "not published" : metrics.recordCount()));
            readings.add("metrics.frontierStalledMillis = none above zero");
        }
        readings.add("snapshot = " + (snapshotRowsLoaded == null ? "could not be read"
                : snapshotRowsLoaded == 0 ? "no rows loaded" : snapshotRowsLoaded + " row(s) loaded"));

        List<String> cannotSay = new ArrayList<>();
        if (observedAgeMillis == null) {
            cannotSay.add("how old any of this is: the observation carries no time, so a run that stopped "
                    + "publishing reads the same as one that has not changed");
        }
        cannotSay.add("whether the source has changes waiting: how far the source could be read to is not "
                + "collected");
        if (lower(state).equals("paused")) {
            cannotSay.add("whether a paused run's job is still alive: a failure is only detected while the "
                    + "job is meant to be running");
        }
        if (metrics == null) {
            cannotSay.add("anything the metrics face answers: it could not be read on this call");
        }
        if (snapshotRowsLoaded == null) {
            cannotSay.add("how much of the initial load is done: the snapshot face could not be read on "
                    + "this call");
        } else {
            cannotSay.add("whether an initial load is still running: the snapshot face reports how many "
                    + "rows each table loaded and no total to measure them against, so a load in flight "
                    + "and a finished one read the same here");
        }
        return new Answer("nothing on this checklist matched -- here is everything it read", readings, null,
                cannotSay);
    }

    /** States in which the server is meant to be driving the pipeline, so a convergence error is news. */
    private static boolean converging(String state) {
        String now = lower(state);
        return now.equals("running") || now.equals("new");
    }

    private static String lower(String state) {
        return state == null ? "" : state.toLowerCase(Locale.ROOT);
    }

    /** A duration a person reads at a glance, rather than a number of milliseconds they have to divide. */
    private static String human(long millis) {
        if (millis < 1_000) {
            return millis + "ms";
        }
        long seconds = millis / 1_000;
        if (seconds < 60) {
            return seconds + "s";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + "m" + (seconds % 60) + "s";
        }
        long hours = minutes / 60;
        return hours + "h" + (minutes % 60) + "m";
    }
}
