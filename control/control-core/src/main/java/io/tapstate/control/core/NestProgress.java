package io.tapstate.control.core;

import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.PipelineState;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * How far each nest step of one pipeline has got: per table the step depends on, how many rows have been
 * read and where that table's load stands; how many whole documents the step has sent; and, in words a
 * reader can act on, what the step is waiting for.
 *
 * <p>Every number here is one the pipeline already published - the per-table load counters the snapshot
 * view shows and the count of documents the root level sent - so this view and the metrics can never
 * disagree. It is the latest reading only; there is no history behind it.
 *
 * @param pipelineId the pipeline asked about
 * @param state the pipeline's lifecycle state when the reading was taken
 * @param phase where the pipeline's initial load stands as a whole, or null when that is not known
 * @param observedAt when the reading was taken, or null when the pipeline did not say
 * @param steps one entry per nest step, in definition order; only the asked-for step when one was named
 */
public record NestProgress(String pipelineId, PipelineState state, InitialLoad.Phase phase, Instant observedAt,
        List<Step> steps) {

    public NestProgress {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(state, "state");
        steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
    }

    /** What a step is doing, in the one word its waiting line is built around. */
    public enum Activity {
        /** The pipeline is not running, so nothing is moving; the waiting line names the state. */
        NOT_RUNNING,
        /** At least one table the step depends on is still being read. */
        LOADING,
        /** Every table is read through and the target has not yet confirmed all of them. */
        LANDING,
        /** Every table's load has landed; the step applies changes as they arrive. */
        MAINTAINING,
        /** The pipeline reported no load for the step's tables, so there is nothing to wait for or count. */
        UNKNOWN
    }

    /**
     * One nest step.
     *
     * @param step the step's id as the pipeline definition names it
     * @param rootTable the table the documents are built from, by the name the load reports it under
     * @param rootsRendered whole documents the step has sent since its count began, or null when it has not
     *     reported any yet
     * @param rootsEstimate how many documents the root table's discovery estimated there are, or null
     * @param activity what the step is doing
     * @param waitingFor the tables the step is waiting on, in the order a reader should look at them
     * @param tables every table the step depends on, root first, then each embed in definition order
     */
    public record Step(String step, String rootTable, Long rootsRendered, Long rootsEstimate, Activity activity,
            List<String> waitingFor, List<Table> tables) {

        public Step {
            Objects.requireNonNull(step, "step");
            Objects.requireNonNull(activity, "activity");
            waitingFor = List.copyOf(Objects.requireNonNull(waitingFor, "waitingFor"));
            tables = List.copyOf(Objects.requireNonNull(tables, "tables"));
        }
    }

    /**
     * One table a step depends on.
     *
     * @param table the table, by the name the load reports it under, or the name the definition gave it when
     *     the load reports nothing for it
     * @param path where in the document its rows go: empty for the root, the embed's path otherwise, dotted
     *     for a nested embed; a flat embed carries its parent's path
     * @param stage where its initial load stands, or null when the pipeline reported none for it
     * @param rowsRead rows read so far, or null when not reported
     * @param rowsEstimate the discovered row estimate, or null when unavailable
     * @param rowsStaged rows held for a build, always null until the build phase is in place
     */
    public record Table(String table, String path, InitialLoad.Stage stage, Long rowsRead, Long rowsEstimate,
            Long rowsStaged) {

        public Table {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(path, "path");
        }
    }
}
