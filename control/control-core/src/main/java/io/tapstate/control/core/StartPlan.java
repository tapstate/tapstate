package io.tapstate.control.core;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.spi.store.StartLoad;

import java.util.List;
import java.util.Objects;

/**
 * What a start would do to each target its pipeline writes: load it afresh, carry on from where the
 * pipeline stopped, or read changes only -- and, for a new full load, what happens to rows already there.
 *
 * <p>A prediction, made before anything is written, by the same judgement the run's preparation makes
 * when it acts. It is never stored: the run decides again for itself, from the same records.
 *
 * @param load    how this start loads the pipeline's targets; the same for every entry today, because the
 *                judgement reads the pipeline's records rather than any one target's
 * @param entries one per target table, in the order the pipeline declares its write elements
 */
public record StartPlan(StartLoad load, List<Entry> entries) {

    public StartPlan {
        Objects.requireNonNull(load, "load");
        entries = List.copyOf(entries);
    }

    /**
     * One target table and what this start does to it.
     *
     * @param target     where the table is and which element writes it
     * @param load       how this start loads it
     */
    public record Entry(PipelineWriteTargets.WriteTarget target, StartLoad load) {

        public Entry {
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(load, "load");
        }

        /** The target's coordinate, {@code <connection>/<table>}: what names it in a finding's key. */
        public String coordinate() {
            return target.connection() + "/" + target.table();
        }

        /** What a new full load does to rows already in this target. */
        public OnFullLoad onFullLoad() {
            return target.onFullLoad();
        }
    }
}
