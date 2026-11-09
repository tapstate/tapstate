package io.tapstate.control.core;

import io.tapstate.core.lifecycle.NestInspection;

import java.util.List;
import java.util.Objects;

/**
 * Reads what a running nest step holds for one root key. Implemented where the run's state lives; the control
 * plane asks it and turns what it answers into a view or a refusal.
 */
public interface NestStateInspector {

    /** An inspector for a build with no run to read: every step reads as not running. */
    NestStateInspector NONE = (pipelineId, step, rootKey) -> new Result.NotRunning();

    /**
     * What {@code step} of {@code pipelineId} holds under {@code rootKey}, one value per root key column, as
     * typed by a person.
     */
    Result inspect(String pipelineId, String step, List<String> rootKey);

    /** What a lookup found. */
    sealed interface Result {

        /** State is held under the key. */
        record Held(NestInspection inspection) implements Result {
            public Held {
                Objects.requireNonNull(inspection, "inspection");
            }
        }

        /** The key was given with a different number of values than the root key has columns. */
        record WrongWidth(List<String> columns) implements Result {
            public WrongWidth {
                columns = List.copyOf(columns);
            }
        }

        /**
         * Nothing is held under the readings of the key that were tried; {@code everyReading} says whether
         * that was every way the key may have been filed, or only a bounded subset of them.
         */
        record NotHeld(List<String> columns, boolean everyReading) implements Result {
            public NotHeld {
                columns = List.copyOf(columns);
            }
        }

        /** There is no live state for the step here to read. */
        record NotRunning() implements Result { }
    }
}
