package io.tapstate.control.core;

/**
 * Which start a set of start checks is evaluated for.
 *
 * <p>{@link #RERUN} is a start that follows a stop clearing this pipeline's state: the shape a rerun
 * from the beginning takes, asked about before the stop is sent so that everything a person has to
 * answer is asked in one round, while the pipeline is still running untouched.
 */
public enum StartIntent {
    START,
    RERUN
}
