package io.tapstate.core.lifecycle;

/** Input queue occupancy observed on one live execution of a pipeline job. */
public record QueueReading(long depth, long capacity, long highWater) {

    public QueueReading {
        if (depth < 0 || capacity < 1 || depth > capacity || highWater < depth) {
            throw new IllegalArgumentException("invalid input queue reading");
        }
    }
}
