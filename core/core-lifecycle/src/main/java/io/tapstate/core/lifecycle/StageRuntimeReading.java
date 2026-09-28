package io.tapstate.core.lifecycle;

/** Independent projections of one validated native job collection. */
public record StageRuntimeReading(StageWorkReading activeWork, StageQueueReading queues) {

    public static final StageRuntimeReading NONE = new StageRuntimeReading(StageWorkReading.NONE, StageQueueReading.NONE);

    public StageRuntimeReading {
        activeWork = activeWork == null ? StageWorkReading.NONE : activeWork;
        queues = queues == null ? StageQueueReading.NONE : queues;
    }
}
