package io.tapstate.core.lifecycle;

import java.io.Serializable;
import java.util.function.Consumer;

/** Member-local capture of an original execution failure before a runtime erases its exception chain. */
@FunctionalInterface
public interface ExecutionFailureRecorder {
    String USER_CONTEXT_KEY = "tapstate.execution-failure-recorder";
    String EXECUTION_IDENTITY_ARGUMENT = "tapstate.execution-identity";

    /** Binds once to the submitted job and immutable execution identity; callbacks retain that binding. */
    Consumer<Throwable> forExecution(String pipelineId, long jobId, Serializable identity);
}
