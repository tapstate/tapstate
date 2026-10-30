package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.Processor;
import io.tapstate.runtime.engine.JobFailureRegistry;

/**
 * Where a nest vertex writes down what killed it, before Jet is told.
 *
 * <p>A nest fails a run with a code of its own - a document wider than one may be, a hand-over parked past
 * its limit, a stream tracking key changes over a source that sends no before image - and every one of them
 * names what an operator would have to change. None of that reaches the read faces on its own: once a job's
 * terminal result is durable, Jet tears down the live context behind the job's future and reconstructs a
 * cause-free throwable from the failure's stored text, so the walk that looks for a coded cause finds none
 * and the pipeline reports the generic engine failure instead. What was a specific, actionable diagnosis
 * becomes "the job died".
 *
 * <p>The linear transform and the sink already avoid that by recording the real exception synchronously, on
 * the way out, before the rethrow reaches Jet's own tasklet machinery. The nest vertices are the two that
 * did not, which is why their codes were the ones that never arrived. This is that same recording, in one
 * place because two vertices need it.
 *
 * <p>Absent where there is no member to record onto - a processor driven directly by a test - which is not
 * a degraded mode: nothing is reading a registry there either.
 */
final class NestFailureRecording {

    private final JobFailureRegistry.Recorder registry;
    private final String pipelineId;

    private NestFailureRecording(JobFailureRegistry.Recorder registry, String pipelineId) {
        this.registry = registry;
        this.pipelineId = pipelineId;
    }

    /** The recording for a vertex running in {@code context}, or one that records nothing. */
    static NestFailureRecording of(Processor.Context context) {
        HazelcastInstance instance = context == null ? null : context.hazelcastInstance();
        String pipelineId = context == null || context.jobConfig() == null
                ? null
                : context.jobConfig().getName();
        return new NestFailureRecording(
                instance == null ? null : JobFailureRegistry.of(instance).forJob(context.jobId(), context.jobConfig()), pipelineId);
    }

    /**
     * Runs {@code work}, writing down whatever it throws before letting it out.
     *
     * <p>Recorded rather than translated: what the read faces publish is decided where both the engine's
     * codes and the observation are in view, and a vertex that decided it here would be a second place that
     * has to agree with the first about what a coded cause is.
     */
    <T> T recording(Work<T> work) {
        try {
            return work.run();
        } catch (RuntimeException | Error failure) {
            if (registry != null && pipelineId != null) {
                registry.record(failure);
            }
            throw failure;
        }
    }

    /** A vertex's own step, which may fail with a code the run should be able to report. */
    @FunctionalInterface
    interface Work<T> {
        T run();
    }
}
