package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Submitting a job to a member that has only just started.
 *
 * <p><b>A member hands back a usable instance before it can take a job.</b> The service that coordinates
 * them finishes starting afterwards, and a named job submitted in that window is refused outright rather
 * than queued - the platform's own name for the refusal says to try again. On an idle machine the window
 * has closed before any test reaches it, which is why every case here submitted straight away and none
 * of them failed for it.
 *
 * <p><b>What made it worth a shared answer is the shape of the failure, not its frequency.</b> A job
 * that was never taken produces no documents and moves no positions - which is word for word what a
 * graph that produces nothing looks like, and what a chain that has been pinned looks like, and those
 * are the two things the cases around here exist to tell apart. So the one way this can go wrong is
 * disguised as the findings it would be reported as. Measured under a full build, where the window stays
 * open long enough to be hit: two cases spent ninety seconds each waiting for a job nobody had accepted.
 */
final class JetJobs {

    /** How long to keep offering the job before giving up and rethrowing the last refusal. */
    private static final long UNTIL_READY_SECONDS = 60;

    private JetJobs() {
    }

    /** The job, submitted under {@code name} once the member will take it. */
    static Job submit(HazelcastInstance member, DAG dag, String name) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(UNTIL_READY_SECONDS);
        RuntimeException refused = null;
        while (System.nanoTime() < deadline) {
            try {
                return member.getJet().newJob(dag, new JobConfig().setName(name));
            } catch (RuntimeException notReadyYet) {
                refused = notReadyYet;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
        throw refused;
    }
}
