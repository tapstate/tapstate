package io.tapstate.runtime.engine;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Running a member out of memory the way the substrate hears of it: an {@link OutOfMemoryError} escapes one of
 * its own threads, and that thread hands the error to the process's out-of-memory handling. The handling, not
 * the caller, decides what becomes of the members.
 *
 * <p><b>Exhausting the heap for real is not usable here.</b> Which thread the collector refuses first is not
 * under anybody's control, and a heap exhausted in this JVM starves the tests along with the member. So this
 * starts from the moment the error reaches the substrate, which is where everything under test begins.
 *
 * <p><b>The handling belongs to the process rather than to a member</b>, and it is handed every member this JVM
 * runs at that moment. A case using this starts the members it measures and shuts them down afterwards, and names
 * them when it raises the error: any other member still running would be taken down along with them, and a case
 * that took down a member some other case left behind would break that case rather than measure its own. So it
 * refuses to raise while any member it was not given is running.
 */
final class OutOfMemoryOnAMemberThread {

    /**
     * The error the collector raises when it gives up, rather than the one raised when a single allocation is
     * refused. The substrate's stock handling weighs a refused allocation against how full the heap is before it
     * acts, and a test JVM's heap is not full; a collector that gave up is reason enough on its own.
     */
    private static final String THE_COLLECTOR_GAVE_UP = "GC overhead limit exceeded";

    /** How long the thread gets to hand its error over and end. The handling runs on it, synchronously. */
    private static final Duration HANDED_OVER_WITHIN = Duration.ofSeconds(60);

    private OutOfMemoryOnAMemberThread() {
    }

    /**
     * The error that escaped the thread, returned once the out-of-memory handling has finished with it.
     * {@code members} are every member the case has running, and the only ones this JVM may be running.
     */
    static OutOfMemoryError raise(HazelcastInstance... members) {
        return escape(members).handled();
    }

    /**
     * The error escaping the thread, returned at once. The handling runs on that thread and may still be at work,
     * for as long as a member it is taking down takes to stop. {@code members} are as for {@link #raise}.
     */
    static Escaping escape(HazelcastInstance... members) {
        Set<HazelcastInstance> others = Hazelcast.getAllHazelcastInstances().stream()
                .filter(OutOfMemoryOnAMemberThread::isRunning)
                .filter(running -> !Set.of(members).contains(running))
                .collect(Collectors.toSet());
        if (!others.isEmpty()) {
            throw new AssertionError("members this case did not start are running in this JVM, and the out-of-memory "
                    + "handling would take them down too: " + others);
        }
        OutOfMemoryError error = new OutOfMemoryError(THE_COLLECTOR_GAVE_UP);
        Thread memberThread = new HazelcastManagedThread(() -> {
            throw error;
        }, "test-member-out-of-memory");
        memberThread.start();
        return new Escaping(error, memberThread);
    }

    private static boolean isRunning(HazelcastInstance member) {
        try {
            return member.getLifecycleService().isRunning();
        } catch (HazelcastInstanceNotActiveException gone) {
            return false;
        }
    }

    /** An error escaping a member thread, and that thread, on which the handling runs. */
    record Escaping(OutOfMemoryError error, Thread memberThread) {

        /** Whether the handling is still at work on the error. */
        boolean stillHandling() {
            return memberThread.isAlive();
        }

        /** The error, once the handling has finished with it. */
        OutOfMemoryError handled() {
            try {
                if (!memberThread.join(HANDED_OVER_WITHIN)) {
                    throw new AssertionError("the member thread did not hand its out-of-memory error over within "
                            + HANDED_OVER_WITHIN + "; it is still " + memberThread.getState());
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while the out-of-memory handling ran", interrupted);
            }
            return error;
        }
    }
}
