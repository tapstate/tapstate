package io.tapstate.runtime.engine;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OutOfMemoryHandler;
import com.hazelcast.instance.impl.DefaultOutOfMemoryHandler;

import java.util.Objects;
import java.util.Optional;

/**
 * Remembers that a member was shut down by its own out-of-memory handling, and tells whoever asked to be told.
 *
 * <p>When the heap runs out on one of a member's own threads, the substrate's out-of-memory handling decides
 * whether the member can go on, and when it cannot, shuts it down on the spot. It tells nobody. The member
 * simply stops, everything that asks it anything from then on is refused with an error that says the instance
 * is not active and nothing about why, and the process around it goes on serving HTTP. A server whose engine
 * was gone therefore kept passing its health check and kept reporting every pipeline as running, while each
 * pass of the converge loop was thrown back by a member that no longer existed.
 *
 * <p>This keeps the substrate's own handling, decision and all: whether a member is taken down, and how, stays
 * the substrate's call. It is wrapped rather than rewritten because that decision tells a heap that is really
 * exhausted from a single allocation too large to fit, and only the first is a reason to lose the engine. What
 * is added is the one thing the handling lacks. Once it has taken a member down, the error is written on that
 * member, where {@link Engine} reads it, and the member's watcher is told.
 *
 * <p>The handling belongs to the process rather than to a member: there is one, and it is handed every member
 * the process runs. What is written, and who is told, is per member, so a member nobody watches is handled
 * exactly as before and nothing is written about it.
 */
public final class MemberOutOfMemory {

    private static final String USER_CONTEXT_KEY = MemberOutOfMemory.class.getName();

    private static final OutOfMemoryHandler HANDLING = new Recording(new DefaultOutOfMemoryHandler());

    private final Runnable whenShutDown;

    private volatile OutOfMemoryError error;

    private MemberOutOfMemory(Runnable whenShutDown) {
        this.whenShutDown = whenShutDown;
    }

    /**
     * Has the process's out-of-memory handling write down that it shut {@code member} down, and run
     * {@code whenShutDown} once it has, on the thread the error reached.
     */
    public static void watch(HazelcastInstance member, Runnable whenShutDown) {
        member.getUserContext().put(USER_CONTEXT_KEY, new MemberOutOfMemory(Objects.requireNonNull(whenShutDown)));
        Hazelcast.setOutOfMemoryHandler(HANDLING);
    }

    /**
     * The error {@code member} was shut down over, or empty while it has not been.
     *
     * <p>Empty too for a member shut down any other way, and the server shutting it down on purpose is the
     * everyday case of that. A member shut down through its lifecycle lets go of everything it held, this
     * included, while the out-of-memory handling stops the member without going that way. So a member that
     * has let go was not taken down by it.
     */
    public static Optional<OutOfMemoryError> of(HazelcastInstance member) {
        Object watched;
        try {
            watched = member.getUserContext().get(USER_CONTEXT_KEY);
        } catch (HazelcastInstanceNotActiveException letGo) {
            return Optional.empty();
        }
        return watched instanceof MemberOutOfMemory record ? Optional.ofNullable(record.error) : Optional.empty();
    }

    /** The substrate's handling, followed by what is written down about each member it took down. */
    private static final class Recording extends OutOfMemoryHandler {

        private final OutOfMemoryHandler substrate;

        Recording(OutOfMemoryHandler substrate) {
            this.substrate = substrate;
        }

        @Override
        public boolean shouldHandle(OutOfMemoryError error) {
            return substrate.shouldHandle(error);
        }

        /**
         * Written down only for a member that has actually stopped. The handling swallows a shutdown that
         * fails, and a member it could not stop is still carrying its pipelines, so it must not be reported
         * as lost.
         */
        @Override
        public void onOutOfMemory(OutOfMemoryError error, HazelcastInstance[] members) {
            substrate.onOutOfMemory(error, members);
            for (HazelcastInstance member : members) {
                if (!member.getLifecycleService().isRunning()
                        && member.getUserContext().get(USER_CONTEXT_KEY) instanceof MemberOutOfMemory record) {
                    record.error = error;
                    record.whenShutDown.run();
                }
            }
        }
    }
}
