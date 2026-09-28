package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.MemberOutOfMemory;
import org.springframework.boot.availability.ApplicationAvailabilityBean;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.AvailabilityState;
import org.springframework.boot.availability.LivenessState;

import java.util.Optional;

/**
 * The application's availability as the application announces it, except that its liveness is broken from the
 * moment the engine's member has been shut down for want of memory.
 *
 * <p>A member its own out-of-memory handling shuts down leaves this process up and serving HTTP over an engine
 * that no longer exists, and nothing short of a restart brings the engine back. So the process is broken, in the
 * liveness its health check reports, where whatever decides on a restart looks.
 *
 * <p>That is read off the member's record each time the liveness is asked for, rather than announced when the
 * member goes. An announcement would run on the thread the error reached, on a heap that has just run out,
 * inside handling that swallows whatever is thrown from it, and one lost there left the health check passing
 * over pipelines already reported failed, until somebody restarted the server by hand. The record is the one the
 * engine fails those pipelines from, so the two cannot disagree.
 *
 * <p>Every other state, and the liveness while the member runs or once it has been shut down any other way, is
 * kept as announced.
 */
final class EngineAvailability extends ApplicationAvailabilityBean {

    private final HazelcastInstance member;

    EngineAvailability(HazelcastInstance member) {
        this.member = member;
    }

    /**
     * Every read of a state goes through here, so a lost engine is a broken liveness however it is asked for.
     * The error is the change's source, as it is for any change of availability a failure causes.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <S extends AvailabilityState> AvailabilityChangeEvent<S> getLastChangeEvent(Class<S> stateType) {
        if (stateType == LivenessState.class) {
            Optional<OutOfMemoryError> lost = MemberOutOfMemory.of(member);
            if (lost.isPresent()) {
                AvailabilityChangeEvent<LivenessState> broken =
                        new AvailabilityChangeEvent<>(lost.get(), LivenessState.BROKEN);
                return (AvailabilityChangeEvent<S>) broken;
            }
        }
        return super.getLastChangeEvent(stateType);
    }
}
