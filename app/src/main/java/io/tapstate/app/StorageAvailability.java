package io.tapstate.app;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ApplicationAvailabilityBean;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.AvailabilityState;
import org.springframework.boot.availability.LivenessState;

/** Keeps root availability while reflecting a broken engine in the currently admitted business owner. */
final class StorageAvailability extends ApplicationAvailabilityBean {
    private final Supplier<Optional<ApplicationAvailability>> current;

    StorageAvailability(Supplier<Optional<ApplicationAvailability>> current) {
        this.current = Objects.requireNonNull(current, "current");
    }

    @Override
    public <S extends AvailabilityState> AvailabilityChangeEvent<S> getLastChangeEvent(Class<S> stateType) {
        if (stateType == LivenessState.class) {
            var admitted = current.get().orElse(null);
            var event = admitted == null ? null : admitted.getLastChangeEvent(stateType);
            if (event != null && LivenessState.BROKEN.equals(event.getState())) return event;
        }
        return super.getLastChangeEvent(stateType);
    }
}
