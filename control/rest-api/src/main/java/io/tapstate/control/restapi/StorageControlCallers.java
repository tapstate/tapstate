package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StorageControlAccess;
import io.tapstate.control.core.StorageControlCaller;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.springframework.security.core.context.SecurityContextHolder;

/** Bridges the selected Spring authentication to the common dispatcher without reading request scope fields. */
public final class StorageControlCallers {
    private StorageControlCallers() {}

    public static StorageControlCaller ordinary(StorageBinding.Mode mode, Supplier<StorageSetupScope> setups) {
        Objects.requireNonNull(mode, "mode"); Objects.requireNonNull(setups, "setups");
        return () -> {
            var authentication = SecurityContextHolder.getContext().getAuthentication();
            if (!(authentication instanceof TapstateAuthentication selected) || selected.original() == null)
                throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
            var original = selected.original();
            original.requireActive().run();
            if (original.storageScope().isEmpty() && (mode != StorageBinding.Mode.CLOUD || original.organizationId().isEmpty()))
                throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
            return StorageControlAccess.ordinary(mode, setups.get(), original.storageScope().orElse(null),
                    selected.getPrincipal(), original.organizationId().orElse(null), original.requireActive());
        };
    }
}
