package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.StorageControlAccess;
import io.tapstate.control.core.StorageControlCaller;
import io.tapstate.core.common.TapstateException;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import io.tapstate.control.core.CredentialAuthentication;
import io.tapstate.control.core.TapstatePrincipal;
import org.springframework.security.core.context.SecurityContextHolder;

/** Bridges the selected Spring authentication to the common dispatcher without reading request scope fields. */
public final class StorageControlCallers {
    private StorageControlCallers() {}

    public static StorageControlCaller withInstallation(StorageControlCaller ordinary,
            java.util.function.Supplier<InstallationHttpAccess> installations) {
        return () -> {
            var attributes = org.springframework.web.context.request.RequestContextHolder.getRequestAttributes();
            var installation = installations.get();
            if (installation != null && attributes instanceof org.springframework.web.context.request.ServletRequestAttributes servlet
                    && installation.isManagementRequest(servlet.getRequest())) return installation.caller(servlet.getRequest());
            return ordinary.current();
        };
    }

    public static StorageControlCaller ordinary(BiFunction<TapstatePrincipal, CredentialAuthentication, StorageControlAccess> access) {
        Objects.requireNonNull(access, "access");
        return () -> {
            var authentication = SecurityContextHolder.getContext().getAuthentication();
            if (!(authentication instanceof TapstateAuthentication selected) || selected.original() == null)
                throw new TapstateException(ControlError.UNAUTHENTICATED, Map.of(), null);
            return access.apply(selected.getPrincipal(), selected.original());
        };
    }
}
