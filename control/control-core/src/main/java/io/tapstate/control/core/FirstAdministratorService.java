package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.StorageSetupStateStore;
import io.tapstate.spi.store.User;
import java.util.Map;
import java.util.Objects;

/** The existing first-administrator transaction under its distinct trusted management permission. */
public final class FirstAdministratorService {
    private final FirstAdminStore administrators;
    private final PasswordHasher passwords;
    private final AuditGate audit;
    public FirstAdministratorService(FirstAdminStore administrators, PasswordHasher passwords, AuditGate audit) {
        this.administrators = Objects.requireNonNull(administrators, "administrators"); this.passwords = Objects.requireNonNull(passwords, "passwords");
        this.audit = Objects.requireNonNull(audit, "audit");
    }
    public FirstAdminStore.Status create(SetupCapabilityService.Permission permission, String username, String password) {
        Objects.requireNonNull(permission, "permission");
        if (permission.purpose() != StorageSetupStateStore.Purpose.FIRST_ADMIN)
            throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null);
        permission.run();
        if (username == null || username.isBlank() || password == null || password.isBlank())
            throw new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", "administrator credentials are required"), null);
        User admin = new User(username, passwords.hash(password), "admin");
        return audit.dispatch(ControlOperations.USER_CREATE, new AuditContext(username, username),
                () -> administrators.guardedBy(permission).create(permission.scope(), admin));
    }
}
