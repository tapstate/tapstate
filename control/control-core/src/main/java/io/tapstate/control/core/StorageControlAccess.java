package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.ConfigurationImportStore;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.StorageSetupStateStore;
import java.util.Map;
import java.util.Objects;

/** Server-established request scope. No frontend deserializes this object from a body or caller header. */
public final class StorageControlAccess {
    private final StorageBinding.Mode mode;
    private final StorageSetupScope setup;
    private final CacheAuthScope cache;
    private final TapstatePrincipal principal;
    private final String organizationId;
    private final Runnable active;
    private final SetupCapabilityService.Permission permission;

    private StorageControlAccess(StorageBinding.Mode mode, StorageSetupScope setup, CacheAuthScope cache,
            TapstatePrincipal principal, String organizationId, Runnable active, SetupCapabilityService.Permission permission) {
        this.mode = Objects.requireNonNull(mode, "mode"); this.setup = Objects.requireNonNull(setup, "setup"); this.cache = cache;
        this.principal = principal; this.organizationId = organizationId; this.active = Objects.requireNonNull(active, "active"); this.permission = permission;
        if (cache != null && (!setup.clusterId().equals(cache.clusterId()) || cache.mode() != mode))
            throw new IllegalArgumentException("verified storage scopes disagree");
    }

    /** Principal and cache scope must come from the same selected authentication context, including its auth generation. */
    public static StorageControlAccess ordinary(StorageBinding.Mode mode, StorageSetupScope setup, CacheAuthScope cache,
            TapstatePrincipal principal, String organizationId, Runnable reauthenticate) {
        Objects.requireNonNull(principal, "principal");
        if (mode == StorageBinding.Mode.CLOUD && (principal.credentialType() != CredentialType.CLOUD_SESSION
                || organizationId == null || organizationId.isBlank()) || mode == StorageBinding.Mode.OP && (organizationId != null || principal.credentialType() == CredentialType.CLOUD_SESSION))
            throw new IllegalArgumentException("storage caller does not match the deployment authentication mode");
        return new StorageControlAccess(mode, setup, cache, principal, organizationId, reauthenticate, null);
    }
    /** Resolves persistent scope only after the original selected credential has been rechecked. */
    public static StorageControlAccess ordinary(StorageBinding.Mode mode, java.util.function.Supplier<StorageSetupScope> setups,
            TapstatePrincipal principal, CredentialAuthentication original) {
        Objects.requireNonNull(mode, "mode"); Objects.requireNonNull(setups, "setups"); Objects.requireNonNull(original, "original");
        original.requireActive().run();
        if (original.storageScope().isEmpty() && (mode != StorageBinding.Mode.CLOUD || original.organizationId().isEmpty()))
            throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        return ordinary(mode, setups.get(), original.storageScope().orElse(null), principal,
                original.organizationId().orElse(null), original.requireActive());
    }
    public static StorageControlAccess management(SetupCapabilityService.Permission verified) {
        Objects.requireNonNull(verified, "verified"); verified.run();
        return new StorageControlAccess(StorageBinding.Mode.OP, verified.scope(), null, null, null, verified, verified);
    }

    public void require(Operation operation) {
        active.run();
        if (permission != null) {
            boolean status = operation.id().equals("storage.status") || operation.id().equals("storage.setup.status");
            boolean setupWrite = operation.id().equals("storage.test") || operation.id().equals("storage.setup.commit");
            if (!status && !(setupWrite && permission.purpose() == StorageSetupStateStore.Purpose.SETUP)) throw forbidden(operation, Scope.ADMIN);
            return;
        }
        Scope required = mode == StorageBinding.Mode.OP ? Scope.ADMIN : operation.scope();
        if (!principal.permits(required)) throw forbidden(operation, required);
        if (mode == StorageBinding.Mode.OP && (operation.id().equals("storage.test") || operation.id().equals("storage.setup.commit")))
            throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null);
    }
    public Runnable guard(Operation operation) { return () -> require(operation); }
    public String subject() { return principal == null ? "installation:" + setup.installationId() : principal.subject(); }
    public StorageBinding.Mode mode() { return mode; }
    public StorageSetupScope setup() { return setup; }
    public java.util.Optional<CacheAuthScope> cacheScope() { return java.util.Optional.ofNullable(cache); }
    public CacheAuthScope cache() {
        if (cache == null) throw new TapstateException(ControlError.STORAGE_UNAVAILABLE, Map.of(), null);
        return cache;
    }
    public ConfigurationImportStore.Scope importScope() {
        var current = cache();
        return new ConfigurationImportStore.Scope(current.clusterId(), current.bindingVersion(), mode, organizationId);
    }
    private static TapstateException forbidden(Operation operation, Scope scope) {
        return new TapstateException(ControlError.FORBIDDEN, Map.of("op", operation.id(), "required", scope.name().toLowerCase(java.util.Locale.ROOT)), null);
    }
    @Override public String toString() { return "StorageControlAccess[verified]"; }
}
