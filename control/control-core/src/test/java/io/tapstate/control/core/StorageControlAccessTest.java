package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class StorageControlAccessTest {
    static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "install", "setup", 1);
    static final class States implements StorageSetupStateStore {
        Status status;
        boolean firstAdminComplete;
        States(Purpose purpose) { status = new Status(SETUP, purpose == Purpose.SETUP ? Phase.CONFIG_PREPARED : Phase.COMPLETE, purpose == Purpose.SETUP ? 0 : 1); }
        @Override public StorageSetupStateStore guardedBy(Runnable active) { active.run(); return this; }
        @Override public Status load() { return status; }
        @Override public Status rotate(StorageSetupScope expected, Purpose purpose) {
            requireCurrent(expected,purpose);
            status = new Status(new StorageSetupScope("cluster", "install", "setup", expected.generation()+1),status.phase(),status.bindingVersion());
            return status;
        }
        @Override public void requireCurrent(StorageSetupScope expected, Purpose purpose) {
            if (!status.scope().equals(expected) || (purpose == Purpose.SETUP ? status.phase() == Phase.COMPLETE : status.phase() != Phase.COMPLETE || firstAdminComplete))
                throw new TapstateException(ControlError.BOOTSTRAP_FORBIDDEN, Map.of(), null);
        }
    }
    static SetupCapabilityService.Permission permission(States states, StorageSetupStateStore.Purpose purpose) {
        var service = new SetupCapabilityService(states, Clock.fixed(Instant.parse("2026-10-20T00:00:00Z"), ZoneOffset.UTC), () -> {}, () -> {}, () -> {});
        var issued = service.issue(SETUP, purpose);
        return service.authorize(issued.token(), issued.scope(), purpose);
    }
    private static CacheAuthScope scope(StorageBinding.Mode mode) { return new CacheAuthScope("cluster",1,mode,"incarnation",1); }
    @Test void cloudUsesItsExistingSessionAndWriteScopeWithoutAnOpAdminConversion() {
        var principal = TapstatePrincipal.cloudSession(new VerifiedToken("cloud-user", Scope.WRITE));
        var access = StorageControlAccess.ordinary(StorageBinding.Mode.CLOUD, SETUP, scope(StorageBinding.Mode.CLOUD), principal, "org", () -> {});
        assertThatCode(() -> access.require(ControlOperations.STORAGE_SETUP_COMMIT)).doesNotThrowAnyException();
        assertThatCode(() -> access.require(ControlOperations.CONFIGURATION_IMPORT_COMMIT)).doesNotThrowAnyException();
        assertThat(access.importScope().organizationId()).isEqualTo("org");
    }
    @Test void existingCloudOperationScopesStillRefuseAReadCredentialAtWrites() {
        var principal = TapstatePrincipal.cloudSession(new VerifiedToken("cloud-user", Scope.READ));
        var access = StorageControlAccess.ordinary(StorageBinding.Mode.CLOUD, SETUP, scope(StorageBinding.Mode.CLOUD), principal, "org", () -> {});
        access.require(ControlOperations.STORAGE_STATUS);
        assertThatThrownBy(() -> access.require(ControlOperations.STORAGE_SETUP_COMMIT)).isInstanceOf(TapstateException.class);
    }
    @Test void opOrdinaryAdminCannotReopenSetupAndAnOrdinaryWriterCannotAdministerStorage() {
        var admin = StorageControlAccess.ordinary(StorageBinding.Mode.OP, SETUP, scope(StorageBinding.Mode.OP),
                TapstatePrincipal.humanJwt(new VerifiedToken("admin", Scope.ADMIN)), null, () -> {});
        admin.require(ControlOperations.CONFIGURATION_IMPORT_COMMIT);
        assertThatThrownBy(() -> admin.require(ControlOperations.STORAGE_SETUP_COMMIT)).isInstanceOf(TapstateException.class);
        var writer = StorageControlAccess.ordinary(StorageBinding.Mode.OP, SETUP, scope(StorageBinding.Mode.OP),
                TapstatePrincipal.humanJwt(new VerifiedToken("writer", Scope.WRITE)), null, () -> {});
        assertThatThrownBy(() -> writer.require(ControlOperations.CONFIGURATION_IMPORT_COMMIT)).isInstanceOf(TapstateException.class);
    }
    @Test void aVerifiedSetupPermissionCannotImportOrRecoverAndAFirstAdminPermissionCannotChangeStorage() {
        var setup = StorageControlAccess.management(permission(new States(StorageSetupStateStore.Purpose.SETUP), StorageSetupStateStore.Purpose.SETUP));
        setup.require(ControlOperations.STORAGE_SETUP_COMMIT);
        assertThatThrownBy(() -> setup.require(ControlOperations.CONFIGURATION_IMPORT_COMMIT)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> setup.require(ControlOperations.STORAGE_RECOVERY_CONFIRM)).isInstanceOf(TapstateException.class);
        var first = StorageControlAccess.management(permission(new States(StorageSetupStateStore.Purpose.FIRST_ADMIN), StorageSetupStateStore.Purpose.FIRST_ADMIN));
        first.require(ControlOperations.STORAGE_STATUS);
        assertThatThrownBy(() -> first.require(ControlOperations.STORAGE_SETUP_COMMIT)).isInstanceOf(TapstateException.class);
    }
    @Test void theServerCannotMixCloudAndOpIdentityOrClusterScopes() {
        var cloud = TapstatePrincipal.cloudSession(new VerifiedToken("cloud-user", Scope.WRITE));
        assertThatThrownBy(() -> StorageControlAccess.ordinary(StorageBinding.Mode.OP, SETUP, scope(StorageBinding.Mode.OP), cloud, null, () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        var op = TapstatePrincipal.humanJwt(new VerifiedToken("admin", Scope.ADMIN));
        assertThatThrownBy(() -> StorageControlAccess.ordinary(StorageBinding.Mode.CLOUD, SETUP, scope(StorageBinding.Mode.CLOUD), op, "org", () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StorageControlAccess.ordinary(StorageBinding.Mode.OP, SETUP,
                new CacheAuthScope("other",1,StorageBinding.Mode.OP,"incarnation",1),op,null,() -> {})).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void aDependencyFailureRemainsUnavailableInsteadOfBecomingAnInvalidIdentity() {
        var access = StorageControlAccess.ordinary(StorageBinding.Mode.OP, SETUP, scope(StorageBinding.Mode.OP),
                TapstatePrincipal.humanJwt(new VerifiedToken("admin", Scope.ADMIN)),null,
                () -> { throw new TapstateException(ControlError.STORAGE_UNAVAILABLE,Map.of(),null); });
        assertThatThrownBy(() -> access.require(ControlOperations.STORAGE_STATUS)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
    }
}
