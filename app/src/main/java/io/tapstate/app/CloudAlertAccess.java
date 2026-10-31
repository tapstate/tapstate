package io.tapstate.app;

import io.tapstate.control.core.CredentialAuthentication;
import io.tapstate.control.core.CredentialType;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.spi.store.CacheAuthScope;

/** Separates request IO revalidation from the pure guards used inside CONFIG callbacks. */
final class CloudAlertAccess {
    private CloudAlertAccess() { }
    static AlertStore.Access bind(CredentialAuthentication caller, AlarmKey.Scope scope,
            CacheAuthScope originalStorage, Runnable localOwner) {
        caller.requireActive().run();
        if (caller.storageScope().filter(originalStorage::equals).isEmpty()
                || caller.organizationId().filter(scope.authorityId()::equals).isEmpty())
            throw AlertHttpModel.stale("managed-caller-scope");
        var evidence = caller.evidence().filter(value -> value.type() == CredentialType.CLOUD_SESSION)
                .orElseThrow(() -> AlertHttpModel.stale("managed-caller-evidence"));
        Runnable local = () -> { localOwner.run(); evidence.requireLocallyActive().run(); };
        local.run();
        return AlertStore.Access.cloud(caller.token().subject(), scope.authorityId(), caller.token().scope().name(),
                caller.requireActive(), local);
    }
}
