package io.tapstate.spi.store;

import java.util.Objects;

/** The exact installation operation and capability generation whose configuration write was authorized. */
public record StorageSetupScope(String clusterId, String installationId, String operationId, long generation) {
    public StorageSetupScope {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(installationId, "installationId");
        Objects.requireNonNull(operationId, "operationId");
        if (clusterId.isBlank() || installationId.isBlank() || operationId.isBlank() || generation < 1) {
            throw new IllegalArgumentException("storage setup scope must have nonblank identities and a positive generation");
        }
    }
}
