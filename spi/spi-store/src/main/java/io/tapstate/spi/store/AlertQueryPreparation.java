package io.tapstate.spi.store;

import java.util.Objects;

/** One bounded current schema-maintenance operation, independent of query data or product readiness. */
public record AlertQueryPreparation(int installedVersion, String operationId, Phase phase, int nextIndex, int indexContractVersion) {
    public enum Phase { PREPARING, READY }
    public AlertQueryPreparation {
        Objects.requireNonNull(operationId, "operationId"); Objects.requireNonNull(phase, "phase");
        if (operationId.isBlank() || operationId.length() > 256 || indexContractVersion != 1 || nextIndex < 0 || nextIndex > 5
                || (phase == Phase.READY ? installedVersion != 3 || nextIndex != 5 : installedVersion != 2)) {
            throw new IllegalArgumentException("unsupported bounded query-preparation state");
        }
    }
}
