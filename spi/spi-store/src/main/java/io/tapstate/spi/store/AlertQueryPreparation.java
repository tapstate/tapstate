package io.tapstate.spi.store;

import java.util.Objects;

/** One bounded current schema-maintenance operation, independent of query data or product readiness. */
public record AlertQueryPreparation(int installedVersion, String operationId, Phase phase, int nextIndex, int indexContractVersion) {
    public enum Phase { PREPARING, READY }
    public AlertQueryPreparation {
        Objects.requireNonNull(operationId, "operationId"); Objects.requireNonNull(phase, "phase");
        boolean legacy=indexContractVersion==1 && nextIndex>=0 && nextIndex<=5
                && (phase==Phase.READY ? (installedVersion==3 || installedVersion==4) && nextIndex==5 : installedVersion==2);
        boolean nodes=indexContractVersion==2 && nextIndex>=5 && nextIndex<=6
                && (phase==Phase.READY ? (installedVersion==5 || installedVersion==6) && nextIndex==6 : installedVersion==4);
        if(operationId.isBlank() || operationId.length()>256 || !legacy && !nodes)
            throw new IllegalArgumentException("unsupported bounded query-preparation state");
    }
}
