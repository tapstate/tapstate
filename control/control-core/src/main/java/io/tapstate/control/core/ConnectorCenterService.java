package io.tapstate.control.core;

import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

/** Audits connector installation and catalog mutation before their first effect. */
public final class ConnectorCenterService {
    private final ConnectorCenter center;
    private final AuditGate audit;

    public ConnectorCenterService(ConnectorCenter center, AuditGate audit) {
        this.center = Objects.requireNonNull(center, "center");
        this.audit = Objects.requireNonNull(audit, "audit");
    }

    public Map<String, Object> catalog() {
        return center.catalog();
    }

    public Map<String, Object> refresh(String principal) {
        return audit.dispatch(ControlOperations.CONNECTOR_CENTER_REFRESH,
                new AuditContext(principal, "connector-catalog"), center::refresh);
    }

    public Map<String, Object> download(String connectorId, String principal) {
        return audit.dispatch(ControlOperations.CONNECTOR_CENTER_DOWNLOAD,
                new AuditContext(principal, connectorId), () -> center.download(connectorId));
    }

    public Map<String, Object> operation(String operationId) {
        return center.operation(operationId);
    }

    public Map<String, Object> upload(InputStream artifact, long contentLength, String principal) {
        Objects.requireNonNull(artifact, "artifact");
        return audit.dispatch(ControlOperations.CONNECTOR_CENTER_UPLOAD,
                new AuditContext(principal, "connector-upload"), () -> center.upload(artifact, contentLength));
    }
}
