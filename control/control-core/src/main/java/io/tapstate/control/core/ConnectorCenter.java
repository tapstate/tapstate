package io.tapstate.control.core;

import java.io.InputStream;
import java.util.Map;

/** Connector availability, installation commands and their observable transfer and install progress. */
public interface ConnectorCenter {
    Map<String, Object> catalog();

    Map<String, Object> refresh();

    Map<String, Object> download(String connectorId);

    Map<String, Object> operation(String operationId);

    /** Consumes the caller-owned stream during this call before returning an installation operation. */
    Map<String, Object> upload(InputStream artifact, long contentLength, String expectedConnectorId);
}
