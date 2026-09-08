package io.tapstate.control.restapi;

import io.tapstate.control.core.ConnectorSummary;

import java.util.List;

/**
 * The connector-list response body: registered connectors available as authoring candidates. A wrapper
 * object rather than a bare array so the response can grow fields without changing the wire shape.
 */
public record ConnectorCatalogList(List<ConnectorSummary> connectors) {
}
