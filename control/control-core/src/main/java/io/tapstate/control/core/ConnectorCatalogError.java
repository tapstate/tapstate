package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.ConnectorCatalogErrorDefinitions;

import java.util.Set;

/** Stable coded failures for the connector catalog read surface. */
public enum ConnectorCatalogError implements TapstateErrorCode {

    NOT_FOUND(ConnectorCatalogErrorDefinitions.NOT_FOUND);

    private final ProductErrorDefinition definition;

    ConnectorCatalogError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
