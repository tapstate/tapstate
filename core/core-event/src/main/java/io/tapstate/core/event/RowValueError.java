package io.tapstate.core.event;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.RowValueErrorDefinitions;
import io.tapstate.core.common.TapstateErrorCode;
import java.util.Set;

/** Diagnosable violations of the portable row-value contract. */
public enum RowValueError implements TapstateErrorCode {
    NON_STRING_MAP_KEY(RowValueErrorDefinitions.NON_STRING_MAP_KEY);

    private final ProductErrorDefinition definition;

    RowValueError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
