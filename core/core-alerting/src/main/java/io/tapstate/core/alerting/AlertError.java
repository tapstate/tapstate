package io.tapstate.core.alerting;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.AlertErrorDefinitions;
import io.tapstate.core.common.TapstateErrorCode;
import java.util.Set;

/** Diagnosable contract refusals; these are separate from alarm types and alarm severity. */
public enum AlertError implements TapstateErrorCode {
    INVALID_CONFIGURATION(AlertErrorDefinitions.INVALID_CONFIGURATION),
    CHANNEL_NOT_ACCEPTED(AlertErrorDefinitions.CHANNEL_NOT_ACCEPTED),
    CHANNEL_RESULT_UNKNOWN(AlertErrorDefinitions.CHANNEL_RESULT_UNKNOWN),
    CHANNEL_UNAVAILABLE(AlertErrorDefinitions.CHANNEL_UNAVAILABLE),
    COMMAND_CONFLICT(AlertErrorDefinitions.COMMAND_CONFLICT),
    COMMAND_EXPIRED(AlertErrorDefinitions.COMMAND_EXPIRED),
    COMMAND_NOT_YET_VALID(AlertErrorDefinitions.COMMAND_NOT_YET_VALID),
    STALE_ACTION(AlertErrorDefinitions.STALE_ACTION);

    private final ProductErrorDefinition definition;

    AlertError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
