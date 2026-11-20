package io.tapstate.app;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.RoleErrorDefinitions;

import java.util.Set;

/**
 * The {@code role} domain's error codes: the assembly root's process-role selection. The single
 * binary can run a subset of planes selected by {@code --role}; at L1 the planes are not separable,
 * so the only supported role is {@code all}. Any other value is a user-facing, diagnosable error
 * carried through the error-code system and rendered through the shared message catalog.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and the build-time placeholder gate checks the catalog templates against it.
 */
enum RoleError implements TapstateErrorCode {

    /** A {@code --role} value other than the L1-supported {@code all}. */
    UNSUPPORTED(RoleErrorDefinitions.UNSUPPORTED);

    private final ProductErrorDefinition definition;

    RoleError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
