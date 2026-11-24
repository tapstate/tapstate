package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.ClusterErrorDefinitions;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The {@code cluster} domain's error codes: what the topology read refuses with when the member it was
 * asked of cannot answer.
 *
 * <p>Declared beside the ports it is refused through rather than in the layer that throws it. A port is
 * what promises an answer, so a port is what has to say when there is not one; an implementation is then
 * the only thing that needs to know why, and no two of them invent a different way of saying they could
 * not.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum ClusterError implements TapstateErrorCode {

    /**
     * This member cannot answer for the cluster, because the engine member it reads from is not active.
     *
     * <p>Two windows produce it and neither is a defect: a member on its way up or down, and a member
     * whose instance the library restarts on the smaller side when a split brain heals. What it must
     * never be confused with is an answer -- a caller told the cluster is empty, or told this member is
     * in none, has been handed a reading nobody took. Telling those two apart is the whole of why this
     * is a code and not a catch.
     */
    MEMBERSHIP_UNREADABLE(ClusterErrorDefinitions.MEMBERSHIP_UNREADABLE);

    private final ProductErrorDefinition definition;

    ClusterError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
