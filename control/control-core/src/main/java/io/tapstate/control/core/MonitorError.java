package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.MonitorErrorDefinitions;

import java.util.Set;

/**
 * The {@code monitor} domain's error codes: user-facing, diagnosable failures of store-backed
 * observation reads — a missing current observation, or a bounded history query whose cursor or runtime
 * budget is invalid — carried through the error-code system and rendered through the shared message
 * catalog. The reads also serve frontends with no stderr/exit channel, so these refusals are coded rather
 * than bare usage errors.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum MonitorError implements TapstateErrorCode {

    /**
     * A runtime-dependent read named a pipeline that has published no observation:
     * {@code pipeline} is the id the caller asked to observe. Catalog status reads can still return a known
     * pipeline's {@code NEW} state before its first observation.
     */
    NO_OBSERVATION(MonitorErrorDefinitions.NO_OBSERVATION),

    /** A history cursor could not be parsed, verified, or matched to the repeated query. */
    INVALID_CURSOR(MonitorErrorDefinitions.INVALID_CURSOR),

    /** A valid history cursor is older than the bounded continuation lifetime. */
    CURSOR_EXPIRED(MonitorErrorDefinitions.CURSOR_EXPIRED),

    /** A history query crossed one of its independently counted runtime budgets. */
    QUERY_BUDGET_EXCEEDED(MonitorErrorDefinitions.QUERY_BUDGET_EXCEEDED);

    private final ProductErrorDefinition definition;

    MonitorError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
