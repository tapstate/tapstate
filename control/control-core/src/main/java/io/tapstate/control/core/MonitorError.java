package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

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
     * A status / metrics / snapshot read named a pipeline that has published no observation:
     * {@code pipeline} is the id the caller asked to observe.
     */
    NO_OBSERVATION("monitor.no-observation", Set.of("pipeline")),

    /** A history cursor could not be parsed, verified, or matched to the repeated query. */
    INVALID_CURSOR("monitor.invalid-cursor", Set.of("reason")),

    /** A valid history cursor is older than the bounded continuation lifetime. */
    CURSOR_EXPIRED("monitor.cursor-expired", Set.of()),

    /** A history query crossed one of its independently counted runtime budgets. */
    QUERY_BUDGET_EXCEEDED("monitor.query-budget-exceeded", Set.of("budget", "limit"));

    private final String code;
    private final Set<String> placeholders;

    MonitorError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
