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
     * A runtime-dependent read named a pipeline that has published no observation:
     * {@code pipeline} is the id the caller asked to observe. Catalog status reads can still return a known
     * pipeline's {@code NEW} state before its first observation.
     */
    NO_OBSERVATION("monitor.no-observation", Set.of("pipeline")),

    /** A history cursor could not be parsed, verified, or matched to the repeated query. */
    INVALID_CURSOR("monitor.invalid-cursor", Set.of("operation", "reason")),

    /** A valid history cursor is older than the bounded continuation lifetime. */
    CURSOR_EXPIRED("monitor.cursor-expired", Set.of("operation")),

    /** A history query crossed one of its independently counted runtime budgets. */
    QUERY_BUDGET_EXCEEDED("monitor.query-budget-exceeded", Set.of("operation", "budget", "limit")),

    /** A nest read named a pipeline that has no nest step at all: {@code pipeline} is the id asked about. */
    NO_NEST_STEP("monitor.no-nest-step", Set.of("pipeline")),

    /**
     * A nest read named a step that is not one of the pipeline's nest steps: {@code step} is the name given
     * and {@code steps} the nest steps the pipeline does have, comma-separated, so the refusal says what to
     * type instead.
     */
    UNKNOWN_NEST_STEP("monitor.unknown-nest-step", Set.of("pipeline", "step", "steps"));

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
