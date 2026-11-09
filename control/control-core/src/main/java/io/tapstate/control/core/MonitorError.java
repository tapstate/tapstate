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
    UNKNOWN_NEST_STEP("monitor.unknown-nest-step", Set.of("pipeline", "step", "steps")),

    /**
     * A read of what a nest step holds named a pipeline that is not running here, so there is no live state
     * to read: {@code pipeline} and {@code step} are what was asked about.
     */
    NEST_NOT_RUNNING("monitor.nest-not-running", Set.of("pipeline", "step")),

    /**
     * A root key was given with a different number of values than the root key has columns: {@code columns}
     * names them in order and {@code given} is how many values arrived.
     */
    NEST_ROOT_KEY_WIDTH("monitor.nest-root-key-width", Set.of("pipeline", "step", "columns", "given")),

    /**
     * Nothing is held under the root key given, read as a number or as text: {@code key} is the key as typed
     * and {@code columns} the root key's columns.
     */
    NEST_ROOT_KEY_NOT_HELD("monitor.nest-root-key-not-held", Set.of("pipeline", "step", "key", "columns")),

    /**
     * A warning from sampling before a run: an {@code object} or {@code flat} embed matched {@code count}
     * sampled rows of {@code table} under one parent, out of {@code sampled} rows read, so its rows are not
     * one per parent.
     */
    NEST_OBJECT_KEY_NOT_UNIQUE("monitor.nest-object-key-not-unique", Set.of("embed", "table", "count", "sampled"),
            Severity.WARNING),

    /**
     * A warning from sampling before a run: documents are estimated to hold {@code count} elements under
     * {@code embed}, near the per-document {@code limit}.
     */
    NEST_NEAR_ELEMENT_LIMIT("monitor.nest-near-element-limit", Set.of("embed", "table", "count", "limit"),
            Severity.WARNING),

    /**
     * A warning from sampling before a run: no single embed is near the per-document {@code limit}, but the
     * embeds named in {@code embeds} together are estimated to put {@code count} elements in one document.
     */
    NEST_DOCUMENT_NEAR_ELEMENT_LIMIT("monitor.nest-document-near-element-limit",
            Set.of("embeds", "count", "limit"), Severity.WARNING),

    /**
     * A warning from sampling before a run: one row of {@code table} behind {@code embed} is estimated to be
     * pointed at by {@code count} documents, near the per-row {@code limit}.
     */
    NEST_REFERENCE_WIDELY_SHARED("monitor.nest-reference-widely-shared", Set.of("embed", "table", "count", "limit"),
            Severity.WARNING);

    private final String code;
    private final Set<String> placeholders;
    private final Severity severity;

    MonitorError(String code, Set<String> placeholders) {
        this(code, placeholders, Severity.ERROR);
    }

    MonitorError(String code, Set<String> placeholders, Severity severity) {
        this.code = code;
        this.placeholders = placeholders;
        this.severity = severity;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return severity;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
