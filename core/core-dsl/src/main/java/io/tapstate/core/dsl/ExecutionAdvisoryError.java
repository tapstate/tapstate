package io.tapstate.core.dsl;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import java.util.Set;

/**
 * The {@code dsl} domain's authoring-time warnings about how a node is asked to run. A batch carrying one
 * is applied and runs; what is reported is that part of what was written will not do what it reads as.
 *
 * <p>Kept apart from the codes that refuse a batch, because nothing here is invalid: the fields are part
 * of the grammar and parse. The point of saying so at apply is that nothing else will - a setting with no
 * effect and a setting that worked look the same from the artifact.
 *
 * <p>{@code placeholders()} is the named-argument contract: every raise site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum ExecutionAdvisoryError implements TapstateErrorCode {

    /**
     * A pipeline-level setting that no longer decides anything is written. {@code setting} names it and
     * {@code instead} names what does decide it now, written on each node.
     */
    SETTING_HAS_NO_EFFECT("dsl.setting-has-no-effect", Set.of("pipeline", "setting", "instead")),

    /**
     * A source asks to be read by more than one processor. One processor reads a source today whatever
     * is written, because a reader split into parts that never overlap and each keep their own progress is
     * a capability no connector has declared yet. {@code requested} is the number written.
     */
    SOURCE_READ_NOT_SPLIT("dsl.source-read-not-split", Set.of("source", "requested"));

    private final String code;
    private final Set<String> placeholders;

    ExecutionAdvisoryError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    /** Nothing written here is invalid, so nothing is refused over it. */
    @Override
    public Severity severity() {
        return Severity.WARNING;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
