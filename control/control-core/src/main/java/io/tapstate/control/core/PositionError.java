package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/**
 * The {@code position} domain's error codes: user-facing, diagnosable refusals of a write-back to where a
 * pipeline resumes from. Every one of them is a request that would otherwise have been accepted and done
 * nothing, or done something to somebody else's pipeline.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for each
 * name, and the build-time placeholder gate checks the catalog templates against it.
 */
public enum PositionError implements TapstateErrorCode {

    /**
     * A chain the write-back would move is still being read: {@code chain} is the chain, {@code pipelines}
     * the ones on it that are not at rest.
     */
    WRITE_BACK_WHILE_LIVE("position.write-back-while-live", Set.of("chain", "pipelines")),

    /**
     * The request named a chain this pipeline does not read: {@code chain} is the id it named,
     * {@code pipeline} the pipeline, {@code known} the chains it does read.
     */
    CHAIN_NOT_READ("position.chain-not-read", Set.of("chain", "pipeline", "known")),

    /**
     * The request changed something other than where a chain resumes from: {@code field} is what it
     * changed, {@code chain} the chain it changed it on. Everything else in the document is a reading,
     * not a setting, and the only thing that could be done with a changed one is ignore it.
     */
    FIELD_NOT_EDITABLE("position.field-not-editable", Set.of("field", "chain")),

    /**
     * The request asked for no move at all — no chain named, or every named chain already sitting where
     * the request puts it: {@code pipeline} is the pipeline. Accepting it would report a write-back that
     * did not happen.
     */
    NOTHING_TO_WRITE("position.nothing-to-write", Set.of("pipeline"));

    private final String code;
    private final Set<String> placeholders;

    PositionError(String code, Set<String> placeholders) {
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
