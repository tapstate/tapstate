package io.tapstate.core.model;

/**
 * The engine a join transform's SQL runs on (ADR-0016 §5.2).
 *
 * <p>One member, and it is still an enum: the field used to be free text, so any spelling was
 * accepted and a pipeline naming an engine that does not exist was refused by nothing until it
 * failed at run time. Adding an engine later is an append-only change here.
 */
@Doc("The engine that runs the join.")
public enum JoinEngine {

    @Doc("The built-in join carrier, the only engine this release runs joins on.")
    BUILTIN("builtin");

    private final String yaml;

    JoinEngine(String yaml) {
        this.yaml = yaml;
    }

    public String yaml() {
        return yaml;
    }
}
