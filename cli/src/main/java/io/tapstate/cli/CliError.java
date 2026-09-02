package io.tapstate.cli;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

import java.util.Set;

/**
 * The {@code cli} domain's error codes (ADR-0024 D1) — surface-layer diagnosables that are not DSL
 * semantics: the scaffolding wizard's refusals and bad-input conditions. Thrown as a base
 * {@link io.tapstate.core.common.TapstateException} (no DSL source position) and rendered through the
 * message catalog like any other coded diagnostic.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and the build-time placeholder gate checks the catalog templates against it.
 */
enum CliError implements TapstateErrorCode {

    /** The scaffold target file already exists and {@code --force} was not given. */
    ARTIFACT_EXISTS("cli.artifact-exists", Set.of("path")),

    /**
     * The workspace already holds a demo resource and {@code --force} was not given; {@code path} is
     * the first one found. Refused rather than overwritten: the moment somebody has edited one of these
     * files it is theirs, and a command whose whole purpose is to save typing must not be the thing
     * that discards an afternoon of it.
     */
    DEMO_WORKSPACE_EXISTS("cli.demo-workspace-exists", Set.of("path")),

    /**
     * A workspace directory or file could not be written; {@code path} is the one that failed and
     * {@code reason} is what the filesystem said. Coded rather than left to crash because it is an
     * ordinary condition a reader meets - a read-only directory, a name already taken by a plain file -
     * and the answer is something they can act on rather than a stack trace.
     */
    WORKSPACE_NOT_WRITABLE("cli.workspace-not-writable", Set.of("path", "reason")),

    /**
     * The optional {@code tap} shortcut cannot be managed because that name belongs to something else;
     * {@code path} is where it sits. Refused rather than replaced or deleted: the name is a working
     * command on that machine, and a convenience shortcut does not get to remove one.
     */
    ALIAS_NAME_TAKEN("cli.alias-name-taken", Set.of("path")),

    /**
     * The shortcut cannot be managed because no installed {@code tapstate} was found where one was
     * expected; {@code path} is the directory looked in. Refused rather than guessed at: another
     * directory would manage a shortcut for an installation the user did not mean.
     */
    ALIAS_INSTALL_DIR_UNKNOWN("cli.alias-install-dir-unknown", Set.of("path")),

    /**
     * The {@code tap} shortcut could not be written or removed; {@code path} is the shortcut and
     * {@code reason} is what the filesystem said. Distinct from the name-taken refusal: nothing here is
     * anyone's file, the link simply could not be made.
     */
    ALIAS_LINK_FAILED("cli.alias-link-failed", Set.of("path", "reason")),

    /** A connector id supplied to the wizard that is not in the bundled catalog. */
    UNKNOWN_CONNECTOR("cli.unknown-connector", Set.of("connector")),

    /**
     * A connector id that is in the catalog but outside the set this release installs. Distinct from
     * {@link #UNKNOWN_CONNECTOR} on purpose: that one means the id resolves to nothing, which is
     * untrue here and points the user at a typo they do not have. {@code connector} is the id asked
     * for; {@code official} lists what this release does install, so the refusal states the boundary
     * instead of leaving it to be guessed.
     */
    CONNECTOR_NOT_OFFICIAL("cli.connector-not-official", Set.of("connector", "official")),

    /** A workspace artifact sits in a directory whose name does not match its declared kind. */
    KIND_DIR_MISMATCH("cli.kind-dir-mismatch", Set.of("path", "kind", "dir")),

    /** A describe / browse verb was given an id that resolves to no resource in the workspace. */
    RESOURCE_NOT_FOUND("cli.resource-not-found", Set.of("id")),

    /** No server among the connect seeds answered the reachability probe. */
    CONNECT_FAILED("cli.connect-failed", Set.of("seeds")),

    /** A heavy online verb reached the server but no response arrived within its timeout window. */
    REQUEST_TIMED_OUT("cli.request-timed-out", Set.of("server")),

    /** A verb that needs a live connection was run before the session connected; {@code verb} names it. */
    NOT_CONNECTED("cli.not-connected", Set.of("verb")),

    /** A connected online verb was run before the session authenticated; {@code verb} names it. */
    NOT_AUTHENTICATED("cli.not-authenticated", Set.of("verb")),

    /** A verb whose name is declared and reserved but which is not built yet; {@code verb} names it. */
    VERB_NOT_IMPLEMENTED("cli.verb-not-implemented", Set.of("verb")),

    /** A REPL builtin typed as a one-shot command, where it does not exist; {@code verb} names it. */
    REPL_BUILTIN_ONLY("cli.repl-builtin-only", Set.of("verb")),

    /** The installed MCP sidecar or its required Java runtime cannot be launched. */
    MCP_UNAVAILABLE("cli.mcp-unavailable", Set.of("reason")),

    /**
     * The in-place view was asked for where its output does not go to a terminal. Refused rather than
     * degraded: redrawing in place is cursor movement, and cursor movement down a pipe is not a
     * degraded view but a file of control characters. Decided entirely on the client — the server is
     * never asked, and would have no way to know.
     */
    WATCH_NEEDS_A_TERMINAL("cli.watch-needs-a-terminal", Set.of()),

    /**
     * A verb that clears what a pipeline accumulated was run where there is no terminal to confirm at,
     * and nothing said to go ahead unasked; {@code verb} names it. Refused rather than either of the
     * two things it could have done instead: asking would stop and wait on an input that is never
     * going to arrive, and going ahead would make an irreversible clearing the default of the one
     * situation -- a script, a job, a container step -- where nobody is watching it happen.
     */
    CONFIRMATION_NEEDS_A_TERMINAL("cli.confirmation-needs-a-terminal", Set.of("verb")),

    /**
     * A version precondition was offered for a batch holding more than one resource; {@code count} is how
     * many it holds. One hash names one version, so there is no resource it could be describing.
     */
    IF_MATCH_NEEDS_ONE_RESOURCE("cli.if-match-needs-one-resource", Set.of("count"));

    private final String code;
    private final Set<String> placeholders;

    CliError(String code, Set<String> placeholders) {
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
