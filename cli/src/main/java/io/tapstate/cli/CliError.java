package io.tapstate.cli;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.CliErrorDefinitions;

import java.util.Set;

/**
 * The {@code cli} domain's error codes — surface-layer diagnosables that are not DSL
 * semantics: the scaffolding wizard's refusals and bad-input conditions. Thrown as a base
 * {@link io.tapstate.core.common.TapstateException} (no DSL source position) and rendered through the
 * message catalog like any other coded diagnostic.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and the build-time placeholder gate checks the catalog templates against it.
 */
enum CliError implements TapstateErrorCode {

    /** The scaffold target file already exists and {@code --force} was not given. */
    ARTIFACT_EXISTS(CliErrorDefinitions.ARTIFACT_EXISTS),

    /**
     * The workspace already holds a demo resource and {@code --force} was not given; {@code path} is
     * the first one found. Refused rather than overwritten: the moment somebody has edited one of these
     * files it is theirs, and a command whose whole purpose is to save typing must not be the thing
     * that discards an afternoon of it.
     */
    DEMO_WORKSPACE_EXISTS(CliErrorDefinitions.DEMO_WORKSPACE_EXISTS),

    /**
     * A workspace directory or file could not be written; {@code path} is the one that failed and
     * {@code reason} is what the filesystem said. Coded rather than left to crash because it is an
     * ordinary condition a reader meets - a read-only directory, a name already taken by a plain file -
     * and the answer is something they can act on rather than a stack trace.
     */
    WORKSPACE_NOT_WRITABLE(CliErrorDefinitions.WORKSPACE_NOT_WRITABLE),

    /**
     * A workspace {@code up} was asked to bring up could not be read; {@code path} is the workspace and
     * {@code reason} is what the filesystem said - typically the directory or file under it that refused.
     * Its own code rather than the write one above: the two ask for opposite fixes (grant reading,
     * grant writing), and a reader who is told "could not write" about a command that writes nothing
     * goes looking in the wrong place.
     */
    WORKSPACE_UNREADABLE(CliErrorDefinitions.WORKSPACE_UNREADABLE),

    /**
     * A workspace {@code up} was asked to bring up holds no pipeline, so there is nothing to start;
     * {@code path} is the workspace. A refusal rather than a quiet no-op: an empty or half-written
     * workspace is the state a first run most often gets stuck in, and the way out is a scaffold.
     */
    WORKSPACE_HAS_NO_PIPELINE(CliErrorDefinitions.WORKSPACE_HAS_NO_PIPELINE),

    /**
     * The optional {@code tap} shortcut cannot be managed because that name belongs to something else;
     * {@code path} is where it sits. Refused rather than replaced or deleted: the name is a working
     * command on that machine, and a convenience shortcut does not get to remove one.
     */
    ALIAS_NAME_TAKEN(CliErrorDefinitions.ALIAS_NAME_TAKEN),

    /**
     * The shortcut cannot be managed because no installed {@code tapstate} was found where one was
     * expected; {@code path} is the directory looked in. Refused rather than guessed at: another
     * directory would manage a shortcut for an installation the user did not mean.
     */
    ALIAS_INSTALL_DIR_UNKNOWN(CliErrorDefinitions.ALIAS_INSTALL_DIR_UNKNOWN),

    /**
     * The {@code tap} shortcut could not be written or removed; {@code path} is the shortcut and
     * {@code reason} is what the filesystem said. Distinct from the name-taken refusal: nothing here is
     * anyone's file, the link simply could not be made.
     */
    ALIAS_LINK_FAILED(CliErrorDefinitions.ALIAS_LINK_FAILED),

    /**
     * The local development stack the guided first run starts in Docker could not be started, or was
     * started and never answered; {@code reason} says which - no {@code docker} on the PATH, no Compose
     * plugin, what {@code docker compose} said, or where the stack is and how to stop it when it came
     * up and stayed silent. One code for the whole route rather than one per step: what a reader does
     * about any of them is the same - fix Docker, or point the workspace at a server they already run.
     */
    DOCKER_UNAVAILABLE(CliErrorDefinitions.DOCKER_UNAVAILABLE),

    /**
     * {@code up} was asked to bring up a workspace that names no server, with nobody to ask which one;
     * {@code server} is the default it declined to assume. Refused rather than defaulted: adopting the
     * loopback address would bind a script to whatever happens to be listening on that machine, which
     * is a different server on every machine the script runs on.
     */
    SERVER_NOT_NAMED(CliErrorDefinitions.SERVER_NOT_NAMED),

    /**
     * Signing in to {@code server} needs a password that was neither asked for nor supplied;
     * {@code variable} names the environment variable a script puts it in. Its own code rather than a
     * login refusal: nothing was sent, so the credential was never wrong - it was never given.
     */
    PASSWORD_REQUIRED(CliErrorDefinitions.PASSWORD_REQUIRED),

    /**
     * Signing in to {@code server} while binding a workspace returned nothing that can be kept: the
     * server could not be reached for the login, or answered without a persistent session this release
     * can use. Names the server rather than a context, because a binding that fails here has settled a
     * context name without writing it - naming it would point at an entry no later command can find.
     */
    SIGN_IN_UNUSABLE(CliErrorDefinitions.SIGN_IN_UNUSABLE),

    /** A connector id supplied to the wizard that is not in the bundled catalog. */
    UNKNOWN_CONNECTOR(CliErrorDefinitions.UNKNOWN_CONNECTOR),

    /**
     * A connector id that is in the catalog but outside the set this release installs. Distinct from
     * {@link #UNKNOWN_CONNECTOR} on purpose: that one means the id resolves to nothing, which is
     * untrue here and points the user at a typo they do not have. {@code connector} is the id asked
     * for; {@code official} lists what this release does install, so the refusal states the boundary
     * instead of leaving it to be guessed.
     */
    CONNECTOR_NOT_OFFICIAL(CliErrorDefinitions.CONNECTOR_NOT_OFFICIAL),

    /** A published connector id could not be fetched as a complete jar before registration. */
    CONNECTOR_DOWNLOAD_FAILED(CliErrorDefinitions.CONNECTOR_DOWNLOAD_FAILED),

    /** A workspace artifact sits in a directory whose name does not match its declared kind. */
    KIND_DIR_MISMATCH(CliErrorDefinitions.KIND_DIR_MISMATCH),

    /** A describe / browse verb was given an id that resolves to no resource in the workspace. */
    RESOURCE_NOT_FOUND(CliErrorDefinitions.RESOURCE_NOT_FOUND),

    /** No server among the connect seeds answered the reachability probe. */
    CONNECT_FAILED(CliErrorDefinitions.CONNECT_FAILED),

    /** A heavy online verb reached the server but no response arrived within its timeout window. */
    REQUEST_TIMED_OUT(CliErrorDefinitions.REQUEST_TIMED_OUT),

    /** A verb that needs a live connection was run before the session connected; {@code verb} names it. */
    NOT_CONNECTED(CliErrorDefinitions.NOT_CONNECTED),

    /** A connected online verb was run before the session authenticated; {@code verb} names it. */
    NOT_AUTHENTICATED(CliErrorDefinitions.NOT_AUTHENTICATED),

    /** A verb whose name is declared and reserved but which is not built yet; {@code verb} names it. */
    VERB_NOT_IMPLEMENTED(CliErrorDefinitions.VERB_NOT_IMPLEMENTED),

    /** A REPL builtin typed as a one-shot command, where it does not exist; {@code verb} names it. */
    REPL_BUILTIN_ONLY(CliErrorDefinitions.REPL_BUILTIN_ONLY),

    /** The installed MCP sidecar or its required Java runtime cannot be launched. */
    MCP_UNAVAILABLE(CliErrorDefinitions.MCP_UNAVAILABLE),

    /**
     * The in-place view was asked for where its output does not go to a terminal. Refused rather than
     * degraded: redrawing in place is cursor movement, and cursor movement down a pipe is not a
     * degraded view but a file of control characters. Decided entirely on the client — the server is
     * never asked, and would have no way to know.
     */
    WATCH_NEEDS_A_TERMINAL(CliErrorDefinitions.WATCH_NEEDS_A_TERMINAL),

    /**
     * A verb that clears what a pipeline accumulated was run where there is no terminal to confirm at,
     * and nothing said to go ahead unasked; {@code verb} names it. Refused rather than either of the
     * two things it could have done instead: asking would stop and wait on an input that is never
     * going to arrive, and going ahead would make an irreversible clearing the default of the one
     * situation -- a script, a job, a container step -- where nobody is watching it happen.
     */
    CONFIRMATION_NEEDS_A_TERMINAL(CliErrorDefinitions.CONFIRMATION_NEEDS_A_TERMINAL),

    /**
     * A version precondition was offered for a batch holding more than one resource; {@code count} is how
     * many it holds. One hash names one version, so there is no resource it could be describing.
     */
    IF_MATCH_NEEDS_ONE_RESOURCE(CliErrorDefinitions.IF_MATCH_NEEDS_ONE_RESOURCE),

    /**
     * A start whose start checks ask questions, where nothing answered them and there is no terminal to ask
     * at: {@code count} questions are open, {@code commands} are the command lines that answer them. Refused
     * rather than answered by default, because every default here keeps rows or deletes them.
     */
    START_NEEDS_AN_ANSWER(CliErrorDefinitions.START_NEEDS_AN_ANSWER),

    /**
     * An option that only start checks can honour, sent to a server that runs none: {@code option} is the
     * option. Refused before anything is started, since the server would take the start and drop the option.
     */
    START_CHECKS_UNSUPPORTED(CliErrorDefinitions.START_CHECKS_UNSUPPORTED),

    /** A server started {@code pipeline} without running start checks; it is likely older than this client. */
    START_CHECKS_NOT_RUN(CliErrorDefinitions.START_CHECKS_NOT_RUN),

    /** A {@code --decide} that is not {@code <check>[/<subject>]=<action>}: {@code decide} is what was given. */
    DECIDE_MALFORMED(CliErrorDefinitions.DECIDE_MALFORMED),

    /**
     * A {@code --decide} naming an answer some of its questions do not offer: {@code findings} are those
     * questions, which have to be answered one by one instead.
     */
    DECIDE_NOT_OFFERED(CliErrorDefinitions.DECIDE_NOT_OFFERED),

    /** A {@code --decide} answering a start check that refuses the start, which no answer changes. */
    DECIDE_ON_A_REFUSAL(CliErrorDefinitions.DECIDE_ON_A_REFUSAL),

    /**
     * A start that was sent and got no answer in time. It may have gone ahead, so it is neither sent again
     * nor sent to another member: a person checks first.
     */
    START_MAY_HAVE_GONE_AHEAD(CliErrorDefinitions.START_MAY_HAVE_GONE_AHEAD),

    /** The context configuration is not a valid, unambiguous versioned document. */
    CONTEXT_CONFIG_INVALID(CliErrorDefinitions.CONTEXT_CONFIG_INVALID),

    /** No registered migration can safely interpret the context configuration version. */
    CONTEXT_CONFIG_VERSION(CliErrorDefinitions.CONTEXT_CONFIG_VERSION),

    /** The context configuration path cannot prove that only its owner may change it. */
    CONTEXT_CONFIG_PERMISSIONS(CliErrorDefinitions.CONTEXT_CONFIG_PERMISSIONS),

    /** The auth cache is not a valid, unambiguous versioned session document. */
    AUTH_CACHE_INVALID(CliErrorDefinitions.AUTH_CACHE_INVALID),

    /** No registered CLI release can safely interpret the auth cache version. */
    AUTH_CACHE_VERSION(CliErrorDefinitions.AUTH_CACHE_VERSION),

    /** The auth cache path cannot prove that only its owner may read or change it. */
    AUTH_CACHE_PERMISSIONS(CliErrorDefinitions.AUTH_CACHE_PERMISSIONS),

    /** A cached session could not be exchanged because the server rejected it. */
    AUTH_SESSION_REJECTED(CliErrorDefinitions.AUTH_SESSION_REJECTED),

    /** A cached session could not be exchanged because no server answered safely. */
    AUTH_SESSION_UNREACHABLE(CliErrorDefinitions.AUTH_SESSION_UNREACHABLE),

    /** Persistent login could not obtain the complete access-and-session response safely. */
    AUTH_LOGIN_UNREACHABLE(CliErrorDefinitions.AUTH_LOGIN_UNREACHABLE),

    /** Persistent login was rejected without rendering an untrusted server response body. */
    AUTH_LOGIN_REJECTED(CliErrorDefinitions.AUTH_LOGIN_REJECTED),

    /** Remote session revocation could not be confirmed, so the local cache was retained. */
    AUTH_LOGOUT_UNREACHABLE(CliErrorDefinitions.AUTH_LOGOUT_UNREACHABLE),

    /** Logout revoked an older session while a newer local session replaced its cache entry. */
    AUTH_LOGOUT_CACHE_CHANGED(CliErrorDefinitions.AUTH_LOGOUT_CACHE_CHANGED),

    /** The persistent auth namespace was invoked with an invalid action or operand shape. */
    STORAGE_USAGE(CliErrorDefinitions.STORAGE_USAGE),

    AUTH_USAGE(CliErrorDefinitions.AUTH_USAGE),

    /** The interactive context manager could not continue with the current input or terminal state. */
    CONTEXT_USAGE(CliErrorDefinitions.CONTEXT_USAGE),

    /** A context definition supplied to the manager violates the persisted schema. */
    CONTEXT_INVALID(CliErrorDefinitions.CONTEXT_INVALID),

    /** A context manager operation named a context that does not exist. */
    CONTEXT_NOT_FOUND(CliErrorDefinitions.CONTEXT_NOT_FOUND),

    /** A context manager create operation tried to reuse an existing name. */
    CONTEXT_ALREADY_EXISTS(CliErrorDefinitions.CONTEXT_ALREADY_EXISTS),

    /** Mutually exclusive temporary and durable context sources were supplied together. */
    CONTEXT_SOURCE_CONFLICT(CliErrorDefinitions.CONTEXT_SOURCE_CONFLICT),

    /** An online verb has no explicit, environment, or exact workspace target. */
    CONTEXT_REQUIRED(CliErrorDefinitions.CONTEXT_REQUIRED),

    /** A non-loopback seed would expose authentication traffic over plaintext HTTP. */
    REMOTE_PLAINTEXT(CliErrorDefinitions.REMOTE_PLAINTEXT),

    /** A configured seed did not answer anonymous issuer discovery. */
    ISSUER_DISCOVERY_FAILED(CliErrorDefinitions.ISSUER_DISCOVERY_FAILED),

    /** A seed answered discovery with fields that cannot identify a Tapstate cluster. */
    ISSUER_DISCOVERY_INVALID(CliErrorDefinitions.ISSUER_DISCOVERY_INVALID),

    /**
     * A seed refused anonymous discovery outright; {@code reason} carries what it said. A server older
     * than this CLI does not serve the discovery endpoint, and answers this way every time.
     */
    ISSUER_DISCOVERY_REJECTED(CliErrorDefinitions.ISSUER_DISCOVERY_REJECTED),

    /** A cached issuer or another seed differs from the anonymously discovered issuer. */
    AUTH_ISSUER_MISMATCH(CliErrorDefinitions.AUTH_ISSUER_MISMATCH),

    /**
     * This CLI and the server it just connected to are different versions; {@code cli} and
     * {@code server} are the two numbers. A warning, not a refusal: the connection is good and every
     * verb still works. It is said at all because the two halves are installed by different paths, so
     * they drift silently, and a reader who can see only one number reports it as though it were both.
     */
    VERSION_MISMATCH(CliErrorDefinitions.VERSION_MISMATCH);

    private final ProductErrorDefinition definition;

    CliError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
