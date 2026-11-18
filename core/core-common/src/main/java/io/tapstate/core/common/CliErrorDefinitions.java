package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** CLI diagnostics retain command outcomes and authentication boundaries. */
public final class CliErrorDefinitions {
    public static final ProductErrorDefinition ARTIFACT_EXISTS =
            new ProductErrorDefinition(ProductErrorModule.CLI, 1, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition DEMO_WORKSPACE_EXISTS =
            new ProductErrorDefinition(ProductErrorModule.CLI, 2, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition WORKSPACE_NOT_WRITABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 3, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition WORKSPACE_UNREADABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 4, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition WORKSPACE_HAS_NO_PIPELINE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 5, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition ALIAS_NAME_TAKEN =
            new ProductErrorDefinition(ProductErrorModule.CLI, 6, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition ALIAS_INSTALL_DIR_UNKNOWN =
            new ProductErrorDefinition(ProductErrorModule.CLI, 7, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition ALIAS_LINK_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 8, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition DOCKER_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 9, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition SERVER_NOT_NAMED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 10, Severity.ERROR, Set.of("server"));
    public static final ProductErrorDefinition PASSWORD_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 11, Severity.ERROR, Set.of("server", "variable"));
    public static final ProductErrorDefinition SIGN_IN_UNUSABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 12, Severity.ERROR, Set.of("server"));
    public static final ProductErrorDefinition UNKNOWN_CONNECTOR =
            new ProductErrorDefinition(ProductErrorModule.CLI, 13, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition CONNECTOR_NOT_OFFICIAL =
            new ProductErrorDefinition(ProductErrorModule.CLI, 14, Severity.ERROR, Set.of("connector", "official"));
    public static final ProductErrorDefinition CONNECTOR_DOWNLOAD_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 15, Severity.ERROR, Set.of("connector", "reason"));
    public static final ProductErrorDefinition KIND_DIR_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.CLI, 16, Severity.ERROR, Set.of("path", "kind", "dir"));
    public static final ProductErrorDefinition RESOURCE_NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.CLI, 17, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition CONNECT_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 18, Severity.ERROR, Set.of("seeds"));
    public static final ProductErrorDefinition REQUEST_TIMED_OUT =
            new ProductErrorDefinition(ProductErrorModule.CLI, 19, Severity.ERROR, Set.of("server"));
    public static final ProductErrorDefinition NOT_CONNECTED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 20, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition NOT_AUTHENTICATED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 21, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition VERB_NOT_IMPLEMENTED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 22, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition REPL_BUILTIN_ONLY =
            new ProductErrorDefinition(ProductErrorModule.CLI, 23, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition MCP_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 24, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition WATCH_NEEDS_A_TERMINAL =
            new ProductErrorDefinition(ProductErrorModule.CLI, 25, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CONFIRMATION_NEEDS_A_TERMINAL =
            new ProductErrorDefinition(ProductErrorModule.CLI, 26, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition IF_MATCH_NEEDS_ONE_RESOURCE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 27, Severity.ERROR, Set.of("count"));
    public static final ProductErrorDefinition START_NEEDS_AN_ANSWER =
            new ProductErrorDefinition(ProductErrorModule.CLI, 28, Severity.ERROR, Set.of("pipeline", "count", "commands"));
    public static final ProductErrorDefinition START_CHECKS_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 29, Severity.ERROR, Set.of("option"));
    public static final ProductErrorDefinition START_CHECKS_NOT_RUN =
            new ProductErrorDefinition(ProductErrorModule.CLI, 30, Severity.WARNING, Set.of("pipeline"));
    public static final ProductErrorDefinition DECIDE_MALFORMED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 31, Severity.ERROR, Set.of("decide"));
    public static final ProductErrorDefinition DECIDE_NOT_OFFERED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 32, Severity.ERROR, Set.of("decide", "findings"));
    public static final ProductErrorDefinition DECIDE_ON_A_REFUSAL =
            new ProductErrorDefinition(ProductErrorModule.CLI, 33, Severity.ERROR, Set.of("decide"));
    public static final ProductErrorDefinition START_MAY_HAVE_GONE_AHEAD =
            new ProductErrorDefinition(ProductErrorModule.CLI, 34, Severity.ERROR, Set.of("pipeline", "server"));
    public static final ProductErrorDefinition CONTEXT_CONFIG_INVALID =
            new ProductErrorDefinition(ProductErrorModule.CLI, 35, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition CONTEXT_CONFIG_VERSION =
            new ProductErrorDefinition(ProductErrorModule.CLI, 36, Severity.ERROR, Set.of("path", "version"));
    public static final ProductErrorDefinition CONTEXT_CONFIG_PERMISSIONS =
            new ProductErrorDefinition(ProductErrorModule.CLI, 37, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition AUTH_CACHE_INVALID =
            new ProductErrorDefinition(ProductErrorModule.CLI, 38, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition AUTH_CACHE_VERSION =
            new ProductErrorDefinition(ProductErrorModule.CLI, 39, Severity.ERROR, Set.of("path", "version"));
    public static final ProductErrorDefinition AUTH_CACHE_PERMISSIONS =
            new ProductErrorDefinition(ProductErrorModule.CLI, 40, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition AUTH_SESSION_REJECTED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 41, Severity.ERROR, Set.of("code", "principal"));
    public static final ProductErrorDefinition AUTH_SESSION_UNREACHABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 42, Severity.ERROR, Set.of("principal"));
    public static final ProductErrorDefinition AUTH_LOGIN_UNREACHABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 43, Severity.ERROR, Set.of("context"));
    public static final ProductErrorDefinition AUTH_LOGIN_REJECTED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 44, Severity.ERROR, Set.of("code", "principal"));
    public static final ProductErrorDefinition AUTH_LOGOUT_UNREACHABLE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 45, Severity.ERROR, Set.of("context"));
    public static final ProductErrorDefinition AUTH_LOGOUT_CACHE_CHANGED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 46, Severity.ERROR, Set.of("context"));
    public static final ProductErrorDefinition STORAGE_USAGE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 47, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition AUTH_USAGE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 48, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition CONTEXT_USAGE =
            new ProductErrorDefinition(ProductErrorModule.CLI, 49, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition CONTEXT_INVALID =
            new ProductErrorDefinition(ProductErrorModule.CLI, 50, Severity.ERROR, Set.of("name", "reason"));
    public static final ProductErrorDefinition CONTEXT_NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.CLI, 51, Severity.ERROR, Set.of("name"));
    public static final ProductErrorDefinition CONTEXT_ALREADY_EXISTS =
            new ProductErrorDefinition(ProductErrorModule.CLI, 52, Severity.ERROR, Set.of("name"));
    public static final ProductErrorDefinition CONTEXT_SOURCE_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.CLI, 53, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CONTEXT_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 54, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition REMOTE_PLAINTEXT =
            new ProductErrorDefinition(ProductErrorModule.CLI, 55, Severity.ERROR, Set.of("seed"));
    public static final ProductErrorDefinition ISSUER_DISCOVERY_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 56, Severity.ERROR, Set.of("seed"));
    public static final ProductErrorDefinition ISSUER_DISCOVERY_INVALID =
            new ProductErrorDefinition(ProductErrorModule.CLI, 57, Severity.ERROR, Set.of("seed", "reason"));
    public static final ProductErrorDefinition ISSUER_DISCOVERY_REJECTED =
            new ProductErrorDefinition(ProductErrorModule.CLI, 58, Severity.ERROR, Set.of("seed", "reason"));
    public static final ProductErrorDefinition AUTH_ISSUER_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.CLI, 59, Severity.ERROR, Set.of("expected", "actual", "seed"));
    public static final ProductErrorDefinition VERSION_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.CLI, 60, Severity.WARNING, Set.of("cli", "server"));

    private CliErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(ARTIFACT_EXISTS, DEMO_WORKSPACE_EXISTS, WORKSPACE_NOT_WRITABLE, WORKSPACE_UNREADABLE, WORKSPACE_HAS_NO_PIPELINE, ALIAS_NAME_TAKEN, ALIAS_INSTALL_DIR_UNKNOWN, ALIAS_LINK_FAILED, DOCKER_UNAVAILABLE, SERVER_NOT_NAMED, PASSWORD_REQUIRED, SIGN_IN_UNUSABLE, UNKNOWN_CONNECTOR, CONNECTOR_NOT_OFFICIAL, CONNECTOR_DOWNLOAD_FAILED, KIND_DIR_MISMATCH, RESOURCE_NOT_FOUND, CONNECT_FAILED, REQUEST_TIMED_OUT, NOT_CONNECTED, NOT_AUTHENTICATED, VERB_NOT_IMPLEMENTED, REPL_BUILTIN_ONLY, MCP_UNAVAILABLE, WATCH_NEEDS_A_TERMINAL, CONFIRMATION_NEEDS_A_TERMINAL, IF_MATCH_NEEDS_ONE_RESOURCE, START_NEEDS_AN_ANSWER, START_CHECKS_UNSUPPORTED, START_CHECKS_NOT_RUN, DECIDE_MALFORMED, DECIDE_NOT_OFFERED, DECIDE_ON_A_REFUSAL, START_MAY_HAVE_GONE_AHEAD, CONTEXT_CONFIG_INVALID, CONTEXT_CONFIG_VERSION, CONTEXT_CONFIG_PERMISSIONS, AUTH_CACHE_INVALID, AUTH_CACHE_VERSION, AUTH_CACHE_PERMISSIONS, AUTH_SESSION_REJECTED, AUTH_SESSION_UNREACHABLE, AUTH_LOGIN_UNREACHABLE, AUTH_LOGIN_REJECTED, AUTH_LOGOUT_UNREACHABLE, AUTH_LOGOUT_CACHE_CHANGED, STORAGE_USAGE, AUTH_USAGE, CONTEXT_USAGE, CONTEXT_INVALID, CONTEXT_NOT_FOUND, CONTEXT_ALREADY_EXISTS, CONTEXT_SOURCE_CONFLICT, CONTEXT_REQUIRED, REMOTE_PLAINTEXT, ISSUER_DISCOVERY_FAILED, ISSUER_DISCOVERY_INVALID, ISSUER_DISCOVERY_REJECTED, AUTH_ISSUER_MISMATCH, VERSION_MISMATCH);
    }
}
