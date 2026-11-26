package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** CLI diagnostics retain command outcomes and authentication boundaries. */
public final class CliErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.CLI);

    public static final ProductErrorDefinition ARTIFACT_EXISTS =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition DEMO_WORKSPACE_EXISTS =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition WORKSPACE_NOT_WRITABLE =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition WORKSPACE_UNREADABLE =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition WORKSPACE_HAS_NO_PIPELINE =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition ALIAS_NAME_TAKEN =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition ALIAS_INSTALL_DIR_UNKNOWN =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition ALIAS_LINK_FAILED =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition DOCKER_UNAVAILABLE =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition SERVER_NOT_NAMED =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("server"));
    public static final ProductErrorDefinition PASSWORD_REQUIRED =
            DEFINITIONS.define(11, Severity.ERROR, Set.of("server", "variable"));
    public static final ProductErrorDefinition SIGN_IN_UNUSABLE =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("server"));
    public static final ProductErrorDefinition UNKNOWN_CONNECTOR =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition CONNECTOR_NOT_OFFICIAL =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("connector", "official"));
    public static final ProductErrorDefinition CONNECTOR_DOWNLOAD_FAILED =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("connector", "reason"));
    public static final ProductErrorDefinition KIND_DIR_MISMATCH =
            DEFINITIONS.define(16, Severity.ERROR, Set.of("path", "kind", "dir"));
    public static final ProductErrorDefinition RESOURCE_NOT_FOUND =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition CONNECT_FAILED =
            DEFINITIONS.define(18, Severity.ERROR, Set.of("seeds"));
    public static final ProductErrorDefinition REQUEST_TIMED_OUT =
            DEFINITIONS.define(19, Severity.ERROR, Set.of("server"));
    public static final ProductErrorDefinition NOT_CONNECTED =
            DEFINITIONS.define(20, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition NOT_AUTHENTICATED =
            DEFINITIONS.define(21, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition VERB_NOT_IMPLEMENTED =
            DEFINITIONS.define(22, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition REPL_BUILTIN_ONLY =
            DEFINITIONS.define(23, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition MCP_UNAVAILABLE =
            DEFINITIONS.define(24, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition WATCH_NEEDS_A_TERMINAL =
            DEFINITIONS.define(25, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CONFIRMATION_NEEDS_A_TERMINAL =
            DEFINITIONS.define(26, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition IF_MATCH_NEEDS_ONE_RESOURCE =
            DEFINITIONS.define(27, Severity.ERROR, Set.of("count"));
    public static final ProductErrorDefinition START_NEEDS_AN_ANSWER =
            DEFINITIONS.define(28, Severity.ERROR, Set.of("pipeline", "count", "commands"));
    public static final ProductErrorDefinition START_CHECKS_UNSUPPORTED =
            DEFINITIONS.define(29, Severity.ERROR, Set.of("option"));
    public static final ProductErrorDefinition START_CHECKS_NOT_RUN =
            DEFINITIONS.define(30, Severity.WARNING, Set.of("pipeline"));
    public static final ProductErrorDefinition DECIDE_MALFORMED =
            DEFINITIONS.define(31, Severity.ERROR, Set.of("decide"));
    public static final ProductErrorDefinition DECIDE_NOT_OFFERED =
            DEFINITIONS.define(32, Severity.ERROR, Set.of("decide", "findings"));
    public static final ProductErrorDefinition DECIDE_ON_A_REFUSAL =
            DEFINITIONS.define(33, Severity.ERROR, Set.of("decide"));
    public static final ProductErrorDefinition START_MAY_HAVE_GONE_AHEAD =
            DEFINITIONS.define(34, Severity.ERROR, Set.of("pipeline", "server"));
    public static final ProductErrorDefinition CONTEXT_CONFIG_INVALID =
            DEFINITIONS.define(35, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition CONTEXT_CONFIG_VERSION =
            DEFINITIONS.define(36, Severity.ERROR, Set.of("path", "version"));
    public static final ProductErrorDefinition CONTEXT_CONFIG_PERMISSIONS =
            DEFINITIONS.define(37, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition AUTH_CACHE_INVALID =
            DEFINITIONS.define(38, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition AUTH_CACHE_VERSION =
            DEFINITIONS.define(39, Severity.ERROR, Set.of("path", "version"));
    public static final ProductErrorDefinition AUTH_CACHE_PERMISSIONS =
            DEFINITIONS.define(40, Severity.ERROR, Set.of("path", "reason"));
    public static final ProductErrorDefinition AUTH_SESSION_REJECTED =
            DEFINITIONS.define(41, Severity.ERROR, Set.of("code", "principal"));
    public static final ProductErrorDefinition AUTH_SESSION_UNREACHABLE =
            DEFINITIONS.define(42, Severity.ERROR, Set.of("principal"));
    public static final ProductErrorDefinition AUTH_LOGIN_UNREACHABLE =
            DEFINITIONS.define(43, Severity.ERROR, Set.of("context"));
    public static final ProductErrorDefinition AUTH_LOGIN_REJECTED =
            DEFINITIONS.define(44, Severity.ERROR, Set.of("code", "principal"));
    public static final ProductErrorDefinition AUTH_LOGOUT_UNREACHABLE =
            DEFINITIONS.define(45, Severity.ERROR, Set.of("context"));
    public static final ProductErrorDefinition AUTH_LOGOUT_CACHE_CHANGED =
            DEFINITIONS.define(46, Severity.ERROR, Set.of("context"));
    public static final ProductErrorDefinition STORAGE_USAGE =
            DEFINITIONS.define(47, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition AUTH_USAGE =
            DEFINITIONS.define(48, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition CONTEXT_USAGE =
            DEFINITIONS.define(49, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition CONTEXT_INVALID =
            DEFINITIONS.define(50, Severity.ERROR, Set.of("name", "reason"));
    public static final ProductErrorDefinition CONTEXT_NOT_FOUND =
            DEFINITIONS.define(51, Severity.ERROR, Set.of("name"));
    public static final ProductErrorDefinition CONTEXT_ALREADY_EXISTS =
            DEFINITIONS.define(52, Severity.ERROR, Set.of("name"));
    public static final ProductErrorDefinition CONTEXT_SOURCE_CONFLICT =
            DEFINITIONS.define(53, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CONTEXT_REQUIRED =
            DEFINITIONS.define(54, Severity.ERROR, Set.of("verb"));
    public static final ProductErrorDefinition REMOTE_PLAINTEXT =
            DEFINITIONS.define(55, Severity.ERROR, Set.of("seed"));
    public static final ProductErrorDefinition ISSUER_DISCOVERY_FAILED =
            DEFINITIONS.define(56, Severity.ERROR, Set.of("seed"));
    public static final ProductErrorDefinition ISSUER_DISCOVERY_INVALID =
            DEFINITIONS.define(57, Severity.ERROR, Set.of("seed", "reason"));
    public static final ProductErrorDefinition ISSUER_DISCOVERY_REJECTED =
            DEFINITIONS.define(58, Severity.ERROR, Set.of("seed", "reason"));
    public static final ProductErrorDefinition AUTH_ISSUER_MISMATCH =
            DEFINITIONS.define(59, Severity.ERROR, Set.of("expected", "actual", "seed"));
    public static final ProductErrorDefinition VERSION_MISMATCH =
            DEFINITIONS.define(60, Severity.WARNING, Set.of("cli", "server"));

    public static final ProductErrorDefinition REQUEST_FAILED =
            DEFINITIONS.define(61, Severity.ERROR, Set.of("server"));
    public static final ProductErrorDefinition CONNECTION_LOST =
            DEFINITIONS.define(62, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition STREAM_NO_FRAMES =
            DEFINITIONS.define(63, Severity.ERROR, Set.of("attempts"));

    public static final ProductErrorDefinition WORKBENCH_NEEDS_A_TERMINAL =
            DEFINITIONS.define(64, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WORKBENCH_UNAVAILABLE =
            DEFINITIONS.define(65, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition WORKBENCH_SESSION_FAILED =
            DEFINITIONS.define(66, Severity.ERROR, Set.of());

    private CliErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
