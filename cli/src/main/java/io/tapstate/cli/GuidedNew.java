package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * The guided first run ({@code docs/first-run/README.md}): which server, then which outcome. Two
 * questions at most, each skipped when something already answers it — a workspace that is bound
 * already knows its server, a recipe id on the command line already names the outcome — and never
 * asked at all without a prompter, which is what {@code --yes} promises a script.
 *
 * <p>The server answer is saved as a registered context bound to the workspace directory, so the
 * question is not asked again there, and it is signed in to, so that {@code up} can go there without
 * being told anything. Registration goes through {@link ContextManager}, the session through
 * {@link AuthService}, and both happen only after the server answered its health probe; the binding
 * is written last, after the sign-in, so an abort, an unreachable server or a refused login leaves the
 * directory unbound.
 *
 * <p>The default answer, when nothing is listening on it, starts the local development stack in
 * Docker ({@link LocalStack}) — at a terminal after saying so and being told to go on, and from a
 * script only when {@code --start-local} was passed. Never otherwise: a script that did not ask for
 * containers does not get them.
 */
final class GuidedNew {

    /** The server on this machine, taken on an empty reply and used when no flag says otherwise. */
    static final String DEFAULT_SERVER_TEXT = "http://127.0.0.1:8080";
    static final URI DEFAULT_SERVER = URI.create(DEFAULT_SERVER_TEXT);

    /** The context name the default server is registered under. */
    static final String LOCAL_CONTEXT = "local";

    /** The environment name a script supplies the sign-in password in, as every other sign-in reads it. */
    static final String PASSWORD_ENV = "TAPSTATE_PASSWORD";

    static final String OPENING_LINE =
            "Building a workspace: a directory of .tap.yml files you can read and edit.";
    static final String SERVER_QUESTION = "Which Tapstate server";
    static final String USERNAME_QUESTION = "Username";
    static final String PASSWORD_QUESTION = "Password";
    static final String RECIPE_QUESTION = "What is this workspace for?";
    static final String LOCAL_STACK_OFFER = "Nothing is listening on " + DEFAULT_SERVER_TEXT
            + "; press Enter to start a local development stack in Docker, or type the URL of a server you already run.";

    private final ContextManager contexts;
    private final ControlPlaneClient probe;
    private final AuthService auth;
    private final LocalStack stack;
    private final UnaryOperator<String> env;
    private final Prompter prompter;
    private final PrintWriter prose;

    /**
     * @param env      the process environment, read for the sign-in password
     * @param prompter what asks the questions, or null to never ask (every answer must then be supplied)
     * @param prose    where the lines a person reads go, or null when nobody is reading them
     */
    GuidedNew(ContextManager contexts, ControlPlaneClient probe, AuthService auth, LocalStack stack,
              UnaryOperator<String> env, Prompter prompter, PrintWriter prose) {
        this.contexts = contexts;
        this.probe = probe;
        this.auth = auth;
        this.stack = stack;
        this.env = env;
        this.prompter = prompter;
        this.prose = prose;
    }

    /**
     * Runs the two questions for {@code workspace} and returns the chosen recipe id.
     *
     * @param recipeId   the recipe already named on the command line, or null to ask
     * @param serverUrl  the server already named on the command line, or null to ask / take the default
     * @param startLocal whether a script allowed the local stack to be started when nothing listens
     * @param user       the user to sign in as, or null to ask (a script, or an empty reply, takes admin)
     */
    String run(Path workspace, String recipeId, URI serverUrl, boolean startLocal, String user) throws IOException {
        say(OPENING_LINE);
        // a bound directory already knows its server; --server there is an override for the run, not a rebind
        if (!isBound(workspace)) {
            bindServer(workspace, serverUrl != null ? serverUrl : askServer(), startLocal, user);
        }
        return recipeId != null ? recipeId : askRecipe();
    }

    /** A directory that does not exist yet cannot be bound, so it reads as unbound rather than as an error. */
    private boolean isBound(Path workspace) {
        return Files.isDirectory(workspace) && contexts.contextBoundExactlyTo(workspace).isPresent();
    }

    /** Asks for a URL until one parses; an empty reply is the default. Never asked without a prompter. */
    private URI askServer() {
        if (prompter == null) {
            return DEFAULT_SERVER;
        }
        while (true) {
            String reply = prompter.ask(SERVER_QUESTION, DEFAULT_SERVER_TEXT);
            if (reply.isEmpty()) {
                return DEFAULT_SERVER;
            }
            URI url = serverUrl(reply);
            if (url != null) {
                return url;
            }
            say("  not an http(s) URL; type one like " + DEFAULT_SERVER_TEXT + ", or press Enter for it");
        }
    }

    /**
     * Probe first, write second. A server that answers is signed in to and bound; the default that does
     * not answer is the one case with a way forward - the local stack - and a typed server that does not
     * answer is a refusal, since nothing here can start a server somewhere else.
     */
    private void bindServer(Path workspace, URI server, boolean startLocal, String user) throws IOException {
        if (probe.isHealthy(server)) {
            signInAndBind(workspace, server, credentialsFor(server, user), false);
            return;
        }
        if (!server.equals(DEFAULT_SERVER)) {
            throw connectFailed(server);
        }
        if (prompter == null) {
            if (!startLocal) {
                throw connectFailed(server);
            }
        } else {
            say(LOCAL_STACK_OFFER);
            URI instead = askServer();
            if (!instead.equals(DEFAULT_SERVER)) {
                bindServer(workspace, instead, startLocal, user);
                return;
            }
        }
        LocalStack.Admin admin = stack.start(prose);
        signInAndBind(workspace, DEFAULT_SERVER, new Credentials(admin.user(), admin.password()), true);
        say("local stack: " + stack.dir());
        say("to stop it: " + stack.stopCommand());
    }

    /**
     * The credentials for a server that is already listening: a local stack started by an earlier run
     * signs in as the admin it wrote down, without asking; anything else is asked for, or taken from
     * the flags and the environment when nobody can be asked. Resolved before anything is registered,
     * so a script missing its password is refused with the store exactly as it was.
     */
    private Credentials credentialsFor(URI server, String user) throws IOException {
        if (server.equals(DEFAULT_SERVER)) {
            LocalStack.Admin saved = stack.savedAdmin().orElse(null);
            if (saved != null) {
                return new Credentials(saved.user(), saved.password());
            }
        }
        String username = user != null ? user : askUsername();
        String password = env.apply(PASSWORD_ENV);
        if (password == null || password.isEmpty()) {
            if (prompter == null) {
                throw new RecipeRun.Usage("signing in to " + server + " needs a password: set " + PASSWORD_ENV
                        + " (with --yes, nothing is asked), or pass --server for a different server");
            }
            password = prompter.secret(PASSWORD_QUESTION);
        }
        return new Credentials(username, password);
    }

    /** The default is admin either way: it is what a fresh server's first administrator is called. */
    private String askUsername() {
        if (prompter == null) {
            return LocalStack.ADMIN_USER;
        }
        String reply = prompter.ask(USERNAME_QUESTION, LocalStack.ADMIN_USER).trim();
        return reply.isEmpty() ? LocalStack.ADMIN_USER : reply;
    }

    /**
     * Registers the context when it is new, signs in through the same service every other sign-in
     * uses, and only then binds the directory. {@code justStarted} says the server is a stack that came
     * up a moment ago: its bootstrap creates the admin right after the server first answers, so a
     * refused login there is retried for as long as the stack was given to answer at all - and once
     * signed in, the binding waits for the stack's boot-time sweep to register the bundled connectors,
     * so the first {@code up} never lands in the seconds between the server listening and them existing.
     */
    private void signInAndBind(Path workspace, URI server, Credentials credentials, boolean justStarted)
            throws IOException {
        String name = server.equals(DEFAULT_SERVER) ? LOCAL_CONTEXT : contextNameFor(server);
        ContextDefinition definition = contexts.suggestions().stream()
                .filter(choice -> choice.name().equals(name))
                .map(ContextManager.ContextChoice::definition)
                .findFirst()
                .orElseGet(() -> contexts.create(name, List.of(server), true));
        ResolvedContext.Named context = new ResolvedContext.Named(name, definition, ResolvedContext.Source.EXPLICIT);
        Supplier<AuthService.LoginResult> attempt =
                () -> auth.login(context, credentials.user(), credentials.password(), false);
        AuthService.LoginResult result = justStarted
                ? stack.retryWhile(attempt, outcome -> outcome instanceof AuthService.LoginResult.Rejected)
                : attempt.get();
        switch (result) {
            case AuthService.LoginResult.Success success -> {
                if (justStarted) {
                    stack.awaitConnectors(() -> registeredConnectors(success.session()));
                }
            }
            case AuthService.LoginResult.Rejected rejected -> throw new TapstateException(
                    CliError.AUTH_LOGIN_REJECTED,
                    Map.of("code", rejected.code(), "principal", rejected.principal()), null);
            case AuthService.LoginResult.Unreachable ignored -> throw new TapstateException(
                    CliError.AUTH_LOGIN_UNREACHABLE, Map.of("context", name), null);
        }
        // the binding is keyed by the directory's real path, so the directory has to be there first
        Files.createDirectories(workspace);
        contexts.bind(workspace, name);
    }

    /**
     * The connectors the server reports as actually loaded for {@code session}; none while the list
     * cannot be read. A stack that has just come up lists its whole bundled catalog at once - the staged
     * jars among them show as {@code bundled} until the boot-time sweep loads them and flips them to
     * {@code registered}. Only the latter can be tested against, so the id of a merely bundled entry
     * does not count as ready.
     */
    private Set<String> registeredConnectors(AuthService.ActiveSession session) {
        return switch (probe.connectorList(session.seed(), session.accessToken())) {
            case ConnectorListOutcome.Listed listed -> listed.connectors().stream()
                    .filter(connector -> "registered".equals(connector.origin()))
                    .map(CatalogConnector::id)
                    .collect(Collectors.toSet());
            default -> Set.of();
        };
    }

    private static TapstateException connectFailed(URI server) {
        return new TapstateException(CliError.CONNECT_FAILED, Map.of("seeds", server.toString()), null);
    }

    private String askRecipe() {
        List<String> titles = Recipe.CATALOG.stream().map(Recipe::title).toList();
        String title = prompter.choose(RECIPE_QUESTION, titles, titles.get(0));
        return Recipe.CATALOG.stream()
                .filter(recipe -> recipe.title().equals(title))
                .map(Recipe::id)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("prompter returned an option it was not offered: " + title));
    }

    private void say(String line) {
        if (prose != null) {
            prose.println(line);
            prose.flush();
        }
    }

    /** A username and password on their way to one login call; never printed. */
    private record Credentials(String user, String password) {
        @Override
        public String toString() {
            return "Credentials[user=" + user + ", password=<redacted>]";
        }
    }

    /** {@code text} as an absolute http(s) URL with a host, or null when it is not one. */
    static URI serverUrl(String text) {
        try {
            URI url = new URI(text.trim());
            boolean http = "http".equalsIgnoreCase(url.getScheme()) || "https".equalsIgnoreCase(url.getScheme());
            return http && url.getHost() != null && !url.getHost().isEmpty() ? url : null;
        } catch (URISyntaxException notAUrl) {
            return null;
        }
    }

    /**
     * The context name a typed server is registered under: its host, with anything a context name
     * cannot carry (an IPv6 literal's brackets, say) folded to dashes.
     */
    static String contextNameFor(URI server) {
        String name = server.getHost()
                .replaceAll("[^A-Za-z0-9._-]", "-")
                .replaceFirst("^[^A-Za-z0-9]+", "");
        return name.isEmpty() ? "server" : name;
    }
}
