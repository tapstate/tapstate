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

/**
 * The guided first run ({@code docs/first-run/README.md}): which server, then which outcome. Two
 * questions at most, each skipped when something already answers it — a workspace that is bound
 * already knows its server, a recipe id on the command line already names the outcome — and never
 * asked at all without a prompter, which is what {@code --yes} promises a script.
 *
 * <p>The server answer is saved as a registered context bound to the workspace directory, so the
 * question is not asked again there. Registration and binding go through {@link ContextManager} and
 * happen only after the server answered its health probe: an abort or an unreachable server leaves
 * nothing behind. Nothing here starts a server; a default nobody is listening on is a refusal.
 */
final class GuidedNew {

    /** The server on this machine, taken on an empty reply and used when no flag says otherwise. */
    static final String DEFAULT_SERVER_TEXT = "http://127.0.0.1:8080";
    static final URI DEFAULT_SERVER = URI.create(DEFAULT_SERVER_TEXT);

    /** The context name the default server is registered under. */
    static final String LOCAL_CONTEXT = "local";

    static final String OPENING_LINE =
            "Building a workspace: a directory of .tap.yml files you can read and edit.";
    static final String SERVER_QUESTION = "Which Tapstate server";
    static final String RECIPE_QUESTION = "What is this workspace for?";

    private final ContextManager contexts;
    private final ControlPlaneClient probe;
    private final Prompter prompter;
    private final PrintWriter prose;

    /**
     * @param prompter what asks the questions, or null to never ask (every answer must then be supplied)
     * @param prose    where the lines a person reads go, or null when nobody is reading them
     */
    GuidedNew(ContextManager contexts, ControlPlaneClient probe, Prompter prompter, PrintWriter prose) {
        this.contexts = contexts;
        this.probe = probe;
        this.prompter = prompter;
        this.prose = prose;
    }

    /**
     * Runs the two questions for {@code workspace} and returns the chosen recipe id.
     *
     * @param recipeId  the recipe already named on the command line, or null to ask
     * @param serverUrl the server already named on the command line, or null to ask / take the default
     */
    String run(Path workspace, String recipeId, URI serverUrl) throws IOException {
        if (prose != null) {
            prose.println(OPENING_LINE);
            prose.flush();
        }
        // a bound directory already knows its server; --server there is an override for the run, not a rebind
        if (!isBound(workspace)) {
            bindServer(workspace, serverUrl != null ? serverUrl : askServer());
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
            if (prose != null) {
                prose.println("  not an http(s) URL; type one like " + DEFAULT_SERVER_TEXT + ", or press Enter for it");
                prose.flush();
            }
        }
    }

    /**
     * Probe first, write second: the context is registered and the directory bound only once the
     * server has answered, so a refusal leaves the store exactly as it was.
     */
    private void bindServer(Path workspace, URI server) throws IOException {
        if (!probe.isHealthy(server)) {
            throw new TapstateException(CliError.CONNECT_FAILED, Map.of("seeds", server.toString()), null);
        }
        String name = server.equals(DEFAULT_SERVER) ? LOCAL_CONTEXT : contextNameFor(server);
        boolean exists = contexts.suggestions().stream().anyMatch(choice -> choice.name().equals(name));
        if (!exists) {
            contexts.create(name, List.of(server), true);
        }
        // the binding is keyed by the directory's real path, so the directory has to be there first
        Files.createDirectories(workspace);
        contexts.bind(workspace, name);
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
