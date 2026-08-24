package io.tapstate.cli;

import io.tapstate.core.common.TapstateException;

import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** The intentionally narrow interactive context manager shared by {@code tapstate context} and {@code :ctx}. */
final class ContextConsole {

    private static final String CREATE = "Create a context";
    private static final String CHOOSE = "Choose a context";
    private static final String EDIT = "Edit a context";
    private static final String BIND = "Bind context to this workspace";
    private static final String UNBIND = "Unbind this workspace";
    private static final String DELETE = "Delete a context";
    private static final String QUIT = "Quit";

    private final ContextManager manager;
    private final Prompter prompter;
    private final Path workspace;
    private final PrintWriter out;
    private final PrintWriter err;

    ContextConsole(ContextManager manager, Prompter prompter, Path workspace, PrintWriter out, PrintWriter err) {
        this.manager = manager;
        this.prompter = prompter;
        this.workspace = workspace;
        this.out = out;
        this.err = err;
    }

    int run() {
        if (prompter == null) {
            return usage("context manager needs an interactive terminal");
        }
        try {
            return runAction(prompter.choose("Context action", actions()));
        } catch (TapstateException failure) {
            Diagnostics.printText(err, failure.code(), failure.args());
            return Cli.EXIT_DIAGNOSTIC;
        } catch (IllegalArgumentException failure) {
            Diagnostics.printText(err, CliError.CONTEXT_USAGE, Map.of("reason", reason(failure)));
            return Cli.EXIT_USAGE;
        }
    }

    private int runAction(String action) {
        return switch (action) {
            case CREATE -> create();
            case CHOOSE -> choose();
            case EDIT -> edit();
            case BIND -> bind();
            case UNBIND -> unbind();
            case DELETE -> delete();
            case QUIT -> Cli.EXIT_OK;
            default -> usage("unknown context action");
        };
    }

    private List<String> actions() {
        List<String> actions = new ArrayList<>();
        actions.add(CREATE);
        if (!manager.suggestions().isEmpty()) {
            actions.add(CHOOSE);
            actions.add(EDIT);
            actions.add(BIND);
            actions.add(UNBIND);
            actions.add(DELETE);
        }
        actions.add(QUIT);
        return actions;
    }

    private int create() {
        String name = prompter.ask("Context name", "");
        List<URI> seeds = seeds(prompter.ask("Server URL", ""));
        boolean verifyTls = yes(prompter.ask("Verify TLS", "Y"), true);
        manager.create(name, seeds, verifyTls);
        out.println("created context " + name);
        if (yes(prompter.ask("Bind " + name + " to " + workspace.toAbsolutePath().normalize(), "Y"), true)) {
            manager.bind(workspace, name);
            out.println("bound " + name + " to " + canonical(workspace));
        }
        out.flush();
        return Cli.EXIT_OK;
    }

    private int choose() {
        String name = chooseContext();
        manager.choose(name);
        out.println("chose context " + name);
        out.flush();
        return Cli.EXIT_OK;
    }

    private int edit() {
        String name = chooseContext();
        ContextDefinition current = definition(name);
        String currentSeeds = String.join(",", current.seeds().stream().map(URI::toString).toList());
        String seedReply = prompter.ask("Server URL", currentSeeds);
        List<URI> seeds = seedReply.isBlank() ? current.seeds() : seeds(seedReply);
        boolean verifyTls = yes(prompter.ask("Verify TLS", current.tls().verify() ? "Y" : "n"),
                current.tls().verify());
        manager.edit(name, seeds, verifyTls);
        out.println("updated context " + name);
        out.flush();
        return Cli.EXIT_OK;
    }

    private int bind() {
        String name = chooseContext();
        manager.bind(workspace, name);
        out.println("bound " + name + " to " + canonical(workspace));
        out.flush();
        return Cli.EXIT_OK;
    }

    private int unbind() {
        manager.unbind(workspace).ifPresentOrElse(
                name -> out.println("unbound " + name + " from " + canonical(workspace)),
                () -> out.println("no context is bound to " + canonical(workspace)));
        out.flush();
        return Cli.EXIT_OK;
    }

    private int delete() {
        String name = chooseContext();
        ContextManager.DeletionImpact impact = manager.previewDelete(name);
        out.println("delete context " + impact.name());
        out.println("authRef " + impact.authRef());
        if (impact.workspaceBindings().isEmpty()) {
            out.println("bindings none");
        } else {
            impact.workspaceBindings().forEach(binding -> out.println("binding " + binding));
        }
        out.flush();
        if (!yes(prompter.ask("Delete context " + name, "no"), false)) {
            out.println("kept context " + name);
            out.flush();
            return Cli.EXIT_OK;
        }
        if (!yes(prompter.ask("Remove listed workspace bindings and keep auth cache", "no"), false)) {
            out.println("kept context " + name);
            out.flush();
            return Cli.EXIT_OK;
        }
        manager.delete(name);
        out.println("deleted context " + name);
        out.flush();
        return Cli.EXIT_OK;
    }

    private String chooseContext() {
        List<String> names = manager.suggestions().stream()
                .map(ContextManager.ContextChoice::name)
                .toList();
        if (names.isEmpty()) {
            throw new TapstateException(CliError.CONTEXT_REQUIRED, Map.of("verb", "context"), null);
        }
        return prompter.choose("Context", names);
    }

    private ContextDefinition definition(String name) {
        return manager.suggestions().stream()
                .filter(choice -> choice.name().equals(name))
                .findFirst()
                .map(ContextManager.ContextChoice::definition)
                .orElseThrow(() -> new TapstateException(CliError.CONTEXT_NOT_FOUND, Map.of("name", name), null));
    }

    private static List<URI> seeds(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("at least one server URL is required");
        }
        return List.of(expression.split(",")).stream()
                .map(String::trim)
                .filter(seed -> !seed.isEmpty())
                .map(URI::create)
                .toList();
    }

    private static boolean yes(String reply, boolean defaultValue) {
        if (reply == null || reply.isBlank()) {
            return defaultValue;
        }
        String normalized = reply.trim().toLowerCase(java.util.Locale.ROOT);
        return normalized.equals("y") || normalized.equals("yes") || normalized.equals("true");
    }

    private int usage(String reason) {
        Diagnostics.printText(err, CliError.CONTEXT_USAGE, Map.of("reason", reason));
        return Cli.EXIT_USAGE;
    }

    private static Path canonical(Path path) {
        try {
            return path.toRealPath();
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException(reason(failure), failure);
        }
    }

    private static String reason(Throwable failure) {
        return failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
    }
}
