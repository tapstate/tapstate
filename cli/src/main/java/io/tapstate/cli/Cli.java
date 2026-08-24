package io.tapstate.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.UsageMessageSpec;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The Tapstate CLI: the surface-ring product front-end. Dual-mode — bare {@code tapstate} opens the
 * offline REPL; one-shot subcommands share the same verb table for scripting / AI.
 *
 * <p>Offline verbs are a whitelist: {@code validate} / {@code new} / {@code explain} / {@code ls} /
 * {@code desc} run fully without any server. The server-state verbs are registered too, so they are
 * discoverable and report a coded "not connected" diagnostic rather than going missing; they reach a
 * service through the REPL, where a connection is established and held — the CLI talks to a running
 * Tapstate over HTTP only (rule R6).
 */
@Command(name = "tapstate", mixinStandardHelpOptions = true, version = Cli.VERSION,
        subcommands = {
                ValidateCmd.class, NewCmd.class, DemoCmd.class, ExplainCmd.class, LsCmd.class,
                DescCmd.class, McpCmd.class, AliasCmd.class},
        // the second line is indented by hand under the "Usage: " heading picocli prints before the first
        customSynopsis = {
                "tapstate [LAUNCH]                   open a session (interactive)",
                "       tapstate [LAUNCH] COMMAND [ARGS...]  run one command and exit"},
        description = {
                "",
                "With no command, opens a session: a prompt that holds a workspace and, once you",
                "connect, a server connection. The session commands are listed below.",
                "",
                "With a command, runs it once and exits -- the form for scripts. A command takes",
                "its own options, so the workspace is `tapstate validate -w DIR`, not",
                "`tapstate -w DIR validate`.",
                "",
                "LAUNCH options come before the command and shape how the CLI starts:",
                "  -w, --workdir DIR   workspace to start in (default: tap-work, or",
                "                        $TAPSTATE_WORKDIR)",
                "  -c, --connect URL   reach a server before doing anything else; takes the",
                "                        same seed list as `connect`",
                "      --context NAME  select a saved context for online commands",
                "                        (or place it after an auth action)",
                "  -u, --user NAME     sign in as this user once connected (needs -c); the password",
                "                        comes from $TAPSTATE_PASSWORD or a masked prompt",
                "      --token TOKEN   use a machine token for this process only (or",
                "                        $TAPSTATE_TOKEN); it is never saved in a user session",
                ""},
        footerHeading = "%nExamples:%n",
        footer = {
                "  tapstate                      open a session in the default workspace",
                "  tapstate -w ./work            open a session in ./work",
                "  tapstate validate ./work      validate a workspace and exit",
                "  tapstate help apply           describe one command",
                "  TAPSTATE_PASSWORD=secret tapstate -c localhost:8080 -u admin",
                "                                open a session already signed in",
                "  tapstate -c localhost:8080 -u admin ls",
                "                                run one command against a server and exit"},
        exitCodeListHeading = "%nExit codes:%n",
        exitCodeList = {
                "0:success",
                "1:a coded diagnostic was reported (an invalid workspace, or a refused operation)",
                "2:usage error (bad arguments, or a path that is not a usable workspace)",
                "3:the verb is unavailable here (it needs a connection, or is not implemented yet)"
        })
public final class Cli implements Runnable {

    /**
     * What {@code -V} prints, on the root and on every verb alike. A compile-time constant rather than a
     * manifest or bundled-resource lookup: neither survives into the native image without extra
     * declaration, and the version is wanted on a path that must not depend on either. The build pins it
     * to the project version, so the string here cannot quietly drift from what was released.
     */
    static final String VERSION = "tapstate 0.3.0";

    /** Exit code for a verb that ran and did what was asked. */
    static final int EXIT_OK = 0;

    /** Exit code for a reported coded diagnostic: an invalid workspace, or an operation refused. */
    static final int EXIT_DIAGNOSTIC = 1;

    /** Exit code for a usage mistake: bad arguments, or a path that is not a usable workspace. */
    static final int EXIT_USAGE = 2;

    /** Exit code for a verb that cannot run as invoked: no connection yet, or no implementation yet. */
    static final int EXIT_VERB_UNAVAILABLE = 3;

    /**
     * The offline verb whitelist — the single source of truth for which verbs run without a server.
     * The connected-verb derivation below and the registration-vs-whitelist guard test both read it,
     * so the declared list can never drift from the verbs actually wired up.
     */
    static final List<String> OFFLINE_VERBS =
            List.of("validate", "new", "demo", "explain", "ls", "desc");

    /**
     * How this face spells each operation it projects: operation id → verb name. The spelling is not
     * derivable from the id ({@code artifact.list} is {@code ls}, {@code connector.list} is
     * {@code connectors}), so the projection is declared rather than computed — this map is what the
     * face-projection gate checks against the operation registry in both directions, and the command
     * table below is built from it, so a verb cannot be projected here and left unregistered.
     *
     * <p>Operation ids are plain strings: the CLI reaches a server over HTTP and must not link the
     * control ring to read them (rule R6).
     */
    /**
     * The one verb the three data-browser reads project onto. They are one verb rather than three because
     * what a reader names is a place in the data — {@code <source>.<collection>} — and a verb table has no
     * way to spell that; the calls are a small language typed after it, or bare at the prompt. Named here
     * so the projection below, the online routing, and the help all read the same word.
     */
    static final String DATA_BROWSER_VERB = "data-browser";

    public static final Map<String, String> VERB_BY_OPERATION = Map.ofEntries(
            Map.entry("data-browser.collections", DATA_BROWSER_VERB),
            Map.entry("data-browser.find", DATA_BROWSER_VERB),
            Map.entry("data-browser.stats", DATA_BROWSER_VERB),
            Map.entry("artifact.apply", "apply"),
            Map.entry("artifact.validate", "validate"),
            Map.entry("artifact.get", "get"),
            Map.entry("artifact.list", "ls"),
            Map.entry("artifact.delete", "delete"),
            Map.entry("connection.test", "test"),
            Map.entry("connection.test-result", "test-result"),
            Map.entry("connection.discover-schema", "discover-schema"),
            Map.entry("connection.schema", "schema"),
            Map.entry("connector.register", "register"),
            Map.entry("connector.list", "connectors"),
            Map.entry("token.create", "token"),
            Map.entry("token.list", "token"),
            Map.entry("token.revoke", "token"),
            Map.entry("pipeline.start", "start"),
            Map.entry("pipeline.stop", "stop"),
            Map.entry("pipeline.pause", "pause"),
            Map.entry("pipeline.resume", "resume"),
            Map.entry("pipeline.status", "status"),
            Map.entry("pipeline.metrics", "metrics"),
            Map.entry("pipeline.snapshot", "snapshot"),
            Map.entry("pipeline.logs", "logs"));

    /**
     * Verbs that chain several registered operations rather than projecting one ({@code run} is apply
     * then start), and are not implemented yet. They are registered so they stay discoverable, but they
     * report that they do not exist yet — not that a connection is missing, which would be false in both
     * states: connecting does not implement them.
     */
    static final List<String> UNIMPLEMENTED_COMPOSITE_VERBS = List.of("run", "export", "diff", "edit");

    /**
     * The live views over a collection. They project no registered operation and never will: each is a
     * client-side loop over reads that are already registered, in the same category as the streaming
     * flags on the two pipeline read verbs. The verb itself carries the choice a reader is making —
     * one row, refreshed where it stands, or every change to the whole table — which is why there is no
     * flag anywhere that turns one into the other.
     */
    static final List<String> LIVE_VIEW_VERBS = List.of("watch", "tail");

    /**
     * Verbs that need a live server connection — every projected verb that does not also run offline
     * ({@code ls} browses the local workspace when there is no session). Derived from the projection
     * above so the two can never disagree. Registered so they are listed and explained rather than
     * reported as unknown, and each reports a coded "not connected" diagnostic until a session exists.
     * {@code connect} is not here — it is a REPL builtin, since a connection is session-scoped and
     * meaningful only inside the read loop, not as a one-shot verb.
     */
    static final List<String> CONNECTED_VERBS = VERB_BY_OPERATION.values().stream()
            .filter(verb -> !OFFLINE_VERBS.contains(verb))
            .distinct()
            .sorted()
            .toList();

    /**
     * What one verb's own help says: the operands it takes, and what it does.
     *
     * @param operands the operand grammar, worded exactly as the runtime's own "missing operand" lines
     *                 word it, so the two can be read side by side without translating between them
     * @param summary  one line saying what the verb does
     */
    record VerbHelp(String operands, String summary) {
    }

    /**
     * Per-verb help for the twenty verbs behind the two shared handlers. Each handler is a single class
     * registered under many names, so an annotation can only give all of them the same sentence — which
     * is why every one of these verbs used to render "(requires a connection to a Tapstate server)" and
     * nothing else, telling the reader that a verb exists but never what it is for. The table is applied
     * to each registered command's spec instead, and a guard test pins it to the registered names so a
     * verb cannot be added with no help of its own.
     *
     * <p>The operand grammar is quoted from the runtime's own usage lines rather than restated, so a
     * verb that changes its operands in one place fails the other. It carries the flags that are easy to
     * discover only by accident: the streaming forms of the two read verbs, and the output-format option
     * that only some of these verbs accept.
     */
    static final Map<String, VerbHelp> VERB_HELP = Map.ofEntries(
            Map.entry("apply", new VerbHelp("[<path>] [--if-match <hash>]",
                    "Upload the workspace, or one artifact, creating or updating each resource.")),
            Map.entry("get", new VerbHelp("<id>",
                    "Fetch one stored artifact back as canonical YAML.")),
            Map.entry("delete", new VerbHelp("<id> [--if-match <hash>] [-o text|json|yaml]",
                    "Remove one stored artifact for good; --if-match pins the version removed.")),
            Map.entry("connectors", new VerbHelp("[-o text|json|yaml]",
                    "List the connectors registered on the server.")),
            Map.entry("register", new VerbHelp("<path> [-o text|json|yaml]",
                    "Upload a connector artifact, or a directory of them.")),
            Map.entry("test", new VerbHelp("<id> [-o text|json|yaml]",
                    "Try a connection's configuration against the live endpoint.")),
            Map.entry("test-result", new VerbHelp("<id> [-o text|json|yaml]",
                    "Read back the stored result of that connection's last test.")),
            Map.entry("discover-schema", new VerbHelp("<id> [-o text|json|yaml]",
                    "Discover a connection's source schema and store it.")),
            Map.entry("schema", new VerbHelp("<id> [table] [-o text|json|yaml]",
                    "Show a connection's discovered schema, or one table of it.")),
            Map.entry("token", new VerbHelp("<create|list|revoke> [ARGS...] [-o text|json|yaml]",
                    "Create, list, or revoke machine tokens.")),
            Map.entry("start", new VerbHelp("<pipeline-id>",
                    "Start a pipeline.")),
            Map.entry("stop", new VerbHelp("<pipeline-id>",
                    "Stop a pipeline.")),
            Map.entry("pause", new VerbHelp("<pipeline-id>",
                    "Pause a running pipeline, holding its position.")),
            Map.entry("resume", new VerbHelp("<pipeline-id>",
                    "Resume a paused pipeline from where it stopped.")),
            Map.entry("status", new VerbHelp("<pipeline-id> [--watch]",
                    "Show a pipeline's current state; --watch streams it until Ctrl-C.")),
            Map.entry("metrics", new VerbHelp("<pipeline-id>",
                    "Show a pipeline's counters and per-table positions.")),
            Map.entry("snapshot", new VerbHelp("<pipeline-id>",
                    "Show a pipeline's per-table snapshot progress.")),
            Map.entry("logs", new VerbHelp("<pipeline-id> [--follow]",
                    "Tail a pipeline's log on its node; --follow streams until Ctrl-C.")),
            // The summary is one line because picocli wraps a longer one, and a wrapped line is a line the
            // help guard cannot pin. The call grammar lives where it is needed instead: in the usage this
            // verb prints when it cannot read a call.
            Map.entry(DATA_BROWSER_VERB, new VerbHelp("\"<call>\"",
                    "Read a source's data: list collections, preview rows, report size.")),
            // One line, for the same reason the read shell's summary is one: picocli wraps a longer one
            // and the help guard cannot pin a wrapped line.
            Map.entry("watch", new VerbHelp("<source>.<collection> [<filter>]",
                    "Watch one row in place until Ctrl-C; needs a terminal.")),
            Map.entry("tail", new VerbHelp("<source>.<collection> [<filter>]",
                    "Follow a whole collection's changes until Ctrl-C; pipes fine.")),
            // The reserved verbs. Each says what it is reserved for: "not implemented yet" answers the
            // question only once the reader knows what was going to be there.
            Map.entry("run", new VerbHelp("[<path>]",
                    "Apply a workspace and start its pipelines in one step.")),
            Map.entry("export", new VerbHelp("<id>",
                    "Write a stored artifact back out as canonical YAML.")),
            Map.entry("diff", new VerbHelp("<file>",
                    "Compare a local file against the stored artifact it declares.")),
            Map.entry("edit", new VerbHelp("<id>",
                    "Fetch an artifact, open it in $EDITOR, and apply what was saved.")));

    @Spec
    CommandSpec spec;

    /**
     * Commands that are about the CLI rather than about a resource. They are registered like verbs and
     * so appear on the table, but they project no operation and belong to no whitelist — the guard that
     * pins registered verbs to the offline whitelist reads this to tell "meta" from "undeclared".
     */
    static final List<String> META_VERBS = List.of("help", "mcp", "alias", "auth", "context");

    /**
     * Help for the words the REPL handles itself. They are deliberately not subcommands — a connection
     * is session state, so {@code connect} as a one-shot would establish one and immediately drop it —
     * but not being on the table is why {@code help} never listed them, while tab completion (built from
     * the same list the REPL dispatches) offered them all along. The not-connected diagnostic tells the
     * user to run {@code connect}, so help was the one place that word could not be found.
     *
     * <p>Rendered as their own help section rather than folded in with the verbs, because the difference
     * is real: these are typed at the prompt, and the verbs above can also be passed as arguments.
     */
    static final Map<String, VerbHelp> BUILTIN_HELP = Map.ofEntries(
            Map.entry("connect", new VerbHelp("<host:port>[,<host:port>...]",
                    "Reach a server; seeds tried in order.")),
            Map.entry("disconnect", new VerbHelp("",
                    "Drop the connection, keep the workspace.")),
            Map.entry("login", new VerbHelp("<username>",
                    "Sign in; prompts for the password.")),
            Map.entry("logout", new VerbHelp("",
                    "Drop the credential, stay connected.")),
            Map.entry(":ctx", new VerbHelp("",
                    "Manage saved contexts.")),
            Map.entry("cd", new VerbHelp("<dir>",
                    "Change the session workspace.")),
            Map.entry("pwd", new VerbHelp("",
                    "Print the session workspace.")),
            Map.entry("help", new VerbHelp("[<verb>]",
                    "List these, or describe one verb.")),
            Map.entry("exit", new VerbHelp("",
                    "End the session (same as quit).")),
            Map.entry("quit", new VerbHelp("",
                    "End the session (same as exit).")));

    /** The help section key for the REPL-builtin list, inserted just after the command list. */
    private static final String SECTION_REPL_BUILTINS = "replBuiltins";

    /** Builds the shared command table used by both one-shot mode and the REPL. */
    public static CommandLine newCommandLine() {
        CommandLine commandLine = new CommandLine(new Cli());
        // `help` is a word the CLI hands the user itself -- the REPL banner says to type it -- so it has
        // to be a word the CLI answers to, in both modes. Unregistered it was an unmatched argument,
        // answered with a spelling suggestion for a word that was spelt right.
        commandLine.addSubcommand(new CommandLine.HelpCommand());
        commandLine.addSubcommand(new AuthCmd());
        commandLine.addSubcommand(new ContextCmd());
        for (String verb : CONNECTED_VERBS) {
            commandLine.addSubcommand(verb, new ConnectedVerb());
        }
        for (String verb : LIVE_VIEW_VERBS) {
            commandLine.addSubcommand(verb, new ConnectedVerb());
        }
        for (String verb : UNIMPLEMENTED_COMPOSITE_VERBS) {
            commandLine.addSubcommand(verb, new UnimplementedVerb());
        }
        // The version belongs to the binary, not to any one verb, so every verb reports the same one.
        // Set centrally rather than annotated per class: the standard help mixin registers -V wherever
        // it is applied, and a spec with no version answers that advertised option with an empty line
        // and a success code. Two of these commands are also a single class registered under many
        // names, which an annotation could not give distinct versions to anyway.
        for (CommandLine subcommand : commandLine.getSubcommands().values()) {
            subcommand.getCommandSpec().version(VERSION);
        }
        // Give each verb behind a shared handler its own help. The synopsis carries the operand grammar,
        // which is the half the shared description could never say, and the summary leads the description
        // so that what the verb does is read before the reason it cannot run here.
        VERB_HELP.forEach((verb, help) -> {
            CommandSpec verbSpec = commandLine.getSubcommands().get(verb).getCommandSpec();
            verbSpec.usageMessage()
                    .customSynopsis(commandLine.getCommandName() + " " + verb + " " + help.operands() + " [-hV]")
                    .description(help.summary(), verbSpec.usageMessage().description()[0]);
        });
        addReplBuiltinHelpSection(commandLine);
        reportReplBuiltinsInsteadOfGuessingASpelling(commandLine);
        // accept -o json / -o JSON alike; the lower-case forms are the documented spelling
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        return commandLine;
    }

    /**
     * Adds the REPL-builtin list to the usage text, right after the command list. A section rather than
     * more subcommands: registering them would put them on the table, and the table is what one-shot mode
     * runs — a {@code connect} that opened a connection and exited would be worse than one that is
     * absent. They still have to be *listed*, because a user cannot run what nothing tells them exists.
     */
    private static void addReplBuiltinHelpSection(CommandLine commandLine) {
        // one column, wide enough for the longest call, so the summaries line up with each other
        int column = BUILTIN_HELP.entrySet().stream().mapToInt(e -> call(e).length()).max().orElse(0);
        commandLine.getHelpSectionMap().put(SECTION_REPL_BUILTINS, help -> {
            StringBuilder text = new StringBuilder(String.format(
                    "%nSession commands (type these at the prompt, after starting `tapstate`):%n"));
            // sorted by name so the rendering is stable across runs
            BUILTIN_HELP.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> text.append(String.format(
                            "  %-" + column + "s  %s%n", call(entry), entry.getValue().summary())));
            return text.toString();
        });
        List<String> sections = new ArrayList<>(commandLine.getHelpSectionKeys());
        sections.add(sections.indexOf(UsageMessageSpec.SECTION_KEY_COMMAND_LIST) + 1, SECTION_REPL_BUILTINS);
        commandLine.setHelpSectionKeys(sections);
    }

    /** How one session command is typed: its name, plus the operands it takes. */
    private static String call(Map.Entry<String, VerbHelp> builtin) {
        String operands = builtin.getValue().operands();
        return operands.isEmpty() ? builtin.getKey() : builtin.getKey() + " " + operands;
    }

    /**
     * Answers a REPL builtin typed as a one-shot with a coded diagnostic naming where it does live.
     * Unregistered words fall to picocli's "Did you mean" suggestion, which for these treats a correctly
     * spelt word the user was told to type as a typo, and offers an unrelated verb as the correction —
     * {@code connect} was answered with {@code connectors}. Anything genuinely unknown still gets the
     * suggestion, which is what it is good for.
     */
    private static void reportReplBuiltinsInsteadOfGuessingASpelling(CommandLine commandLine) {
        CommandLine.IParameterExceptionHandler fallback = commandLine.getParameterExceptionHandler();
        commandLine.setParameterExceptionHandler((ex, args) -> {
            String first = args.length > 0 ? args[0] : "";
            if (Repl.BUILTINS.contains(first)) {
                CommandLine offending = ex.getCommandLine();
                Diagnostics.printText(offending.getErr(), CliError.REPL_BUILTIN_ONLY, Map.of("verb", first));
                offending.getErr().flush();
                return EXIT_VERB_UNAVAILABLE;
            }
            return fallback.handleParseException(ex, args);
        });
    }

    /** Invoked when {@code tapstate} runs with no subcommand under {@code execute}; prints usage. */
    @Override
    public void run() {
        spec.commandLine().usage(CliIo.out(spec));
    }

    public static void main(String[] args) {
        LaunchOptions launch;
        try {
            launch = LaunchOptions.parse(args);
        } catch (RuntimeException e) {
            // malformed launch flags (a -c or -w with no value): let the command table render the error
            System.exit(newCommandLine().execute(args));
            return;
        }
        // -w before a command is refused rather than parsed here; see LaunchOptions for why
        if (launch.misplacesTheWorkspaceOption(args)) {
            System.exit(newCommandLine().execute(args));
            return;
        }
        if (launch.hasConflictingTargets()) {
            CommandLine commandLine = newCommandLine();
            Diagnostics.printText(commandLine.getErr(), CliError.CONTEXT_SOURCE_CONFLICT, Map.of());
            System.exit(EXIT_USAGE);
            return;
        }
        if (launch.isOneShot() && !Repl.isOnlineVerb(launch.command().get(0))) {
            // Offline verbs finish before context resolution. Launch-only target flags are intentionally
            // discarded here, so even a configured or temporary seed cannot cause DNS or a socket.
            System.exit(newCommandLine().execute(launch.command().toArray(new String[0])));
            return;
        }
        System.exit(runSession(launch, new HttpControlPlaneClient(), Cli::terminalPrompter));
    }

    /** The production password reader: a JLine prompter over the terminal, opened only if one is needed. */
    private static Prompter terminalPrompter() {
        try {
            return JLinePrompter.system();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Runs whatever the launch options asked for: establish the connection they name, then either run
     * the one command they carry and leave, or open the session and hand over to the read loop.
     *
     * <p>A failed connection or sign-in stops there. Dropping into a session that is not connected after
     * being asked for one would look like it had worked, and running the command anyway would report a
     * missing connection rather than the reason there is none.
     */
    static int runSession(LaunchOptions launch, ControlPlaneClient controlPlane,
                          Supplier<Prompter> prompter) {
        Path home = Path.of(System.getProperty("user.home"));
        ContextResolver resolver = new ContextResolver(ContextConfigStore.underHome(home), launch::environment);
        AuthService authService = new AuthService(
                controlPlane, AuthFileStore.underHome(home), java.time.Clock.systemUTC());
        return runSession(launch, controlPlane, prompter, resolver, authService);
    }

    static int runSession(LaunchOptions launch, ControlPlaneClient controlPlane,
                          Supplier<Prompter> prompter, ContextResolver resolver) {
        return runSession(launch, controlPlane, prompter, resolver, null);
    }

    private static int runSession(LaunchOptions launch, ControlPlaneClient controlPlane,
                                  Supplier<Prompter> prompter, ContextResolver resolver,
                                  AuthService authService) {
        Prompter oneShotPrompter = null;
        try {
            if (launch.hasConflictingTargets()) {
                Diagnostics.printText(newCommandLine().getErr(), CliError.CONTEXT_SOURCE_CONFLICT, Map.of());
                return EXIT_USAGE;
            }
            if (launch.isOneShot() && launch.command().size() >= 2
                    && launch.command().get(0).equals("auth")
                    && launch.command().get(1).equals("login")
                    && System.console() != null) {
                oneShotPrompter = prompter.get();
            }
            if (launch.isOneShot() && launch.command().size() == 1
                    && launch.command().get(0).equals("context")
                    && System.console() != null) {
                oneShotPrompter = prompter.get();
            }
            Repl repl = new Repl(newCommandLine(), launch.root(), controlPlane, oneShotPrompter,
                    launch::environment, resolver, launch.context(), authService,
                    new ContextManager(ContextConfigStore.underHome(Path.of(System.getProperty("user.home")))));
            String machineToken = launch.machineToken();
            if (machineToken != null) {
                repl.installMachineToken(machineToken);
            }
            if (launch.connects()) {
                int established = machineToken == null
                        ? repl.signIn(launch.connect(), launch.user(),
                                () -> launch.resolvePassword(prompter), launch.isOneShot())
                        : repl.connectForLaunch(launch.connect(), launch.isOneShot());
                if (established != EXIT_OK) {
                    return established;
                }
            }
            if (launch.isOneShot()) {
                repl.dispatch(launch.command(), true);
                return repl.lastExitCode();
            }
            repl.run();
            return EXIT_OK;
        } finally {
            if (oneShotPrompter instanceof JLinePrompter jline) {
                jline.close();
            }
            controlPlane.close();
        }
    }
}
