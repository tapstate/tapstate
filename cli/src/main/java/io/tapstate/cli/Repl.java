package io.tapstate.cli;

import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Interpolator;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.schema.SchemaNavigator;
import io.tapstate.messages.MessageCatalog;
import org.jline.reader.EndOfFileException;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * The offline REPL: a JLine read loop over the same command table the one-shot mode uses, so a verb
 * behaves identically whether typed at the prompt or passed as arguments. Builtins are {@code help}
 * (usage), {@code exit} / {@code quit}, and the workspace builtins {@code cd} / {@code pwd}; everything
 * else is dispatched as a verb. {@link #dispatch} is the testable seam; {@link #run} wraps it with the
 * JLine terminal.
 *
 * <p>The REPL carries a session workspace (the current {@code cd} directory). A dispatched verb that
 * declares {@code --workdir} but does not set it on the line inherits this session workspace — so a bare
 * {@code validate} targets where the session sits, not the process-relative {@code tap-work} default.
 */
final class Repl {

    /**
     * Printed under a non-empty metrics read. The metric names are not a compatibility promise in this
     * preview: they may be renamed as the metric model settles, and the read face is where a user decides
     * whether to build on them, so the disclaimer belongs there rather than only in the documentation.
     */
    private static final String METRIC_NAMES_UNSTABLE =
            "(metric names are unstable in this preview and may change)";

    /** REPL-only words handled here rather than by the command table; completed alongside the verbs. */
    static final List<String> BUILTINS =
            List.of("help", "exit", "quit", "cd", "pwd", "connect", "disconnect", "login", "logout", ":ctx");

    /**
     * Registry verbs a connected session routes to the server instead of the offline command table. The
     * artifact verbs ({@code apply} = {@code artifact.apply}, {@code get} = {@code artifact.get},
     * {@code ls} = {@code artifact.list}), the four pipeline lifecycle verbs ({@code start} / {@code stop}
     * / {@code pause} / {@code resume} = {@code pipeline.*}), the three observation read faces
     * ({@code status} / {@code metrics} / {@code snapshot} = {@code pipeline.status} / {@code pipeline.metrics}
     * / {@code pipeline.snapshot}) and the log tail ({@code logs} = {@code pipeline.logs}), the connection
     * verbs ({@code test} = {@code connection.test}, {@code test-result} = {@code connection.test-result},
     * {@code discover-schema} = {@code connection.discover-schema}, {@code schema} = {@code connection.schema}),
     * and the connector verbs ({@code register} = {@code connector.register}, {@code connectors} =
     * {@code connector.list}). Offline they fall through to the table, where the connected verbs report a
     * coded {@code cli.not-connected} and {@code ls} browses the local workspace. {@code validate} is not here — it
     * runs the full local stack in either state until a server validate endpoint exists.
     */
    /** The screen width assumed when there is no terminal to ask, or it answers nothing useful. */
    private static final int DEFAULT_SCREEN_WIDTH = 100;

    /** How often the in-place view asks again. Stated in its own header, because it is a poll. */
    private static final Duration WATCH_INTERVAL = Duration.ofSeconds(1);

    /** How often a wait wakes to notice the user interrupted it. */
    private static final Duration CANCEL_POLL = Duration.ofMillis(200);

    /**
     * The refusals a repeating background read rides out rather than dying on. Both mean the connector
     * was busy serving somebody else this second, which is the arrangement working: the interactive
     * reads that took its turn are the ones a person is waiting on.
     */
    private static final Set<String> BUSY_CODES =
            Set.of("connector.instances-busy", "connector.instance-limit-reached");

    private static final List<String> ONLINE_VERBS = List.of(
            "apply", "get", "delete", "ls", "start", "stop", "pause", "resume", "status", "metrics",
            "snapshot", "logs", "test", "test-result", "discover-schema", "schema", "register",
            "connectors", "token");

    private final CommandLine commandLine;

    /** The transport seam to a server; a network-free fake is injected in tests. */
    private final ControlPlaneClient controlPlane;

    /** The one shared lazy target resolver; absent only in legacy unit seams that exercise no contexts. */
    private final ContextResolver contextResolver;

    /** The durable target selected at process launch, or null when resolution may use lower sources. */
    private final String explicitContext;

    /** Shared persistent human-auth service; absent only from legacy network-free unit seams. */
    private final AuthService authService;

    /** Shared interactive context manager; absent only from legacy seams that exercise no contexts. */
    private final ContextManager contextManager;

    /** The named context that established the current transport, if this is not a temporary connect. */
    private ResolvedContext.Named namedContext;

    /** Reads the login password masked; a scripted fake is injected in tests, a JLine one bound in {@link #run}. */
    private Prompter prompter;

    /**
     * Whether this process's output goes to a terminal, which is what the in-place view needs and the
     * only thing it refuses for. A seam rather than a direct call so both answers can be exercised: a
     * test process has no terminal, so the refusal would be the only branch ever reached.
     */
    private BooleanSupplier terminal = () -> System.console() != null;

    /**
     * How wide the screen is. Asked again on every frame rather than captured once, so a window
     * resized while the in-place view is running is picked up by the next redraw. Falls back to a
     * conventional width when there is no terminal to ask -- a dumb terminal answers zero.
     */
    private IntSupplier screenWidth = () -> DEFAULT_SCREEN_WIDTH;

    /** The connection state, carried across read-loop iterations (offline until {@code connect}). */
    private final Session session = new Session();

    /** The session workspace: the current {@code cd} directory, injected into workspace-aware verbs. */
    private Path workdir;

    /** The status of the last dispatched line; see {@link #lastExitCode()}. */
    private int lastExitCode;

    /** Whether to drop the connect / sign-in confirmations; see {@link #confirm}. */
    private boolean quiet;

    /**
     * The environment {@code ${...}} references are substituted from — the author's own, since this side
     * loads the files. A scripted stand-in is injected in tests; the real one is the process environment.
     */
    private final UnaryOperator<String> env;

    /**
     * Set by the terminal's interrupt handler to stop an in-flight {@code --watch} / {@code --follow}
     * stream; reset at the start of each stream. Volatile because the interrupt handler runs on another
     * thread than the stream loop that polls it.
     */
    private volatile boolean streamCancelled;

    Repl(CommandLine commandLine) {
        this(commandLine, WorkspaceOption.resolve());
    }

    Repl(CommandLine commandLine, Path workdir) {
        this(commandLine, workdir, new HttpControlPlaneClient());
    }

    Repl(CommandLine commandLine, Path workdir, ControlPlaneClient controlPlane) {
        this(commandLine, workdir, controlPlane, null);
    }

    Repl(CommandLine commandLine, Path workdir, ControlPlaneClient controlPlane, Prompter prompter) {
        this(commandLine, workdir, controlPlane, prompter, System::getenv);
    }

    Repl(CommandLine commandLine, Path workdir, ControlPlaneClient controlPlane, Prompter prompter,
         UnaryOperator<String> env) {
        this(commandLine, workdir, controlPlane, prompter, env, null, null);
    }

    Repl(CommandLine commandLine, Path workdir, ControlPlaneClient controlPlane, Prompter prompter,
         UnaryOperator<String> env, ContextResolver contextResolver, String explicitContext) {
        this(commandLine, workdir, controlPlane, prompter, env, contextResolver, explicitContext, null);
    }

    Repl(CommandLine commandLine, Path workdir, ControlPlaneClient controlPlane, Prompter prompter,
         UnaryOperator<String> env, ContextResolver contextResolver, String explicitContext,
         AuthService authService) {
        this(commandLine, workdir, controlPlane, prompter, env, contextResolver, explicitContext,
                authService, null);
    }

    Repl(CommandLine commandLine, Path workdir, ControlPlaneClient controlPlane, Prompter prompter,
         UnaryOperator<String> env, ContextResolver contextResolver, String explicitContext,
         AuthService authService, ContextManager contextManager) {
        this.commandLine = commandLine;
        this.workdir = workdir;
        this.controlPlane = controlPlane;
        this.prompter = prompter;
        this.env = env;
        this.contextResolver = contextResolver;
        this.explicitContext = explicitContext;
        this.authService = authService;
        this.contextManager = contextManager;
    }

    /** Whether this verb can be routed to the control plane when a context resolves. */
    static boolean isOnlineVerb(String verb) {
        return ONLINE_VERBS.contains(verb) || Cli.LIVE_VIEW_VERBS.contains(verb)
                || verb.equals("auth") || verb.equals("context");
    }

    /** The live-view verb this line opens with, or null when it opens with something else. */
    private static String liveVerbOf(String line) {
        for (String verb : Cli.LIVE_VIEW_VERBS) {
            if (line.equals(verb) || line.startsWith(verb + " ")) {
                return verb;
            }
        }
        return null;
    }

    /** Answers how wide the screen is; overridden so the narrow layout can be exercised. */
    void screenWidth(IntSupplier width) {
        this.screenWidth = width;
    }

    /** Answers whether this process has a terminal; overridden so both branches can be exercised. */
    void terminalCheck(BooleanSupplier check) {
        this.terminal = check;
    }

    /** The current session workspace. */
    Path workdir() {
        return workdir;
    }

    /** The current connection state. */
    Session session() {
        return session;
    }

    /**
     * The status the last dispatched line produced, in the same scheme one-shot mode exits with. The
     * read loop ignores it — a line that failed does not end a session — but a scripted invocation that
     * runs one line and leaves has nothing else to report with.
     */
    int lastExitCode() {
        return lastExitCode;
    }

    /** Requests any in-flight {@code --watch} / {@code --follow} stream to stop; wired to Ctrl-C in {@link #run}. */
    void cancelStream() {
        streamCancelled = true;
    }

    private boolean isStreamCancelled() {
        return streamCancelled;
    }

    /**
     * The prompt: {@code tapstate(offline:<workspace>)> } while offline, {@code tapstate(<host:port>)> }
     * naming the landing node once connected, and {@code tapstate(<principal>@<host:port>)> } once
     * authenticated (the cluster name is not shown while membership is undiscovered in L1).
     */
    String prompt() {
        if (session.isAuthenticated()) {
            return "tapstate(" + session.principal() + "@" + hostPort(session.landingNode()) + ")> ";
        }
        if (session.isConnected()) {
            return "tapstate(" + hostPort(session.landingNode()) + ")> ";
        }
        Path name = workdir.getFileName();
        String label = name != null ? name.toString() : workdir.toString();
        return "tapstate(offline:" + label + ")> ";
    }

    /**
     * Establishes the session a one-line launch asked for: reach the server, then sign in if a user was
     * named. Returns the status of the first step that failed, or success. Nothing is persisted — the
     * credential lives in this session's memory and goes when the process does — so every invocation
     * that wants a connected session establishes its own.
     *
     * <p>Signing in is optional: being connected is a usable state on its own (the prompt reflects it
     * and {@code login} can follow), so a launch that names no user simply lands connected. The password
     * arrives as a supplier so that one that has to be asked for is only asked for once the connection
     * has been made — there is no point prompting for a password to a server that is not there.
     */
    int signIn(String seeds, String username, Supplier<String> password, boolean quiet) {
        this.quiet = quiet;
        try {
            int connected = connect(List.of("connect", seeds));
            if (connected != Cli.EXIT_OK || username == null || username.isBlank()) {
                return connected;
            }
            return login(username, password);
        } finally {
            this.quiet = false;
        }
    }

    /**
     * Where a confirmation of having connected or signed in goes. In a session it is the answer to what
     * was just typed, so it goes to stdout with everything else. Running one command from a script it is
     * not: the command's own output is, and two lines about the connection ahead of it would land in
     * whatever is reading the result. Quiet drops them; the failures are unaffected, since those go to
     * stderr and are the reason the run stopped.
     */
    private void confirm(String line) {
        if (quiet) {
            return;
        }
        PrintWriter out = commandLine.getOut();
        out.println(line);
        out.flush();
    }

    /**
     * Handles one input line. Returns {@code false} when the loop should stop (exit / quit); the status
     * the line produced is left in {@link #lastExitCode()}, which is what a one-shot invocation exits
     * with. Inside the read loop nothing consumes it — a failed line does not end a session.
     */
    boolean dispatch(String line) {
        return dispatchLine(line == null ? "" : line.trim());
    }

    /**
     * Runs one already-split command, as a one-shot launch hands it over. Re-joining the words into a
     * line only to split them again would put the quoting rules between the shell and the verb twice,
     * and the shell has already done that job.
     */
    boolean dispatch(List<String> words) {
        return dispatchWords(words);
    }

    /** Dispatches a scripted command without letting lazy target setup contaminate its stdout. */
    boolean dispatch(List<String> words, boolean quiet) {
        boolean previous = this.quiet;
        this.quiet = quiet;
        try {
            return dispatchWords(words);
        } finally {
            this.quiet = previous;
        }
    }

    private boolean dispatchLine(String trimmed) {
        if (trimmed.isEmpty()) {
            lastExitCode = Cli.EXIT_OK;
            return true;
        }
        if (trimmed.equals("exit") || trimmed.equals("quit")) {
            lastExitCode = Cli.EXIT_OK;
            return false;
        }
        if (trimmed.equals("help")) {
            commandLine.usage(commandLine.getOut());
            commandLine.getOut().flush();
            lastExitCode = Cli.EXIT_OK;
            return true;
        }
        if (trimmed.equals("pwd")) {
            commandLine.getOut().println(workdir.toString());
            commandLine.getOut().flush();
            lastExitCode = Cli.EXIT_OK;
            return true;
        }
        if (trimmed.equals(":ctx")) {
            lastExitCode = context();
            return true;
        }
        // The read shell is matched on the whole line rather than on its first word, because what it names
        // is a place in the data — `views.orders.find({...})` — and a filter written across several words
        // does not survive being split into them.
        DataBrowserCall call = DataBrowserCall.parse(trimmed);
        if (call != null) {
            lastExitCode = dataBrowser(call, trimmed.startsWith("show ") ? "show" : "find");
            return true;
        }
        // A live view is matched on the whole line for the same reason the read shell is: its filter is
        // written the way a read's is, and splitting it into words takes it apart.
        String live = liveVerbOf(trimmed);
        if (live != null) {
            lastExitCode = dataBrowser(
                    DataBrowserCall.parseLive(live, trimmed.substring(live.length())), live);
            return true;
        }
        return dispatchWords(tokenize(trimmed));
    }

    /** The shared tail of both entry points: everything decided by the words rather than the raw line. */
    private boolean dispatchWords(List<String> words) {
        if (words.isEmpty()) {
            lastExitCode = Cli.EXIT_OK;
            return true;
        }
        // the line-shaped builtins are matched here too, so a one-shot `exit` behaves the same way
        if (words.size() == 1 && (words.get(0).equals("exit") || words.get(0).equals("quit"))) {
            lastExitCode = Cli.EXIT_OK;
            return false;
        }
        if (words.size() == 1 && words.get(0).equals("pwd")) {
            commandLine.getOut().println(workdir.toString());
            commandLine.getOut().flush();
            lastExitCode = Cli.EXIT_OK;
            return true;
        }
        if (words.get(0).equals("cd")) {
            lastExitCode = changeDir(words);
            return true;
        }
        if (words.get(0).equals("connect")) {
            namedContext = null;
            lastExitCode = connect(words);
            return true;
        }
        if (words.get(0).equals("disconnect")) {
            lastExitCode = disconnect();
            return true;
        }
        if (words.get(0).equals("login")) {
            lastExitCode = login(words);
            return true;
        }
        if (words.get(0).equals("logout")) {
            lastExitCode = logout();
            return true;
        }
        if (words.get(0).equals("context")
                && words.stream().anyMatch(word -> word.equals("-h") || word.equals("--help")
                        || word.equals("-V") || word.equals("--version"))) {
            lastExitCode = commandLine.execute(words.toArray(new String[0]));
            return true;
        }
        if (words.get(0).equals("context")) {
            lastExitCode = context();
            return true;
        }
        if (words.get(0).equals("auth")
                && words.stream().anyMatch(word -> word.equals("-h") || word.equals("--help")
                        || word.equals("-V") || word.equals("--version"))) {
            lastExitCode = commandLine.execute(words.toArray(new String[0]));
            return true;
        }
        if (words.get(0).equals("auth")) {
            if (!session.isConnected()) {
                int resolved = isLocalOnlyLogout(words)
                        ? resolveNamedContextWithoutConnect(words)
                        : resolveTarget(words);
                if (resolved != Cli.EXIT_OK) {
                    lastExitCode = resolved;
                    return true;
                }
            }
            lastExitCode = auth(words);
            return true;
        }
        // `data-browser <call>` is the same shell reached as a verb, which is how a one-shot invocation
        // gets at it: the words arrive already split by the caller's own shell, so they are rejoined and
        // read as the one line they were typed as.
        if (words.get(0).equals(Cli.DATA_BROWSER_VERB)) {
            lastExitCode = dataBrowserVerb(words);
            return true;
        }
        // A live view reached as a one-shot: the words arrive already split by the caller's own shell,
        // so they are rejoined into the line they were typed as, exactly as the read shell does it.
        if (Cli.LIVE_VIEW_VERBS.contains(words.get(0))) {
            String verb = words.get(0);
            lastExitCode = dataBrowser(
                    DataBrowserCall.parseLive(verb, String.join(" ", words.subList(1, words.size()))), verb);
            return true;
        }
        if (!session.isConnected() && ONLINE_VERBS.contains(words.get(0)) && contextResolver != null) {
            int resolved = resolveTarget(words);
            if (resolved != Cli.EXIT_OK) {
                lastExitCode = resolved;
                return true;
            }
        }
        if (session.isConnected() && ONLINE_VERBS.contains(words.get(0))) {
            lastExitCode = onlineVerb(words);
            return true;
        }
        // the offline path already had a status -- picocli returns one -- and it was being discarded
        lastExitCode = commandLine.execute(withWorkspace(words));
        return true;
    }

    private static boolean isLocalOnlyLogout(List<String> words) {
        return words.equals(List.of("auth", "logout", "--local-only"));
    }

    /** Resolves durable local state for local-only logout without probing or discovering a server. */
    private int resolveNamedContextWithoutConnect(List<String> words) {
        try {
            Optional<ResolvedContext> resolution = contextResolver.resolve(
                    null, explicitContext, workspaceFor(words));
            if (resolution.orElse(null) instanceof ResolvedContext.Named named) {
                namedContext = named;
                return Cli.EXIT_OK;
            }
            Diagnostics.printText(commandLine.getErr(), CliError.CONTEXT_REQUIRED,
                    Map.of("verb", "auth"));
            return Cli.EXIT_VERB_UNAVAILABLE;
        } catch (io.tapstate.core.common.TapstateException error) {
            Diagnostics.printText(commandLine.getErr(), error.code(), error.args());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    /** Resolves and reaches a target only after dispatch has established that the verb is online. */
    private int resolveTarget(List<String> words) {
        String verb = words.get(0);
        try {
            Optional<ResolvedContext> resolution = contextResolver.resolve(null, explicitContext,
                    workspaceFor(words));
            if (resolution.isEmpty()) {
                Diagnostics.printText(commandLine.getErr(), CliError.CONTEXT_REQUIRED, Map.of("verb", verb));
                return Cli.EXIT_VERB_UNAVAILABLE;
            }
            ResolvedContext target = resolution.orElseThrow();
            String seeds = switch (target) {
                case ResolvedContext.Temporary temporary -> temporary.seedExpression();
                case ResolvedContext.Named named -> named.definition().seeds().stream()
                        .map(URI::toString)
                        .collect(Collectors.joining(","));
            };
            int connected = connect(List.of("connect", seeds));
            if (connected == Cli.EXIT_OK) {
                namedContext = target instanceof ResolvedContext.Named named ? named : null;
            }
            return connected;
        } catch (io.tapstate.core.common.TapstateException error) {
            Diagnostics.printText(commandLine.getErr(), error.code(), error.args());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    /** The exact workspace root named on this line, otherwise the session's current root. */
    private Path workspaceFor(List<String> words) {
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if ((word.equals("-w") || word.equals("--workdir")) && i + 1 < words.size()) {
                return Path.of(words.get(i + 1));
            }
            if (word.startsWith("-w=") || word.startsWith("--workdir=")) {
                return Path.of(word.substring(word.indexOf('=') + 1));
            }
        }
        return workdir;
    }

    /**
     * Routes a connected-session verb (apply / get / ls) to the server. Every {@code /api} verb requires a
     * credential, so an unauthenticated session short-circuits with a benign "run login" line rather than
     * provoking a server 401. On a request the landing node cannot answer, {@link #failover} re-lands on
     * another member and the verb is retried once against the new node.
     */
    private int onlineVerb(List<String> words) {
        int resumed = resumeNamedSession();
        if (resumed != Cli.EXIT_OK) {
            return resumed;
        }
        PrintWriter err = commandLine.getErr();
        if (!session.isAuthenticated()) {
            Diagnostics.printText(err, CliError.NOT_AUTHENTICATED, Map.of("verb", words.get(0)));
            return Cli.EXIT_VERB_UNAVAILABLE;
        }
        // `test` and its read-back `test-result` return a structured report that is worth machine-reading, so
        // they accept an `-o` output flag and parse their own options — routed before the positional-only
        // guard the other verbs share.
        if (words.get(0).equals("test")) {
            return testOnline(words);
        }
        if (words.get(0).equals("test-result")) {
            return testResultOnline(words);
        }
        if (words.get(0).equals("discover-schema")) {
            return discoverSchemaOnline(words);
        }
        if (words.get(0).equals("schema")) {
            return schemaOnline(words);
        }
        // `register` uploads a local artifact and returns a structured report worth machine-reading, so it
        // accepts an `-o` output flag and parses its own operand (a local path) — routed before the
        // positional-only guard the other verbs share.
        if (words.get(0).equals("register")) {
            return registerOnline(words);
        }
        // `connectors` lists the online catalog and returns a structured list worth machine-reading, so it
        // accepts an `-o` output flag and takes no operand — routed before the positional-only guard.
        if (words.get(0).equals("connectors")) {
            return connectorsOnline(words);
        }
        if (words.get(0).equals("token")) {
            return tokenOnline(words);
        }
        // `delete` and `apply` each carry a precondition of their own (`--if-match <hash>`), so they parse
        // their own words rather than falling into the positional-only guard below, which would refuse it.
        if (words.get(0).equals("delete")) {
            return deleteOnline(words);
        }
        if (words.get(0).equals("apply")) {
            return applyOnline(words);
        }
        // The two streaming sugars ride the read verbs over the websocket channel: `status --watch` and
        // `logs --follow`. They are the only dash-options a connected verb accepts, and only on their verb.
        if (words.get(0).equals("status") && words.contains("--watch")) {
            return statusWatch(words);
        }
        if (words.get(0).equals("logs") && words.contains("--follow")) {
            return logsFollow(words);
        }
        // The other connected verbs take positional operands only; a dash-option (e.g. `-o json`) is not yet
        // supported and must not be silently misread as an id / kind / path.
        for (int i = 1; i < words.size(); i++) {
            if (words.get(i).startsWith("-")) {
                err.println(words.get(0) + ": options are not supported on a connected verb yet");
                err.flush();
                return Cli.EXIT_USAGE;
            }
        }
        return switch (words.get(0)) {
            case "apply" -> applyOnline(words);
            case "get" -> getOnline(words);
            case "ls" -> lsOnline(words);
            case "start", "stop", "pause", "resume" -> lifecycleOnline(words);
            case "status" -> statusOnline(words);
            case "metrics" -> metricsOnline(words);
            case "snapshot" -> snapshotOnline(words);
            case "logs" -> logsOnline(words);
            default -> throw new IllegalStateException("not an online verb: " + words.get(0));
        };
    }

    /** Restores a named context's cached session after issuer verification and before an API call. */
    private int resumeNamedSession() {
        if (namedContext == null || authService == null) {
            return Cli.EXIT_OK;
        }
        try {
            boolean wasAuthenticated = session.isAuthenticated();
            authService.resume(namedContext).ifPresent(active -> {
                activate(active);
                if (!wasAuthenticated) {
                    confirm("resumed " + active.record().principal() + "@" + namedContext.name());
                }
            });
            return Cli.EXIT_OK;
        } catch (io.tapstate.core.common.TapstateException failure) {
            Diagnostics.printText(commandLine.getErr(), failure.code(), failure.args());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    /** Dispatches machine-token administration while keeping bearer creation a one-time output. */
    private int tokenOnline(List<String> words) {
        if (words.size() < 2) {
            return tokenUsage("missing action (usage: token <create|list|revoke> [ARGS...] [-o text|json|yaml])");
        }
        return switch (words.get(1)) {
            case "create" -> tokenCreate(words);
            case "list" -> tokenList(words);
            case "revoke" -> tokenRevoke(words);
            default -> tokenUsage("unknown action '" + words.get(1)
                    + "' (usage: token <create|list|revoke> [ARGS...] [-o text|json|yaml])");
        };
    }

    private int tokenCreate(List<String> words) {
        String scope = null;
        OutputFormat format = OutputFormat.TEXT;
        for (int i = 2; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals("--scope") && i + 1 < words.size()) {
                scope = words.get(++i).toLowerCase(Locale.ROOT);
            } else if ((word.equals("-o") || word.equals("--output")) && i + 1 < words.size()) {
                format = outputFormat(words.get(++i));
                if (format == null) {
                    return tokenUsage("unknown output format '" + words.get(i)
                            + "' (expected text|json|yaml)");
                }
            } else {
                return tokenUsage("unknown or incomplete option '" + word + "'");
            }
        }
        if (scope == null || !List.of("read", "write", "admin").contains(scope)) {
            return tokenUsage("--scope must be read, write, or admin");
        }
        final String requestedScope = scope;
        TokenCreateOutcome outcome = withFailover(() -> controlPlane.tokenCreate(
                session.landingNode(), session.credential(), requestedScope),
                o -> o instanceof TokenCreateOutcome.Unreachable);
        return switch (outcome) {
            case TokenCreateOutcome.Issued issued -> {
                renderCreatedToken(issued.token(), format);
                yield Cli.EXIT_OK;
            }
            case TokenCreateOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case TokenCreateOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    private int tokenList(List<String> words) {
        OutputFormat format = parseTokenFormat(words, 2);
        if (format == null) {
            return Cli.EXIT_USAGE;
        }
        TokenListOutcome outcome = withFailover(() -> controlPlane.tokenList(
                session.landingNode(), session.credential()),
                o -> o instanceof TokenListOutcome.Unreachable);
        return switch (outcome) {
            case TokenListOutcome.Listed listed -> {
                renderTokens(listed.tokens(), format);
                yield Cli.EXIT_OK;
            }
            case TokenListOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case TokenListOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    private int tokenRevoke(List<String> words) {
        if (words.size() < 3 || words.get(2).isBlank() || words.get(2).startsWith("-")) {
            return tokenUsage("missing token id (usage: token revoke <id> [-o text|json|yaml])");
        }
        String tokenId = words.get(2);
        OutputFormat format = parseTokenFormat(words, 3);
        if (format == null) {
            return Cli.EXIT_USAGE;
        }
        TokenRevokeOutcome outcome = withFailover(() -> controlPlane.tokenRevoke(
                session.landingNode(), session.credential(), tokenId),
                o -> o instanceof TokenRevokeOutcome.Unreachable);
        return switch (outcome) {
            case TokenRevokeOutcome.Revoked ignored -> {
                renderRevokedToken(tokenId, format);
                yield Cli.EXIT_OK;
            }
            case TokenRevokeOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case TokenRevokeOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    private OutputFormat parseTokenFormat(List<String> words, int start) {
        OutputFormat format = OutputFormat.TEXT;
        for (int i = start; i < words.size(); i++) {
            String word = words.get(i);
            if ((word.equals("-o") || word.equals("--output")) && i + 1 < words.size()) {
                format = outputFormat(words.get(++i));
                if (format == null) {
                    tokenUsage("unknown output format '" + words.get(i)
                            + "' (expected text|json|yaml)");
                    return null;
                }
            } else {
                tokenUsage("unknown or incomplete option '" + word + "'");
                return null;
            }
        }
        return format;
    }

    private int tokenUsage(String message) {
        PrintWriter err = commandLine.getErr();
        err.println("token: " + message);
        err.flush();
        return Cli.EXIT_USAGE;
    }

    private void renderCreatedToken(RemoteCreatedToken token, OutputFormat format) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("tokenId", token.tokenId());
        document.put("scope", token.scope());
        document.put("token", token.token());
        document.put("createdAt", token.createdAt());
        renderTokenDocument(document, format, "created " + token.tokenId() + " " + token.scope()
                + System.lineSeparator() + "token " + token.token());
    }

    private void renderTokens(List<RemoteToken> tokens, OutputFormat format) {
        List<Object> rows = tokens.stream().map(Repl::tokenDocument).map(value -> (Object) value).toList();
        if (format == OutputFormat.TEXT) {
            PrintWriter out = commandLine.getOut();
            if (tokens.isEmpty()) {
                out.println("no machine tokens");
            }
            for (RemoteToken token : tokens) {
                out.println(token.tokenId() + "  " + token.scope() + "  "
                        + (token.revoked() ? "revoked" : "active") + "  " + token.createdAt());
            }
            out.flush();
            return;
        }
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("tokens", rows);
        renderTokenDocument(document, format, "");
    }

    private static Map<String, Object> tokenDocument(RemoteToken token) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("tokenId", token.tokenId());
        row.put("scope", token.scope());
        row.put("revoked", token.revoked());
        row.put("createdAt", token.createdAt());
        return row;
    }

    private void renderRevokedToken(String tokenId, OutputFormat format) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("tokenId", tokenId);
        document.put("revoked", true);
        renderTokenDocument(document, format, "revoked " + tokenId);
    }

    private void renderTokenDocument(Map<String, Object> document, OutputFormat format, String text) {
        PrintWriter out = commandLine.getOut();
        out.println(switch (format) {
            case TEXT -> text;
            case JSON -> JsonOut.write(document);
            case YAML -> YamlOut.write(document);
        });
        out.flush();
    }

    /**
     * {@code <verb> <pipeline-id>} — issues a pipeline lifecycle verb (start / stop / pause / resume) on the
     * server and prints the pipeline's new desired state ({@code <id>  <state>}). A missing id is a benign
     * usage line; a coded refusal — an unknown pipeline, a transition the state machine forbids, or a
     * start/resume at a stale revision — renders its code and message. On a request the landing node cannot
     * answer, {@link #withFailover} re-lands and retries once. There is no {@code rewind} verb: a re-dig is
     * the explicit two-step {@code stop} then {@code start}.
     */
    // ---- the read shell ------------------------------------------------------------------------------

    /**
     * {@code data-browser <call>} — the read shell reached as a verb, for a one-shot invocation. The words
     * arrive already split by the caller's own shell, so they are rejoined into the line they were typed
     * as; inside a session the same calls are typed bare at the prompt.
     */
    private int dataBrowserVerb(List<String> words) {
        String line = String.join(" ", words.subList(1, words.size())).trim();
        if (line.isEmpty()) {
            return dataBrowserUsage("missing call");
        }
        DataBrowserCall call = DataBrowserCall.parse(line);
        if (call == null) {
            return dataBrowserUsage("`" + line + "` is not a read");
        }
        return dataBrowser(call, Cli.DATA_BROWSER_VERB);
    }

    private int dataBrowserUsage(String problem) {
        PrintWriter err = commandLine.getErr();
        err.println(Cli.DATA_BROWSER_VERB + ": " + problem + " (usage: " + Cli.DATA_BROWSER_VERB
                + " \"show collections [<source>]\" | \"<source>.<collection>.find(...)\""
                + " | \"<source>.<collection>.stats()\")");
        err.flush();
        return Cli.EXIT_USAGE;
    }

    /**
     * Runs one parsed read. The connection checks come first and in this order because they are different
     * answers: offline, the shell has nothing to read from; connected but signed out, it has somewhere to
     * ask and no right to.
     */
    private int dataBrowser(DataBrowserCall call, String verb) {
        PrintWriter err = commandLine.getErr();
        if (call instanceof DataBrowserCall.Malformed malformed) {
            err.println(verb + ": " + malformed.reason());
            err.flush();
            return Cli.EXIT_USAGE;
        }
        if (!session.isConnected() && contextResolver != null) {
            int resolved = resolveTarget(List.of(verb));
            if (resolved != Cli.EXIT_OK) {
                return resolved;
            }
        }
        if (!session.isConnected()) {
            Diagnostics.printText(err, CliError.NOT_CONNECTED, Map.of("verb", verb));
            return Cli.EXIT_VERB_UNAVAILABLE;
        }
        int resumed = resumeNamedSession();
        if (resumed != Cli.EXIT_OK) {
            return resumed;
        }
        if (!session.isAuthenticated()) {
            Diagnostics.printText(err, CliError.NOT_AUTHENTICATED, Map.of("verb", verb));
            return Cli.EXIT_VERB_UNAVAILABLE;
        }
        return switch (call) {
            case DataBrowserCall.Collections listing -> browseCollections(listing);
            case DataBrowserCall.Stats stats -> browseStats(stats);
            case DataBrowserCall.Find find -> browseFind(find);
            case DataBrowserCall.Live live -> live.verb().equals("tail")
                    ? tailLive(live) : watchLive(live);
            case DataBrowserCall.Malformed ignored -> Cli.EXIT_USAGE;    // handled above
        };
    }

    /**
     * {@code watch <source>.<collection> [<filter>]} — one row, redrawn where it stands, until Ctrl-C.
     *
     * <p>It refuses outright when its output is not a terminal, rather than degrading. Redrawing in
     * place is cursor movement, and cursor movement down a pipe is not a worse view — it is control
     * bytes in the middle of what the reader piped it into. The refusal names both alternatives,
     * because a reader who reached for this verb wants one of them: a stream that pipes, or a look that
     * ends.
     */
    private int watchLive(DataBrowserCall.Live live) {
        PrintWriter out = commandLine.getOut();
        if (!terminal.getAsBoolean()) {
            Diagnostics.printText(commandLine.getErr(), CliError.WATCH_NEEDS_A_TERMINAL, Map.of());
            return Cli.EXIT_VERB_UNAVAILABLE;
        }
        String namespace = live.sourceId() + "." + live.collection();
        WatchView view = new WatchView(namespace, screenWidth);
        streamCancelled = false;
        int drawn = 0;
        while (!isStreamCancelled()) {
            WatchPoll poll = pollOnce(live);
            if (poll == null) {
                return Cli.EXIT_VERB_UNAVAILABLE;   // a refusal that will not change; already reported
            }
            List<String> lines = view.onPoll(poll, Instant.now());
            if (!lines.isEmpty()) {
                out.print(WatchRenderer.redrawOver(drawn));
                lines.forEach(out::println);
                out.flush();
                drawn = lines.size();
            }
            if (!sleepUnlessCancelled(WATCH_INTERVAL)) {
                break;
            }
        }
        return Cli.EXIT_OK;
    }

    /**
     * {@code tail <source>.<collection> [<filter>]} — every change to the collection, appended, until
     * Ctrl-C. Unlike the in-place view it needs no terminal: it only ever appends, so it pipes.
     *
     * <p>The note under the header is not politeness. Read as a list of everything that happened, an
     * appended stream over-promises: what reaches the store is the settled version of a row, and rapid
     * changes are folded together before they are ever written. That folding happens upstream of the
     * store, so this is not the transport being lossy and a better transport would not change it — the
     * only honest fix is to say so.
     *
     * <p>What each event shows is whatever the connector supplied for it and nothing more. Working out
     * more — which field moved, what it held before — would mean keeping a history here and showing it
     * as the store's, which the reader could not tell apart from the store's own.
     */
    private int tailLive(DataBrowserCall.Live live) {
        PrintWriter out = commandLine.getOut();
        String namespace = live.sourceId() + "." + live.collection();
        out.println("following " + namespace + " · whole collection · streaming changes");
        out.println("note: shows changes as written to the store — not every intermediate version");
        out.flush();
        streamCancelled = false;
        String refusal = controlPlane.tail(session.landingNode(), session.credential(),
                live.sourceId(), live.collection(), live.filter(),
                change -> {
                    TailRenderer.lines(change, screenWidth.getAsInt()).forEach(out::println);
                    out.flush();
                },
                this::isStreamCancelled);
        if (refusal != null) {
            // A follow that ended by itself arrives as a code and nothing else - the close frame has
            // room for one. Rendering it from the bundled catalog is what turns "the screen stopped
            // updating" into a sentence; handed on as a bare code with no message, the reader is told
            // that something has a name and not what happened.
            return renderRejection(refusal, MessageCatalog.bundled().render(refusal, Map.of()).message());
        }
        // A stream ends because the user stopped it, which is the way it is meant to end.
        return Cli.EXIT_OK;
    }

    /**
     * One poll of the watched row: the first row a bounded read of one returns. Null when the read was
     * refused in a way repeating cannot fix — that has already been rendered and the view stops.
     *
     * <p>A busy connector is not such a refusal. This is a background read that runs again in a second,
     * and its turn was taken by an interactive one somebody is waiting on; skipping the frame costs
     * nothing, while dying because a frame was missed costs the whole view.
     */
    private WatchPoll pollOnce(DataBrowserCall.Live live) {
        DataBrowserOutcome.Find outcome = withFailover(() ->
                controlPlane.find(session.landingNode(), session.credential(), live.sourceId(),
                        live.collection(), live.filter(), null, 1),
                o -> o instanceof DataBrowserOutcome.Find.Unreachable);
        return switch (outcome) {
            case DataBrowserOutcome.Find.Read read -> read.rows().isEmpty()
                    ? new WatchPoll.NoRow()
                    : new WatchPoll.Row(read.rows().get(0), read.approximateTotal());
            case DataBrowserOutcome.Find.Rejected rejected -> {
                if (BUSY_CODES.contains(rejected.code())) {
                    yield new WatchPoll.Skipped("busy");
                }
                renderRejection(rejected.code(), rejected.message());
                yield null;
            }
            case DataBrowserOutcome.Find.Unreachable ignored -> new WatchPoll.Skipped("unreachable");
        };
    }

    /** Sleeps the poll interval in short steps; false the moment the user interrupts. */
    private boolean sleepUnlessCancelled(Duration interval) {
        long remaining = interval.toMillis();
        try {
            while (remaining > 0) {
                if (isStreamCancelled()) {
                    return false;
                }
                long step = Math.min(remaining, CANCEL_POLL.toMillis());
                Thread.sleep(step);
                remaining -= step;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return !isStreamCancelled();
    }

    /**
     * {@code show collections [<source>]}. With no source it lists every declared one, because that is the
     * question a reader actually opens with — not "what is in this source" but "what can I read at all",
     * and the answer is only useful as the full {@code <source>.<collection>} names they will type next.
     *
     * <p>A source that cannot be listed is reported in place and the rest are still listed. One
     * unreachable database is not a reason to answer nothing about the others.
     */
    private int browseCollections(DataBrowserCall.Collections listing) {
        List<String> sources;
        if (listing.sourceId() != null) {
            sources = List.of(listing.sourceId());
        } else {
            ListOutcome declared = withFailover(() ->
                    controlPlane.list(session.landingNode(), session.credential(), "source"),
                    o -> o instanceof ListOutcome.Unreachable);
            switch (declared) {
                case ListOutcome.Listed listed ->
                        sources = listed.artifacts().stream().map(RemoteArtifact::id).toList();
                case ListOutcome.Rejected rejected -> {
                    return renderRejection(rejected.code(), rejected.message());
                }
                case ListOutcome.Unreachable ignored -> {
                    return reportRequestFailed();
                }
            }
        }
        PrintWriter out = commandLine.getOut();
        if (sources.isEmpty()) {
            out.println("no sources declared");
            out.flush();
            return Cli.EXIT_OK;
        }
        int listed = 0;
        int failed = 0;
        for (String sourceId : sources) {
            DataBrowserOutcome.Collections outcome = withFailover(() ->
                    controlPlane.collections(session.landingNode(), session.credential(), sourceId),
                    o -> o instanceof DataBrowserOutcome.Collections.Unreachable);
            switch (outcome) {
                case DataBrowserOutcome.Collections.Listed found -> {
                    for (String collection : found.collections()) {
                        out.println(sourceId + "." + collection);
                    }
                    listed += found.collections().size();
                }
                case DataBrowserOutcome.Collections.Rejected rejected -> {
                    failed++;
                    renderRejection(rejected.code(), rejected.message());
                }
                case DataBrowserOutcome.Collections.Unreachable ignored -> {
                    failed++;
                    reportRequestFailed();
                }
            }
        }
        // What the list is, said once. It is the collections the connected database actually holds, not
        // the ones the workspace declared — those are different sets, and a source referenced purely as a
        // connection supplier declares none at all.
        out.println();
        out.println(listed + (listed == 1 ? " collection" : " collections")
                + " — what each source's database holds, not what the workspace declares");
        out.flush();
        return failed > 0 ? Cli.EXIT_DIAGNOSTIC : Cli.EXIT_OK;
    }

    /**
     * {@code <source>.<collection>.stats()} — the row count and average row size the connector reports.
     * Both are read off the store's own metadata rather than counted, so the count is a point-in-time
     * estimate that drifts; the rendering says so rather than presenting it as a total.
     */
    private int browseStats(DataBrowserCall.Stats stats) {
        DataBrowserOutcome.Stats outcome = withFailover(() ->
                controlPlane.stats(session.landingNode(), session.credential(),
                        stats.sourceId(), stats.collection()),
                o -> o instanceof DataBrowserOutcome.Stats.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case DataBrowserOutcome.Stats.Reported reported -> {
                out.println(stats.sourceId() + "." + stats.collection());
                out.println("  rows      " + (reported.numOfRows() == null
                        ? "not reported"
                        : "~" + reported.numOfRows() + "  (estimated from store metadata, not counted)"));
                out.println("  avg row   " + (reported.avgObjSize() == null
                        ? "not reported"
                        : reported.avgObjSize() + " bytes"));
                out.flush();
                yield Cli.EXIT_OK;
            }
            case DataBrowserOutcome.Stats.Rejected rejected ->
                    renderRejection(rejected.code(), rejected.message());
            case DataBrowserOutcome.Stats.Unreachable ignored -> reportRequestFailed();
        };
    }

    /** {@code <source>.<collection>.find(...)} — a preview of the rows, and a footer saying what it is. */
    private int browseFind(DataBrowserCall.Find find) {
        DataBrowserOutcome.Find outcome = withFailover(() ->
                controlPlane.find(session.landingNode(), session.credential(), find.sourceId(),
                        find.collection(), find.filter(), find.sort(), find.limit()),
                o -> o instanceof DataBrowserOutcome.Find.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case DataBrowserOutcome.Find.Read read -> {
                // Formatted rather than flattened: a row with anything embedded in it is complete on
                // one line and unreadable on it, because every leaf the reader is looking for sits
                // between two others with nothing but punctuation to separate them.
                read.rows().forEach(row -> out.println(JsonOut.write(row)));
                if (read.rows().isEmpty()) {
                    out.println("no rows matched");
                }
                out.println(previewFooter(find, read));
                out.flush();
                yield Cli.EXIT_OK;
            }
            case DataBrowserOutcome.Find.Rejected rejected ->
                    renderRejection(rejected.code(), rejected.message());
            case DataBrowserOutcome.Find.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * The line under a preview, and the only thing that keeps it from being read as the whole collection.
     * A read is one-shot, so the rows carry nothing else that separates a preview of ten from a
     * collection of ten — no continuation token whose presence would hint at it.
     *
     * <p>It states three things the reader cannot see from the rows, each of which is false if left
     * unsaid: how many the collection holds and that the number is approximate — offered only for an
     * unfiltered read, since counting a filtered one is a full scan; that an unordered read is in the
     * database's own order, which is <em>not</em> stable between two identical reads and is not the
     * newest; and that more rows remain.
     */
    private static String previewFooter(DataBrowserCall.Find find, DataBrowserOutcome.Find.Read read) {
        StringBuilder footer = new StringBuilder("showing ").append(read.rows().size());
        if (read.approximateTotal() != null) {
            footer.append(" of ~").append(read.approximateTotal());
        }
        footer.append(find.sort() == null
                ? " · natural order — not stable, and not the newest"
                : " · ordered by `" + find.sort().field() + "` " + find.sort().dir());
        if (read.moreAvailable()) {
            footer.append(" · more rows remain");
        }
        return footer.toString();
    }

    private int lifecycleOnline(List<String> words) {
        String verb = words.get(0);
        PrintWriter err = commandLine.getErr();
        if (words.size() < 2 || words.get(1).isBlank()) {
            err.println(verb + ": missing operand (usage: " + verb + " <pipeline-id>)");
            err.flush();
            return Cli.EXIT_USAGE;
        }
        String id = words.get(1);
        LifecycleOutcome outcome = withFailover(() ->
                controlPlane.lifecycle(session.landingNode(), session.credential(), id, verb),
                o -> o instanceof LifecycleOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case LifecycleOutcome.Accepted accepted -> {
                out.println(accepted.pipelineId() + "  " + accepted.targetState().toLowerCase(Locale.ROOT));
                out.flush();
                yield Cli.EXIT_OK;
            }
            case LifecycleOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case LifecycleOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code apply [path]} — reads every {@code *.tap.yml} under the path (default: the session workspace)
     * as raw drafts and applies them as one batch via the server, which re-parses and re-validates. Prints
     * one line per artifact naming how it changed (created / updated / unchanged). An empty or unreadable
     * path is a benign usage line; a coded server refusal (a validation failure is a {@code dsl.*} code)
     * renders its code and message.
     */
    private int applyOnline(List<String> words) {
        PrintWriter err = commandLine.getErr();
        String ifMatch = null;
        String operand = null;
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals("--if-match")) {
                if (i + 1 >= words.size()) {
                    return applyUsage("--if-match needs a hash");
                }
                ifMatch = words.get(++i);
            } else if (word.startsWith("-")) {
                return applyUsage("unknown option '" + word + "'");
            } else if (operand == null) {
                operand = word;
            } else {
                return applyUsage("unexpected operand '" + word + "'");
            }
        }
        Path target = operand != null ? workdir.resolve(operand).normalize() : workdir;
        List<LocalDraft> drafts;
        try {
            drafts = collectDrafts(target);
        } catch (IOException e) {
            err.println("apply: cannot read " + target + ": " + e.getMessage());
            err.flush();
            return Cli.EXIT_USAGE;
        } catch (DslException e) {
            // an unresolved reference is refused here, before anything is sent: the server would only see
            // a literal ${...} and take it for a real value
            return renderLocalRefusal(e);
        }
        if (drafts.isEmpty()) {
            err.println("apply: no *.tap.yml artifacts found in " + target);
            err.flush();
            return Cli.EXIT_USAGE;
        }
        if (ifMatch != null) {
            // One hash names one version. Over a batch there is no resource it could be describing, and
            // attaching it to one of them would leave the rest applied with no check at all — an edit
            // landing on resources the caller was not thinking about. Refused before anything is sent.
            if (drafts.size() != 1) {
                MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(
                        CliError.IF_MATCH_NEEDS_ONE_RESOURCE, Map.of("count", drafts.size()));
                err.println(Ansi.AUTO.string("@|bold,red error:|@") + " "
                        + CliError.IF_MATCH_NEEDS_ONE_RESOURCE.code());
                err.println("  " + rendered.message());
                if (rendered.solution() != null) {
                    err.println("  " + rendered.solution());
                }
                err.flush();
                return Cli.EXIT_DIAGNOSTIC;
            }
            LocalDraft only = drafts.get(0);
            drafts = List.of(new LocalDraft(only.source(), only.content(), ifMatch));
        }
        List<LocalDraft> submitted = drafts;
        ApplyOutcome outcome = withFailover(() ->
                controlPlane.apply(session.landingNode(), session.credential(), submitted),
                o -> o instanceof ApplyOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case ApplyOutcome.Applied applied -> {
                for (ApplyOutcome.Item item : applied.items()) {
                    out.println(item.change().toLowerCase(Locale.ROOT) + "  " + item.kind() + "  " + item.id());
                }
                out.flush();
                renderApplyWarnings(applied.warnings());
                yield Cli.EXIT_OK;
            }
            case ApplyOutcome.Rejected rejected ->
                    renderRejection(rejected.code(), rejected.message(), rejected.params());
            case ApplyOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code get <id>} — reads one artifact from the server (server-as-truth) and prints its canonical
     * form. A missing operand is a benign usage line; an id that resolves to nothing is a benign
     * "not found" line; a coded refusal renders its code and message.
     */
    private int getOnline(List<String> words) {
        PrintWriter err = commandLine.getErr();
        if (words.size() < 2 || words.get(1).isBlank()) {
            err.println("get: missing operand (usage: get <id>)");
            err.flush();
            return Cli.EXIT_USAGE;
        }
        String id = words.get(1);
        GetOutcome outcome = withFailover(() ->
                controlPlane.get(session.landingNode(), session.credential(), id),
                o -> o instanceof GetOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case GetOutcome.Found found -> {
                out.println(found.artifact().canonicalForm().stripTrailing());
                out.flush();
                yield Cli.EXIT_OK;
            }
            case GetOutcome.Absent ignored -> {
                err.println("not found: " + id);
                err.flush();
                // asking for an artifact that is not there did not do what was asked, so it is not a
                // success -- a script reading only the status must not take an empty result for one
                yield Cli.EXIT_DIAGNOSTIC;
            }
            case GetOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case GetOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code delete <id> [--if-match <hash>]} — removes one stored artifact of any kind, for good.
     *
     * <p>Without {@code --if-match} the verb reads the artifact first and removes the version it just
     * read. That is not the same as removing unconditionally: if the resource changes in between, the
     * server refuses rather than discarding an edit nobody here has seen. Supplying the flag pins a
     * version the caller already holds and skips the read.
     *
     * <p>{@code -o json|yaml} reports the removal as a document rather than a sentence, including the
     * precondition that was actually used — which the caller has no other way to learn when the verb
     * chose it. A refusal on those surfaces keeps the refusal's parameters, so a script can act on the
     * same facts the text surface turns into a next step. Every other way the verb can fail answers as
     * a document too, the implicit read's failures included: a machine surface that covers only some of
     * them is one nothing can parse against.
     *
     * <p>The removal itself is sent once, to the landing node, and is never re-sent elsewhere. Reads
     * here may be retried; this one may not, because a replay cannot tell its own first attempt's
     * success apart from the resource never having been there.
     */
    private int deleteOnline(List<String> words) {
        PrintWriter err = commandLine.getErr();
        String id = null;
        String ifMatch = null;
        OutputFormat format = OutputFormat.TEXT;
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals("--if-match")) {
                if (i + 1 >= words.size()) {
                    return deleteUsage("--if-match needs a hash");
                }
                ifMatch = words.get(++i);
            } else if (word.equals("-o") || word.equals("--output")) {
                if (i + 1 >= words.size()) {
                    return deleteUsage("-o needs a format");
                }
                OutputFormat chosen = outputFormat(words.get(++i));
                if (chosen == null) {
                    return deleteUsage("unknown output format '" + words.get(i) + "'");
                }
                format = chosen;
            } else if (word.startsWith("-")) {
                return deleteUsage("unknown option '" + word + "'");
            } else if (id == null) {
                id = word;
            } else {
                return deleteUsage("unexpected operand '" + word + "'");
            }
        }
        if (id == null || id.isBlank()) {
            return deleteUsage("missing operand");
        }
        final String target = id;

        String kind = null;
        if (ifMatch == null) {
            GetOutcome read = withFailover(() ->
                    controlPlane.get(session.landingNode(), session.credential(), target),
                    o -> o instanceof GetOutcome.Unreachable);
            switch (read) {
                case GetOutcome.Found found -> {
                    ifMatch = CanonicalHash.of(found.artifact().canonicalForm());
                    kind = found.artifact().kind();
                }
                case GetOutcome.Absent ignored -> {
                    if (format != OutputFormat.TEXT) {
                        return renderDeleteErrorDocument(format, "artifact.not-found",
                                "no artifact with id '" + target + "' is stored");
                    }
                    err.println("not found: " + target);
                    err.flush();
                    return Cli.EXIT_DIAGNOSTIC;
                }
                case GetOutcome.Rejected rejected -> {
                    if (format != OutputFormat.TEXT) {
                        return renderDeleteErrorDocument(format, rejected.code(), rejected.message());
                    }
                    return renderRejection(rejected.code(), rejected.message());
                }
                case GetOutcome.Unreachable ignored -> {
                    if (format != OutputFormat.TEXT) {
                        return renderDeleteErrorDocument(format, null, "the server is unreachable");
                    }
                    return reportRequestFailed();
                }
            }
        }

        String hash = ifMatch;
        String removedKind = kind;
        OutputFormat chosenFormat = format;
        // The removal is sent once and never re-sent. Failing over replays it on another member, and a
        // replay that follows a first attempt which landed but whose answer was lost comes back
        // `artifact.not-found` — reporting the removal as failed at the exact moment it succeeded, and
        // taking a scripted teardown down with it on the non-zero exit. A read may be retried freely;
        // this is not one.
        DeleteOutcome outcome =
                controlPlane.delete(session.landingNode(), session.credential(), target, hash);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case DeleteOutcome.Removed removed -> {
                if (chosenFormat == OutputFormat.TEXT) {
                    out.println("deleted " + (removedKind == null ? "" : removedKind + "  ") + removed.id());
                } else {
                    Map<String, Object> document = new LinkedHashMap<>();
                    document.put("id", removed.id());
                    putIfPresent(document, "kind", removedKind);
                    document.put("removed", true);
                    document.put("expectedContentHash", hash);
                    out.println(chosenFormat == OutputFormat.JSON
                            ? JsonOut.write(document) : YamlOut.write(document));
                }
                out.flush();
                yield Cli.EXIT_OK;
            }
            case DeleteOutcome.Rejected rejected -> renderDeleteRefusal(rejected, chosenFormat, target);
            case DeleteOutcome.Unreachable ignored -> reportUnresolvedRemoval(target, chosenFormat);
        };
    }

    /**
     * Reports a removal that got no answer. Nothing was retried, so whether it was applied is genuinely
     * unknown, and the report says so rather than picking one of the two and sounding certain: a reply
     * that never arrived is not evidence the request never landed. Reading the artifact settles it,
     * which is why the next step is named here.
     */
    private int reportUnresolvedRemoval(String target, OutputFormat format) {
        String message = "no answer from " + hostPort(session.landingNode())
                + "; the removal of '" + target + "' may or may not have been applied";
        if (format != OutputFormat.TEXT) {
            return renderDeleteErrorDocument(format, null, message);
        }
        PrintWriter err = commandLine.getErr();
        err.println("request failed: " + message);
        err.println("  run `get " + target + "` to find out whether it is gone.");
        err.flush();
        return Cli.EXIT_DIAGNOSTIC;
    }

    /**
     * Writes one delete failure as a machine document on the surface the caller asked for. Every way
     * this verb can fail answers in the same shape — the implicit pre-read's failures included, since
     * that read is an implementation detail of the verb rather than a separate command the caller
     * chose. A {@code -o json} contract that holds for the refusals and not for the rest is one a
     * script cannot rely on at all.
     */
    private int renderDeleteErrorDocument(OutputFormat format, String code, String message) {
        PrintWriter out = commandLine.getOut();
        Map<String, Object> document = errorDocument(code, message);
        out.println(format == OutputFormat.JSON ? JsonOut.write(document) : YamlOut.write(document));
        out.flush();
        return Cli.EXIT_DIAGNOSTIC;
    }

    private int applyUsage(String reason) {
        PrintWriter err = commandLine.getErr();
        err.println("apply: " + reason + " (usage: apply [<path>] [--if-match <hash>])");
        err.flush();
        return Cli.EXIT_USAGE;
    }

    private int deleteUsage(String reason) {
        PrintWriter err = commandLine.getErr();
        err.println("delete: " + reason
                + " (usage: delete <id> [--if-match <hash>] [-o text|json|yaml])");
        err.flush();
        return Cli.EXIT_USAGE;
    }

    /**
     * Renders a refused removal, following the coded message with the next step the refusal's own
     * parameters name. Both grounds are acted on, not merely reported: what is still referencing the
     * resource, and what state the pipeline is really in — neither of which the caller can be expected
     * to work out from the code alone, and neither of which this verb does anything about on its own.
     */
    private int renderDeleteRefusal(DeleteOutcome.Rejected rejected, OutputFormat format, String target) {
        if (format != OutputFormat.TEXT) {
            // The machine surfaces carry the parameters too, not just the code and message. They are what
            // the text surface below turns into the next step, so dropping them would leave a script with
            // strictly less to act on than a person reading the same refusal.
            Map<String, Object> error = errorObject(rejected.code(), rejected.message());
            if (!rejected.params().isEmpty()) {
                error.put("params", new LinkedHashMap<>(rejected.params()));
            }
            Map<String, Object> document = new LinkedHashMap<>();
            document.put("error", error);
            PrintWriter out = commandLine.getOut();
            out.println(format == OutputFormat.JSON ? JsonOut.write(document) : YamlOut.write(document));
            out.flush();
            return Cli.EXIT_DIAGNOSTIC;
        }
        int status = renderRejection(rejected.code(), rejected.message(), rejected.params());
        PrintWriter err = commandLine.getErr();
        switch (rejected.code()) {
            case "artifact.in-use" -> {
                Object referrers = rejected.params().get("referrers");
                if (referrers != null) {
                    err.println("  still referenced by: " + renderReferrers(referrers));
                    err.println("  remove those first, or keep this resource.");
                }
            }
            case "artifact.pipeline-not-stopped" -> {
                Object actual = rejected.params().get("actual");
                Object desired = rejected.params().get("desired");
                if (actual != null || desired != null) {
                    err.println("  pipeline state: actual=" + actual + ", desired=" + desired);
                    // The id comes from what was typed, not from the refusal: this line is a command
                    // meant to be pasted, and a refusal that names no id would otherwise render one
                    // that cannot run.
                    err.println("  run `stop " + rejected.params().getOrDefault("id", target)
                            + "` and wait for it to settle.");
                }
            }
            case "artifact.version-conflict" ->
                    err.println("  it changed since you read it; read it again and redo the removal.");
            case "artifact.reclaim-incomplete" -> {
                // Not a refusal: the resource is gone. Saying "retry" here — the next step every other
                // branch implies — sends the operator at a removal that can now only answer not-found,
                // and the residue that does need clearing goes unmentioned.
                err.println("  the removal stands: '" + target + "' is gone. Do not retry it.");
                Object residue = rejected.params().get("residue");
                if (residue != null) {
                    err.println("  left behind, clear by hand: " + renderReferrers(residue));
                }
                if ("pipeline-live".equals(String.valueOf(rejected.params().get("reason")))) {
                    // Nothing was cleared here on purpose, and clearing it by hand while the job runs
                    // would discard the fencing epoch that keeps it from colliding with a later
                    // pipeline under the same id. Stopping comes first.
                    err.println("  it was started again while the removal ran: `stop " + target
                            + "` first, or its job keeps running with no artifact.");
                }
            }
            default -> {
                // Every other refusal is fully said by its own rendered message.
            }
        }
        err.flush();
        return status;
    }

    private static String renderReferrers(Object referrers) {
        if (referrers instanceof List<?> rows) {
            return rows.stream().map(String::valueOf).collect(Collectors.joining(", "));
        }
        return String.valueOf(referrers);
    }

    /**
     * {@code ls [kind]} — lists the artifacts the server holds, optionally filtered by kind (the connected
     * counterpart of the offline workspace browser). Prints {@code kind  id} per artifact, or a benign
     * "no resources" line when the store is empty; a coded refusal renders its code and message.
     */
    private int lsOnline(List<String> words) {
        String kind = words.size() > 1 ? words.get(1) : null;
        ListOutcome outcome = withFailover(() ->
                controlPlane.list(session.landingNode(), session.credential(), kind),
                o -> o instanceof ListOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case ListOutcome.Listed listed -> {
                if (listed.artifacts().isEmpty()) {
                    out.println("no resources");
                } else {
                    for (RemoteArtifact artifact : listed.artifacts()) {
                        out.println(artifact.kind() + "  " + artifact.id());
                    }
                }
                out.flush();
                yield Cli.EXIT_OK;
            }
            case ListOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case ListOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code test <id> [-o text|json|yaml]} — tests a stored connection. It reads the connection from the
     * server first (server-as-truth), parses the connector and connection config it holds, then posts the
     * connection test and renders the report the connector returned. A missing operand or an unknown option
     * is a benign usage line; an id that resolves to nothing is a benign "not found"; an id that is not a
     * source connection is a benign "not a testable connection"; a coded refusal renders its code and
     * message. A failed connection is still a rendered report (the test ran), not an error.
     */
    private int testOnline(List<String> words) {
        IdAndFormat parsed = parseIdAndFormat("test", words);
        if (parsed == null) {
            return Cli.EXIT_USAGE;
        }
        SourceResource source = fetchSourceConnection("test", parsed.id(), "testable");
        if (source == null) {
            return Cli.EXIT_DIAGNOSTIC;
        }

        final String connectionId = parsed.id();
        OutputFormat chosen = parsed.format();
        ConnectionTestOutcome outcome = withFailover(() -> controlPlane.test(
                session.landingNode(), session.credential(), connectionId, source.connector(), source.config()),
                o -> o instanceof ConnectionTestOutcome.Unreachable);
        return switch (outcome) {
            case ConnectionTestOutcome.Tested tested -> {
                renderReport(tested.report(), chosen);
                yield reportStatus(tested.report());
            }
            case ConnectionTestOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case ConnectionTestOutcome.TimedOut ignored -> reportRequestTimedOut();
            case ConnectionTestOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * Reads the stored connection a probing verb targets (server-as-truth, so the probe runs against
     * exactly what is stored) and parses it to a source connection, or reports why it cannot be probed
     * and returns {@code null}: a benign "not found" for a missing id, a benign "not a {adjective}
     * connection" for a non-source kind (using the reliable stored kind, without parsing a body that is
     * not a connection at all), and a benign "cannot read" for a stored body that no longer parses to a
     * source — a diagnosable state, not a crash.
     */
    private SourceResource fetchSourceConnection(String verb, String connectionId, String adjective) {
        PrintWriter err = commandLine.getErr();
        GetOutcome got = withFailover(() ->
                controlPlane.get(session.landingNode(), session.credential(), connectionId),
                o -> o instanceof GetOutcome.Unreachable);
        if (!(got instanceof GetOutcome.Found found)) {
            switch (got) {
                case GetOutcome.Absent ignored -> {
                    err.println("not found: " + connectionId);
                    err.flush();
                }
                case GetOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
                case GetOutcome.Unreachable ignored -> reportRequestFailed();
                case GetOutcome.Found ignored -> { }   // handled by the outer guard; unreachable here
            }
            return null;
        }
        if (!found.artifact().kind().equals("source")) {
            err.println(verb + ": '" + connectionId + "' is a " + found.artifact().kind()
                    + ", not a " + adjective + " connection");
            err.flush();
            return null;
        }
        Resource resource;
        try {
            resource = new DslParser().parse(found.artifact().canonicalForm());
        } catch (RuntimeException malformed) {
            err.println(verb + ": cannot read connection '" + connectionId + "'");
            err.flush();
            return null;
        }
        if (!(resource instanceof SourceResource source)) {
            // the stored kind claimed source but the body did not parse to one — treat as unreadable
            err.println(verb + ": cannot read connection '" + connectionId + "'");
            err.flush();
            return null;
        }
        return source;
    }

    /**
     * {@code discover-schema <id> [-o text|json|yaml]} — discovers a stored connection's source model. It
     * reads the connection from the server first (server-as-truth), parses the connector and connection
     * config it holds, then posts the discovery and renders the discovered tables. A missing operand or an
     * unknown option is a benign usage line; an id that resolves to nothing is a benign "not found"; an id
     * that is not a source connection is a benign "not a discoverable connection"; a coded refusal renders
     * its code and message.
     */
    private int discoverSchemaOnline(List<String> words) {
        IdAndFormat parsed = parseIdAndFormat("discover-schema", words);
        if (parsed == null) {
            return Cli.EXIT_USAGE;
        }
        SourceResource source = fetchSourceConnection("discover-schema", parsed.id(), "discoverable");
        if (source == null) {
            return Cli.EXIT_DIAGNOSTIC;
        }

        final String connectionId = parsed.id();
        ConnectionDiscoverSchemaOutcome outcome = withFailover(() -> controlPlane.discoverSchema(
                session.landingNode(), session.credential(), connectionId, source.connector(), source.config()),
                o -> o instanceof ConnectionDiscoverSchemaOutcome.Unreachable);
        return switch (outcome) {
            case ConnectionDiscoverSchemaOutcome.Discovered discovered -> {
                renderSchema(discovered.schema(), parsed.format());
                yield Cli.EXIT_OK;
            }
            case ConnectionDiscoverSchemaOutcome.Rejected rejected ->
                    renderRejection(rejected.code(), rejected.message());
            case ConnectionDiscoverSchemaOutcome.TimedOut ignored -> reportRequestTimedOut();
            case ConnectionDiscoverSchemaOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code schema <id> [table] [-o text|json|yaml]} — reads a connection's latest discovered source model
     * and renders it (the read peer of {@code discover-schema}, no discovery run). With a table operand the
     * view narrows to that table — a presentation-side projection of the full stored model, not a separate
     * server call; a table not in the model is a benign line naming the miss. A connection that has never
     * been discovered is a benign "not discovered yet" line; a coded refusal renders its code and message.
     */
    private int schemaOnline(List<String> words) {
        IdTableAndFormat parsed = parseIdTableAndFormat(words);
        if (parsed == null) {
            return Cli.EXIT_USAGE;
        }
        final String connectionId = parsed.id();
        ConnectionSchemaOutcome outcome = withFailover(() ->
                controlPlane.schema(session.landingNode(), session.credential(), connectionId),
                o -> o instanceof ConnectionSchemaOutcome.Unreachable);
        return switch (outcome) {
            case ConnectionSchemaOutcome.Found found -> {
                ConnectionSchema schema = found.schema();
                if (parsed.table() != null) {
                    ConnectionSchema narrowed = filterToTable(schema, parsed.table());
                    if (narrowed == null) {
                        PrintWriter err = commandLine.getErr();
                        err.println("schema: '" + parsed.table() + "' is not in the discovered model of '"
                                + connectionId + "' (tables: " + tableNames(schema) + ")");
                        err.flush();
                        yield Cli.EXIT_DIAGNOSTIC;
                    }
                    schema = narrowed;
                }
                renderSchema(schema, parsed.format());
                yield Cli.EXIT_OK;
            }
            case ConnectionSchemaOutcome.Absent ignored -> {
                PrintWriter err = commandLine.getErr();
                err.println("schema: '" + connectionId + "' has not been discovered yet");
                err.flush();
                yield Cli.EXIT_DIAGNOSTIC;
            }
            case ConnectionSchemaOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case ConnectionSchemaOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /** The model narrowed to one table by exact name, or {@code null} when the model has no such table. */
    private static ConnectionSchema filterToTable(ConnectionSchema schema, String table) {
        List<ConnectionSchema.Table> match = schema.tables().stream()
                .filter(t -> t.name().equals(table))
                .toList();
        return match.isEmpty()
                ? null
                : new ConnectionSchema(schema.connectionId(), schema.connectorId(), match, schema.discoveredAt());
    }

    /** The model's table names joined for a diagnostic line. */
    private static String tableNames(ConnectionSchema schema) {
        return schema.tables().stream().map(ConnectionSchema.Table::name)
                .reduce((a, b) -> a + ", " + b).orElse("none");
    }

    /**
     * {@code test-result <id> [-o text|json|yaml]} — reads a connection's latest stored test result and
     * renders it (the read peer of {@code test}, no probe run). A missing operand or an unknown option is a
     * benign usage line; a connection that has never been tested is a benign "not tested yet" line; a coded
     * refusal renders its code and message. The rendered report is the last test's — its outcome may itself
     * be a failure, which is a valid result to read back, not an error.
     */
    private int testResultOnline(List<String> words) {
        IdAndFormat parsed = parseIdAndFormat("test-result", words);
        if (parsed == null) {
            return Cli.EXIT_USAGE;
        }
        final String connectionId = parsed.id();
        ConnectionTestResultOutcome outcome = withFailover(() ->
                controlPlane.testResult(session.landingNode(), session.credential(), connectionId),
                o -> o instanceof ConnectionTestResultOutcome.Unreachable);
        return switch (outcome) {
            case ConnectionTestResultOutcome.Found found -> {
                renderReport(found.report(), parsed.format());
                yield reportStatus(found.report());
            }
            case ConnectionTestResultOutcome.Absent ignored -> {
                PrintWriter err = commandLine.getErr();
                err.println("test-result: '" + connectionId + "' has not been tested yet");
                err.flush();
                yield Cli.EXIT_DIAGNOSTIC;
            }
            case ConnectionTestResultOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case ConnectionTestResultOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code register <path> [-o text|json|yaml]} — registers a local connector artifact with the server. A
     * file path uploads that one jar; a directory path uploads every {@code *.jar} directly under it as a
     * batch. The server introspects each artifact and stores it content-hash idempotently, then reports what
     * was registered (newly, or an already-registered no-op). A missing operand or unknown option is a benign
     * usage line; an unreadable path is a benign "cannot read" line; a coded refusal (a bad artifact, an id
     * conflict) renders its code and message, and on the machine surfaces an {@code {"error":{...}}} document.
     */
    private int registerOnline(List<String> words) {
        PathAndFormat parsed = parsePathAndFormat(words);
        if (parsed == null) {
            return Cli.EXIT_USAGE;
        }
        Path artifactPath = workdir.resolve(parsed.path()).normalize();
        if (Files.isDirectory(artifactPath)) {
            return registerDirectory(artifactPath, parsed.format());
        }
        PrintWriter err = commandLine.getErr();
        byte[] artifact;
        try {
            artifact = Files.readAllBytes(artifactPath);
        } catch (IOException e) {
            err.println("register: cannot read " + artifactPath + ": " + e.getMessage());
            err.flush();
            return Cli.EXIT_USAGE;
        }
        echoUploading(artifactPath.getFileName().toString(), artifact.length, parsed.format());
        ConnectorRegisterOutcome outcome = withFailover(() -> controlPlane.register(
                session.landingNode(), session.credential(), artifact),
                o -> o instanceof ConnectorRegisterOutcome.Unreachable);
        return switch (outcome) {
            case ConnectorRegisterOutcome.Registered registered -> {
                renderRegistered(registered.connector(), parsed.format());
                yield Cli.EXIT_OK;
            }
            case ConnectorRegisterOutcome.Rejected rejected -> {
                renderRegisterRejection(rejected.code(), rejected.message(), parsed.format());
                yield Cli.EXIT_DIAGNOSTIC;
            }
            case ConnectorRegisterOutcome.TimedOut ignored -> reportRequestTimedOut();
            case ConnectorRegisterOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * Parses {@code <path> [-o text|json|yaml]} for the register verb, printing its usage line to err and
     * returning {@code null} on any error. The single positional operand is the local artifact path to upload.
     */
    private PathAndFormat parsePathAndFormat(List<String> words) {
        PrintWriter err = commandLine.getErr();
        String path = null;
        OutputFormat format = OutputFormat.TEXT;
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals("-o") || word.equals("--output")) {
                if (i + 1 >= words.size()) {
                    err.println("register: " + word + " needs a format (text|json|yaml)");
                    err.flush();
                    return null;
                }
                OutputFormat chosen = outputFormat(words.get(++i));
                if (chosen == null) {
                    err.println("register: unknown output format '" + words.get(i) + "' (expected text|json|yaml)");
                    err.flush();
                    return null;
                }
                format = chosen;
            } else if (word.startsWith("-")) {
                err.println("register: unknown option " + word);
                err.flush();
                return null;
            } else if (path == null) {
                path = word;
            } else {
                err.println("register: too many operands (usage: register <path> [-o text|json|yaml])");
                err.flush();
                return null;
            }
        }
        if (path == null || path.isBlank()) {
            err.println("register: missing operand (usage: register <path> [-o text|json|yaml])");
            err.flush();
            return null;
        }
        return new PathAndFormat(path, format);
    }

    /** The parsed operands of the register verb: the local artifact path and the chosen output format. */
    private record PathAndFormat(String path, OutputFormat format) {
    }

    /** Renders a connector registration on the chosen surface: a human line, or the structured machine form. */
    private void renderRegistered(RegisteredConnector connector, OutputFormat format) {
        PrintWriter out = commandLine.getOut();
        switch (format) {
            case TEXT -> out.println(registeredHeadline(connector));
            case JSON -> out.println(JsonOut.write(registeredMap(connector)));
            case YAML -> out.println(YamlOut.write(registeredMap(connector)));
        }
        out.flush();
    }

    /** The human line: whether the artifact was newly registered or already present, then its id and hash. */
    private static String registeredHeadline(RegisteredConnector connector) {
        String state = connector.newlyRegistered() ? "registered" : "already registered";
        return state + "  " + connector.connectorId() + "  " + connector.contentHash();
    }

    /** The registration as an ordered tree for the machine surfaces, omitting a null PDK API version. */
    private static Map<String, Object> registeredMap(RegisteredConnector connector) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("connectorId", connector.connectorId());
        map.put("contentHash", connector.contentHash());
        putIfPresent(map, "pdkApiVersion", connector.pdkApiVersion());
        map.put("newlyRegistered", connector.newlyRegistered());
        return map;
    }

    /**
     * Renders a coded server refusal of a registration on the chosen surface. Text keeps the shared human
     * diagnostic (the {@code code} then the message, to err); the machine surfaces emit a structured
     * {@code {"error":{"code","message"}}} document to out, so {@code register -o json|yaml} stays
     * parseable even when the server refuses the artifact.
     */
    private void renderRegisterRejection(String code, String message, OutputFormat format) {
        if (format == OutputFormat.TEXT) {
            renderRejection(code, message);
            return;
        }
        PrintWriter out = commandLine.getOut();
        Map<String, Object> document = errorDocument(code, message);
        out.println(format == OutputFormat.JSON ? JsonOut.write(document) : YamlOut.write(document));
        out.flush();
    }

    /** A coded refusal wrapped as a machine document: {@code {"error":{code?,message}}}. */
    private static Map<String, Object> errorDocument(String code, String message) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("error", errorObject(code, message));
        return document;
    }

    /** The inner error object for the machine surfaces: the code (when present) then the message. */
    private static Map<String, Object> errorObject(String code, String message) {
        Map<String, Object> error = new LinkedHashMap<>();
        putIfPresent(error, "code", code == null || code.isBlank() ? null : code);
        error.put("message", message);
        return error;
    }

    /**
     * Registers every {@code *.jar} directly under a directory: a case-insensitive, non-recursive scan in
     * filename order, uploading each artifact and collecting a per-artifact outcome. The batch stops early
     * once the server is unreachable (there is no point uploading the rest). The collected outcomes render
     * as a human report, or on the machine surfaces as an {@code {"artifacts":[...],"summary":{...}}} document.
     */
    private int registerDirectory(Path directory, OutputFormat format) {
        List<Path> jars;
        try (var entries = Files.list(directory)) {
            jars = entries.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar"))
                    .sorted()
                    .toList();
        } catch (IOException e) {
            PrintWriter err = commandLine.getErr();
            err.println("register: cannot read " + directory + ": " + e.getMessage());
            err.flush();
            return Cli.EXIT_USAGE;
        }
        List<BatchEntry> outcomes = new ArrayList<>();
        for (Path jar : jars) {
            String name = jar.getFileName().toString();
            byte[] read;
            try {
                read = Files.readAllBytes(jar);
            } catch (IOException e) {
                outcomes.add(new BatchEntry.Unreadable(name, e.getMessage()));
                continue;   // one unreadable jar does not abort the rest of the batch
            }
            byte[] artifact = read;
            echoUploading(name, artifact.length, format);
            ConnectorRegisterOutcome outcome = withFailover(
                    () -> controlPlane.register(session.landingNode(), session.credential(), artifact),
                    o -> o instanceof ConnectorRegisterOutcome.Unreachable);
            outcomes.add(new BatchEntry.Attempted(name, outcome));
            if (outcome instanceof ConnectorRegisterOutcome.Unreachable) {
                break;   // failover found no healthy member; the server is gone, so stop uploading the rest
            }
        }
        renderBatch(directory, outcomes, jars.size(), format);
        // A batch is only a success if nothing in it failed. Reporting the failures and still exiting 0
        // would let a script register a directory, have half of it refused, and read that as done.
        BatchCounts counts = BatchCounts.of(outcomes);
        boolean anyFailed = counts.rejected() + counts.timedOut() + counts.unreachable()
                + counts.unreadable() > 0;
        return anyFailed ? Cli.EXIT_DIAGNOSTIC : Cli.EXIT_OK;
    }

    /** One artifact's place in a directory batch: uploaded (with the server's outcome) or unreadable locally. */
    private sealed interface BatchEntry {
        String artifact();

        record Attempted(String artifact, ConnectorRegisterOutcome outcome) implements BatchEntry {
        }

        record Unreadable(String artifact, String message) implements BatchEntry {
        }
    }

    /** The counts closing a batch report: newly registered, no-ops, coded refusals, unreachable, unreadable. */
    private record BatchCounts(int registered, int alreadyRegistered, int rejected, int timedOut, int unreachable, int unreadable) {
        static BatchCounts of(List<BatchEntry> outcomes) {
            int registered = 0;
            int alreadyRegistered = 0;
            int rejected = 0;
            int timedOut = 0;
            int unreachable = 0;
            int unreadable = 0;
            for (BatchEntry entry : outcomes) {
                switch (entry) {
                    case BatchEntry.Unreadable ignored -> unreadable++;
                    case BatchEntry.Attempted attempted -> {
                        switch (attempted.outcome()) {
                            case ConnectorRegisterOutcome.Registered r -> {
                                if (r.connector().newlyRegistered()) {
                                    registered++;
                                } else {
                                    alreadyRegistered++;
                                }
                            }
                            case ConnectorRegisterOutcome.Rejected ignored -> rejected++;
                            case ConnectorRegisterOutcome.TimedOut ignored -> timedOut++;
                            case ConnectorRegisterOutcome.Unreachable ignored -> unreachable++;
                        }
                    }
                }
            }
            return new BatchCounts(registered, alreadyRegistered, rejected, timedOut, unreachable, unreadable);
        }
    }

    /** Renders a directory batch on the chosen surface: a human report, or a machine artifacts/summary document. */
    private void renderBatch(Path directory, List<BatchEntry> outcomes, int scanned, OutputFormat format) {
        if (format == OutputFormat.TEXT) {
            renderBatchText(directory, outcomes, scanned);
            return;
        }
        PrintWriter out = commandLine.getOut();
        Map<String, Object> document = batchDocument(outcomes, scanned);
        out.println(format == OutputFormat.JSON ? JsonOut.write(document) : YamlOut.write(document));
        out.flush();
    }

    /** The human batch report: one line per artifact then a counts summary; an empty scan says so plainly. */
    private void renderBatchText(Path directory, List<BatchEntry> outcomes, int scanned) {
        PrintWriter out = commandLine.getOut();
        if (scanned == 0) {
            out.println("no connector jars found in " + directory);
            out.flush();
            return;
        }
        for (BatchEntry entry : outcomes) {
            out.println(batchLine(entry));
        }
        out.println(batchSummary(outcomes, scanned));
        out.flush();
    }

    /** One human report line for an artifact: its state and identity, its coded refusal, unreachable, or unreadable. */
    private static String batchLine(BatchEntry entry) {
        return switch (entry) {
            case BatchEntry.Unreadable unreadable -> unreadable.artifact() + "  error: cannot read  " + unreadable.message();
            case BatchEntry.Attempted attempted -> attempted.artifact() + "  " + attemptedLine(attempted.outcome());
        };
    }

    /** The state portion of a human batch line for an uploaded artifact. */
    private static String attemptedLine(ConnectorRegisterOutcome outcome) {
        return switch (outcome) {
            case ConnectorRegisterOutcome.Registered registered -> (registered.connector().newlyRegistered() ? "registered" : "already registered")
                    + "  " + registered.connector().connectorId() + "  " + registered.connector().contentHash();
            case ConnectorRegisterOutcome.Rejected rejected -> "error: " + rejected.code() + "  " + rejected.message();
            case ConnectorRegisterOutcome.TimedOut ignored -> "timed out";
            case ConnectorRegisterOutcome.Unreachable ignored -> "unreachable";
        };
    }

    /** The counts line closing a batch report: how many jars were attempted of those scanned, then a breakdown. */
    private static String batchSummary(List<BatchEntry> outcomes, int scanned) {
        BatchCounts counts = BatchCounts.of(outcomes);
        int notAttempted = scanned - outcomes.size();
        StringBuilder summary = new StringBuilder();
        if (notAttempted > 0) {
            summary.append(outcomes.size()).append(" of ").append(scanned).append(" artifacts attempted: ");
        } else {
            summary.append(scanned).append(" artifacts: ");
        }
        summary.append(counts.registered()).append(" registered, ")
                .append(counts.alreadyRegistered()).append(" no-op, ")
                .append(counts.rejected()).append(" rejected");
        if (counts.unreadable() > 0) {
            summary.append(", ").append(counts.unreadable()).append(" unreadable");
        }
        if (counts.timedOut() > 0) {
            summary.append(", ").append(counts.timedOut()).append(" timed out");
        }
        if (counts.unreachable() > 0) {
            summary.append(", ").append(counts.unreachable()).append(" unreachable");
        }
        if (notAttempted > 0) {
            summary.append("; ").append(notAttempted).append(" not attempted");
        }
        return summary.toString();
    }

    /** A directory batch as a machine document: an ordered {@code artifacts} array and a counts {@code summary}. */
    private static Map<String, Object> batchDocument(List<BatchEntry> outcomes, int scanned) {
        List<Object> artifacts = new ArrayList<>();
        for (BatchEntry entry : outcomes) {
            artifacts.add(batchRow(entry));
        }
        BatchCounts counts = BatchCounts.of(outcomes);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total", scanned);
        summary.put("attempted", outcomes.size());
        summary.put("registered", counts.registered());
        summary.put("alreadyRegistered", counts.alreadyRegistered());
        summary.put("rejected", counts.rejected());
        if (counts.unreadable() > 0) {
            summary.put("unreadable", counts.unreadable());
        }
        if (counts.timedOut() > 0) {
            summary.put("timedOut", counts.timedOut());
        }
        if (counts.unreachable() > 0) {
            summary.put("unreachable", counts.unreachable());
        }
        int notAttempted = scanned - outcomes.size();
        if (notAttempted > 0) {
            summary.put("notAttempted", notAttempted);
        }
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("artifacts", artifacts);
        document.put("summary", summary);
        return document;
    }

    /** One artifact row for the machine batch document: the registration fields, or an {@code error} object. */
    private static Map<String, Object> batchRow(BatchEntry entry) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("artifact", entry.artifact());
        switch (entry) {
            case BatchEntry.Unreadable unreadable -> row.put("error", errorObject(null, "cannot read: " + unreadable.message()));
            case BatchEntry.Attempted attempted -> {
                switch (attempted.outcome()) {
                    case ConnectorRegisterOutcome.Registered registered -> {
                        RegisteredConnector connector = registered.connector();
                        row.put("connectorId", connector.connectorId());
                        row.put("contentHash", connector.contentHash());
                        putIfPresent(row, "pdkApiVersion", connector.pdkApiVersion());
                        row.put("newlyRegistered", connector.newlyRegistered());
                    }
                    case ConnectorRegisterOutcome.Rejected rejected -> row.put("error", errorObject(rejected.code(), rejected.message()));
                    case ConnectorRegisterOutcome.TimedOut ignored -> row.put("error", errorObject("cli.request-timed-out", "the request timed out before the server answered"));
                    case ConnectorRegisterOutcome.Unreachable ignored -> row.put("error", errorObject(null, "the server is unreachable"));
                }
            }
        }
        return row;
    }

    /** In the human surface, announces an upload before it starts (name and size), so a large or bulk upload shows progress; the machine surfaces stay silent so their document is not polluted. */
    private void echoUploading(String artifact, long bytes, OutputFormat format) {
        if (format != OutputFormat.TEXT) {
            return;
        }
        PrintWriter err = commandLine.getErr();
        err.println("uploading " + artifact + " (" + humanSize(bytes) + ")");
        err.flush();
    }

    /** A short human byte size: {@code B} under a kibibyte, else one decimal in {@code KB}/{@code MB}/{@code GB}/{@code TB}. */
    private static String humanSize(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        String[] units = {"KB", "MB", "GB", "TB"};
        double size = bytes;
        int unit = -1;
        do {
            size /= 1024;
            unit++;
        } while (size >= 1024 && unit < units.length - 1);
        return String.format(Locale.ROOT, "%.1f %s", size, units[unit]);
    }

    /**
     * {@code connectors [-o text|json|yaml]} — lists the connectors the online catalog exposes (the bundled
     * snapshot union the connectors registered at runtime), each tagged bundled or registered. Takes no
     * operand; an unknown option is a benign usage line; a coded refusal renders its code and message.
     */
    private int connectorsOnline(List<String> words) {
        OutputFormat format = parseFormatOnly("connectors", words);
        if (format == null) {
            return Cli.EXIT_USAGE;
        }
        ConnectorListOutcome outcome = withFailover(() ->
                controlPlane.connectorList(session.landingNode(), session.credential()),
                o -> o instanceof ConnectorListOutcome.Unreachable);
        return switch (outcome) {
            case ConnectorListOutcome.Listed listed -> {
                renderConnectors(listed.connectors(), format);
                yield Cli.EXIT_OK;
            }
            case ConnectorListOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case ConnectorListOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * Parses {@code [-o text|json|yaml]} for a verb that takes no operand, printing its usage line to err
     * and returning {@code null} on any error (an unknown option or a stray operand). Defaults to TEXT.
     */
    private OutputFormat parseFormatOnly(String verb, List<String> words) {
        PrintWriter err = commandLine.getErr();
        OutputFormat format = OutputFormat.TEXT;
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals("-o") || word.equals("--output")) {
                if (i + 1 >= words.size()) {
                    err.println(verb + ": " + word + " needs a format (text|json|yaml)");
                    err.flush();
                    return null;
                }
                OutputFormat chosen = outputFormat(words.get(++i));
                if (chosen == null) {
                    err.println(verb + ": unknown output format '" + words.get(i) + "' (expected text|json|yaml)");
                    err.flush();
                    return null;
                }
                format = chosen;
            } else {
                err.println(verb + ": takes no operand (usage: " + verb + " [-o text|json|yaml])");
                err.flush();
                return null;
            }
        }
        return format;
    }

    /** Renders the connector catalog on the chosen surface: one human line per connector, or the machine tree. */
    private void renderConnectors(List<CatalogConnector> connectors, OutputFormat format) {
        PrintWriter out = commandLine.getOut();
        switch (format) {
            case TEXT -> {
                if (connectors.isEmpty()) {
                    out.println("no connectors");
                } else {
                    for (CatalogConnector connector : connectors) {
                        out.println(connectorHeadline(connector));
                    }
                }
            }
            case JSON -> out.println(JsonOut.write(connectorsMap(connectors)));
            case YAML -> out.println(YamlOut.write(connectorsMap(connectors)));
        }
        out.flush();
    }

    /** The human line: origin, group, id, the modes it may be paired with, and whether it can sink. */
    private static String connectorHeadline(CatalogConnector connector) {
        String modes = connector.modes().isEmpty() ? "-" : String.join(",", connector.modes());
        String sink = connector.sink() ? "sink" : "no-sink";
        return connector.origin() + "  " + connector.group() + "  " + connector.id() + "  [" + modes + "]  " + sink;
    }

    /** The connector list as an ordered tree for the machine surfaces, omitting null name / group / origin. */
    private static Map<String, Object> connectorsMap(List<CatalogConnector> connectors) {
        List<Object> rows = new ArrayList<>();
        for (CatalogConnector connector : connectors) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", connector.id());
            putIfPresent(row, "name", connector.name());
            putIfPresent(row, "group", connector.group());
            row.put("modes", connector.modes());
            row.put("sink", connector.sink());
            putIfPresent(row, "origin", connector.origin());
            rows.add(row);
        }
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("connectors", rows);
        return map;
    }

    /**
     * Parses {@code <id> [-o text|json|yaml]} for the report verbs that self-parse the output flag, printing
     * the matching usage line to err and returning {@code null} on any error. The verb name is threaded
     * through so each verb's messages name it (routed before the positional-only guard the other verbs share).
     */
    private IdAndFormat parseIdAndFormat(String verb, List<String> words) {
        PrintWriter err = commandLine.getErr();
        String id = null;
        OutputFormat format = OutputFormat.TEXT;
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals("-o") || word.equals("--output")) {
                if (i + 1 >= words.size()) {
                    err.println(verb + ": " + word + " needs a format (text|json|yaml)");
                    err.flush();
                    return null;
                }
                OutputFormat chosen = outputFormat(words.get(++i));
                if (chosen == null) {
                    err.println(verb + ": unknown output format '" + words.get(i) + "' (expected text|json|yaml)");
                    err.flush();
                    return null;
                }
                format = chosen;
            } else if (word.startsWith("-")) {
                err.println(verb + ": unknown option " + word);
                err.flush();
                return null;
            } else if (id == null) {
                id = word;
            } else {
                err.println(verb + ": too many operands (usage: " + verb + " <id> [-o text|json|yaml])");
                err.flush();
                return null;
            }
        }
        if (id == null || id.isBlank()) {
            err.println(verb + ": missing operand (usage: " + verb + " <id> [-o text|json|yaml])");
            err.flush();
            return null;
        }
        return new IdAndFormat(id, format);
    }

    /** The parsed operands of a report verb: the connection id and the chosen output format. */
    private record IdAndFormat(String id, OutputFormat format) {
    }

    /**
     * Parses {@code <id> [table] [-o text|json|yaml]} for the schema read verb, printing its usage line to
     * err and returning {@code null} on any error. The second positional operand is the optional table to
     * narrow the view to.
     */
    private IdTableAndFormat parseIdTableAndFormat(List<String> words) {
        PrintWriter err = commandLine.getErr();
        String id = null;
        String table = null;
        OutputFormat format = OutputFormat.TEXT;
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals("-o") || word.equals("--output")) {
                if (i + 1 >= words.size()) {
                    err.println("schema: " + word + " needs a format (text|json|yaml)");
                    err.flush();
                    return null;
                }
                OutputFormat chosen = outputFormat(words.get(++i));
                if (chosen == null) {
                    err.println("schema: unknown output format '" + words.get(i) + "' (expected text|json|yaml)");
                    err.flush();
                    return null;
                }
                format = chosen;
            } else if (word.startsWith("-")) {
                err.println("schema: unknown option " + word);
                err.flush();
                return null;
            } else if (id == null) {
                id = word;
            } else if (table == null) {
                table = word;
            } else {
                err.println("schema: too many operands (usage: schema <id> [table] [-o text|json|yaml])");
                err.flush();
                return null;
            }
        }
        if (id == null || id.isBlank()) {
            err.println("schema: missing operand (usage: schema <id> [table] [-o text|json|yaml])");
            err.flush();
            return null;
        }
        return new IdTableAndFormat(id, table, format);
    }

    /** The parsed operands of the schema verb: the connection id, the optional table, and the format. */
    private record IdTableAndFormat(String id, String table, OutputFormat format) {
    }

    /** The {@code -o} format spelled text / json / yaml (case-insensitive), or {@code null} if unrecognised. */
    private static OutputFormat outputFormat(String raw) {
        return switch (raw.toLowerCase(Locale.ROOT)) {
            case "text" -> OutputFormat.TEXT;
            case "json" -> OutputFormat.JSON;
            case "yaml" -> OutputFormat.YAML;
            default -> null;
        };
    }

    /** Renders a connection report on the chosen surface: a human summary, or the structured machine form. */
    private void renderReport(ConnectionReport report, OutputFormat format) {
        PrintWriter out = commandLine.getOut();
        switch (format) {
            case TEXT -> renderReportText(out, report);
            case JSON -> out.println(JsonOut.write(reportMap(report)));
            case YAML -> out.println(YamlOut.write(reportMap(report)));
        }
        out.flush();
    }

    /**
     * The human summary: an outcome header naming the connection + connector, then one line per check,
     * and under a check that carried them, the connector's own reason and remedy.
     *
     * <p>Those two are printed here rather than left to the machine surfaces because they are the
     * reason a check is worth running. A connector that can say "wal_level is replica" and "set it to
     * logical" has answered the question the person is about to ask, and reaching that answer only by
     * knowing to re-run with {@code -o json} asks them to already know what they are looking for. The
     * connector's own code is printed alongside, since it is what an operator quotes when they go
     * looking for the connector's documentation.
     */
    private static void renderReportText(PrintWriter out, ConnectionReport report) {
        out.println(report.outcome() + "  " + report.connectionId() + " (" + report.connectorId() + ")");
        for (ConnectionReport.Check check : report.checks()) {
            StringBuilder line = new StringBuilder(String.format("  %-7s %s", check.status(), check.name()));
            // The message is resolved the same way as the reason and the solution: the connector API
            // puts its keys in this field too, and a key printed where the eye lands first is the worst
            // place of the three to leave one.
            String headline = readable(check.message());
            if (headline != null) {
                line.append("  ").append(headline);
            }
            if (present(check.connectorErrorCode())) {
                line.append("  [").append(check.connectorErrorCode()).append(']');
            }
            out.println(line);
            String reason = readable(check.reason());
            if (reason != null) {
                out.println("          " + reason);
            }
            String solution = readable(check.solution());
            if (solution != null) {
                out.println("          " + solution);
            }
        }
        if (changeCaptureUnavailable(report)) {
            out.println();
            out.println("  Change capture will not work on this connection until the "
                    + CHANGE_STREAM_CHECK + " check passes.");
            out.println("  A snapshot will; a pipeline reading changes will start and never see any.");
        }
    }

    /**
     * Whether this connection cannot carry change capture, despite the test as a whole passing.
     *
     * <p>Connectors report the change-stream check as a warning rather than a failure, and a warning
     * does not fail the overall outcome — so a database with change capture switched off answers
     * PASSED. That is not wrong: the connection is genuinely usable, and a snapshot over it works. It
     * is only wrong as the whole answer, because the person reads PASSED, builds a pipeline that
     * reads changes, and is told nothing when its capture half never produces a row.
     *
     * <p>The outcome is left as the connector reported it — {@code test} asks about a connection, not
     * about a pipeline, and failing it would refuse connections that snapshot-only work needs. What
     * changes is that the consequence is stated instead of left to be discovered later.
     */
    private static boolean changeCaptureUnavailable(ConnectionReport report) {
        for (ConnectionReport.Check check : report.checks()) {
            if (CHANGE_STREAM_CHECK.equalsIgnoreCase(check.name())
                    && !"PASSED".equalsIgnoreCase(check.status())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The connector API's own name for the change-stream check. It is a fixed constant of that API,
     * which is what makes it usable as an identifier: every connector reporting this check reports it
     * under this name, so recognising it does not depend on any one connector's wording.
     */
    private static final String CHANGE_STREAM_CHECK = "Read log";

    private static boolean present(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * The diagnostic to show a person, or null when there is none at all.
     *
     * <p>The connector API's typed test exceptions are constructed with translation keys rather than
     * sentences - a privilege check carries {@code check.cdc.privilege.reason} - and nothing resolves
     * them on the way here: they are plain string fields, no connector sets them to text, and the
     * bundle that would translate them belongs to the platform those connectors were written for. So
     * this repository supplies the wording, under a reserved prefix that cannot collide with a
     * first-party code.
     *
     * <p><b>The catalog decides, not the shape.</b> Judging by shape - dotted lowercase segments -
     * cannot tell a key from the many ordinary values that look exactly like one: {@code 10.10.0.5},
     * {@code db.internal}, {@code 8.0.13}. Those are precisely what a host or version check reports,
     * and dropping them left the reader a check with no message, no reason and no solution, which is
     * less than they had before any of this. The set of keys the connector API defines is closed and
     * held closed by a gate, so membership of the catalog answers the question exactly. Anything the
     * catalog does not know is shown as it arrived.
     */
    private static String readable(String value) {
        if (!present(value)) {
            return null;
        }
        String trimmed = value.trim();
        String key = PDK_TEST_ITEM_PREFIX + trimmed;
        String rendered = MessageCatalog.bundled().render(key, Map.of()).message();
        // The catalog answers an unknown code with the code itself. The original is returned in that
        // case rather than the lookup's answer, so a value carrying braces cannot come back mangled.
        return rendered.equals(key) ? trimmed : rendered;
    }

    /** The reserved catalog namespace the connector API's test-item keys are given wording under. */
    private static final String PDK_TEST_ITEM_PREFIX = "pdk.testitem.";

    /** The report as an ordered tree for the machine surfaces, omitting the optional check fields left null. */
    private static Map<String, Object> reportMap(ConnectionReport report) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("connectionId", report.connectionId());
        map.put("connectorId", report.connectorId());
        map.put("outcome", report.outcome());
        List<Object> checks = new ArrayList<>();
        for (ConnectionReport.Check check : report.checks()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", check.name());
            entry.put("status", check.status());
            putIfPresent(entry, "message", check.message());
            putIfPresent(entry, "reason", check.reason());
            putIfPresent(entry, "solution", check.solution());
            putIfPresent(entry, "connectorErrorCode", check.connectorErrorCode());
            checks.add(entry);
        }
        map.put("checks", checks);
        map.put("testedAt", report.testedAt());
        return map;
    }

    /** Puts a string value under {@code key} only when it is present (non-null, non-blank). */
    private static void putIfPresent(Map<String, Object> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value);
        }
    }

    /** Renders a discovered model on the chosen surface: a human summary, or the structured machine form. */
    private void renderSchema(ConnectionSchema schema, OutputFormat format) {
        PrintWriter out = commandLine.getOut();
        switch (format) {
            case TEXT -> renderSchemaText(out, schema);
            case JSON -> out.println(JsonOut.write(schemaMap(schema)));
            case YAML -> out.println(YamlOut.write(schemaMap(schema)));
        }
        out.flush();
    }

    /**
     * The human summary: a header naming the connection + connector and the table count, then each table.
     * A single-table view (the narrowed {@code schema <id> <table>} form) expands the fields, primary-key
     * markers and indexes; the multi-table view keeps to one summary line per table.
     */
    private static void renderSchemaText(PrintWriter out, ConnectionSchema schema) {
        List<ConnectionSchema.Table> tables = schema.tables();
        out.println(schema.connectionId() + " (" + schema.connectorId() + ")  "
                + tables.size() + (tables.size() == 1 ? " table" : " tables"));
        if (tables.size() == 1) {
            renderTableDetail(out, tables.get(0));
            return;
        }
        for (ConnectionSchema.Table table : tables) {
            StringBuilder line = new StringBuilder(String.format("  %-20s %d %s", table.name(),
                    table.fields().size(), table.fields().size() == 1 ? "field" : "fields"));
            if (!table.primaryKey().isEmpty()) {
                line.append("  pk(").append(String.join(", ", table.primaryKey())).append(')');
            }
            out.println(line);
        }
    }

    /** One table expanded under its name: each field with its type and pk marker, then each index. */
    private static void renderTableDetail(PrintWriter out, ConnectionSchema.Table table) {
        out.println("  " + table.name());
        for (ConnectionSchema.Field field : table.fields()) {
            StringBuilder line = new StringBuilder(String.format("    %-20s %s",
                    field.name(), field.type() == null ? "?" : field.type()));
            if (table.primaryKey().contains(field.name())) {
                line.append("  pk");
            }
            out.println(line);
        }
        for (ConnectionSchema.Index index : table.indexes()) {
            StringBuilder line = new StringBuilder(
                    "    index " + index.name() + " (" + String.join(", ", index.fields()) + ")");
            if (index.unique()) {
                line.append("  unique");
            }
            out.println(line);
        }
    }

    /** The model as an ordered tree for the machine surfaces, omitting a field type left unresolved. */
    private static Map<String, Object> schemaMap(ConnectionSchema schema) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("connectionId", schema.connectionId());
        map.put("connectorId", schema.connectorId());
        List<Object> tables = new ArrayList<>();
        for (ConnectionSchema.Table table : schema.tables()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("name", table.name());
            List<Object> fields = new ArrayList<>();
            for (ConnectionSchema.Field field : table.fields()) {
                Map<String, Object> f = new LinkedHashMap<>();
                f.put("name", field.name());
                putIfPresent(f, "type", field.type());
                fields.add(f);
            }
            entry.put("fields", fields);
            entry.put("primaryKey", table.primaryKey());
            List<Object> indexes = new ArrayList<>();
            for (ConnectionSchema.Index index : table.indexes()) {
                Map<String, Object> i = new LinkedHashMap<>();
                i.put("name", index.name());
                i.put("fields", index.fields());
                i.put("unique", index.unique());
                indexes.add(i);
            }
            entry.put("indexes", indexes);
            tables.add(entry);
        }
        map.put("tables", tables);
        map.put("discoveredAt", schema.discoveredAt());
        return map;
    }

    /**
     * {@code status <pipeline-id>} — reads the pipeline's lifecycle state from the server and prints
     * {@code <id>  <state>}. A missing id is a benign usage line; a pipeline that has published no
     * observation is a coded refusal ({@code monitor.no-observation}) rendering its code and message.
     */
    private int statusOnline(List<String> words) {
        String id = readTargetId(words);
        if (id == null) {
            return Cli.EXIT_USAGE;
        }
        StatusOutcome outcome = withFailover(() ->
                controlPlane.status(session.landingNode(), session.credential(), id),
                o -> o instanceof StatusOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case StatusOutcome.Found found -> {
                out.println(found.pipelineId() + "  " + found.state().toLowerCase(Locale.ROOT));
                if (found.failureCode() != null) {
                    // A failed state that cannot say what failed sends the user hunting through logs. This
                    // read succeeded -- it is not a refusal -- so it renders to stdout, not alongside a
                    // coded refusal on stderr.
                    renderStatusFailure(found.failureCode(), found.failureMessage());
                }
                out.flush();
                yield Cli.EXIT_OK;
            }
            case StatusOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case StatusOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code metrics <pipeline-id>} — reads the pipeline's open map of run statistics and its per-table source
     * positions and prints one {@code <name>  <value>} line each in name order (a per-table position under a
     * {@code perTableOffset.<table>} key), or a benign {@code no metrics} line when none are wired yet
     * (unavailable, never faked). A coded refusal renders its code and message.
     */
    private int metricsOnline(List<String> words) {
        String id = readTargetId(words);
        if (id == null) {
            return Cli.EXIT_USAGE;
        }
        MetricsOutcome outcome = withFailover(() ->
                controlPlane.metrics(session.landingNode(), session.credential(), id),
                o -> o instanceof MetricsOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case MetricsOutcome.Found found -> {
                Map<String, String> lines = new TreeMap<>();
                found.metrics().forEach((name, value) -> lines.put(name, String.valueOf(value)));
                found.perTableOffset().forEach((table, position) -> lines.put("perTableOffset." + table, position));
                if (lines.isEmpty()) {
                    out.println("no metrics");
                } else {
                    lines.forEach((name, value) -> out.println(name + "  " + value));
                    // The names above are not a compatibility promise yet. Saying so here, next to them, is
                    // the difference between a user who knowingly accepts the churn and one who wires a
                    // dashboard to them and is surprised later; a note buried in a document reaches neither.
                    // Absent when nothing was named -- there is no naming promise to disclaim.
                    out.println(METRIC_NAMES_UNSTABLE);
                }
                out.flush();
                yield Cli.EXIT_OK;
            }
            case MetricsOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case MetricsOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /**
     * {@code snapshot <pipeline-id>} — reads the pipeline's per-table initial-load progress and prints one
     * {@code <table>  <rowsDone>/<rowsTotal> (<pct>%)} line per table in name order (a table with no total
     * shows {@code <rowsDone>/?} — honest partial data), or a benign {@code no snapshot} line when there is
     * none. A coded refusal renders its code and message.
     */
    private int snapshotOnline(List<String> words) {
        String id = readTargetId(words);
        if (id == null) {
            return Cli.EXIT_USAGE;
        }
        SnapshotOutcome outcome = withFailover(() ->
                controlPlane.snapshot(session.landingNode(), session.credential(), id),
                o -> o instanceof SnapshotOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case SnapshotOutcome.Found found -> {
                if (found.tables().isEmpty()) {
                    out.println("no snapshot");
                } else {
                    new TreeMap<>(found.tables()).forEach((table, progress) ->
                            out.println(table + "  " + renderProgress(progress)));
                }
                out.flush();
                yield Cli.EXIT_OK;
            }
            case SnapshotOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case SnapshotOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    private int logsOnline(List<String> words) {
        String id = readTargetId(words);
        if (id == null) {
            return Cli.EXIT_USAGE;
        }
        LogsOutcome outcome = withFailover(() ->
                controlPlane.logs(session.landingNode(), session.credential(), id),
                o -> o instanceof LogsOutcome.Unreachable);
        PrintWriter out = commandLine.getOut();
        return switch (outcome) {
            case LogsOutcome.Found found -> {
                if (found.lines().isEmpty()) {
                    out.println("no logs");
                } else {
                    found.lines().forEach(line -> out.println(renderLogLine(line)));
                }
                out.flush();
                yield Cli.EXIT_OK;
            }
            case LogsOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case LogsOutcome.Unreachable ignored -> reportRequestFailed();
        };
    }

    /** One tailed line as {@code <iso-timestamp> <level> <message>}. */
    private static String renderLogLine(RemoteLogLine line) {
        return Instant.ofEpochMilli(line.timestampMillis()) + "  " + line.level() + "  " + line.message();
    }

    /**
     * {@code status <pipeline-id> --watch} — streams the pipeline's lifecycle state and each subsequent
     * change over the websocket, printing {@code <id>  <state>} per frame, until the connection ends or the
     * user interrupts (Ctrl-C). A missing id is a benign usage line. The state stream re-attaches across a
     * dropped connection; nothing is printed until the pipeline has published an observation.
     */
    private int statusWatch(List<String> words) {
        String id = streamTargetId(words, "--watch");
        if (id == null) {
            return Cli.EXIT_USAGE;
        }
        PrintWriter out = commandLine.getOut();
        streamCancelled = false;
        String refusal = controlPlane.watchStatus(session.landingNode(), session.credential(), id,
                (pipelineId, state, failureCode, failureMessage) -> {
                    out.println(pipelineId + "  " + state.toLowerCase(Locale.ROOT));
                    if (failureCode != null) {
                        // Mirrors the one-shot `status` read: a failed state that cannot say what failed
                        // sends the watcher hunting through logs instead of the frame that just reported it.
                        // This frame arrived over an open stream, not a refusal, so it renders to stdout.
                        renderStatusFailure(failureCode, failureMessage);
                    }
                    out.flush();
                },
                this::isStreamCancelled);
        if (refusal != null) {
            return renderStreamRefusal(refusal, id);
        }
        // a stream ends because the user stopped it, which is the way it is meant to end
        return Cli.EXIT_OK;
    }

    /**
     * {@code logs <pipeline-id> --follow} — streams the pipeline's node-local log tail and each newly
     * appended line over the websocket ({@code tail -f}), until the connection ends or the user interrupts
     * (Ctrl-C). A missing id is a benign usage line.
     */
    private int logsFollow(List<String> words) {
        String id = streamTargetId(words, "--follow");
        if (id == null) {
            return Cli.EXIT_USAGE;
        }
        PrintWriter out = commandLine.getOut();
        streamCancelled = false;
        String refusal = controlPlane.followLogs(session.landingNode(), session.credential(), id,
                (pipelineId, lines) -> {
                    lines.forEach(line -> out.println(renderLogLine(line)));
                    out.flush();
                },
                this::isStreamCancelled);
        if (refusal != null) {
            return renderStreamRefusal(refusal, id);
        }
        // a stream ends because the user stopped it, which is the way it is meant to end
        return Cli.EXIT_OK;
    }

    /**
     * Renders the coded refusal a stream was deliberately closed with — the server ended the watch or
     * follow because it can never be served (e.g. the id was never applied), not because the connection
     * dropped. The close frame carries only the code, so the message is rendered locally from the bundled
     * catalog with the id this stream was for; it prints as a refusal, on stderr, exactly like its
     * one-shot twin would have been.
     */
    private int renderStreamRefusal(String code, String pipelineId) {
        Map<String, Object> params = Map.of("pipeline", pipelineId);
        MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(code, params);
        renderRejection(code, rendered.message(), params);
        return Cli.EXIT_DIAGNOSTIC;
    }

    /**
     * The pipeline id operand for a streaming verb ({@code <verb> <pipeline-id> <flag>}), the {@code flag}
     * ignored wherever it appears; or {@code null} after a benign line when the id is missing or an
     * unsupported option is present.
     */
    private String streamTargetId(List<String> words, String flag) {
        PrintWriter err = commandLine.getErr();
        String id = null;
        for (int i = 1; i < words.size(); i++) {
            String word = words.get(i);
            if (word.equals(flag)) {
                continue;
            }
            if (word.startsWith("-")) {
                err.println(words.get(0) + ": options are not supported on a connected verb yet");
                err.flush();
                return null;
            }
            if (id == null) {
                id = word;
            }
        }
        if (id == null || id.isBlank()) {
            err.println(words.get(0) + ": missing operand (usage: " + words.get(0) + " <pipeline-id> " + flag + ")");
            err.flush();
            return null;
        }
        return id;
    }

    /**
     * The pipeline id operand shared by the observation read verbs ({@code <verb> <pipeline-id>}), or
     * {@code null} after a benign usage line when it is missing — a read names exactly one pipeline.
     */
    private String readTargetId(List<String> words) {
        if (words.size() < 2 || words.get(1).isBlank()) {
            PrintWriter err = commandLine.getErr();
            err.println(words.get(0) + ": missing operand (usage: " + words.get(0) + " <pipeline-id>)");
            err.flush();
            return null;
        }
        return words.get(1);
    }

    /**
     * One table's snapshot progress: {@code rowsDone/rowsTotal (donePct%)} when the total is known, or
     * {@code rowsDone/?} when it is unavailable — honest partial data, never faked as a percentage.
     */
    private static String renderProgress(RemoteTableSnapshot progress) {
        if (progress.rowsTotal() != null && progress.donePct() != null) {
            return progress.rowsDone() + "/" + progress.rowsTotal() + " (" + progress.donePct() + "%)";
        }
        return progress.rowsDone() + "/?";
    }

    /**
     * Runs an online call, and if the landing node could not answer it, fails over to another member and
     * retries the call once against the new landing node. When {@link #failover} cannot re-land, the
     * original unreachable outcome is returned (and the session is now offline). The unreachable predicate
     * lets one wrapper serve every verb's distinct sealed outcome type.
     */
    private <T> T withFailover(Supplier<T> call, Predicate<T> unreachable) {
        T outcome = call.get();
        if (unreachable.test(outcome) && failover()) {
            return call.get();
        }
        return outcome;
    }

    /**
     * Reads every {@code *.tap.yml} under a path (recursively for a directory) as drafts, in name order,
     * with each file's {@code ${...}} references substituted from this session's environment.
     *
     * <p>Substituting here, rather than letting the server do it, is what keeps the variables read the
     * author's own: this side loads the files, so this side resolves them, and only values cross the
     * wire. The drafts stay raw text otherwise — the server remains the only parser.
     */
    private List<LocalDraft> collectDrafts(Path target) throws IOException {
        List<LocalDraft> drafts = new ArrayList<>();
        if (Files.isDirectory(target)) {
            try (var files = Files.walk(target)) {
                List<Path> yamls = files.filter(Files::isRegularFile)
                        .filter(f -> f.getFileName().toString().endsWith(".tap.yml"))
                        .sorted()
                        .toList();
                for (Path f : yamls) {
                    drafts.add(draft(target.relativize(f).toString(), f));
                }
            } catch (UncheckedIOException e) {
                // Files.walk surfaces a mid-traversal access error (an unreadable or concurrently-removed
                // subdirectory) as an unchecked wrapper thrown from the terminal operation; normalize it to
                // the checked IOException the caller renders as a benign "cannot read" line rather than
                // letting it escape and crash the read loop.
                throw e.getCause() != null ? e.getCause() : new IOException(e.getMessage(), e);
            }
        } else if (Files.isRegularFile(target)) {
            drafts.add(draft(target.getFileName().toString(), target));
        }
        return drafts;
    }

    /** Reads one artifact and resolves its references, naming the file on whatever it refuses. */
    private LocalDraft draft(String source, Path file) throws IOException {
        String text = Files.readString(file);
        try {
            return new LocalDraft(source, Interpolator.interpolate(text, env));
        } catch (DslException e) {
            throw e.withSource(source);
        }
    }

    /**
     * Renders a coded refusal raised on this side of the wire, located at the file and line it was found
     * on. Distinct from {@link #renderRejection} only in where the message comes from: a server refusal
     * arrives rendered, while this one is rendered here from the code and its arguments.
     */
    private int renderLocalRefusal(DslException e) {
        MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(e.code(), e.args());
        PrintWriter err = commandLine.getErr();
        String at = e.source() + (e.line() > 0 ? ":" + e.line() + ":" + e.column() : "");
        err.println(Ansi.AUTO.string("@|bold,red error:|@") + " " + at + "  " + e.code().code());
        err.println("  " + rendered.message());
        if (rendered.solution() != null) {
            err.println("  " + rendered.solution());
        }
        err.flush();
        return Cli.EXIT_DIAGNOSTIC;
    }

    /**
     * The exit status a rendered connection report carries. The server's {@code outcome} is otherwise
     * rendered rather than interpreted, and this reads exactly the one value that means the test did not
     * pass — a `test` that reports FAILED and still exits 0 would be no use to a script, which is what
     * the verb's machine-readable output exists for. Any other status, including one added later, is
     * left alone rather than guessed at.
     */
    private static int reportStatus(ConnectionReport report) {
        return "FAILED".equalsIgnoreCase(report.outcome()) ? Cli.EXIT_DIAGNOSTIC : Cli.EXIT_OK;
    }

    /**
     * Renders the server's advisory findings about a batch it applied: each one's code, then its message
     * rendered from this CLI's own bundled catalog, then the catalogued next step when there is one.
     *
     * <p>They print to stderr, apart from the per-artifact outcome lines on stdout, so a caller piping the
     * apply (e.g. {@code apply > applied.txt}) keeps a machine-readable stdout and still sees the notes.
     * A finding is never a refusal: nothing here touches the exit status, which stays the applied batch's.
     *
     * <p>A code this catalog does not know renders as the bare code — a server one version ahead makes a
     * finding less legible, never invisible.
     */
    private void renderApplyWarnings(List<ApplyOutcome.Warning> warnings) {
        if (warnings.isEmpty()) {
            return;
        }
        PrintWriter err = commandLine.getErr();
        MessageCatalog catalog = MessageCatalog.bundled();
        for (ApplyOutcome.Warning warning : warnings) {
            MessageCatalog.Rendered rendered = catalog.render(warning.code(), warning.params());
            err.println(Ansi.AUTO.string("@|bold,yellow warning:|@") + " " + warning.code());
            err.println("  " + rendered.message());
            if (rendered.solution() != null) {
                err.println("  " + rendered.solution());
            }
        }
        err.flush();
    }

    /** Renders a coded server refusal: the {@code code} (when present) then the rendered message, to err. */
    private int renderRejection(String code, String message) {
        return renderRejection(code, message, Map.of());
    }

    /**
     * Reports a refused command: the code, the message the server rendered, and — where the catalog has
     * one for that code — the remedy.
     *
     * <p>The message says what is wrong; the solution says what to do about it, and the second is the
     * half a reader is actually looking for. Both live in the same catalog entry, but only the message
     * arrives rendered, so the remedy is rendered here from the code and the parameters the refusal
     * carried.
     *
     * <p>A remedy that could not be filled in is left out. The catalog leaves an unbound name verbatim,
     * so rendering with parameters a caller does not have prints the template itself - braces and all -
     * where the most useful sentence should be. Most refusals carry no parameters, and every one of
     * them reaches this method, so the check lives here rather than at the call sites: a caller that
     * has parameters passes them, and one that does not costs the reader a sentence they could not
     * have acted on anyway.
     */
    private int renderRejection(String code, String message, Map<String, Object> params) {
        PrintWriter err = commandLine.getErr();
        if (!code.isBlank()) {
            err.println(Ansi.AUTO.string("@|bold,red error:|@") + " " + code);
        }
        err.println("  " + message);
        if (!code.isBlank()) {
            String solution = MessageCatalog.bundled().render(code, params).solution();
            if (present(solution) && !hasUnboundName(solution)) {
                err.println("  " + solution);
            }
        }
        err.flush();
        return Cli.EXIT_DIAGNOSTIC;
    }

    /**
     * Whether a rendered template still carries a name nothing filled in - a brace pair the catalog
     * left as it found it, which is how it reports that no argument was supplied for that name.
     *
     * <p>Read the same way the catalog reads a template, so the two agree on what a name is: an
     * opening brace with a closing one after it. Anything left in that shape reached the end of
     * substitution unbound.
     */
    private static boolean hasUnboundName(String rendered) {
        int open = rendered.indexOf('{');
        return open >= 0 && rendered.indexOf('}', open + 1) > open;
    }

    /**
     * Renders why a pipeline died, for a status read that succeeded and simply reports an unhealthy
     * pipeline -- distinct from {@link #renderRejection}, which reports that the command itself was
     * refused. Both arrive as a code plus a rendered message, but this one is not a refusal: it prints to
     * stdout, without the red {@code error:} banner, so a caller separating the streams (piped or
     * redirected input, e.g. {@code status pl1 > out.txt 2> err.txt}) can still tell "your command was
     * refused" from "the pipeline you asked about is dead" by which stream carried it.
     */
    private void renderStatusFailure(String code, String message) {
        PrintWriter out = commandLine.getOut();
        if (!code.isBlank()) {
            out.println(Ansi.AUTO.string("@|bold reason:|@") + " " + code);
        }
        out.println("  " + message);
        out.flush();
    }

    /**
     * Reports that a request could not be completed after failover. Only reached when the retry itself was
     * unreachable while a landing node is still held; a total loss of the cluster has already been reported
     * by {@link #failover} (which then took the session offline), so this stays silent in that case.
     */
    private int reportRequestFailed() {
        if (!session.isConnected()) {
            return Cli.EXIT_DIAGNOSTIC;   // failover already reported the connection loss and went offline
        }
        PrintWriter err = commandLine.getErr();
        err.println("request failed: " + hostPort(session.landingNode()) + " is unreachable");
        err.flush();
        return Cli.EXIT_DIAGNOSTIC;
    }

    /**
     * Reports that a request reached the landing node but the server did not answer within the verb's
     * timeout window — a distinct, coded outcome from {@link #reportRequestFailed()}, since the server is
     * busy rather than gone and a heavy verb (a large register) may even have completed there already.
     */
    private int reportRequestTimedOut() {
        if (!session.isConnected()) {
            return Cli.EXIT_DIAGNOSTIC;   // failover already reported the connection loss and went offline
        }
        Diagnostics.printText(commandLine.getErr(), CliError.REQUEST_TIMED_OUT,
                Map.of("server", hostPort(session.landingNode())));
        return Cli.EXIT_DIAGNOSTIC;
    }

    /**
     * Establishes a transport target from a comma-separated seed list, probing each seed in order and
     * landing on the first that answers {@code /healthz}. Connecting does not authenticate — the
     * session carries no credential. A blank argument, or a malformed / hostless seed token, is a
     * usage mistake (a benign message, not a coded error); a single invalid token rejects the whole
     * line before any probe, so a typo never silently connects to a subset. No reachable well-formed
     * seed is the coded {@code cli.connect-failed} diagnostic.
     */
    private int connect(List<String> words) {
        PrintWriter err = commandLine.getErr();
        String arg = words.size() > 1 ? words.get(1) : "";
        ParsedSeeds parsed = parseSeeds(arg);
        if (parsed.invalidToken() != null) {
            err.println("connect: invalid seed '" + parsed.invalidToken()
                    + "' (usage: connect <host:port>[,<host:port>...])");
            err.flush();
            return Cli.EXIT_USAGE;
        }
        List<URI> seeds = parsed.valid();
        if (seeds.isEmpty()) {
            err.println("connect: missing operand (usage: connect <host:port>[,<host:port>...])");
            err.flush();
            return Cli.EXIT_USAGE;
        }
        for (URI seed : seeds) {
            if (controlPlane.isHealthy(seed)) {
                session.connect(seeds, seed);
                confirm("connected to " + hostPort(seed));
                return Cli.EXIT_OK;
            }
        }
        reportConnectFailed(seeds);
        return Cli.EXIT_DIAGNOSTIC;
    }

    /** Clears the connection back to offline; a benign line either way, never an error. */
    private int disconnect() {
        PrintWriter out = commandLine.getOut();
        if (session.isConnected()) {
            session.disconnect();
            out.println("disconnected");
        } else {
            out.println("not connected");
        }
        out.flush();
        // dropping a connection that was not held is not a failure -- the session ends up where asked
        return Cli.EXIT_OK;
    }

    /**
     * Authenticates the connected session as a human user: reads the password masked, verifies it via
     * {@code POST /auth/login}, and on success stores the returned bearer credential. Login requires an
     * established connection (authenticating is decoupled from connecting) and a username operand — both
     * absences are benign usage lines, not coded errors. A server refusal renders the server's coded
     * message (a bad credential is {@code control.auth-failed}, revealing nothing about which half was
     * wrong); an unreachable landing node is a benign transient line. The member set for failover is the
     * seeds until membership discovery lands.
     */
    private int login(List<String> words) {
        PrintWriter out = commandLine.getOut();
        PrintWriter err = commandLine.getErr();
        if (!session.isConnected()) {
            Diagnostics.printText(err, CliError.NOT_CONNECTED, Map.of("verb", "login"));
            return Cli.EXIT_VERB_UNAVAILABLE;
        }
        if (words.size() < 2 || words.get(1).isBlank()) {
            err.println("login: missing operand (usage: login <username>)");
            err.flush();
            return Cli.EXIT_USAGE;
        }
        return login(words.get(1), () -> prompter.secret("Password"));
    }

    /**
     * Signs in only after the connected seeds pass anonymous issuer discovery. The password supplier
     * keeps an interactive prompt behind that preflight while allowing one-line launches to provide it.
     */
    private int login(String username, Supplier<String> password) {
        if (namedContext != null && authService != null) {
            return persistentLogin(username, password);
        }
        PrintWriter err = commandLine.getErr();
        IssuerBinding.Verified verified;
        try {
            verified = new IssuerBinding(controlPlane).verify(session.seeds(), null);
        } catch (io.tapstate.core.common.TapstateException failure) {
            Diagnostics.printText(err, failure.code(), failure.args());
            return Cli.EXIT_DIAGNOSTIC;
        }
        return switch (verified.withCredential(password.get(),
                (node, credential) -> controlPlane.login(node, username, credential))) {
            case LoginOutcome.Success success -> {
                session.reland(verified.seed());
                session.authenticate(success.token(), username, null, session.seeds());
                confirm("logged in as " + username);
                yield Cli.EXIT_OK;
            }
            case LoginOutcome.Rejected rejected -> renderRejection(rejected.code(), rejected.message());
            case LoginOutcome.Unreachable ignored -> {
                err.println("login: cannot reach " + hostPort(verified.seed()));
                err.flush();
                yield Cli.EXIT_DIAGNOSTIC;
            }
        };
    }

    private int persistentLogin(String username, Supplier<String> password) {
        PrintWriter out = commandLine.getOut();
        PrintWriter err = commandLine.getErr();
        try {
            return switch (authService.login(namedContext, username, password.get(), terminal.getAsBoolean())) {
                case AuthService.LoginResult.Success success -> {
                    activate(success.session());
                    if (success.storage() == AuthFileStore.SaveResult.PERSISTED) {
                        PrintWriter confirmation = quiet ? err : out;
                        confirmation.println("signed in as " + success.session().record().principal()
                                + "; session saved");
                        confirmation.flush();
                    } else {
                        err.println("signed in for this process only; owner-only session storage is unavailable");
                        err.flush();
                    }
                    yield Cli.EXIT_OK;
                }
                case AuthService.LoginResult.Rejected rejected -> {
                    Diagnostics.printText(err, CliError.AUTH_LOGIN_REJECTED,
                            Map.of("code", rejected.code(), "principal", rejected.principal()));
                    yield Cli.EXIT_DIAGNOSTIC;
                }
                case AuthService.LoginResult.Unreachable ignored -> {
                    Diagnostics.printText(err, CliError.AUTH_LOGIN_UNREACHABLE,
                            Map.of("context", namedContext.name()));
                    yield Cli.EXIT_DIAGNOSTIC;
                }
            };
        } catch (io.tapstate.core.common.TapstateException failure) {
            Diagnostics.printText(err, failure.code(), failure.args());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    private void activate(AuthService.ActiveSession active) {
        session.reland(active.seed());
        session.authenticate(active.accessToken(), active.record().principal(), null, session.seeds());
    }

    /**
     * Re-establishes the landing node after the current one is found unreachable: probes the member set
     * in order and re-lands on the first that answers, keeping the (cluster-wide) credential so the
     * session stays authenticated across the move. When no member answers the connection is lost and the
     * session returns to offline; an offline session is a no-op. This is the seam a connected verb
     * invokes on a request failure — L1's single-node member set exercises the same path (it is not
     * omitted for one node). Returns whether a landing node was kept.
     */
    boolean failover() {
        if (!session.isConnected()) {
            return false;
        }
        for (URI member : session.members()) {
            if (controlPlane.isHealthy(member)) {
                session.reland(member);
                PrintWriter out = commandLine.getOut();
                out.println("reconnected to " + hostPort(member));
                out.flush();
                return true;
            }
        }
        session.disconnect();
        PrintWriter err = commandLine.getErr();
        err.println("connection lost: no reachable cluster member");
        err.flush();
        return false;
    }

    /** Drops the credential while keeping the transport connection; a benign line either way. */
    private int logout() {
        if (namedContext != null && authService != null) {
            return authLogout(false);
        }
        PrintWriter out = commandLine.getOut();
        if (session.isAuthenticated()) {
            session.logout();
            out.println("logged out");
        } else {
            out.println("not logged in");
        }
        out.flush();
        // dropping a credential that was not held leaves the session where asked, so it is not a failure
        return Cli.EXIT_OK;
    }

    /** Dispatches the persistent auth namespace while keeping connect/disconnect transport-only. */
    private int auth(List<String> words) {
        PrintWriter err = commandLine.getErr();
        if (namedContext == null || authService == null) {
            Diagnostics.printText(err, CliError.CONTEXT_REQUIRED, Map.of("verb", "auth"));
            return Cli.EXIT_VERB_UNAVAILABLE;
        }
        if (words.size() < 2) {
            return authUsage("missing action");
        }
        return switch (words.get(1)) {
            case "login" -> authLogin(words);
            case "status" -> words.size() == 2 ? authStatus() : authUsage("status takes no arguments");
            case "logout" -> authLogoutWords(words);
            default -> authUsage("unknown action '" + words.get(1) + "'");
        };
    }

    private int authLogin(List<String> words) {
        if (words.size() > 3) {
            return authUsage("login takes at most one username");
        }
        String username = words.size() == 3 ? words.get(2) : null;
        if (username == null || username.isBlank()) {
            if (!terminal.getAsBoolean()) {
                return authUsage("login needs a username outside a terminal");
            }
            username = prompter.ask("Username", "");
        }
        if (username == null || username.isBlank()) {
            return authUsage("login needs a non-empty username");
        }
        String environmentPassword = env.apply("TAPSTATE_PASSWORD");
        if (!terminal.getAsBoolean() && (environmentPassword == null || environmentPassword.isEmpty())) {
            return authUsage("login needs TAPSTATE_PASSWORD outside a terminal");
        }
        Supplier<String> password = environmentPassword != null && !environmentPassword.isEmpty()
                ? () -> environmentPassword
                : () -> prompter.secret("Password");
        return persistentLogin(username, password);
    }

    private int authStatus() {
        PrintWriter out = commandLine.getOut();
        PrintWriter err = commandLine.getErr();
        try {
            return switch (authService.status(namedContext)) {
                case AuthService.Status.SignedIn signedIn -> {
                    activate(signedIn.session());
                    out.println("signed in as " + signedIn.session().record().principal()
                            + " (" + String.join(", ", signedIn.session().record().scopes()) + ")");
                    out.flush();
                    yield Cli.EXIT_OK;
                }
                case AuthService.Status.SignedOut ignored -> {
                    out.println("not signed in");
                    out.flush();
                    yield Cli.EXIT_OK;
                }
            };
        } catch (io.tapstate.core.common.TapstateException failure) {
            Diagnostics.printText(err, failure.code(), failure.args());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    private int authLogoutWords(List<String> words) {
        if (words.size() == 2) {
            return authLogout(false);
        }
        if (words.size() == 3 && words.get(2).equals("--local-only")) {
            return authLogout(true);
        }
        return authUsage("logout accepts only --local-only");
    }

    private int authLogout(boolean localOnly) {
        PrintWriter out = commandLine.getOut();
        PrintWriter err = commandLine.getErr();
        try {
            return switch (authService.logout(namedContext, localOnly)) {
                case AuthService.LogoutResult.Removed removed -> {
                    session.logout();
                    if (removed.localOnly()) {
                        err.println("local session removed; the remote session remains valid until expiry");
                        err.flush();
                    } else {
                        out.println("session revoked and local cache removed");
                        out.flush();
                    }
                    yield Cli.EXIT_OK;
                }
                case AuthService.LogoutResult.SignedOut ignored -> {
                    session.logout();
                    out.println("not signed in");
                    out.flush();
                    yield Cli.EXIT_OK;
                }
                case AuthService.LogoutResult.Rejected rejected -> {
                    Diagnostics.printText(err, CliError.AUTH_SESSION_REJECTED,
                            Map.of("code", rejected.code(), "principal", rejected.principal()));
                    yield Cli.EXIT_DIAGNOSTIC;
                }
                case AuthService.LogoutResult.Unreachable ignored -> {
                    Diagnostics.printText(err, CliError.AUTH_LOGOUT_UNREACHABLE,
                            Map.of("context", namedContext.name()));
                    yield Cli.EXIT_DIAGNOSTIC;
                }
                case AuthService.LogoutResult.CacheChanged ignored -> {
                    Diagnostics.printText(err, CliError.AUTH_LOGOUT_CACHE_CHANGED,
                            Map.of("context", namedContext.name()));
                    yield Cli.EXIT_DIAGNOSTIC;
                }
            };
        } catch (io.tapstate.core.common.TapstateException failure) {
            Diagnostics.printText(err, failure.code(), failure.args());
            return Cli.EXIT_DIAGNOSTIC;
        }
    }

    private int authUsage(String reason) {
        Diagnostics.printText(commandLine.getErr(), CliError.AUTH_USAGE, Map.of("reason", reason));
        return Cli.EXIT_USAGE;
    }

    private int context() {
        if (contextManager == null) {
            Diagnostics.printText(commandLine.getErr(), CliError.CONTEXT_USAGE,
                    Map.of("reason", "context manager is unavailable in this session"));
            return Cli.EXIT_USAGE;
        }
        return new ContextConsole(contextManager, prompter, workdir,
                commandLine.getOut(), commandLine.getErr()).run();
    }

    /** Renders the {@code cli.connect-failed} diagnostic through the shared coded-error renderer. */
    private void reportConnectFailed(List<URI> seeds) {
        String display = seeds.stream().map(URI::toString).collect(Collectors.joining(", "));
        Diagnostics.printText(commandLine.getErr(), CliError.CONNECT_FAILED, Map.of("seeds", display));
    }

    /**
     * The outcome of parsing a seed argument: the host-bearing base URLs in order and, when a token
     * could not be turned into one, that first offending token ({@code invalidToken} is {@code null}
     * when every non-blank token parsed). A token is invalid when it is not a legal URI or resolves to
     * one with no host. Blank tokens are dropped, so an all-blank argument yields an empty list and a
     * {@code null} invalid token.
     */
    record ParsedSeeds(List<URI> valid, String invalidToken) {
    }

    /**
     * Parses a comma-separated seed argument into host-bearing base URLs without ever throwing. Each
     * element is trimmed and blanks dropped; one that already carries a scheme ({@code ://}) is kept
     * as-is, and a bare {@code host:port} gets an {@code http://} scheme. Parsing stops at the first
     * token that is not a legal URI or resolves to one with no host, returning it as the invalid token
     * so the caller can reject the whole line instead of crashing on it.
     */
    static ParsedSeeds parseSeeds(String arg) {
        List<URI> seeds = new ArrayList<>();
        for (String raw : arg.split(",")) {
            String s = raw.trim();
            if (s.isEmpty()) {
                continue;
            }
            URI uri;
            try {
                uri = URI.create(s.contains("://") ? s : "http://" + s);
            } catch (IllegalArgumentException e) {
                return new ParsedSeeds(List.of(), s);
            }
            if (uri.getHost() == null) {
                return new ParsedSeeds(List.of(), s);
            }
            seeds.add(uri);
        }
        return new ParsedSeeds(seeds, null);
    }

    /** The {@code host} or {@code host:port} of a base URL, for the prompt and success line. */
    private static String hostPort(URI uri) {
        String host = uri.getHost();
        int port = uri.getPort();
        return port == -1 ? host : host + ":" + port;
    }

    /** Changes the session workspace to an existing directory, resolved against the current one. */
    private int changeDir(List<String> words) {
        PrintWriter err = commandLine.getErr();
        if (words.size() < 2) {
            err.println("cd: missing operand");
            err.flush();
            return Cli.EXIT_USAGE;
        }
        String arg = words.get(1);
        Path target = workdir.resolve(arg).normalize();
        if (!Files.isDirectory(target)) {
            err.println("cd: not a directory: " + arg);
            err.flush();
            return Cli.EXIT_USAGE;
        }
        workdir = target;
        return Cli.EXIT_OK;
    }

    /**
     * Appends the session {@code --workdir} to a verb that declares it but did not set it on the line,
     * so the session workspace governs. Verbs without the option (e.g. {@code explain}) are left alone —
     * injecting there would be an unknown option. An explicit {@code -w} on the line is left to win.
     */
    private String[] withWorkspace(List<String> words) {
        CommandLine sub = commandLine.getSubcommands().get(words.get(0));
        boolean acceptsWorkdir = sub != null && sub.getCommandSpec().findOption("--workdir") != null;
        boolean alreadySet = words.stream().anyMatch(w ->
                w.equals("-w") || w.equals("--workdir") || w.startsWith("-w=") || w.startsWith("--workdir="));
        if (acceptsWorkdir && !alreadySet) {
            List<String> augmented = new ArrayList<>(words);
            augmented.add("--workdir");
            augmented.add(workdir.toString());
            return augmented.toArray(new String[0]);
        }
        return words.toArray(new String[0]);
    }

    /**
     * Splits a REPL line into argument words, honoring single / double quotes so a path with spaces
     * survives as one argument — the one-shot form gets this de-quoting from the OS shell, so the
     * REPL must do it itself to keep the two forms identical. Matched quotes are stripped; an
     * unmatched quote runs to end of line.
     */
    static List<String> tokenize(String line) {
        List<String> words = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        char quote = 0;
        boolean inWord = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                } else {
                    current.append(c);
                }
            } else if (c == '"' || c == '\'') {
                quote = c;
                inWord = true;
            } else if (Character.isWhitespace(c)) {
                if (inWord) {
                    words.add(current.toString());
                    current.setLength(0);
                    inWord = false;
                }
            } else {
                current.append(c);
                inWord = true;
            }
        }
        if (inWord) {
            words.add(current.toString());
        }
        return words;
    }

    /**
     * The interactive reader, as a seam the line-reading rules can be exercised against — the rest of
     * the REPL is tested through {@link #dispatch}, which is downstream of everything this decides.
     *
     * <p>History expansion is off. It rewrites the line before anyone parses it: it consumes
     * backslashes as escapes, and takes {@code !} as a reference to an earlier command. Both belong to
     * an interactive shell's own language, and a line here is not in that language -- it carries field
     * names and values that are the user's data. A field whose name holds a dot is addressed by
     * escaping the dot, so the expansion silently turned the one spelling that reaches that column
     * into the spelling that reaches a nested path instead.
     */
    static LineReader readerFor(Terminal terminal, Completer completer) {
        LineReaderBuilder builder = LineReaderBuilder.builder()
                .terminal(terminal)
                .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true);
        if (completer != null) {
            builder.completer(completer);
        }
        return builder.build();
    }

    /** Runs the interactive read loop until {@code exit} / {@code quit} or end-of-input. */
    void run() {
        PrintWriter out = commandLine.getOut();
        // not "offline": a session can now start connected, and the prompt is what reports which it is
        out.println("Tapstate CLI. Type 'help' for commands, 'exit' to quit.");
        out.flush();
        // system(true) for a real terminal; dumb(true) degrades silently to a dumb terminal when
        // there is no TTY (piped / redirected input) instead of printing a JLine warning.
        try (Terminal terminal = TerminalBuilder.builder().system(true).dumb(true).build()) {
            this.terminal = () -> !"dumb".equalsIgnoreCase(terminal.getType());
            if (prompter == null) {
                // bind the masked-input reader to the REPL's own terminal (which this try owns and closes)
                prompter = new JLinePrompter(terminal, false);
            }
            LineReader reader = readerFor(terminal,
                    TapstateCompleter.forRepl(commandLine, SchemaNavigator.bundled()));
            // Ctrl-C stops an in-flight watch/follow stream. The line reader saves and restores the signal
            // handlers around readLine (where Ctrl-C stays "clear the line"), so this handler is active only
            // while a dispatched verb runs -- exactly when a stream is blocking the loop.
            terminal.handle(Terminal.Signal.INT, signal -> cancelStream());
            // A dumb terminal answers zero rather than failing, which would render every frame at
            // nothing wide; the conventional width stands in for it.
            screenWidth = () -> terminal.getWidth() > 0 ? terminal.getWidth() : DEFAULT_SCREEN_WIDTH;
            while (true) {
                String line;
                try {
                    line = reader.readLine(prompt());
                } catch (UserInterruptException e) {
                    continue;   // Ctrl-C clears the current line and keeps the session
                } catch (EndOfFileException e) {
                    break;      // Ctrl-D ends the session
                }
                if (!dispatch(line)) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        out.println("bye");
        out.flush();
    }
}
