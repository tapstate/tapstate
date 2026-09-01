package io.tapstate.cli;

import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.Display;
import org.jline.utils.InfoCmp;
import org.jline.utils.NonBlockingReader;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Runtime for the first TUI surface. It owns the terminal lifecycle and delegates command semantics to
 * the existing REPL, so the full-screen face cannot drift from the tested CLI command language.
 */
final class TuiApp {

    private static final int DEFAULT_WIDTH = 100;
    private static final int DEFAULT_HEIGHT = 24;
    private static final int READ_TIMEOUT_MILLIS = 250;
    private static final long DASHBOARD_REFRESH_MILLIS = 1000L;
    private static final String PALETTE_NOTICE =
            "commands: ↑/↓ choose · Enter select · Esc close";
    private static final List<String> PALETTE_COMMANDS = List.of(
            "ls", "pwd", "help", "context", "auth status", "connect", "disconnect", "exit",
            "auth login", "auth logout", ":ctx", "logout");

    private final Repl repl;
    private final StringWriter out;
    private final StringWriter err;
    private final TuiDashboard dashboard = new TuiDashboard();
    private final String initialContext;
    private final ContextResolver contextResolver;
    private final TuiSessionRecovery sessionRecovery;
    private final AtomicBoolean interrupted = new AtomicBoolean();

    private NonBlockingReader reader;
    private Display display;
    private Terminal terminal;
    private TuiAppState uiState;
    private final TuiKernel kernel;
    private final TuiCommandHistory history = new TuiCommandHistory();
    /** True while a command is resolving a target or waiting on the control plane. */
    private boolean commandRunning;
    private long recoveryScheduledGeneration = -1L;
    private String pendingContextSelection;

    TuiApp(Repl repl, StringWriter out, StringWriter err, String initialContext) {
        this(repl, out, err, initialContext, null, null);
    }

    TuiApp(Repl repl, StringWriter out, StringWriter err, String initialContext,
           ContextResolver contextResolver, AuthService authService) {
        this.repl = repl;
        this.out = out;
        this.err = err;
        this.initialContext = initialContext;
        this.contextResolver = contextResolver;
        this.sessionRecovery = authService == null ? null : new TuiSessionRecovery(authService,
                command -> Thread.startVirtualThread(command));
        this.uiState = TuiAppState.initial(consumeOutput());
        this.kernel = new TuiKernel(uiState);
        repl.tuiContextSelection(name -> pendingContextSelection = name);
        repl.prompter(new TuiPrompter(this::promptText, this::promptChoice, this::promptLines));
    }

    int run() {
        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
            this.terminal = terminal;
            Attributes original = terminal.enterRawMode();
            this.display = new Display(terminal, true);
            terminal.handle(Terminal.Signal.INT, signal -> {
                interrupted.set(true);
                repl.cancelStream();
            });
            terminal.puts(InfoCmp.Capability.enter_ca_mode);
            terminal.puts(InfoCmp.Capability.clear_screen);
            terminal.puts(InfoCmp.Capability.cursor_invisible);
            terminal.flush();
            try {
                initializeContextSession();
                draw(display, terminal);
                startContextRecoveryIfNeeded();
                return eventLoop(display, terminal);
            } finally {
                if (sessionRecovery != null) {
                    sessionRecovery.clear();
                }
                display.reset();
                terminal.puts(InfoCmp.Capability.cursor_visible);
                terminal.puts(InfoCmp.Capability.exit_ca_mode);
                terminal.setAttributes(original);
                terminal.flush();
                this.uiState = reduce(new TuiAction.ClearPrompt());
                this.reader = null;
                this.display = null;
                this.terminal = null;
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    static int requireInteractiveTerminal(BooleanSupplier terminalCheck, PrintWriter err) {
        if (terminalCheck != null && terminalCheck.getAsBoolean()) {
            return Cli.EXIT_OK;
        }
        Diagnostics.printText(err, CliError.TUI_REQUIRES_TTY, Map.of());
        return Cli.EXIT_USAGE;
    }

    static boolean hasInteractiveTerminal() {
        try (Terminal candidate = TerminalBuilder.builder().system(true).dumb(true).build()) {
            return !"dumb".equalsIgnoreCase(candidate.getType());
        } catch (IOException ignored) {
            return false;
        }
    }

    private int eventLoop(Display display, Terminal terminal) throws IOException {
        this.reader = terminal.reader();
        int lastWidth = -1;
        int lastHeight = -1;
        long nextDashboardRefresh = 0L;
        while (true) {
            if (kernel.drain()) {
                uiState = kernel.state();
                installRecoveredSessionIfPresent();
                startContextRecoveryIfNeeded();
                draw(display, terminal);
            }
            if (kernel.exitRequested()) {
                return Cli.EXIT_OK;
            }
            if (interrupted.getAndSet(false)) {
                cancelCurrentOperation();
                draw(display, terminal);
                continue;
            }
            int width = dimension(terminal.getWidth(), DEFAULT_WIDTH);
            int height = dimension(terminal.getHeight(), DEFAULT_HEIGHT);
            if (width != lastWidth || height != lastHeight) {
                kernel.dispatch(new TuiEvent.Resize(width, height));
                draw(display, terminal);
                lastWidth = width;
                lastHeight = height;
                nextDashboardRefresh = System.nanoTime() + Duration.ofMillis(DASHBOARD_REFRESH_MILLIS).toNanos();
            }
            int code = reader.read(READ_TIMEOUT_MILLIS);
            if (code == NonBlockingReader.READ_EXPIRED) {
                if (System.nanoTime() >= nextDashboardRefresh) {
                    kernel.dispatch(new TuiEvent.Tick());
                    uiState = kernel.state();
                    draw(display, terminal);
                    nextDashboardRefresh = System.nanoTime() + Duration.ofMillis(DASHBOARD_REFRESH_MILLIS).toNanos();
                }
                continue;
            }
            if (code < 0) {
                kernel.dispatch(new TuiEvent.InputClosed());
                return Cli.EXIT_OK;
            }
            if (code == TuiCommandBar.ESCAPE) {
                EscapeKey key = readEscapeKey(reader);
                if (key == EscapeKey.UP || key == EscapeKey.DOWN) {
                    navigate(key);
                } else {
                    closePaletteOrClearCommand();
                }
                draw(display, terminal);
                continue;
            }
            TuiCommandBar.Update update = TuiCommandBar.accept(uiState.command(), code);
            kernel.dispatch(new TuiEvent.Key(code));
            uiState = kernel.state();
            if (uiState.paletteOpen() && code == TuiCommandBar.CTRL_P) {
                uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
                draw(display, terminal);
                continue;
            }
            if (uiState.paletteOpen() && code == TuiCommandBar.ENTER) {
                String selected = uiState.palette().get(uiState.paletteIndex());
                uiState = reduce(
                        new TuiAction.SelectPaletteCommand(selected, "selected: " + selected + " · Enter run"));
                draw(display, terminal);
                continue;
            }
            if (uiState.paletteOpen() && (code == TuiCommandBar.BACKSPACE || code == TuiCommandBar.DELETE
                    || (code >= 32 && !Character.isISOControl(code)))) {
                uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
            }
            switch (update.event()) {
                case CANCEL -> cancelCurrentOperation();
                case QUIT -> {
                    return Cli.EXIT_OK;
                }
                case PALETTE -> togglePalette();
                case SUBMIT -> {
                    if (!submit()) {
                        return Cli.EXIT_OK;
                    }
                }
                case NONE -> {
                    // The changed command buffer is rendered below.
                }
            }
            draw(display, terminal);
            nextDashboardRefresh = System.nanoTime() + Duration.ofMillis(DASHBOARD_REFRESH_MILLIS).toNanos();
        }
    }

    private void cancelCurrentOperation() {
        repl.cancelStream();
        uiState = reduce(new TuiAction.ClearCommand());
        uiState = reduce(
                new TuiAction.SetNotice(commandRunning ? "cancellation requested" : "command cleared"));
    }

    private boolean submit() {
        String line = uiState.command().trim();
        uiState = reduce(new TuiAction.ClearCommand());
        uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
        if (line.isEmpty()) {
            history.reset();
            uiState = reduce(new TuiAction.SetNotice("ready"));
            return true;
        }
        history.record(line);
        clearOutput();
        String safeCommand = TuiActivity.command(line);
        uiState = reduce(new TuiAction.AppendActivity("> " + safeCommand));
        uiState = reduce(new TuiAction.SetNotice("running: " + safeCommand));
        commandRunning = true;
        pendingContextSelection = null;
        // Give the user one frame that reflects the network transition before a lazy context
        // resolution or control-plane request blocks this event-loop thread.
        draw(display, terminal);
        CommandResult commandResult;
        try {
            commandResult = repl.registry().dispatch(repl, repl.registry().invocation(line));
        } finally {
            commandRunning = false;
        }
        if (line.equals(":ctx")) {
            if (pendingContextSelection != null) {
                switchToSelectedContext(pendingContextSelection);
            } else {
                switchToCurrentWorkspaceBinding();
            }
        }
        String result = consumeOutput();
        if (!result.isBlank()) {
            String marker = commandResult.exitCode() == Cli.EXIT_OK ? "✓ " : "✕ ";
            uiState = reduce(new TuiAction.AppendActivity(marker + result));
            uiState = reduce(new TuiAction.SetNotice(result));
        } else if (commandResult.exitCode() == Cli.EXIT_OK) {
            uiState = reduce(new TuiAction.AppendActivity("✓ ready"));
            uiState = reduce(new TuiAction.SetNotice("ready"));
        } else {
            String failure = "command failed (exit " + commandResult.exitCode() + ")";
            uiState = reduce(new TuiAction.AppendActivity("✕ " + failure));
            uiState = reduce(
                    new TuiAction.SetNotice(failure));
        }
        return commandResult.keepRunning();
    }

    /** Reads a single TUI-owned answer while keeping the dashboard and raw terminal active. */
    private String promptText(String question, String defaultValue, boolean secret) {
        if (reader == null || display == null || terminal == null) {
            return "";
        }
        String hint = defaultValue == null || defaultValue.isBlank()
                ? "Enter submit · Esc close"
                : "default: " + defaultValue + " · Enter accept · Esc close";
        StringBuilder input = new StringBuilder();
        uiState = reduce(new TuiAction.SetPrompt(
                TuiDashboard.Prompt.text(question, "", hint, secret)));
        draw(display, terminal);
        try {
            while (true) {
                int code = readPromptCode();
                if (code < 0 || code == TuiCommandBar.CTRL_C || code == TuiCommandBar.ESCAPE) {
                    return "";
                }
                if (code == TuiCommandBar.ENTER || code == TuiCommandBar.CARRIAGE_RETURN) {
                    return input.toString();
                }
                if (code == TuiCommandBar.BACKSPACE || code == TuiCommandBar.DELETE) {
                    deleteLastCodePoint(input);
                } else if (isPrintable(code)) {
                    appendCodePoint(input, code);
                } else {
                    continue;
                }
                uiState = reduce(new TuiAction.SetPrompt(
                        TuiDashboard.Prompt.text(question, input.toString(), hint, secret)));
                draw(display, terminal);
            }
        } finally {
            uiState = reduce(new TuiAction.ClearPrompt());
            draw(display, terminal);
        }
    }

    /** Reads a menu answer with arrow navigation and the same numbered choices shown by the plain REPL. */
    private String promptChoice(String question, List<String> options) {
        if (reader == null || display == null || terminal == null || options == null || options.isEmpty()) {
            return "";
        }
        int selected = 0;
        uiState = reduce(new TuiAction.SetPrompt(
                TuiDashboard.Prompt.choice(question, options, selected)));
        draw(display, terminal);
        try {
            while (true) {
                int code = readPromptCode();
                if (code < 0 || code == TuiCommandBar.CTRL_C) {
                    return options.getLast();
                }
                if (code == TuiCommandBar.ESCAPE) {
                    return options.getLast();
                }
                if (code == TuiCommandBar.ENTER || code == TuiCommandBar.CARRIAGE_RETURN) {
                    return options.get(selected);
                }
                if (code == '1' && options.size() >= 1) {
                    return options.getFirst();
                }
                if (code >= '2' && code <= '9' && code - '1' < options.size()) {
                    return options.get(code - '1');
                }
                EscapeKey key = escapeKeyFrom(code);
                if (key == EscapeKey.UP) {
                    selected = Math.max(0, selected - 1);
                } else if (key == EscapeKey.DOWN) {
                    selected = Math.min(options.size() - 1, selected + 1);
                } else {
                    continue;
                }
                uiState = reduce(new TuiAction.SetPrompt(
                        TuiDashboard.Prompt.choice(question, options, selected)));
                draw(display, terminal);
            }
        } finally {
            uiState = reduce(new TuiAction.ClearPrompt());
            draw(display, terminal);
        }
    }

    /** Captures a block in-place, terminating on a line containing only a dot. */
    private String promptLines(String question) {
        if (reader == null || display == null || terminal == null) {
            return "";
        }
        List<String> lines = new ArrayList<>();
        StringBuilder input = new StringBuilder();
        uiState = reduce(new TuiAction.SetPrompt(
                TuiDashboard.Prompt.lines(question, lines, "")));
        draw(display, terminal);
        try {
            while (true) {
                int code = readPromptCode();
                if (code < 0 || code == TuiCommandBar.CTRL_C || code == TuiCommandBar.ESCAPE) {
                    return String.join("\n", lines);
                }
                if (code == TuiCommandBar.ENTER || code == TuiCommandBar.CARRIAGE_RETURN) {
                    String line = input.toString();
                    if (line.strip().equals(".")) {
                        return String.join("\n", lines);
                    }
                    lines.add(line);
                    input.setLength(0);
                } else if (code == TuiCommandBar.BACKSPACE || code == TuiCommandBar.DELETE) {
                    deleteLastCodePoint(input);
                } else if (isPrintable(code)) {
                    appendCodePoint(input, code);
                } else {
                    continue;
                }
                uiState = reduce(new TuiAction.SetPrompt(
                        TuiDashboard.Prompt.lines(question, lines, input.toString())));
                draw(display, terminal);
            }
        } finally {
            uiState = reduce(new TuiAction.ClearPrompt());
            draw(display, terminal);
        }
    }

    private int readPromptCode() {
        try {
            while (true) {
                int code = reader.read(READ_TIMEOUT_MILLIS);
                if (code != NonBlockingReader.READ_EXPIRED) {
                    if (code == TuiCommandBar.ESCAPE) {
                        EscapeKey key = readEscapeKey(reader);
                        if (key == EscapeKey.UP) {
                            return EscapeCodes.UP;
                        }
                        if (key == EscapeKey.DOWN) {
                            return EscapeCodes.DOWN;
                        }
                        return TuiCommandBar.ESCAPE;
                    }
                    return code;
                }
                if (interrupted.get()) {
                    return TuiCommandBar.CTRL_C;
                }
            }
        } catch (IOException failure) {
            uiState = reduce(
                    new TuiAction.SetNotice("prompt failed: " + failure.getMessage()));
            return -1;
        }
    }

    private static EscapeKey escapeKeyFrom(int code) {
        if (code == EscapeCodes.UP) {
            return EscapeKey.UP;
        }
        if (code == EscapeCodes.DOWN) {
            return EscapeKey.DOWN;
        }
        return EscapeKey.ESCAPE;
    }

    private static boolean isPrintable(int code) {
        return code >= 32 && code != 127 && !Character.isISOControl(code);
    }

    private static void appendCodePoint(StringBuilder value, int code) {
        if (Character.isValidCodePoint(code)) {
            value.appendCodePoint(code);
        } else {
            value.append((char) code);
        }
    }

    private static void deleteLastCodePoint(StringBuilder value) {
        if (value.length() > 0) {
            value.delete(value.offsetByCodePoints(value.length(), -1), value.length());
        }
    }

    private void draw(Display display, Terminal terminal) {
        int width = dimension(terminal.getWidth(), DEFAULT_WIDTH);
        int height = dimension(terminal.getHeight(), DEFAULT_HEIGHT);
        display.resize(width, height);
        // JLine's Display mutates the frame while reconciling rows. Keep the pure renderer's
        // immutable result at the boundary and hand JLine its own mutable frame.
        display.update(new ArrayList<>(dashboard.render(state(), width, height)), -1);
        terminal.flush();
    }

    private TuiAppState reduce(TuiAction action) {
        kernel.dispatch(new TuiEvent.ActionPosted(action));
        return kernel.state();
    }

    private void initializeContextSession() {
        ResolvedContext.Named context = resolveInitialContext();
        uiState = initializeContextSessionState(kernel, uiState, context, uiState.notice());
        if (context != null) {
            repl.selectContextForTui(context);
        }
    }

    static TuiAppState initializeContextSessionState(TuiAppState state, ResolvedContext.Named context,
                                                     String resolverNotice) {
        TuiAppState initialized = TuiReducer.reduce(state,
                new TuiAction.ContextSession(new TuiContextSessionAction.Initialize(context)));
        if (resolverNotice == null || resolverNotice.isBlank()) {
            return initialized;
        }
        return TuiReducer.reduce(initialized, new TuiAction.SetNotice(resolverNotice));
    }

    static TuiAppState initializeContextSessionState(TuiKernel kernel, TuiAppState state,
                                                     ResolvedContext.Named context, String resolverNotice) {
        Objects.requireNonNull(kernel, "kernel");
        if (kernel.state() != state) {
            throw new IllegalArgumentException("kernel must own the supplied state");
        }
        kernel.dispatch(new TuiEvent.ContextSessionPosted(new TuiContextSessionAction.Initialize(context)));
        if (resolverNotice != null && !resolverNotice.isBlank()) {
            kernel.dispatch(new TuiEvent.ActionPosted(new TuiAction.SetNotice(resolverNotice)));
        }
        return kernel.state();
    }

    private ResolvedContext.Named resolveInitialContext() {
        if (contextResolver == null) {
            return null;
        }
        try {
            return contextResolver.resolve(null, initialContext, repl.workdir())
                    .filter(ResolvedContext.Named.class::isInstance)
                    .map(ResolvedContext.Named.class::cast)
                    .orElse(null);
        } catch (io.tapstate.core.common.TapstateException failure) {
            uiState = reduce(new TuiAction.SetNotice(failure.code().code()));
            return null;
        }
    }

    private void startContextRecoveryIfNeeded() {
        TuiContextSessionState contextSession = uiState.contextSession();
        if (sessionRecovery == null || !contextSession.recoveryRequested()
                || contextSession.generation() == recoveryScheduledGeneration) {
            return;
        }
        recoveryScheduledGeneration = contextSession.generation();
        sessionRecovery.start(contextSession, kernel::post);
    }

    private void installRecoveredSessionIfPresent() {
        if (sessionRecovery == null) {
            return;
        }
        sessionRecovery.take(uiState.contextSession()).ifPresent(active ->
                repl.installRecoveredSessionForTui(uiState.contextSession().context(), active));
    }

    /** Switches scope on the UI thread; recovery completion is generation-gated by the reducer. */
    void switchContext(ResolvedContext.Named context) {
        TuiContextSessionState before = uiState.contextSession();
        uiState = reduce(new TuiAction.ContextSession(new TuiContextSessionAction.SwitchContext(context)));
        if (uiState.contextSession().generation() != before.generation()) {
            repl.selectContextForTui(context);
            startContextRecoveryIfNeeded();
        }
    }

    private void switchToCurrentWorkspaceBinding() {
        if (contextResolver == null || uiState.contextSession().writeOperationId() != null) {
            return;
        }
        try {
            ResolvedContext.Named resolved = contextResolver.resolve(null, null, repl.workdir())
                    .filter(ResolvedContext.Named.class::isInstance)
                    .map(ResolvedContext.Named.class::cast)
                    .orElse(null);
            if (resolved == null) {
                if (uiState.contextSession().context() != null) {
                    clearContextSession();
                }
            } else if (!Objects.equals(resolved, uiState.contextSession().context())) {
                switchContext(resolved);
            }
        } catch (io.tapstate.core.common.TapstateException failure) {
            uiState = reduce(new TuiAction.SetNotice(failure.code().code()));
        }
    }

    private void switchToSelectedContext(String name) {
        if (contextResolver == null || uiState.contextSession().writeOperationId() != null) {
            return;
        }
        try {
            ResolvedContext.Named resolved = contextResolver.resolve(null, name, repl.workdir())
                    .filter(ResolvedContext.Named.class::isInstance)
                    .map(ResolvedContext.Named.class::cast)
                    .orElse(null);
            if (resolved != null && !Objects.equals(resolved, uiState.contextSession().context())) {
                switchContext(resolved);
            }
        } catch (io.tapstate.core.common.TapstateException failure) {
            uiState = reduce(new TuiAction.SetNotice(failure.code().code()));
        }
    }

    private void clearContextSession() {
        repl.clearContextForTui();
        uiState = reduce(new TuiAction.ContextSession(new TuiContextSessionAction.Initialize(null)));
        recoveryScheduledGeneration = -1L;
    }

    private TuiDashboard.State state() {
        Session session = repl.session();
        TuiContextSessionState contextSession = uiState.contextSession();
        TuiDashboard.Connection connection = contextSession.connection();
        if (connection == TuiDashboard.Connection.ONBOARDING && session.isConnected()) {
            connection = TuiDashboard.Connection.ONLINE;
        } else if (connection == TuiDashboard.Connection.ONBOARDING && commandRunning) {
            connection = TuiDashboard.Connection.CONNECTING;
        }
        String context = contextSession.context() == null ? repl.contextName() : contextSession.context().name();
        if (context == null || context.isBlank()) {
            context = initialContext;
        }
        String principal = contextSession.principal() == null ? session.principal() : contextSession.principal();
        String notice = contextSession.notice().isBlank() ? uiState.notice() : contextSession.notice();
        return new TuiDashboard.State(repl.workdir(), context, principal, connection, notice,
                uiState.command(), uiState.palette(), uiState.paletteIndex(), uiState.prompt(),
                session.landingNode() == null ? null : session.landingNode().toString(),
                session.clusterName(), authStatus(contextSession, session), uiState.activity(),
                TuiWorkspaceSnapshot.scan(repl.workdir()));
    }

    private static String authStatus(TuiContextSessionState contextSession, Session session) {
        if (contextSession.connection() == TuiDashboard.Connection.ONLINE) {
            return "persistent session · refresh on demand";
        }
        if (contextSession.connection() == TuiDashboard.Connection.SIGNED_OUT
                || contextSession.connection() == TuiDashboard.Connection.SESSION_EXPIRED) {
            return "sign in required";
        }
        if (!session.isConnected()) {
            return "not connected";
        }
        if (!session.isAuthenticated()) {
            return "not authenticated";
        }
        if (session.hasMachineCredential()) {
            return "machine token";
        }
        Instant absolute = session.sessionAbsoluteExpiresAt();
        if (absolute == null) {
            return "process session · access refresh unavailable";
        }
        return "persistent session · refresh on demand · expires " + remaining(absolute);
    }

    private static String remaining(Instant expiry) {
        long seconds = Duration.between(Instant.now(), expiry).toSeconds();
        if (seconds <= 0) {
            return "expired";
        }
        if (seconds < 60) {
            return "<1m";
        }
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + "m";
        }
        long hours = minutes / 60;
        long remainder = minutes % 60;
        return remainder == 0 ? hours + "h" : hours + "h " + remainder + "m";
    }

    private String consumeOutput() {
        String stdout = out.toString();
        String stderr = err.toString();
        clearOutput();
        List<String> lines = new ArrayList<>();
        addNonBlank(lines, stdout);
        addNonBlank(lines, stderr);
        if (lines.isEmpty()) {
            return "";
        }
        return TuiActivity.result(lines.getLast());
    }

    private void clearOutput() {
        out.getBuffer().setLength(0);
        err.getBuffer().setLength(0);
    }

    private static void addNonBlank(List<String> lines, String value) {
        for (String line : value.split("\\R")) {
            if (!line.isBlank()) {
                lines.add(line);
            }
        }
    }

    private void togglePalette() {
        if (uiState.paletteOpen()) {
            uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
        } else {
            uiState = reduce(
                    new TuiAction.OpenPalette(PALETTE_COMMANDS, PALETTE_NOTICE));
        }
    }

    private void navigate(EscapeKey key) {
        if (uiState.paletteOpen()) {
            uiState = reduce(
                    new TuiAction.MovePalette(key == EscapeKey.UP ? -1 : 1));
            uiState = reduce(new TuiAction.SetNotice(PALETTE_NOTICE));
            return;
        }
        String next = key == EscapeKey.UP ? history.previous(uiState.command()) : history.next();
        uiState = reduce(new TuiAction.SetCommand(next));
    }

    private void closePaletteOrClearCommand() {
        if (uiState.paletteOpen()) {
            uiState = reduce(new TuiAction.ClosePalette("ready"));
        } else {
            TuiCommandBar.Update update = TuiCommandBar.accept(uiState.command(), TuiCommandBar.ESCAPE);
            uiState = reduce(new TuiAction.SetCommand(update.value()));
        }
    }

    private static EscapeKey readEscapeKey(NonBlockingReader reader) throws IOException {
        int next = reader.read(15);
        if (next != '[' && next != 'O') {
            return EscapeKey.ESCAPE;
        }
        return switch (reader.read(15)) {
            case 'A' -> EscapeKey.UP;
            case 'B' -> EscapeKey.DOWN;
            case 'C' -> EscapeKey.RIGHT;
            case 'D' -> EscapeKey.LEFT;
            default -> EscapeKey.ESCAPE;
        };
    }

    private enum EscapeKey {
        ESCAPE, UP, DOWN, LEFT, RIGHT
    }

    private static final class EscapeCodes {
        private static final int UP = -1001;
        private static final int DOWN = -1002;

        private EscapeCodes() {
        }
    }

    private static int dimension(int value, int fallback) {
        return value > 0 ? value : fallback;
    }

}
