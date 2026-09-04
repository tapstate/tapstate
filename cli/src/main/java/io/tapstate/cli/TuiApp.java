package io.tapstate.cli;

import dev.tamboui.backend.jline3.JLineBackend;
import dev.tamboui.inline.InlineDisplay;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.NonBlockingReader;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Runtime for the interactive CLI surface. It owns the terminal lifecycle and delegates command
 * semantics to the existing REPL, so the inline face cannot drift from the tested CLI language.
 */
final class TuiApp {

    private static final int DEFAULT_WIDTH = 100;
    private static final int DEFAULT_HEIGHT = 24;
    private static final int INLINE_DISPLAY_HEIGHT = 16;
    private static final int READ_TIMEOUT_MILLIS = 50;
    // Terminal emulators may deliver ESC and the rest of an arrow sequence in separate input chunks.
    // Keep standalone Escape responsive while allowing that split sequence to remain prompt-owned.
    private static final int ESCAPE_SEQUENCE_START_TIMEOUT_MILLIS = 250;
    private static final int ESCAPE_SEQUENCE_BODY_TIMEOUT_MILLIS = 100;
    private static final long DASHBOARD_REFRESH_MILLIS = 5000L;
    private static final String PALETTE_NOTICE =
            "commands: ↑/↓ choose · Enter select · Esc close";
    private static final String SUGGESTIONS_NOTICE =
            "suggestions · ↑/↓ choose · Enter select";
    private final Repl repl;
    private final StringWriter out;
    private final StringWriter err;
    private final TamboDashboard dashboard = new TamboDashboard();
    private final String initialContext;
    private final ContextResolver contextResolver;
    private final TuiSessionRecovery sessionRecovery;
    private final TuiResourceRefresh resourceRefresh;
    private final AtomicBoolean interrupted = new AtomicBoolean();
    private final AtomicBoolean terminationRequested = new AtomicBoolean();
    private final AtomicBoolean resizeRequested = new AtomicBoolean();
    private final ConcurrentLinkedQueue<TuiCommandExecution.Completion> commandCompletions =
            new ConcurrentLinkedQueue<>();

    private NonBlockingReader reader;
    private InlineDisplay display;
    private JLineBackend backend;
    private Terminal terminal;
    private TuiAppState uiState;
    private final TuiKernel kernel;
    private final TuiCommandHistory history = new TuiCommandHistory();
    /** True while a command is resolving a target or waiting on the control plane. */
    private boolean commandRunning;
    private String activeCommandLine;
    private String activeOperationId;
    private String stoppedOperationId;
    private boolean commandExitRequested;
    private final TuiCommandExecution commandExecution;
    private long recoveryScheduledGeneration = -1L;
    private long refreshSequence;
    private String pendingContextSelection;
    private String commandInput = "";
    private boolean suggestionsVisible;
    private int pendingInputCode = Integer.MIN_VALUE;
    private java.nio.file.Path workspaceSnapshotRoot;
    private List<TuiDashboard.ResourceSummary> workspaceSnapshot = List.of();

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
        this.resourceRefresh = new TuiResourceRefresh(repl.controlPlane(),
                command -> Thread.startVirtualThread(command));
        this.commandExecution = new TuiCommandExecution(commandCompletions::add);
        this.uiState = TuiAppState.initial(consumeOutput());
        this.kernel = new TuiKernel(uiState);
        repl.tuiContextSelection(name -> pendingContextSelection = name);
        repl.prompter(new TuiPrompter(this::promptText, this::promptChoice, this::promptLines));
    }

    int run() {
        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
            this.terminal = terminal;
            this.backend = new JLineBackend(terminal);
            backend.enableRawMode();
            this.display = InlineDisplay.withBackend(INLINE_DISPLAY_HEIGHT, backend);
            backend.onResize(() -> resizeRequested.set(true));
            terminal.handle(Terminal.Signal.INT, signal -> {
                interrupted.set(true);
                repl.cancelStream();
            });
            Thread shutdownHook = new Thread(() -> restoreTerminal(display),
                    "tapstate-tui-terminal-restore");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                initializeContextSession();
                this.reader = terminal.reader();
                draw(display, terminal);
                startContextRecoveryIfNeeded();
                return eventLoop(display, terminal);
            } finally {
                if (sessionRecovery != null) {
                    sessionRecovery.clear();
                }
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
                commandExecution.close();
                restoreTerminal(display);
                if (display != null) {
                    display.close();
                }
                this.uiState = reduce(new TuiAction.ClearPrompt());
                this.reader = null;
                this.display = null;
                this.terminal = null;
                this.backend = null;
            }
        } catch (Exception failure) {
            if (failure instanceof IOException ioFailure) {
                throw new UncheckedIOException(ioFailure);
            }
            throw new IllegalStateException("Unable to initialize the terminal workbench", failure);
        }
    }

    private void restoreTerminal(InlineDisplay display) {
        if (!terminationRequested.compareAndSet(false, true)) {
            return;
        }
        try {
            if (display != null) {
                display.release();
            }
            if (backend != null) {
                backend.disableRawMode();
                backend.flush();
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

    static List<String> paletteCommands(CommandRegistry registry) {
        return TuiCommandBar.paletteCommands(registry);
    }

    static TuiOperation operationFor(String line, int sequenceOrExitCode) {
        return TuiCommandBar.operationFor(line, sequenceOrExitCode);
    }

    static String applyCompletion(String current, TuiCommandBar.Completion completion) {
        String value = current == null ? "" : current;
        if (completion == null || completion.selected().isEmpty()) {
            return value;
        }
        int end = value.length();
        int start = end;
        while (start > 0 && !Character.isWhitespace(value.charAt(start - 1))) {
            start--;
        }
        return value.substring(0, start) + completion.selected() + value.substring(end);
    }

    private static List<String> wordsForCompletion(String value) {
        if (value == null || value.isBlank()) {
            return List.of("");
        }
        List<String> words = new ArrayList<>(List.of(value.trim().split("\\s+")));
        if (Character.isWhitespace(value.charAt(value.length() - 1))) {
            words.add("");
        }
        return List.copyOf(words);
    }

    private int eventLoop(InlineDisplay display, Terminal terminal) throws IOException {
        int lastWidth = -1;
        int lastHeight = -1;
        long nextDashboardRefresh = 0L;
        while (true) {
            drainCommandCompletions();
            if (kernel.drain()) {
                uiState = kernel.state();
                installRecoveredSessionIfPresent();
                startContextRecoveryIfNeeded();
                draw(display, terminal);
            }
            if (kernel.exitRequested()) {
                return Cli.EXIT_OK;
            }
            if (commandExitRequested) {
                return Cli.EXIT_OK;
            }
            if (terminationRequested.get()) {
                return Cli.EXIT_OK;
            }
            if (interrupted.getAndSet(false)) {
                cancelCurrentOperation();
                draw(display, terminal);
                continue;
            }
            dev.tamboui.layout.Size size = backend.size();
            int width = dimension(size.width(), DEFAULT_WIDTH);
            int height = dimension(size.height(), DEFAULT_HEIGHT);
            if (resizeRequested.getAndSet(false) || width != lastWidth || height != lastHeight) {
                kernel.dispatch(new TuiEvent.Resize(width, height));
                draw(display, terminal);
                lastWidth = width;
                lastHeight = height;
                nextDashboardRefresh = System.nanoTime() + Duration.ofMillis(DASHBOARD_REFRESH_MILLIS).toNanos();
            }
            if (System.nanoTime() >= nextDashboardRefresh) {
                startDashboardRefreshIfNeeded(width);
                nextDashboardRefresh = System.nanoTime() + Duration.ofMillis(DASHBOARD_REFRESH_MILLIS).toNanos();
            }
            int code = readInputCode(READ_TIMEOUT_MILLIS);
            if (code == NonBlockingReader.READ_EXPIRED) {
                if (System.nanoTime() >= nextDashboardRefresh) {
                    kernel.dispatch(new TuiEvent.Tick());
                    uiState = kernel.state();
                    startDashboardRefreshIfNeeded(width);
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
                EscapeKey key = readEscapeKey();
                if (key == EscapeKey.UP || key == EscapeKey.DOWN) {
                    navigate(key);
                } else {
                    closePaletteOrClearCommand();
                }
                draw(display, terminal);
                continue;
            }
            if (code == 9) {
                List<String> words = wordsForCompletion(commandInput);
                int wordIndex = Math.max(0, words.size() - 1);
                TuiCommandBar.Completion completion = TuiCommandBar.suggestions(
                        repl.registry().completer(), history, words, wordIndex);
                if (completion.candidates().size() == 1) {
                    commandInput = applyCompletion(commandInput, completion);
                    uiState = reduce(new TuiAction.SetCommand(
                            commandInput));
                    uiState = reduce(new TuiAction.SetNotice("completed: " + completion.selected()));
                } else if (!completion.candidates().isEmpty()) {
                    uiState = reduce(new TuiAction.OpenPalette(completion.candidates(),
                            completion.candidates().size() + " completions · ↑/↓ choose · Enter select"));
                } else {
                    uiState = reduce(new TuiAction.SetNotice("no completions"));
                }
                draw(display, terminal);
                continue;
            }
            if (uiState.paletteOpen() && isEnter(code)) {
                String selected = uiState.palette().get(uiState.paletteIndex());
                commandInput = selected;
                suggestionsVisible = false;
                uiState = reduce(
                        new TuiAction.SelectPaletteCommand(selected, "selected: " + selected + " · Enter run"));
                draw(display, terminal);
                continue;
            }
            TuiCommandBar.Update update = TuiCommandBar.accept(commandInput, code);
            commandInput = update.value();
            kernel.dispatch(new TuiEvent.Key(code));
            uiState = kernel.state();
            uiState = reduce(new TuiAction.SetCommand(commandInput));
            if (uiState.paletteOpen() && code == TuiCommandBar.CTRL_P) {
                uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
                draw(display, terminal);
                continue;
            }
            if (uiState.paletteOpen() && (code == TuiCommandBar.BACKSPACE || code == TuiCommandBar.DELETE
                    || (code >= 32 && !Character.isISOControl(code)))) {
                if (!suggestionsVisible) {
                    uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
                }
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
                    if (isEditableInput(code)) {
                        refreshSuggestions();
                    }
                }
            }
            draw(display, terminal);
            nextDashboardRefresh = System.nanoTime() + Duration.ofMillis(DASHBOARD_REFRESH_MILLIS).toNanos();
        }
    }

    private void cancelCurrentOperation() {
        TuiOperation operation = uiState.operation();
        if (commandRunning && (operation == null || !operation.submittedWrite())) {
            commandExecution.interrupt();
        }
        if (operation != null) {
            TuiOperation.CtrlCResult result = operation.onCtrlC();
            if (result.remoteCancellationRequested()) {
                repl.cancelStream();
            }
            uiState = reduce(new TuiAction.SetOperation(result.operation()));
            uiState = reduce(new TuiAction.SetNotice(result.message()));
            if (result.action() == TuiOperation.CtrlCAction.STOP_WAITING) {
                stoppedOperationId = operation.id();
                commandRunning = false;
                activeOperationId = null;
                activeCommandLine = null;
            }
        }
        uiState = reduce(new TuiAction.ClearCommand());
        commandInput = "";
        suggestionsVisible = false;
        if (operation == null) {
            uiState = reduce(new TuiAction.AppendActivity("command cleared"));
            uiState = reduce(new TuiAction.SetNotice("command cleared"));
        }
    }

    private boolean submit() {
        if (commandRunning || commandExecution.isRunning()) {
            uiState = reduce(new TuiAction.SetNotice("operation in flight; press Ctrl-C to cancel or wait"));
            return true;
        }
        String line = commandInput.trim();
        commandInput = "";
        suggestionsVisible = false;
        uiState = reduce(new TuiAction.ClearCommand());
        uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
        if (line.isEmpty()) {
            TuiNavigation navigation = navigation();
            if (!navigation.selected().isEmpty()) {
                TuiNavigation detail = navigation.open();
                uiState = reduce(new TuiAction.SetNavigation(detail));
                TuiDashboard.ResourceSummary resource = selectedResource(detail.selected());
                String output = resource == null ? detail.selected()
                        : resource.id() + "\n" + resource.kind() + "\n" + resource.detail();
                uiState = reduce(new TuiAction.SetResultPane(TuiCommandBar.project(
                        new CommandResult(true, Cli.EXIT_OK), output)));
                printToScrollback(output);
                uiState = reduce(new TuiAction.SetNotice("details: " + detail.selected()));
                return true;
            }
            history.reset();
            uiState = reduce(new TuiAction.SetNotice("ready"));
            return true;
        }
        history.record(line);
        clearOutput();
        String safeCommand = TuiActivity.command(line);
        LoginInput loginInput = prepareInteractiveLogin(line);
        if (isInteractiveLogin(line) && loginInput == null) {
            uiState = reduce(new TuiAction.SetNotice("login cancelled"));
            return true;
        }
        int operationSequence = (int) Math.min(Integer.MAX_VALUE, ++refreshSequence);
        TuiOperation operation = operationFor(line, operationSequence);
        if (isContextCommand(line) && uiState.contextSession().writeOperationId() != null) {
            String message = "cannot switch context while write operation "
                    + uiState.contextSession().writeOperationId() + " is in flight";
            uiState = reduce(new TuiAction.SetNotice(message));
            uiState = reduce(new TuiAction.AppendActivity(message));
            return true;
        }
        if (operation.kind() == TuiOperation.Kind.WRITE) {
            if (!writeConfirmationReady()) {
                String message = "write unavailable until the selected context is ready";
                uiState = reduce(new TuiAction.SetNotice(message));
                uiState = reduce(new TuiAction.AppendActivity(message + ": " + safeCommand));
                return true;
            }
            TuiWriteConfirmation confirmation = TuiWriteConfirmation.open(
                    operation.description(), writeTarget(), writeIssuer());
            if (!confirmWrite(confirmation)) {
                uiState = reduce(new TuiAction.SetOperation(
                        new TuiOperation(operation.id(), operation.description(), operation.kind(),
                                TuiOperation.Status.CANCELLED)));
                uiState = reduce(new TuiAction.AppendActivity("cancelled: " + safeCommand));
                uiState = reduce(new TuiAction.SetNotice("write cancelled"));
                return true;
            }
            operation = TuiOperation.submittedWrite(operation.id(), operation.description());
            uiState = reduce(new TuiAction.ContextSession(
                    new TuiContextSessionAction.SetWriteInFlight(operation.id())));
        }
        uiState = reduce(new TuiAction.SetOperation(operation));
        uiState = reduce(new TuiAction.AppendActivity("> " + safeCommand));
        uiState = reduce(new TuiAction.SetNotice("running: " + safeCommand));
        pendingContextSelection = null;
        commandRunning = true;
        activeCommandLine = line;
        activeOperationId = operation.id();
        commandExitRequested = false;
        boolean async = loginInput != null || !(requiresUiThread(line) || isTuiBuiltin(line));
        if (async) {
            commandExecution.start(operation.id(),
                    loginInput == null
                            ? () -> dispatchOnWorker(line)
                            : () -> new CommandResult(true,
                                    repl.tuiLogin(loginInput.username(), loginInput.password())),
                    this::consumeOutput);
        } else {
            completeCommand(operation.id(), dispatchOnUiThread(line), consumeOutput(), null);
        }
        return true;
    }

    private CommandResult dispatchOnUiThread(String line) {
        try {
            boolean keepRunning = repl.dispatch(line);
            return new CommandResult(keepRunning, repl.lastExitCode());
        } catch (Throwable failure) {
            return new CommandResult(true, Cli.EXIT_DIAGNOSTIC);
        }
    }

    private CommandResult dispatchOnWorker(String line) {
        try {
            boolean keepRunning = repl.dispatch(line);
            return new CommandResult(keepRunning, repl.lastExitCode());
        } catch (Throwable failure) {
            return new CommandResult(true, Cli.EXIT_DIAGNOSTIC);
        }
    }

    private static boolean isTuiBuiltin(String line) {
        return Set.of(":ctx", ":help", ":logout", ":quit").contains(line);
    }

    private static boolean isEditableInput(int code) {
        return code == TuiCommandBar.BACKSPACE || code == TuiCommandBar.DELETE
                || (code >= 32 && !Character.isISOControl(code));
    }

    private void refreshSuggestions() {
        if (commandInput.isBlank()) {
            if (suggestionsVisible) {
                suggestionsVisible = false;
                uiState = reduce(new TuiAction.ClosePalette("ready"));
            }
            return;
        }
        List<String> words = wordsForCompletion(commandInput);
        int wordIndex = Math.max(0, words.size() - 1);
        TuiCommandBar.Completion completion = TuiCommandBar.suggestions(
                repl.registry().completer(), history, words, wordIndex);
        if (completion.candidates().isEmpty()) {
            if (suggestionsVisible) {
                suggestionsVisible = false;
                uiState = reduce(new TuiAction.ClosePalette("ready"));
            }
            return;
        }
        suggestionsVisible = true;
        uiState = reduce(new TuiAction.OpenPalette(completion.candidates(), SUGGESTIONS_NOTICE));
    }

    private void drainCommandCompletions() {
        TuiCommandExecution.Completion completion;
        while ((completion = commandCompletions.poll()) != null) {
            if ((!commandRunning || !Objects.equals(activeOperationId, completion.operationId()))
                    && !Objects.equals(stoppedOperationId, completion.operationId())) {
                continue;
            }
            completeCommand(completion.operationId(), completion.result(), completion.output(), completion.failure());
        }
    }

    private void completeCommand(String operationId, CommandResult commandResult, String result, Throwable failure) {
        boolean stopped = Objects.equals(stoppedOperationId, operationId);
        if ((!commandRunning || !Objects.equals(activeOperationId, operationId)) && !stopped) {
            return;
        }
        if (stopped) {
            // A submitted write has no reliable cancellation acknowledgement. Ignore its late completion
            // so it cannot claim success, failure, or release the context write lock after Ctrl-C.
            stoppedOperationId = null;
            return;
        }
        commandRunning = false;
        activeOperationId = null;
        String line = activeCommandLine;
        activeCommandLine = null;
        TuiOperation operation = uiState.operation();
        if (line != null && line.equals(":ctx")) {
            if (pendingContextSelection != null) {
                switchToSelectedContext(pendingContextSelection);
            } else {
                switchToCurrentWorkspaceBinding();
            }
        }
        if (operation != null && operation.id().equals(operationId)) {
            uiState = reduce(new TuiAction.SetOperation(failure == null ? operation.complete() : operation.failed()));
            if (operation.kind() == TuiOperation.Kind.WRITE) {
                uiState = reduce(new TuiAction.ContextSession(
                        new TuiContextSessionAction.ClearWriteInFlight()));
            }
        }
        CommandResult safeResult = commandResult == null ? new CommandResult(true, Cli.EXIT_DIAGNOSTIC) : commandResult;
        uiState = reduce(new TuiAction.SetResultPane(TuiCommandBar.project(safeResult, result)));
        printToScrollback(result, safeResult);
        invalidateWorkspaceSnapshot();
        if (!result.isBlank()) {
            String marker = failure == null && safeResult.exitCode() == Cli.EXIT_OK ? "✓ " : "✕ ";
            uiState = reduce(new TuiAction.AppendActivity(marker + result));
            uiState = reduce(new TuiAction.SetNotice(result));
        } else if (failure == null && safeResult.exitCode() == Cli.EXIT_OK) {
            uiState = reduce(new TuiAction.AppendActivity("✓ ready"));
            uiState = reduce(new TuiAction.SetNotice("ready"));
        } else {
            String message = "command failed (exit " + safeResult.exitCode() + ")";
            uiState = reduce(new TuiAction.AppendActivity("✕ " + message));
            uiState = reduce(new TuiAction.SetNotice(message));
        }
        commandExitRequested = !safeResult.keepRunning();
    }

    /** Appends command output to terminal scrollback instead of keeping a second virtual viewport. */
    private void printToScrollback(String output, CommandResult result) {
        if (display == null) {
            return;
        }
        TuiCommandBar.ResultPane pane = TuiCommandBar.project(result, output);
        String text = String.join("\n", pane.lines());
        if (text.isBlank()) {
            text = pane.success() ? "ready" : "command failed (exit " + pane.exitCode() + ")";
        }
        display.println(text);
    }

    private void printToScrollback(String output) {
        if (display == null) {
            return;
        }
        String text = String.join("\n", TuiCommandBar.project(
                new CommandResult(true, Cli.EXIT_OK), output).lines());
        if (!text.isBlank()) {
            display.println(text);
        }
    }

    static boolean requiresUiThread(String line) {
        List<String> words = CommandInvocation.parse(line == null ? "" : line).words();
        if (words.isEmpty()) {
            return true;
        }
        String verb = words.getFirst();
        return verb.equals("ls") || verb.equals("pwd") || verb.equals("context") || verb.equals("new")
                || verb.equals(":ctx")
                || verb.equals("connect") || verb.equals("disconnect") || verb.equals("logout")
                || verb.equals("cd") || (verb.equals("auth") && words.size() > 1 && words.get(1).equals("logout"));
    }

    private boolean isInteractiveLogin(String line) {
        List<String> words = CommandInvocation.parse(line).words();
        return line.equals(":login") || (!words.isEmpty() && (words.getFirst().equals("login")
                || (words.getFirst().equals("auth") && words.size() > 1 && words.get(1).equals("login"))));
    }

    private LoginInput prepareInteractiveLogin(String line) {
        if (!isInteractiveLogin(line)) {
            return null;
        }
        List<String> words = CommandInvocation.parse(line).words();
        String username = line.equals(":login") ? ""
                : words.getFirst().equals("auth")
                ? (words.size() > 2 ? words.get(2) : "")
                : (words.size() > 1 ? words.get(1) : "");
        if (username.isBlank()) {
            username = promptText("Username", "", false);
        }
        if (username == null || username.isBlank()) {
            return null;
        }
        String password = promptText("Password", "", true);
        return password == null ? null : new LoginInput(username, password);
    }

    private record LoginInput(String username, String password) {
    }

    private static boolean isContextCommand(String line) {
        return line.equals(":ctx") || line.equals("context") || line.startsWith("context ");
    }

    private boolean confirmWrite(TuiWriteConfirmation confirmation) {
        uiState = reduce(new TuiAction.SetPrompt(
                TuiDashboard.Prompt.confirmWrite(confirmation.command(), confirmation.target(),
                        confirmation.issuer())));
        draw(display, terminal);
        try {
            int selected = 0;
            while (true) {
                int code = readPromptCode();
                if (code < 0 || code == TuiCommandBar.CTRL_C || code == TuiCommandBar.ESCAPE) {
                    return false;
                }
                if (code == TuiCommandBar.ENTER || code == TuiCommandBar.CARRIAGE_RETURN) {
                    return confirmation.options().get(selected).equals("Run");
                }
                EscapeKey key = escapeKeyFrom(code);
                if (key == EscapeKey.UP) {
                    selected = Math.max(0, selected - 1);
                } else if (key == EscapeKey.DOWN) {
                    selected = Math.min(confirmation.options().size() - 1, selected + 1);
                } else if (code == '1') {
                    return false;
                } else if (code == '2') {
                    return true;
                } else {
                    continue;
                }
                uiState = reduce(new TuiAction.SetPrompt(new TuiDashboard.Prompt(
                        confirmation.question(), "", "↑/↓ choose · Enter select · Esc cancel", false,
                        confirmation.options(), selected, List.of())));
                draw(display, terminal);
            }
        } finally {
            uiState = reduce(new TuiAction.ClearPrompt());
            draw(display, terminal);
        }
    }

    private String writeTarget() {
        TuiContextSessionState contextSession = uiState.contextSession();
        if (contextSession.context() != null && !contextSession.context().name().isBlank()) {
            return contextSession.context().name();
        }
        String context = repl.contextName();
        return context == null || context.isBlank() ? repl.workdir().toString() : context;
    }

    private String writeIssuer() {
        String issuer = uiState.contextSession().issuer();
        return issuer == null || issuer.isBlank() ? "" : issuer;
    }

    private boolean writeConfirmationReady() {
        TuiContextSessionState contextSession = uiState.contextSession();
        return contextSession.connection() == TuiDashboard.Connection.ONLINE
                && contextSession.context() != null
                && !writeTarget().isBlank()
                && !writeIssuer().isBlank();
    }

    /** Reads a single TUI-owned answer while keeping the dashboard and raw terminal active. */
    private String promptText(String question, String defaultValue, boolean secret) {
        if (reader == null || display == null || terminal == null) {
            return "";
        }
        String hint = defaultValue == null || defaultValue.isBlank()
                ? "Enter submit · Esc close"
                : "default: " + defaultValue + " · Enter accept · Esc close";
        StringBuilder input = secret ? null : new StringBuilder();
        TuiSecretInput secretInput = secret ? new TuiSecretInput() : null;
        uiState = reduce(new TuiAction.SetPrompt(
                TuiDashboard.Prompt.text(question, "", hint, secret)));
        draw(display, terminal);
        try {
            while (true) {
                int code = readPromptCode();
                if (code < 0 || code == TuiCommandBar.CTRL_C || code == TuiCommandBar.ESCAPE) {
                    return null;
                }
                if (code == TuiCommandBar.ENTER || code == TuiCommandBar.CARRIAGE_RETURN) {
                    return secret ? secretInput.take() : input.toString();
                }
                if (code == TuiCommandBar.BACKSPACE || code == TuiCommandBar.DELETE) {
                    if (secret) {
                        secretInput.deleteLastCodePoint();
                    } else {
                        deleteLastCodePoint(input);
                    }
                } else if (isPrintable(code)) {
                    if (secret) {
                        secretInput.appendCodePoint(code);
                    } else {
                        appendCodePoint(input, code);
                    }
                } else {
                    continue;
                }
                uiState = reduce(new TuiAction.SetPrompt(
                        TuiDashboard.Prompt.text(question, secret ? secretInput.masked() : input.toString(), hint, secret)));
                draw(display, terminal);
            }
        } finally {
            if (secretInput != null) {
                secretInput.close();
            }
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
                if (code == TuiCommandBar.CTRL_C || code == TuiCommandBar.ESCAPE) {
                    return options.getLast();
                }
                EscapeKey key = escapeKeyFrom(code);
                if (key == EscapeKey.UP) {
                    selected = movePromptSelection(selected, -1, options.size());
                } else if (key == EscapeKey.DOWN) {
                    selected = movePromptSelection(selected, 1, options.size());
                } else if (code < 0) {
                    return options.getLast();
                } else if (code == TuiCommandBar.ENTER || code == TuiCommandBar.CARRIAGE_RETURN) {
                    return options.get(selected);
                } else if (code == '1' && options.size() >= 1) {
                    return options.getFirst();
                } else if (code >= '2' && code <= '9' && code - '1' < options.size()) {
                    return options.get(code - '1');
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
                int code = readInputCode(READ_TIMEOUT_MILLIS);
                if (code != NonBlockingReader.READ_EXPIRED) {
                    if (code == TuiCommandBar.ESCAPE) {
                        EscapeKey key = readEscapeKey();
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

    private void draw(InlineDisplay display, Terminal terminal) {
        int height = Math.min(INLINE_DISPLAY_HEIGHT,
                Math.max(9, TamboDashboard.displayHeight(state(),
                        terminal.getHeight() > 0 ? terminal.getHeight() : DEFAULT_HEIGHT)));
        display.render((area, buffer) -> dashboard.render(
                dev.tamboui.terminal.Frame.forTesting(buffer), state()), height, -1, -1);
    }

    private void startDashboardRefreshIfNeeded(int width) {
        Session session = repl.session();
        if (width < TuiDashboard.COMPACT_WIDTH || uiState.refreshInFlight()
                || uiState.paletteOpen() || uiState.prompt() != null || commandRunning
                || uiState.contextSession().connection() != TuiDashboard.Connection.ONLINE
                || !session.isAuthenticated() || session.landingNode() == null || session.credential() == null) {
            return;
        }
        long requestId = ++refreshSequence;
        long generation = uiState.contextSession().generation();
        kernel.dispatch(new TuiEvent.ActionPosted(new TuiAction.RefreshStarted(requestId, generation)));
        uiState = kernel.state();
        resourceRefresh.refresh(requestId, generation, session.landingNode(), session.credential(), kernel::post);
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
        long oldRefreshRequestId = uiState.refreshRequestId();
        boolean refreshInFlight = uiState.refreshInFlight();
        uiState = reduce(new TuiAction.ContextSession(new TuiContextSessionAction.SwitchContext(context)));
        if (uiState.contextSession().generation() != before.generation()) {
            if (refreshInFlight) {
                resourceRefresh.cancel(oldRefreshRequestId);
                uiState = reduce(new TuiAction.RefreshCancelled(oldRefreshRequestId));
            }
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
        String notice = uiState.notice();
        if (notice == null || notice.isBlank() || notice.startsWith("ready")) {
            notice = contextSession.notice();
        }
        return new TuiDashboard.State(repl.workdir(), context, principal, connection, notice,
                uiState.command(), uiState.palette(), uiState.paletteIndex(), uiState.prompt(),
                session.landingNode() == null ? null : session.landingNode().toString(),
                session.clusterName(), authStatus(contextSession, session), uiState.activity(),
                uiState.resources().isEmpty() && uiState.lastRefreshAt() == null
                        ? localWorkspaceSnapshot() : uiState.resources(),
                uiState.pipelines(), uiState.lastRefreshAt(), uiState.resultPane());
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
        return String.join("\n", lines);
    }

    private void clearOutput() {
        out.getBuffer().setLength(0);
        err.getBuffer().setLength(0);
    }

    private void invalidateWorkspaceSnapshot() {
        workspaceSnapshotRoot = null;
        workspaceSnapshot = List.of();
    }

    private List<TuiDashboard.ResourceSummary> localWorkspaceSnapshot() {
        java.nio.file.Path root = repl.workdir().toAbsolutePath().normalize();
        if (!root.equals(workspaceSnapshotRoot)) {
            workspaceSnapshotRoot = root;
            workspaceSnapshot = TuiWorkspaceSnapshot.scan(root);
        }
        return workspaceSnapshot;
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
            if (suggestionsVisible) {
                suggestionsVisible = false;
                uiState = reduce(new TuiAction.OpenPalette(paletteCommands(repl.registry()), PALETTE_NOTICE));
            } else {
                uiState = reduce(new TuiAction.ClosePalette(uiState.notice()));
            }
        } else {
            uiState = reduce(
                    new TuiAction.OpenPalette(paletteCommands(repl.registry()), PALETTE_NOTICE));
        }
    }

    private void navigate(EscapeKey key) {
        if (uiState.paletteOpen()) {
            uiState = reduce(
                    new TuiAction.MovePalette(key == EscapeKey.UP ? -1 : 1));
            if (!suggestionsVisible) {
                uiState = reduce(new TuiAction.SetNotice(PALETTE_NOTICE));
            }
            return;
        }
        if (commandInput.isEmpty()) {
            TuiNavigation navigation = navigation();
            TuiNavigation next = navigation.move(key == EscapeKey.UP ? -1 : 1);
            uiState = reduce(new TuiAction.SetNavigation(next));
            if (!next.selected().isEmpty()) {
                uiState = reduce(new TuiAction.SetNotice("selected: " + next.selected()));
            }
            return;
        }
        commandInput = key == EscapeKey.UP ? history.previous(commandInput) : history.next();
        uiState = reduce(new TuiAction.SetCommand(commandInput));
    }

    private TuiNavigation navigation() {
        List<String> items = uiState.resources().isEmpty() && uiState.lastRefreshAt() == null
                ? localWorkspaceSnapshot().stream().map(TuiDashboard.ResourceSummary::id).toList()
                : uiState.resources().stream().map(TuiDashboard.ResourceSummary::id).toList();
        TuiNavigation navigation = uiState.navigation();
        return navigation.items().equals(items) ? navigation : TuiNavigation.initial(items);
    }

    private TuiDashboard.ResourceSummary selectedResource(String id) {
        List<TuiDashboard.ResourceSummary> resources = uiState.resources().isEmpty() && uiState.lastRefreshAt() == null
                ? localWorkspaceSnapshot() : uiState.resources();
        return resources.stream().filter(resource -> resource.id().equals(id)).findFirst().orElse(null);
    }

    private void closePaletteOrClearCommand() {
        if (uiState.paletteOpen()) {
            suggestionsVisible = false;
            uiState = reduce(new TuiAction.ClosePalette("ready"));
        } else if (navigation().detailOpen()) {
            uiState = reduce(new TuiAction.SetNavigation(navigation().back()));
            uiState = reduce(new TuiAction.SetResultPane(null));
            uiState = reduce(new TuiAction.SetNotice("resources"));
        } else {
            TuiCommandBar.Update update = TuiCommandBar.accept(commandInput, TuiCommandBar.ESCAPE);
            commandInput = update.value();
            uiState = reduce(new TuiAction.SetCommand(commandInput));
        }
    }

    private int readInputCode(long timeout) throws IOException {
        if (pendingInputCode != Integer.MIN_VALUE) {
            int code = pendingInputCode;
            pendingInputCode = Integer.MIN_VALUE;
            return code;
        }
        return reader.read(timeout);
    }

    private EscapeKey readEscapeKey() throws IOException {
        int next = reader.read(ESCAPE_SEQUENCE_START_TIMEOUT_MILLIS);
        if (next == NonBlockingReader.READ_EXPIRED || next < 0) {
            return EscapeKey.ESCAPE;
        }
        if (next != '[' && next != 'O') {
            rememberPendingInput(next);
            return EscapeKey.ESCAPE;
        }
        do {
            next = reader.read(ESCAPE_SEQUENCE_BODY_TIMEOUT_MILLIS);
            if (next == NonBlockingReader.READ_EXPIRED || next < 0) {
                return EscapeKey.ESCAPE;
            }
        } while (next == '[' || next == 'O' || next == '?' || next == ';'
                || (next >= '0' && next <= '9'));
        return switch (next) {
            case 'A' -> EscapeKey.UP;
            case 'B' -> EscapeKey.DOWN;
            case 'C' -> EscapeKey.RIGHT;
            case 'D' -> EscapeKey.LEFT;
            default -> EscapeKey.ESCAPE;
        };
    }

    private void rememberPendingInput(int code) {
        if (code >= 0) {
            pendingInputCode = code;
        }
    }

    static int movePromptSelection(int selected, int delta, int size) {
        if (size <= 0) {
            return 0;
        }
        return Math.max(0, Math.min(size - 1, selected + delta));
    }

    static boolean isEnter(int code) {
        return code == TuiCommandBar.ENTER || code == TuiCommandBar.CARRIAGE_RETURN;
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
