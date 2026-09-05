package io.tapstate.cli;

import dev.tamboui.inline.InlineDisplay;
import dev.tamboui.terminal.Backend;
import dev.tamboui.terminal.Frame;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Owns the temporary inline region. Completed text is committed through this boundary and is never
 * retained as widget state, so ordinary terminal scrollback remains the source of truth for history.
 */
final class InlineRenderer implements AutoCloseable {

    private static final Pattern ANSI_SEQUENCE = Pattern.compile(
            "\\u001B(?:\\[[0-?]*[ -/]*[@-~]|\\][^\\u0007]*(?:\\u0007|\\u001B\\\\))");

    interface Surface {
        void render(Consumer<Frame> view, int height);

        void commit(String line);

        void release();
    }

    private final Surface surface;
    private final int capacity;
    private boolean closed;

    InlineRenderer(Surface surface, int capacity) {
        this.surface = Objects.requireNonNull(surface, "surface");
        if (capacity < 1) {
            throw new IllegalArgumentException("inline renderer capacity must be positive");
        }
        this.capacity = capacity;
    }

    static InlineRenderer open(Backend backend, int capacity) throws IOException {
        return new InlineRenderer(new DisplaySurface(InlineDisplay.withBackend(capacity, backend)), capacity);
    }

    void render(Consumer<Frame> view, int height) {
        ensureOpen();
        surface.render(Objects.requireNonNull(view, "view"), clamp(height));
    }

    void update(Consumer<Frame> view, int height) {
        render(view, height);
    }

    void clear() {
        ensureOpen();
        surface.render(frame -> frame.buffer().clear(), 0);
    }

    /** Commits each logical line as normal terminal output, including intentional blank lines. */
    void commit(String text) {
        ensureOpen();
        if (text == null || text.isEmpty()) {
            return;
        }
        String clean = cleanText(text);
        String[] lines = clean.split("\\R", -1);
        int end = lines.length;
        if (end > 1 && lines[end - 1].isEmpty()) {
            end--;
        }
        for (int i = 0; i < end; i++) {
            surface.commit(lines[i]);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            surface.release();
        }
    }

    private int clamp(int height) {
        return Math.max(1, Math.min(capacity, height));
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("inline renderer is closed");
        }
    }

    private static String cleanText(String value) {
        String withoutAnsi = ANSI_SEQUENCE.matcher(value).replaceAll("");
        StringBuilder clean = new StringBuilder(withoutAnsi.length());
        for (int i = 0; i < withoutAnsi.length(); i++) {
            char character = withoutAnsi.charAt(i);
            if (character == '\r') {
                if (i + 1 >= withoutAnsi.length() || withoutAnsi.charAt(i + 1) != '\n') {
                    clean.append('\n');
                }
            } else if (character == '\n' || character == '\t' || character >= ' ') {
                clean.append(character);
            }
        }
        return clean.toString();
    }

    private static final class DisplaySurface implements Surface {
        private final InlineDisplay display;

        private DisplaySurface(InlineDisplay display) {
            this.display = display.clearOnClose();
        }

        @Override
        public void render(Consumer<Frame> view, int height) {
            display.render((area, buffer) -> view.accept(Frame.forTesting(buffer)), height, -1, -1);
        }

        @Override
        public void commit(String line) {
            display.println(line);
        }

        @Override
        public void release() {
            try {
                display.close();
            } catch (IOException failure) {
                throw new UncheckedIOException(failure);
            }
        }
    }
}

/**
 * Inline session: JLine owns the terminal and input stream, while TamboUI paints only the current
 * temporary region. Submitted lines and completed command output are committed to ordinary terminal
 * scrollback; running state stays temporary.
 */
final class InlineTui {

    private static final int INPUT_REGION_HEIGHT = 4;
    private static final int INLINE_REGION_CAPACITY = 12;
    private static final int READ_TIMEOUT_MILLIS = 50;
    private static final int ESCAPE_TIMEOUT_MILLIS = 100;
    private static final int CTRL_C = 3;
    private static final int CTRL_D = 4;
    private static final int BACKSPACE = 8;
    private static final int DELETE = 127;
    private static final String[] RUNNING_FRAMES = {
            "⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"
    };

    private static final dev.tamboui.style.Color INPUT_BACKGROUND =
            dev.tamboui.style.Color.rgb(35, 38, 43);
    private static final dev.tamboui.style.Color INPUT_FOREGROUND =
            dev.tamboui.style.Color.rgb(226, 229, 235);
    private static final dev.tamboui.style.Color ACCENT_FOREGROUND =
            dev.tamboui.style.Color.rgb(180, 155, 224);
    private static final dev.tamboui.style.Color HINT_FOREGROUND =
            dev.tamboui.style.Color.rgb(138, 145, 156);
    private static final dev.tamboui.style.Color CURSOR_FOREGROUND =
            dev.tamboui.style.Color.rgb(82, 166, 118);
    private static final dev.tamboui.style.Color SELECTION_BACKGROUND =
            dev.tamboui.style.Color.rgb(62, 64, 70);

    private final Repl repl;
    private final java.io.StringWriter capturedOut;
    private final java.io.StringWriter capturedErr;
    private final StringBuilder input = new StringBuilder();
    private final java.util.ArrayDeque<Integer> deferredInput = new java.util.ArrayDeque<>();
    private volatile boolean interrupted;
    private volatile boolean resizeRequested;
    private org.jline.terminal.Terminal terminal;
    private dev.tamboui.backend.jline3.JLineBackend backend;
    private org.jline.utils.NonBlockingReader reader;
    private InlineRenderer renderer;
    private InlinePrompter prompter;
    private java.util.concurrent.ExecutorService operationExecutor;
    private java.util.concurrent.Future<Boolean> activeOperation;
    private int runningFrame;
    private boolean cancelRequested;

    InlineTui(Repl repl, java.io.StringWriter capturedOut, java.io.StringWriter capturedErr) {
        this.repl = java.util.Objects.requireNonNull(repl, "repl");
        this.capturedOut = java.util.Objects.requireNonNull(capturedOut, "capturedOut");
        this.capturedErr = java.util.Objects.requireNonNull(capturedErr, "capturedErr");
    }

    /** Returns whether this process has a terminal suitable for an inline session. */
    static boolean hasInteractiveTerminal() {
        return System.console() != null;
    }

    /**
     * Returns whether dispatch can run without asking the user for terminal input or changing the
     * session's terminal-owned state. This conservative boundary keeps JLine's prompt reader and the
     * inline reader from ever competing for the same raw input stream.
     */
    static boolean canRunInBackground(String line) {
        String trimmed = line == null ? "" : line.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        String verb = trimmed.split("\\s+", 2)[0];
        return switch (verb) {
            case "auth", "cd", "connect", "context", "disconnect", "exit", "login", "logout",
                    "new", "quit", ":ctx" -> false;
            default -> true;
        };
    }

    int run() {
        boolean stoppedByUser = false;
        try {
            terminal = org.jline.terminal.TerminalBuilder.builder().system(true).dumb(false).build();
            backend = new dev.tamboui.backend.jline3.JLineBackend(terminal);
            backend.onResize(() -> resizeRequested = true);
            backend.enableRawMode();
            reader = terminal.reader();
            renderer = InlineRenderer.open(backend, INLINE_REGION_CAPACITY);
            prompter = new InlinePrompter(
                    this::nextInputCode,
                    new InlinePrompter.View() {
                        @Override
                        public void showInput(String question, String value, int cursor,
                                              String defaultValue, boolean secret) {
                            renderPromptInput(question, value, cursor, defaultValue, secret);
                        }

                        @Override
                        public void showChoices(String question, java.util.List<String> options,
                                                int selected, String query) {
                            renderPromptChoices(question, options, selected, query);
                        }
                    });
            repl.installPrompterIfMissing(prompter);
            operationExecutor = java.util.concurrent.Executors.newSingleThreadExecutor(task -> {
                Thread worker = new Thread(task, "tapstate-inline-operation");
                worker.setDaemon(true);
                return worker;
            });

            commitCaptured();
            renderer.commit("Tapstate");
            renderer.commit("Workspace: " + repl.workdir());
            renderInput();

            terminal.handle(org.jline.terminal.Terminal.Signal.INT, signal -> {
                interrupted = true;
                repl.cancelStream();
            });

            while (!stoppedByUser) {
                if (resizeRequested) {
                    resizeRequested = false;
                    renderActiveRegion();
                }
                int code = nextInputCode(READ_TIMEOUT_MILLIS);
                if (code == org.jline.utils.NonBlockingReader.READ_EXPIRED) {
                    if (interrupted) {
                        interrupted = false;
                        if (activeOperation != null) {
                            cancelRequested = true;
                            renderRunning();
                        } else {
                            input.setLength(0);
                            renderInput();
                        }
                    }
                    if (activeOperation != null && activeOperation.isDone()) {
                        stoppedByUser = finishOperation();
                    }
                    continue;
                }
                if (code == org.jline.utils.NonBlockingReader.EOF) {
                    if (activeOperation == null) {
                        stoppedByUser = true;
                    }
                    continue;
                }
                if (activeOperation != null) {
                    if (code == CTRL_C && !cancelRequested) {
                        cancelRequested = true;
                        repl.cancelStream();
                        renderRunning();
                    } else {
                        deferredInput.addLast(code);
                    }
                    if (activeOperation.isDone()) {
                        stoppedByUser = finishOperation();
                    }
                    continue;
                }
                if (code == CTRL_C) {
                    interrupted = false;
                    input.setLength(0);
                    renderInput();
                    continue;
                }
                if (code == CTRL_D) {
                    stoppedByUser = input.isEmpty();
                    continue;
                }
                if (code == BACKSPACE || code == DELETE) {
                    removeLastCodePoint();
                    renderInput();
                    continue;
                }
                if (code == '\r' || code == '\n') {
                    stoppedByUser = submit();
                    continue;
                }
                if (code == 27) {
                    consumeEscapeSequence();
                    continue;
                }
                if (code >= 32) {
                    input.appendCodePoint(code);
                    renderInput();
                }
            }
            renderer.clear();
            renderer.commit("bye");
            return Cli.EXIT_OK;
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        } finally {
            if (renderer != null) {
                renderer.close();
            } else if (backend != null) {
                try {
                    backend.close();
                } catch (IOException ignored) {
                    // Terminal restoration is best effort when initialization failed.
                }
            }
            if (terminal != null) {
                try {
                    terminal.close();
                } catch (IOException ignored) {
                    // Terminal restoration is best effort during shutdown.
                }
            }
            if (operationExecutor != null) {
                operationExecutor.shutdownNow();
            }
        }
    }

    private boolean submit() {
        String line = input.toString();
        input.setLength(0);
        if (line.isBlank()) {
            renderInput();
            return false;
        }
        renderer.commit("$ " + line);
        if (canRunInBackground(line)) {
            runningFrame = 0;
            cancelRequested = false;
            activeOperation = operationExecutor.submit(() -> repl.dispatch(line));
            renderRunning();
            return false;
        }
        // Prompting commands use the same reader and renderer through InlinePrompter. Leaving a
        // running widget mounted here would let a second prompt move the temporary region into
        // scrollback and would put two cursor owners on the terminal.
        renderer.clear();
        boolean keepRunning = repl.dispatch(line);
        renderer.clear();
        commitCaptured();
        if (keepRunning) {
            renderInput();
        }
        return !keepRunning;
    }

    private boolean finishOperation() {
        java.util.concurrent.Future<Boolean> completed = activeOperation;
        activeOperation = null;
        boolean keepRunning;
        try {
            keepRunning = completed.get();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("inline command was interrupted", failure);
        } catch (java.util.concurrent.ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("inline command failed", cause);
        }
        cancelRequested = false;
        renderer.clear();
        commitCaptured();
        if (keepRunning) {
            renderInput();
        }
        return !keepRunning;
    }

    private void commitCaptured() {
        String stdout = capturedOut.toString();
        String stderr = capturedErr.toString();
        capturedOut.getBuffer().setLength(0);
        capturedErr.getBuffer().setLength(0);
        renderer.commit(stdout);
        renderer.commit(stderr);
        if (repl.lastExitCode() != Cli.EXIT_OK && stdout.isEmpty() && stderr.isEmpty()) {
            renderer.commit("command failed (exit code " + repl.lastExitCode() + ")");
        }
    }

    private void renderInput() {
        renderer.update(frame -> {
            dev.tamboui.layout.Rect area = frame.area();
            int inputHeight = Math.min(2, area.height());
            dev.tamboui.layout.Rect inputArea = new dev.tamboui.layout.Rect(
                    area.x(), area.y(), area.width(), inputHeight);
            renderInputBackground(frame, inputArea);
            dev.tamboui.layout.Rect content = inputArea;
            if (!content.isEmpty()) {
                dev.tamboui.text.Text text = dev.tamboui.text.Text.from(input.toString())
                        .fg(INPUT_FOREGROUND)
                        .bg(INPUT_BACKGROUND)
                        .append(dev.tamboui.text.Text.from("▌")
                                .fg(CURSOR_FOREGROUND)
                                .bg(INPUT_BACKGROUND));
                frame.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                        .text(text)
                        .overflow(dev.tamboui.style.Overflow.CLIP)
                        .background(INPUT_BACKGROUND)
                        .build(), content);
            }
            if (area.height() > inputHeight) {
                dev.tamboui.layout.Rect hintArea = new dev.tamboui.layout.Rect(
                        area.x(), area.y() + inputHeight, area.width(), 1);
                renderStatus(frame, hintArea, "Enter run  ·  Ctrl-C clear  ·  Ctrl-D exit");
            }
        }, INPUT_REGION_HEIGHT);
    }

    private void renderRunning() {
        String frame = RUNNING_FRAMES[runningFrame++ % RUNNING_FRAMES.length];
        String status = cancelRequested ? frame + " Cancelling…" : frame + " Running…";
        renderer.update(view -> {
            dev.tamboui.layout.Rect area = view.area();
            int inputHeight = Math.min(2, area.height());
            dev.tamboui.layout.Rect inputArea = new dev.tamboui.layout.Rect(
                    area.x(), area.y(), area.width(), inputHeight);
            renderInputBackground(view, inputArea);
            dev.tamboui.layout.Rect content = inputArea;
            if (!content.isEmpty()) {
                view.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                        .text(dev.tamboui.text.Text.from(status)
                                .fg(INPUT_FOREGROUND)
                                .bg(INPUT_BACKGROUND))
                        .overflow(dev.tamboui.style.Overflow.CLIP)
                        .background(INPUT_BACKGROUND)
                        .build(), content);
            }
            if (area.height() > inputHeight) {
                dev.tamboui.layout.Rect hintArea = new dev.tamboui.layout.Rect(
                        area.x(), area.y() + inputHeight, area.width(), 1);
                renderStatus(view, hintArea, cancelRequested
                        ? "Ctrl-C cancelling  ·  Please wait"
                        : "Ctrl-C cancel  ·  Please wait");
            }
        }, INPUT_REGION_HEIGHT);
    }

    private void renderPromptInput(String question, String value, int cursor, String defaultValue,
                                   boolean secret) {
        renderer.update(frame -> {
            dev.tamboui.layout.Rect area = frame.area();
            int inputHeight = Math.min(2, area.height());
            if (area.height() > 0) {
                frame.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                        .text(dev.tamboui.text.Text.from(question).fg(ACCENT_FOREGROUND).bold())
                        .overflow(dev.tamboui.style.Overflow.CLIP)
                        .build(), new dev.tamboui.layout.Rect(area.x(), area.y(), area.width(), 1));
            }
            if (area.height() > 1) {
                dev.tamboui.layout.Rect inputArea = new dev.tamboui.layout.Rect(
                        area.x(), area.y() + 1, area.width(), inputHeight - 1);
                renderInputBackground(frame, inputArea);
                renderCursorText(frame, inputArea, value, cursor, secret);
            }
            if (area.height() > 2) {
                String hint = defaultValue == null || defaultValue.isEmpty()
                        ? "Enter submit  ·  Esc cancel"
                        : "Enter submit  ·  Esc cancel  ·  default " + defaultValue;
                renderStatus(frame, new dev.tamboui.layout.Rect(
                        area.x(), area.bottom() - 1, area.width(), 1), hint);
            }
        }, INPUT_REGION_HEIGHT);
    }

    private void renderPromptChoices(String question, java.util.List<String> options, int selected,
                                     String query) {
        renderer.update(frame -> {
            dev.tamboui.layout.Rect area = frame.area();
            if (area.height() == 0) {
                return;
            }
            frame.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                    .text(dev.tamboui.text.Text.from(question).fg(ACCENT_FOREGROUND).bold())
                    .overflow(dev.tamboui.style.Overflow.CLIP)
                    .build(), new dev.tamboui.layout.Rect(area.x(), area.y(), area.width(), 1));
            int visible = Math.max(1, Math.min(options.size(), area.height() - 2));
            int start = Math.min(Math.max(0, selected - visible + 1), options.size() - visible);
            for (int row = 0; row < visible; row++) {
                int index = start + row;
                boolean active = index == selected;
                dev.tamboui.text.Text option = dev.tamboui.text.Text.from(
                        (active ? "› " : "  ") + options.get(index))
                        .fg(active ? INPUT_FOREGROUND : HINT_FOREGROUND)
                        .bg(active ? SELECTION_BACKGROUND : INPUT_BACKGROUND);
                frame.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                        .text(option)
                        .background(active ? SELECTION_BACKGROUND : INPUT_BACKGROUND)
                        .overflow(dev.tamboui.style.Overflow.CLIP)
                        .build(), new dev.tamboui.layout.Rect(
                                area.x(), area.y() + 1 + row, area.width(), 1));
            }
            String hint = "↑↓ choose  ·  Enter select  ·  Esc cancel";
            if (query != null && !query.isEmpty()) {
                hint += "  ·  " + query;
            }
            renderStatus(frame, new dev.tamboui.layout.Rect(
                    area.x(), area.bottom() - 1, area.width(), 1), hint);
        }, Math.min(INLINE_REGION_CAPACITY, Math.max(4, options.size() + 2)));
    }

    private static void renderInputBackground(dev.tamboui.terminal.Frame frame,
                                              dev.tamboui.layout.Rect area) {
        if (!area.isEmpty()) {
            frame.renderWidget(dev.tamboui.widgets.block.Block.builder()
                    .borders(dev.tamboui.widgets.block.Borders.NONE)
                    .background(INPUT_BACKGROUND)
                    .build(), area);
        }
    }

    private static void renderCursorText(dev.tamboui.terminal.Frame frame,
                                         dev.tamboui.layout.Rect area, String value, int cursor,
                                         boolean secret) {
        if (area.isEmpty()) {
            return;
        }
        String shown = secret ? "*".repeat(value.codePointCount(0, value.length())) : value;
        int safeCursor = Math.max(0, Math.min(cursor, value.length()));
        int visibleCursor = secret ? value.codePointCount(0, safeCursor) : safeCursor;
        String prefix = shown.substring(0, Math.min(visibleCursor, shown.length()));
        String suffix = shown.substring(Math.min(visibleCursor, shown.length()));
        dev.tamboui.text.Text text = dev.tamboui.text.Text.from(prefix)
                .fg(INPUT_FOREGROUND)
                .bg(INPUT_BACKGROUND)
                .append(dev.tamboui.text.Text.from("▌")
                        .fg(CURSOR_FOREGROUND)
                        .bg(INPUT_BACKGROUND))
                .append(dev.tamboui.text.Text.from(suffix)
                        .fg(INPUT_FOREGROUND)
                        .bg(INPUT_BACKGROUND));
        frame.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                .text(text)
                .overflow(dev.tamboui.style.Overflow.CLIP)
                .background(INPUT_BACKGROUND)
                .build(), area);
    }

    private void renderStatus(dev.tamboui.terminal.Frame frame, dev.tamboui.layout.Rect area,
                              String controls) {
        dev.tamboui.text.Text status = dev.tamboui.text.Text.from(
                "local  ·  dir " + compactWorkdir() + "  ·  " + controls)
                .fg(HINT_FOREGROUND);
        frame.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                .text(status)
                .overflow(dev.tamboui.style.Overflow.CLIP)
                .build(), area);
    }

    private String compactWorkdir() {
        java.nio.file.Path current = repl.workdir().toAbsolutePath().normalize();
        java.nio.file.Path home = java.nio.file.Path.of(System.getProperty("user.home"))
                .toAbsolutePath().normalize();
        if (current.startsWith(home)) {
            java.nio.file.Path relative = home.relativize(current);
            String value = relative.toString().replace(java.io.File.separatorChar, '/');
            if (value.isEmpty()) {
                return "~";
            }
            String[] parts = value.split("/");
            return parts.length > 3 ? "~/…/" + parts[parts.length - 1] : "~/" + value;
        }
        return current.toString();
    }

    private void renderActiveRegion() {
        if (activeOperation == null) {
            renderInput();
        } else {
            renderRunning();
        }
    }

    private void removeLastCodePoint() {
        if (input.isEmpty()) {
            return;
        }
        input.deleteCharAt(input.offsetByCodePoints(input.length(), -1));
    }

    /** Consume arrows and other escape sequences without allowing them to reach command dispatch. */
    private void consumeEscapeSequence() throws IOException {
        int next = nextInputCode(ESCAPE_TIMEOUT_MILLIS);
        if (next == org.jline.utils.NonBlockingReader.READ_EXPIRED ||
                next == org.jline.utils.NonBlockingReader.EOF) {
            return;
        }
        if (next != '[' && next != 'O') {
            return;
        }
        while (true) {
            int tail = nextInputCode(ESCAPE_TIMEOUT_MILLIS);
            if (tail == org.jline.utils.NonBlockingReader.READ_EXPIRED ||
                    tail == org.jline.utils.NonBlockingReader.EOF) {
                return;
            }
            if ((tail >= 'A' && tail <= 'Z') || (tail >= 'a' && tail <= 'z') || tail == '~') {
                return;
            }
        }
    }

    private int nextInputCode(int timeoutMillis) throws IOException {
        if (!deferredInput.isEmpty()) {
            return deferredInput.removeFirst();
        }
        return reader.read(timeoutMillis);
    }
}

/**
 * Prompt adapter for the inline session. It shares the session reader and renderer, so a context
 * picker never starts a second line editor or lets navigation keys reach the outer command bar.
 */
final class InlinePrompter implements Prompter {

    static final int UP = -10;
    static final int DOWN = -11;
    private static final int LEFT = -12;
    private static final int RIGHT = -13;
    private static final int HOME = -14;
    private static final int END = -15;
    private static final int READ_TIMEOUT_MILLIS = 50;
    private static final int ESCAPE_TIMEOUT_MILLIS = 100;
    private static final int CTRL_C = 3;
    private static final int CTRL_D = 4;
    private static final int BACKSPACE = 8;
    private static final int DELETE = 127;
    private static final int ENTER = 13;
    private static final int ESCAPE = 27;

    @FunctionalInterface
    interface KeyReader {
        int read(int timeoutMillis) throws IOException;
    }

    interface View {
        void showInput(String question, String value, int cursor, String defaultValue, boolean secret);

        void showChoices(String question, java.util.List<String> options, int selected, String query);
    }

    private final KeyReader reader;
    private final View view;
    private boolean cancelled;

    InlinePrompter(KeyReader reader, View view) {
        this.reader = java.util.Objects.requireNonNull(reader, "reader");
        this.view = java.util.Objects.requireNonNull(view, "view");
    }

    @Override
    public String ask(String question, String defaultValue) {
        return readLine(question, defaultValue, false, true);
    }

    @Override
    public String secret(String question) {
        return readLine(question, null, true, false);
    }

    @Override
    public String choose(String question, java.util.List<String> options) {
        if (options == null || options.isEmpty()) {
            return "";
        }
        int selected = 0;
        StringBuilder query = new StringBuilder();
        view.showChoices(question, options, selected, query.toString());
        while (true) {
            int key = nextKey();
            switch (key) {
                case UP, DOWN -> selected = selectionAfterKey(selected, options.size(), key);
                case ENTER, 10 -> {
                    return options.get(selected);
                }
                case ESCAPE, CTRL_C, CTRL_D -> {
                    return options.get(options.size() - 1);
                }
                case BACKSPACE, DELETE -> removeLastCodePoint(query);
                default -> {
                    if (key >= 32) {
                        query.appendCodePoint(key);
                        selected = matchingSelection(options, query.toString(), selected);
                    }
                }
            }
            view.showChoices(question, options, selected, query.toString());
        }
    }

    @Override
    public String lines(String question) {
        java.util.List<String> lines = new java.util.ArrayList<>();
        while (true) {
            String line = readLine(question + " (end with a single '.' on its own line)",
                    null, false, false);
            if (cancelled || line.strip().equals(".")) {
                return String.join("\n", lines);
            }
            lines.add(line);
        }
    }

    static int selectionAfterKey(int current, int size, int key) {
        if (size <= 0) {
            return 0;
        }
        if (key == DOWN) {
            return Math.min(size - 1, Math.max(0, current) + 1);
        }
        if (key == UP) {
            return Math.max(0, Math.min(size - 1, current) - 1);
        }
        return Math.max(0, Math.min(size - 1, current));
    }

    private String readLine(String question, String defaultValue, boolean secret, boolean trim) {
        StringBuilder value = new StringBuilder();
        int cursor = 0;
        cancelled = false;
        view.showInput(question, value.toString(), cursor, defaultValue, secret);
        while (true) {
            int key = nextKey();
            switch (key) {
                case ENTER, 10 -> {
                    return trim ? value.toString().trim() : value.toString();
                }
                case ESCAPE, CTRL_C, CTRL_D -> {
                    cancelled = true;
                    return "";
                }
                case BACKSPACE -> {
                    if (cursor > 0) {
                        int previous = value.offsetByCodePoints(cursor, -1);
                        value.delete(previous, cursor);
                        cursor = previous;
                    }
                }
                case DELETE -> {
                    if (cursor < value.length()) {
                        int next = value.offsetByCodePoints(cursor, 1);
                        value.delete(cursor, next);
                    }
                }
                case LEFT -> cursor = value.offsetByCodePoints(cursor, -1);
                case RIGHT -> cursor = value.offsetByCodePoints(cursor, 1);
                case HOME -> cursor = 0;
                case END -> cursor = value.length();
                case UP, DOWN -> {
                    // Vertical navigation belongs to a choice surface, not a text prompt.
                }
                default -> {
                    if (key >= 32) {
                        char[] codePoint = Character.toChars(key);
                        value.insert(cursor, codePoint);
                        cursor += codePoint.length;
                    }
                }
            }
            view.showInput(question, value.toString(), cursor, defaultValue, secret);
        }
    }

    private int nextKey() {
        try {
            while (true) {
                int key = reader.read(READ_TIMEOUT_MILLIS);
                if (key == org.jline.utils.NonBlockingReader.READ_EXPIRED) {
                    continue;
                }
                if (key == org.jline.utils.NonBlockingReader.EOF) {
                    return CTRL_D;
                }
                if (key != ESCAPE) {
                    return key;
                }
                return readEscapeKey();
            }
        } catch (IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private int readEscapeKey() throws IOException {
        int next = reader.read(ESCAPE_TIMEOUT_MILLIS);
        if (next == org.jline.utils.NonBlockingReader.READ_EXPIRED
                || next == org.jline.utils.NonBlockingReader.EOF) {
            return ESCAPE;
        }
        if (next != '[' && next != 'O') {
            return ESCAPE;
        }
        while (true) {
            int tail = reader.read(ESCAPE_TIMEOUT_MILLIS);
            if (tail == org.jline.utils.NonBlockingReader.READ_EXPIRED
                    || tail == org.jline.utils.NonBlockingReader.EOF) {
                return ESCAPE;
            }
            if ((tail >= 'A' && tail <= 'Z') || (tail >= 'a' && tail <= 'z') || tail == '~') {
                return switch (tail) {
                    case 'A' -> UP;
                    case 'B' -> DOWN;
                    case 'C' -> RIGHT;
                    case 'D' -> LEFT;
                    case 'H' -> HOME;
                    case 'F' -> END;
                    default -> ESCAPE;
                };
            }
        }
    }

    private static int matchingSelection(java.util.List<String> options, String query, int fallback) {
        try {
            int number = Integer.parseInt(query);
            if (number >= 1 && number <= options.size()) {
                return number - 1;
            }
        } catch (NumberFormatException ignored) {
            // Text queries are matched below.
        }
        String normalized = query.toLowerCase(java.util.Locale.ROOT);
        for (int i = 0; i < options.size(); i++) {
            if (options.get(i).toLowerCase(java.util.Locale.ROOT).startsWith(normalized)) {
                return i;
            }
        }
        return fallback;
    }

    private static void removeLastCodePoint(StringBuilder value) {
        if (!value.isEmpty()) {
            value.deleteCharAt(value.offsetByCodePoints(value.length(), -1));
        }
    }
}

/** Plain line-oriented fallback for pipes and redirected input; it emits no terminal control sequences. */
final class PlainSession {

    private PlainSession() {
    }

    static int run(Repl repl) {
        java.io.PrintWriter out = new java.io.PrintWriter(System.out, true);
        java.io.BufferedReader input = new java.io.BufferedReader(
                new java.io.InputStreamReader(System.in, java.nio.charset.StandardCharsets.UTF_8));
        out.println("Tapstate CLI (plain mode). Enter one command per line.");
        try {
            String line;
            while ((line = input.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                out.println("$ " + line);
                if (!repl.dispatch(line)) {
                    break;
                }
            }
            out.println("bye");
            return Cli.EXIT_OK;
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }
}
