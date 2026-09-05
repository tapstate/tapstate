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
 * Phase-one inline session: JLine owns the terminal and input stream, while TamboUI paints only the
 * current input region. A submitted line is committed before dispatch and command output is committed
 * after dispatch, leaving both in ordinary terminal scrollback.
 */
final class InlineTui {

    private static final int INPUT_REGION_HEIGHT = 4;
    private static final int READ_TIMEOUT_MILLIS = 50;
    private static final int ESCAPE_TIMEOUT_MILLIS = 100;
    private static final int CTRL_C = 3;
    private static final int CTRL_D = 4;
    private static final int BACKSPACE = 8;
    private static final int DELETE = 127;

    private static final dev.tamboui.style.Color INPUT_BACKGROUND =
            dev.tamboui.style.Color.rgb(35, 38, 43);
    private static final dev.tamboui.style.Color INPUT_BORDER =
            dev.tamboui.style.Color.rgb(112, 118, 130);
    private static final dev.tamboui.style.Color INPUT_FOREGROUND =
            dev.tamboui.style.Color.rgb(226, 229, 235);
    private static final dev.tamboui.style.Color HINT_FOREGROUND =
            dev.tamboui.style.Color.rgb(138, 145, 156);
    private static final dev.tamboui.style.Color CURSOR_FOREGROUND =
            dev.tamboui.style.Color.rgb(82, 166, 118);

    private final Repl repl;
    private final java.io.StringWriter capturedOut;
    private final java.io.StringWriter capturedErr;
    private final StringBuilder input = new StringBuilder();
    private volatile boolean interrupted;
    private volatile boolean resizeRequested;
    private org.jline.terminal.Terminal terminal;
    private dev.tamboui.backend.jline3.JLineBackend backend;
    private org.jline.utils.NonBlockingReader reader;
    private InlineRenderer renderer;

    InlineTui(Repl repl, java.io.StringWriter capturedOut, java.io.StringWriter capturedErr) {
        this.repl = java.util.Objects.requireNonNull(repl, "repl");
        this.capturedOut = java.util.Objects.requireNonNull(capturedOut, "capturedOut");
        this.capturedErr = java.util.Objects.requireNonNull(capturedErr, "capturedErr");
    }

    /** Returns whether this process has a terminal suitable for an inline session. */
    static boolean hasInteractiveTerminal() {
        return System.console() != null;
    }

    int run() {
        boolean stoppedByUser = false;
        try {
            terminal = org.jline.terminal.TerminalBuilder.builder().system(true).dumb(false).build();
            backend = new dev.tamboui.backend.jline3.JLineBackend(terminal);
            backend.onResize(() -> resizeRequested = true);
            backend.enableRawMode();
            reader = terminal.reader();
            renderer = InlineRenderer.open(backend, INPUT_REGION_HEIGHT);

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
                    renderInput();
                }
                int code = reader.read(READ_TIMEOUT_MILLIS);
                if (code == org.jline.utils.NonBlockingReader.READ_EXPIRED) {
                    if (interrupted) {
                        interrupted = false;
                        input.setLength(0);
                        renderInput();
                    }
                    continue;
                }
                if (code == org.jline.utils.NonBlockingReader.EOF) {
                    stoppedByUser = true;
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
        renderer.clear();
        boolean keepRunning = repl.dispatch(line);
        commitCaptured();
        if (!keepRunning) {
            return true;
        }
        renderInput();
        return false;
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
            int inputHeight = Math.min(3, area.height());
            dev.tamboui.layout.Rect inputArea = new dev.tamboui.layout.Rect(
                    area.x(), area.y(), area.width(), inputHeight);
            dev.tamboui.widgets.block.Block block = dev.tamboui.widgets.block.Block.builder()
                    .borderType(dev.tamboui.widgets.block.BorderType.ROUNDED)
                    .borders(dev.tamboui.widgets.block.Borders.ALL)
                    .borderColor(INPUT_BORDER)
                    .background(INPUT_BACKGROUND)
                    .build();
            frame.renderWidget(block, inputArea);
                dev.tamboui.layout.Rect content = block.inner(inputArea);
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
                frame.renderWidget(dev.tamboui.widgets.paragraph.Paragraph.builder()
                        .text(dev.tamboui.text.Text.from("Enter run  ·  Ctrl-C clear  ·  Ctrl-D exit")
                                .fg(HINT_FOREGROUND))
                        .overflow(dev.tamboui.style.Overflow.CLIP)
                        .build(), hintArea);
            }
        }, INPUT_REGION_HEIGHT);
    }

    private void removeLastCodePoint() {
        if (input.isEmpty()) {
            return;
        }
        input.deleteCharAt(input.offsetByCodePoints(input.length(), -1));
    }

    /** Consume arrows and other escape sequences without allowing them to reach command dispatch. */
    private void consumeEscapeSequence() throws IOException {
        int next = reader.read(ESCAPE_TIMEOUT_MILLIS);
        if (next == org.jline.utils.NonBlockingReader.READ_EXPIRED ||
                next == org.jline.utils.NonBlockingReader.EOF) {
            input.setLength(0);
            renderInput();
            return;
        }
        if (next != '[' && next != 'O') {
            return;
        }
        while (true) {
            int tail = reader.read(ESCAPE_TIMEOUT_MILLIS);
            if (tail == org.jline.utils.NonBlockingReader.READ_EXPIRED ||
                    tail == org.jline.utils.NonBlockingReader.EOF) {
                return;
            }
            if ((tail >= 'A' && tail <= 'Z') || (tail >= 'a' && tail <= 'z') || tail == '~') {
                return;
            }
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
