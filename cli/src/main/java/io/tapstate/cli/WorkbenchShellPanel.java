package io.tapstate.cli;

import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.Span;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.MouseEvent;
import dev.tamboui.tui.event.MouseEventKind;
import dev.tamboui.widgets.Clear;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.block.Title;
import org.jline.builtins.ScreenTerminal;
import org.jline.terminal.Size;
import org.jline.terminal.impl.LineDisciplineTerminal;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** A resident virtual terminal panel, not a system shell or child process. */
final class WorkbenchShellPanel implements AutoCloseable {

    private static final int COMPACT_HEIGHT = 10;
    private static final int EXPANDED_HEIGHT = 18;
    private static final int MOUSE_SCROLL_LINES = 3;

    private final Repl repl;
    private final Runnable redraw;
    private final Consumer<Runnable> scheduler;
    private final AtomicBoolean redrawQueued = new AtomicBoolean();

    private volatile boolean open;
    private volatile boolean expanded;
    private volatile boolean destroyed;
    private ScreenTerminal screen;
    private LineDisciplineTerminal terminal;
    private Thread shellThread;
    private volatile Rect lastArea;
    private volatile int lastInnerHeight;
    private volatile int scrollOffset;
    private final Transcript transcript = new Transcript();

    WorkbenchShellPanel(Repl repl, Runnable redraw, Consumer<Runnable> scheduler) {
        this.repl = Objects.requireNonNull(repl, "repl");
        this.redraw = Objects.requireNonNull(redraw, "redraw");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    boolean isOpen() {
        return open;
    }

    int heightFor(Rect area) {
        int desired = expanded ? EXPANDED_HEIGHT : COMPACT_HEIGHT;
        return Math.min(desired, Math.max(7, area.height() - 8));
    }

    void open() {
        if (destroyed) {
            return;
        }
        open = true;
        requestRedraw();
    }

    void hide() {
        open = false;
        requestRedraw();
    }

    boolean handle(KeyEvent key) {
        if (key.isKey(KeyCode.F6) && !key.hasShift()) {
            hide();
            return true;
        }
        if (key.isKey(KeyCode.F6) && key.hasShift()) {
            expanded = !expanded;
            requestRedraw();
            return true;
        }
        if (key.isPageUp()) {
            scrollOffset = Math.min(scrollOffset + Math.max(1, lastInnerHeight), transcript.lineCount());
            requestRedraw();
            return true;
        }
        if (key.isPageDown()) {
            scrollOffset = Math.max(0, scrollOffset - Math.max(1, lastInnerHeight));
            requestRedraw();
            return true;
        }
        scrollOffset = 0;
        LineDisciplineTerminal current = terminal;
        if (current == null) {
            return true;
        }
        try {
            String sequence = keySequence(key);
            String encoded = screen == null ? sequence : screen.pipe(sequence);
            current.processInputBytes(encoded.getBytes(StandardCharsets.UTF_8));
        } catch (IOException ignored) {
            // The closing terminal cannot accept more input; close() owns its final cleanup.
        }
        return true;
    }

    boolean handle(MouseEvent mouse) {
        Rect area = lastArea;
        if (area == null || mouse.x() < area.left() || mouse.x() >= area.right()
                || mouse.y() < area.top() || mouse.y() >= area.bottom()) {
            return false;
        }
        if (mouse.kind() == MouseEventKind.SCROLL_UP) {
            scrollOffset = Math.min(scrollOffset + MOUSE_SCROLL_LINES, transcript.lineCount());
            requestRedraw();
            return true;
        }
        if (mouse.kind() == MouseEventKind.SCROLL_DOWN) {
            scrollOffset = Math.max(0, scrollOffset - MOUSE_SCROLL_LINES);
            requestRedraw();
            return true;
        }
        return false;
    }

    boolean paste(String text) {
        if (text.isEmpty()) {
            return true;
        }
        scrollOffset = 0;
        LineDisciplineTerminal current = terminal;
        if (current == null) {
            return true;
        }
        try {
            current.processInputBytes(text.getBytes(StandardCharsets.UTF_8));
        } catch (IOException ignored) {
            // The closing terminal cannot accept more input; close() owns its final cleanup.
        }
        return true;
    }

    void render(Frame frame, Rect area, WorkbenchTheme theme) {
        if (!open) {
            return;
        }
        ensureStarted(Math.max(1, area.width() - 2), Math.max(1, area.height() - 2));
        resize(Math.max(1, area.width() - 2), Math.max(1, area.height() - 2));
        frame.renderWidget(Clear.INSTANCE, area);
        frame.renderWidget(Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.accent())
                .title(Title.from(Span.styled(" Shell ", theme.title())))
                .build(), area);
        Rect inner = new Rect(area.x() + 1, area.y() + 1,
                Math.max(0, area.width() - 2), Math.max(0, area.height() - 2));
        lastArea = area;
        lastInnerHeight = inner.height();
        if (scrollOffset > 0) {
            renderScrollback(frame, inner, theme);
        } else {
            renderScreen(frame, inner, theme.base());
        }
    }

    void renderFooter(Frame frame, Rect area, WorkbenchTheme theme) {
        int x = area.x();
        x = write(frame, x, area.y(), " F6 ", theme.hintKey(), area);
        x = write(frame, x, area.y(), " hide   ", theme.base(), area);
        x = write(frame, x, area.y(), " Shift+F6 ", theme.hintKey(), area);
        x = write(frame, x, area.y(), expanded ? " compact   " : " expand   ", theme.base(), area);
        x = write(frame, x, area.y(), " PgUp/PgDn ", theme.hintKey(), area);
        write(frame, x, area.y(), scrollOffset == 0 ? " scroll   " : " live   ", theme.base(), area);
    }

    private void ensureStarted(int width, int height) {
        if (terminal != null || destroyed) {
            return;
        }
        screen = new ScreenTerminal(width, height);
        try {
            terminal = new LineDisciplineTerminal("tapstate-workbench", "xterm-256color",
                    new ScreenOutput(screen), StandardCharsets.UTF_8);
            terminal.setSize(new Size(width, height));
        } catch (IOException failure) {
            screen.write("Unable to start embedded shell: " + failure.getMessage());
            requestRedraw();
            return;
        }
        shellThread = Thread.ofVirtual().name("tapstate-workbench-shell").start(() -> {
            try {
                repl.runEmbeddedShell(terminal);
            } finally {
                requestRedraw();
            }
        });
    }

    private void resize(int width, int height) {
        if (screen == null || terminal == null) {
            return;
        }
        if (screen.setSize(width, height)) {
            terminal.setSize(new Size(width, height));
            requestRedraw();
        }
    }

    private void renderScreen(Frame frame, Rect area, Style style) {
        if (screen == null || area.width() == 0 || area.height() == 0) {
            return;
        }
        long[] cells = new long[area.width() * area.height()];
        screen.dump(cells, 0, 0, area.height(), area.width(), null);
        for (int y = 0; y < area.height(); y++) {
            StringBuilder line = new StringBuilder(area.width());
            for (int x = 0; x < area.width(); x++) {
                int codePoint = (int) cells[y * area.width() + x];
                line.appendCodePoint(codePoint == 0 ? ' ' : codePoint);
            }
            frame.buffer().setString(area.x(), area.y() + y, line.toString(), style);
        }
    }

    private void renderScrollback(Frame frame, Rect area, WorkbenchTheme theme) {
        List<String> lines = wrap(transcript.snapshot(), area.width());
        int end = Math.max(0, lines.size() - scrollOffset);
        int start = Math.max(0, end - area.height());
        int y = area.y() + Math.max(0, area.height() - (end - start));
        for (int index = start; index < end; index++) {
            frame.buffer().setString(area.x(), y++, lines.get(index), theme.muted());
        }
        if (area.width() > 0 && area.height() > 0) {
            frame.buffer().setString(area.right() - 1, area.y(), "↑", theme.accent());
        }
    }

    private static List<String> wrap(List<String> source, int width) {
        if (width <= 0) {
            return List.of();
        }
        List<String> lines = new ArrayList<>();
        for (String value : source) {
            if (value.isEmpty()) {
                lines.add("");
                continue;
            }
            for (int offset = 0; offset < value.length();) {
                int end = offset;
                int count = 0;
                while (end < value.length() && count < width) {
                    int codePoint = value.codePointAt(end);
                    end += Character.charCount(codePoint);
                    count++;
                }
                lines.add(value.substring(offset, end));
                offset = end;
            }
        }
        return lines;
    }

    private static int write(Frame frame, int x, int y, String text, Style style, Rect area) {
        if (x >= area.right()) {
            return x;
        }
        String clipped = text.substring(0, Math.min(text.length(), area.right() - x));
        return frame.buffer().setString(x, y, clipped, style);
    }

    private static String keySequence(KeyEvent key) {
        if (key.isKey(KeyCode.ENTER)) return "\r";
        if (key.isKey(KeyCode.BACKSPACE)) return "\u007f";
        if (key.isKey(KeyCode.TAB)) return "\t";
        if (key.isKey(KeyCode.UP)) return "\u001bOA";
        if (key.isKey(KeyCode.DOWN)) return "\u001bOB";
        if (key.isKey(KeyCode.LEFT)) return "\u001bOD";
        if (key.isKey(KeyCode.RIGHT)) return "\u001bOC";
        if (key.isKey(KeyCode.HOME)) return "\u001bOH";
        if (key.isKey(KeyCode.END)) return "\u001bOF";
        if (key.isKey(KeyCode.PAGE_UP)) return "\u001b[5~";
        if (key.isKey(KeyCode.PAGE_DOWN)) return "\u001b[6~";
        if (key.isKey(KeyCode.DELETE)) return "\u001b[3~";
        if (key.isCancel()) return "\u001b";
        if (key.code() != KeyCode.CHAR) return "";
        String value = key.string();
        if (key.hasCtrl() && value.length() == 1) {
            char character = value.charAt(0);
            if (character >= 'a' && character <= 'z') return String.valueOf((char) (character - 'a' + 1));
            if (character >= 'A' && character <= 'Z') return String.valueOf((char) (character - 'A' + 1));
        }
        return value;
    }

    private void requestRedraw() {
        if (redrawQueued.compareAndSet(false, true)) {
            scheduler.accept(() -> {
                redraw.run();
                redrawQueued.set(false);
            });
        }
    }

    @Override
    public void close() {
        destroyed = true;
        open = false;
        LineDisciplineTerminal current = terminal;
        if (current != null) {
            try {
                current.close();
            } catch (IOException ignored) {
                // Terminal teardown is best effort; the workbench owner restores the host terminal.
            }
        }
        Thread currentThread = shellThread;
        if (currentThread != null) {
            currentThread.interrupt();
        }
    }

    private final class ScreenOutput extends OutputStream {
        private final ScreenTerminal target;

        private ScreenOutput(ScreenTerminal target) {
            this.target = target;
        }

        @Override
        public void write(int value) {
            target.write(String.valueOf((char) value));
            if (transcript.write(value & 0xff)) {
                scrollOffset = 0;
            }
            requestRedraw();
        }

        @Override
        public void write(byte[] values, int offset, int length) {
            target.write(new String(values, offset, length, StandardCharsets.UTF_8));
            if (transcript.write(values, offset, length)) {
                scrollOffset = 0;
            }
            requestRedraw();
        }
    }

    /** Plain-text fallback scrollback for JLine 3, whose ScreenTerminal exposes no public history API. */
    private static final class Transcript {
        private static final int MAX_LINES = 2_000;

        private final ArrayDeque<String> lines = new ArrayDeque<>();
        private final StringBuilder current = new StringBuilder();
        private EscapeState escapeState = EscapeState.NONE;

        synchronized boolean write(byte[] values, int offset, int length) {
            boolean completedLine = false;
            for (int index = offset; index < offset + length; index++) {
                completedLine |= write(values[index] & 0xff);
            }
            return completedLine;
        }

        synchronized boolean write(int value) {
            if (escapeState == EscapeState.ESCAPE) {
                escapeState = value == '[' ? EscapeState.CONTROL_SEQUENCE : EscapeState.NONE;
                return false;
            }
            if (escapeState == EscapeState.CONTROL_SEQUENCE) {
                if (value >= '@' && value <= '~') {
                    escapeState = EscapeState.NONE;
                }
                return false;
            }
            if (value == 0x1b) {
                escapeState = EscapeState.ESCAPE;
                return false;
            }
            if (value == '\n') {
                lines.addLast(current.toString());
                current.setLength(0);
                while (lines.size() > MAX_LINES) {
                    lines.removeFirst();
                }
                return true;
            }
            if (value == '\b' || value == 0x7f) {
                if (!current.isEmpty()) {
                    current.setLength(current.offsetByCodePoints(current.length(), -1));
                }
                return false;
            }
            if (value >= 0x20 && value != '\r') {
                current.append((char) value);
            }
            return false;
        }

        synchronized List<String> snapshot() {
            List<String> snapshot = new ArrayList<>(lines);
            if (!current.isEmpty()) {
                snapshot.add(current.toString());
            }
            return List.copyOf(snapshot);
        }

        synchronized int lineCount() {
            return lines.size() + (current.isEmpty() ? 0 : 1);
        }

        private enum EscapeState {
            NONE,
            ESCAPE,
            CONTROL_SEQUENCE
        }
    }
}
