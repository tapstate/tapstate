package io.tapstate.cli;

import dev.tamboui.layout.Rect;
import dev.tamboui.style.Style;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.Span;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** A resident virtual terminal panel, not a system shell or child process. */
final class WorkbenchShellPanel implements AutoCloseable {

    private static final int COMPACT_HEIGHT = 10;
    private static final int EXPANDED_HEIGHT = 18;

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
        LineDisciplineTerminal current = terminal;
        if (current == null) {
            return true;
        }
        try {
            current.processInputBytes(keySequence(key).getBytes(StandardCharsets.UTF_8));
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
        renderScreen(frame, new Rect(area.x() + 1, area.y() + 1,
                Math.max(0, area.width() - 2), Math.max(0, area.height() - 2)), theme.base());
    }

    void renderFooter(Frame frame, Rect area, WorkbenchTheme theme) {
        int x = area.x();
        x = write(frame, x, area.y(), " F6 ", theme.hintKey(), area);
        x = write(frame, x, area.y(), " hide   ", theme.base(), area);
        x = write(frame, x, area.y(), " Shift+F6 ", theme.hintKey(), area);
        x = write(frame, x, area.y(), expanded ? " compact   " : " expand   ", theme.base(), area);
        x = write(frame, x, area.y(), " PgUp/PgDn ", theme.hintKey(), area);
        write(frame, x, area.y(), " history   ", theme.base(), area);
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
        if (key.isKey(KeyCode.UP)) return "\u001b[A";
        if (key.isKey(KeyCode.DOWN)) return "\u001b[B";
        if (key.isKey(KeyCode.LEFT)) return "\u001b[D";
        if (key.isKey(KeyCode.RIGHT)) return "\u001b[C";
        if (key.isKey(KeyCode.HOME)) return "\u001b[H";
        if (key.isKey(KeyCode.END)) return "\u001b[F";
        if (key.isKey(KeyCode.PAGE_UP)) return "\u001b[5~";
        if (key.isKey(KeyCode.PAGE_DOWN)) return "\u001b[6~";
        if (key.isKey(KeyCode.DELETE)) return "\u001b[3~";
        if (key.isCancel()) return "\u001b";
        return key.code() == KeyCode.CHAR ? key.string() : "";
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
            requestRedraw();
        }

        @Override
        public void write(byte[] values, int offset, int length) {
            target.write(new String(values, offset, length, StandardCharsets.UTF_8));
            requestRedraw();
        }
    }
}
