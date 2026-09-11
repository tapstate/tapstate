package io.tapstate.cli;

import dev.tamboui.layout.Constraint;
import dev.tamboui.layout.Layout;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.AnsiColor;
import dev.tamboui.style.Color;
import dev.tamboui.style.Overflow;
import dev.tamboui.style.Style;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.Line;
import dev.tamboui.text.Span;
import dev.tamboui.text.Text;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.MouseEvent;
import dev.tamboui.tui.event.MouseEventKind;
import dev.tamboui.widgets.block.Block;
import dev.tamboui.widgets.block.BorderType;
import dev.tamboui.widgets.block.Borders;
import dev.tamboui.widgets.block.Title;
import dev.tamboui.widgets.paragraph.Paragraph;
import dev.tamboui.widgets.scrollbar.Scrollbar;
import dev.tamboui.widgets.scrollbar.ScrollbarState;
import org.jline.terminal.Size;
import org.jline.terminal.impl.LineDisciplineTerminal;
import org.jline.utils.ScreenTerminal;
import org.jline.utils.ScreenTerminalOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** A resident virtual terminal panel rendered from JLine's real screen buffer. */
final class WorkbenchShellPanel {

    private static final int[] HEIGHT_PERCENTS = {25, 50, 75, 100};
    private static final int INITIAL_HEIGHT_INDEX = 1;
    private static final int MOUSE_SCROLL_LINES = 3;

    private final Repl repl;
    private final Runnable redraw;
    private final Consumer<Runnable> scheduler;
    private final AtomicBoolean redrawQueued = new AtomicBoolean();
    private final ScrollbarState scrollbarState = new ScrollbarState();

    private volatile boolean open;
    private volatile boolean destroyed;
    private volatile boolean shellExited;
    private int heightIndex = INITIAL_HEIGHT_INDEX;
    private ScreenTerminal screen;
    private LineDisciplineTerminal terminal;
    private Thread shellThread;
    private Rect lastArea;
    private Rect closeArea;
    private int lastWidth;
    private int lastHeight;
    private int lastHistorySize;
    private int scrollOffset;
    private String startError;

    WorkbenchShellPanel(Repl repl, Runnable redraw, Consumer<Runnable> scheduler) {
        this.repl = Objects.requireNonNull(repl, "repl");
        this.redraw = Objects.requireNonNull(redraw, "redraw");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    }

    boolean isOpen() {
        return open;
    }

    int heightFor(Rect area) {
        int available = Math.max(7, area.height() - 8);
        return Math.clamp(available * HEIGHT_PERCENTS[heightIndex] / 100, 7, available);
    }

    void open() {
        if (destroyed) {
            return;
        }
        open = true;
        if (shellExited || startError != null) {
            stopShell();
            shellExited = false;
            startError = null;
        }
        requestRedraw();
    }

    void close() {
        open = false;
        requestRedraw();
    }

    void destroy() {
        destroyed = true;
        open = false;
        stopShell();
    }

    boolean handle(KeyEvent key) {
        if (key.isKey(KeyCode.F6)) {
            close();
            return true;
        }
        if (key.isPageUp()) {
            int historySize = screen == null ? 0 : screen.getHistorySize();
            scrollOffset = Math.min(scrollOffset + Math.max(1, lastHeight), historySize);
            requestRedraw();
            return true;
        }
        if (key.isPageDown()) {
            scrollOffset = Math.max(0, scrollOffset - Math.max(1, lastHeight));
            requestRedraw();
            return true;
        }
        scrollOffset = 0;
        if (terminal == null) {
            return true;
        }
        try {
            byte[] sequence = encodeKey(key);
            if (sequence.length > 0) {
                terminal.processInputBytes(sequence);
            }
        } catch (IOException | ArrayIndexOutOfBoundsException ignored) {
            // A shutdown or concurrent resize can close or replace the virtual terminal buffer.
        }
        return true;
    }

    void cycleHeight() {
        heightIndex = (heightIndex + 1) % HEIGHT_PERCENTS.length;
        requestRedraw();
    }

    int heightPercent() {
        return HEIGHT_PERCENTS[heightIndex];
    }

    boolean handle(MouseEvent mouse) {
        if (mouse.isClick() && contains(closeArea, mouse.x(), mouse.y())) {
            close();
            return true;
        }
        if (!contains(lastArea, mouse.x(), mouse.y())) {
            return false;
        }
        if (mouse.kind() == MouseEventKind.SCROLL_UP) {
            int historySize = screen == null ? 0 : screen.getHistorySize();
            scrollOffset = Math.min(scrollOffset + MOUSE_SCROLL_LINES, historySize);
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
        if (terminal == null) {
            return true;
        }
        try {
            terminal.processInputBytes(text.getBytes(StandardCharsets.UTF_8));
        } catch (IOException | ArrayIndexOutOfBoundsException ignored) {
            // See handle(KeyEvent): terminal shutdown and resize are safe no-ops.
        }
        return true;
    }

    void render(Frame frame, Rect area, WorkbenchTheme theme) {
        if (!open) {
            return;
        }
        if (shellExited) {
            close();
            return;
        }
        lastArea = area;
        Block block = Block.builder()
                .borderType(BorderType.ROUNDED)
                .borders(Borders.ALL)
                .borderStyle(theme.accent())
                .title(Title.from(Line.from(Span.styled(" Shell ", theme.title()))))
                .build();
        frame.renderWidget(block, area);
        Rect inner = block.inner(area);
        if (screen == null && inner.width() > 2 && inner.height() > 2) {
            startShell(inner.width(), inner.height());
        }
        resize(inner.width(), inner.height());
        if (startError != null) {
            frame.renderWidget(Paragraph.from(Line.from(Span.styled(startError, theme.error()))), inner);
            return;
        }
        if (screen == null) {
            return;
        }

        long[] cells = new long[inner.width() * inner.height()];
        int[] cursor = new int[2];
        try {
            screen.dump(cells, cursor);
        } catch (ArrayIndexOutOfBoundsException ignored) {
            return;
        }

        int historySize = screen.getHistorySize();
        if (historySize > lastHistorySize && scrollOffset > 0) {
            scrollOffset = 0;
        }
        lastHistorySize = historySize;
        List<Line> lines = scrollOffset > 0
                ? renderScrolledView(cells, inner.width(), inner.height())
                : renderLiveView(cells, inner.width(), inner.height(), cursor[0], cursor[1]);
        Rect contentArea = renderScrollbar(frame, inner, historySize);
        frame.renderWidget(Paragraph.builder()
                .text(Text.from(lines))
                .overflow(Overflow.CLIP)
                .build(), contentArea);
    }

    void renderFooter(Frame frame, Rect area, WorkbenchTheme theme) {
        int x = area.x();
        int closeStart = x;
        x = write(frame, x, area.y(), " F6 ", theme.hintKey(), area);
        x = write(frame, x, area.y(), " close   ", theme.base(), area);
        closeArea = new Rect(closeStart, area.y(), Math.max(0, x - closeStart), 1);
        x = write(frame, x, area.y(), " Shift+F6 ", theme.hintKey(), area);
        x = write(frame, x, area.y(), " resize (" + HEIGHT_PERCENTS[heightIndex] + "%)   ", theme.base(), area);
        x = write(frame, x, area.y(), " PgUp/Dn ", theme.hintKey(), area);
        write(frame, x, area.y(), " scroll", theme.base(), area);
    }

    private Rect renderScrollbar(Frame frame, Rect inner, int historySize) {
        int totalLines = historySize + inner.height();
        if (totalLines <= inner.height()) {
            return inner;
        }
        List<Rect> chunks = Layout.horizontal()
                .constraints(Constraint.fill(), Constraint.length(1))
                .split(inner);
        int viewStart = Math.max(0, totalLines - scrollOffset - inner.height());
        scrollbarState.contentLength(totalLines)
                .viewportContentLength(inner.height())
                .position(viewStart);
        frame.renderStatefulWidget(Scrollbar.builder().build(), chunks.get(1), scrollbarState);
        return chunks.get(0);
    }

    private List<Line> renderLiveView(long[] cells, int width, int height, int cursorColumn, int cursorRow) {
        List<Line> lines = new ArrayList<>(height);
        for (int row = 0; row < height; row++) {
            lines.add(convertRow(cells, row * width, width, row == cursorRow ? cursorColumn : -1));
        }
        return lines;
    }

    private List<Line> renderScrolledView(long[] cells, int width, int height) {
        List<long[]> history = screen.getHistory();
        if (history.isEmpty()) {
            return renderLiveView(cells, width, height, -1, -1);
        }
        int viewStart = Math.max(0, history.size() + height - scrollOffset - height);
        List<Line> lines = new ArrayList<>(height);
        for (int row = 0; row < height; row++) {
            int index = viewStart + row;
            if (index < history.size()) {
                lines.add(convertRow(history.get(index), 0, Math.min(history.get(index).length, width), -1));
            } else if (index - history.size() < height) {
                lines.add(convertRow(cells, (index - history.size()) * width, width, -1));
            } else {
                lines.add(Line.from(Span.raw("")));
            }
        }
        return lines;
    }

    static Line convertRow(long[] buffer, int offset, int width, int cursorColumn) {
        List<Span> spans = new ArrayList<>();
        int column = 0;
        while (column < width) {
            long cell = buffer[offset + column];
            long attribute = ScreenTerminal.cellAttr(cell);
            Style style = convertCellToStyle(cell);
            int codePoint = ScreenTerminal.cellCodePoint(cell);
            if (column == cursorColumn) {
                spans.add(Span.styled(String.valueOf(Character.toChars(codePoint == 0 ? ' ' : codePoint)),
                        style.reversed()));
                column++;
                continue;
            }
            StringBuilder text = new StringBuilder();
            text.appendCodePoint(codePoint == 0 ? ' ' : codePoint);
            int next = column + 1;
            while (next < width && next != cursorColumn) {
                long nextCell = buffer[offset + next];
                if (ScreenTerminal.cellAttr(nextCell) != attribute) {
                    break;
                }
                int nextCodePoint = ScreenTerminal.cellCodePoint(nextCell);
                text.appendCodePoint(nextCodePoint == 0 ? ' ' : nextCodePoint);
                next++;
            }
            spans.add(Span.styled(text.toString(), style));
            column = next;
        }
        return Line.from(spans);
    }

    static Style convertCellToStyle(long cell) {
        Style style = Style.EMPTY;
        if (ScreenTerminal.cellBold(cell)) style = style.bold();
        if (ScreenTerminal.cellUnderline(cell)) style = style.underlined();
        if (ScreenTerminal.cellInverse(cell)) style = style.reversed();
        if (ScreenTerminal.cellDim(cell)) style = style.dim();
        if (ScreenTerminal.cellItalic(cell)) style = style.italic();
        if (ScreenTerminal.cellHasForeground(cell)) style = style.fg(resolveColor(ScreenTerminal.cellForeground(cell)));
        if (ScreenTerminal.cellHasBackground(cell)) style = style.bg(resolveColor(ScreenTerminal.cellBackground(cell)));
        return style;
    }

    static Color resolveColor(int rgb12) {
        AnsiColor ansi = switch (rgb12) {
            case 0x000 -> AnsiColor.BLACK;
            case 0x800 -> AnsiColor.RED;
            case 0x080 -> AnsiColor.GREEN;
            case 0x880 -> AnsiColor.YELLOW;
            case 0x008 -> AnsiColor.BLUE;
            case 0x808 -> AnsiColor.MAGENTA;
            case 0x088 -> AnsiColor.CYAN;
            case 0xccc -> AnsiColor.WHITE;
            case 0x888 -> AnsiColor.BRIGHT_BLACK;
            case 0xf00 -> AnsiColor.BRIGHT_RED;
            case 0x0f0 -> AnsiColor.BRIGHT_GREEN;
            case 0xff0 -> AnsiColor.BRIGHT_YELLOW;
            case 0x00f -> AnsiColor.BRIGHT_BLUE;
            case 0xf0f -> AnsiColor.BRIGHT_MAGENTA;
            case 0x0ff -> AnsiColor.BRIGHT_CYAN;
            case 0xfff -> AnsiColor.BRIGHT_WHITE;
            default -> null;
        };
        if (ansi != null) {
            return Color.ansi(ansi);
        }
        return Color.rgb(((rgb12 >> 8) & 0xf) * 17, ((rgb12 >> 4) & 0xf) * 17, (rgb12 & 0xf) * 17);
    }

    private void startShell(int width, int height) {
        try {
            screen = new ScreenTerminal(width, height);
            lastWidth = width;
            lastHeight = height;
            DelegateOutputStream output = new DelegateOutputStream();
            terminal = new LineDisciplineTerminal("tapstate-workbench", "screen-256color", output,
                    StandardCharsets.UTF_8);
            terminal.setSize(Size.of(width, height));
            OutputStream feedback = new OutputStream() {
                @Override
                public void write(int value) throws IOException {
                    terminal.processInputByte(value);
                }
            };
            output.delegate = new ScreenTerminalOutputStream(screen, StandardCharsets.UTF_8, feedback);
            shellThread = Thread.ofVirtual().name("tapstate-workbench-shell").start(() -> {
                try {
                    repl.runEmbeddedShell(terminal);
                    shellExited = true;
                } finally {
                    requestRedraw();
                }
            });
        } catch (Exception failure) {
            startError = failure.getClass().getSimpleName() + ": " + failure.getMessage();
            stopShell();
        }
    }

    private void resize(int width, int height) {
        if (screen == null || terminal == null || width == lastWidth && height == lastHeight) {
            return;
        }
        screen.setSize(Size.of(width, height));
        terminal.setSize(Size.of(width, height));
        lastWidth = width;
        lastHeight = height;
    }

    private void stopShell() {
        if (shellThread != null) {
            shellThread.interrupt();
            shellThread = null;
        }
        if (terminal != null) {
            try {
                terminal.close();
            } catch (IOException ignored) {
                // Terminal cleanup is best effort during workbench shutdown.
            }
            terminal = null;
        }
        screen = null;
        lastHistorySize = 0;
        scrollOffset = 0;
    }

    private static byte[] encodeKey(KeyEvent key) {
        if (key.code() == KeyCode.CHAR) {
            String value = key.string();
            if (key.hasCtrl() && value.length() == 1) {
                char character = value.charAt(0);
                if (character >= 'a' && character <= 'z') return new byte[] {(byte) (character - 'a' + 1)};
                if (character >= 'A' && character <= 'Z') return new byte[] {(byte) (character - 'A' + 1)};
            }
            return value.getBytes(StandardCharsets.UTF_8);
        }
        return switch (key.code()) {
            case ENTER -> new byte[] {'\r'};
            case BACKSPACE -> new byte[] {0x7f};
            case TAB -> new byte[] {'\t'};
            case UP -> "\033OA".getBytes(StandardCharsets.UTF_8);
            case DOWN -> "\033OB".getBytes(StandardCharsets.UTF_8);
            case RIGHT -> "\033OC".getBytes(StandardCharsets.UTF_8);
            case LEFT -> "\033OD".getBytes(StandardCharsets.UTF_8);
            case HOME -> "\033OH".getBytes(StandardCharsets.UTF_8);
            case END -> "\033OF".getBytes(StandardCharsets.UTF_8);
            case PAGE_UP -> "\033[5~".getBytes(StandardCharsets.UTF_8);
            case PAGE_DOWN -> "\033[6~".getBytes(StandardCharsets.UTF_8);
            case DELETE -> "\033[3~".getBytes(StandardCharsets.UTF_8);
            default -> new byte[0];
        };
    }

    private static boolean contains(Rect area, int x, int y) {
        return area != null && x >= area.left() && x < area.right() && y >= area.top() && y < area.bottom();
    }

    private static int write(Frame frame, int x, int y, String text, Style style, Rect area) {
        if (x >= area.right()) {
            return x;
        }
        return frame.buffer().setString(x, y, text.substring(0, Math.min(text.length(), area.right() - x)), style);
    }

    private void requestRedraw() {
        if (redrawQueued.compareAndSet(false, true)) {
            scheduler.accept(() -> {
                redraw.run();
                redrawQueued.set(false);
            });
        }
    }

    private static final class DelegateOutputStream extends OutputStream {
        private volatile OutputStream delegate;

        @Override
        public void write(int value) throws IOException {
            if (delegate != null) {
                delegate.write(value);
            }
        }

        @Override
        public void write(byte[] values, int offset, int length) throws IOException {
            if (delegate != null) {
                delegate.write(values, offset, length);
            }
        }

        @Override
        public void flush() throws IOException {
            if (delegate != null) {
                delegate.flush();
            }
        }

        @Override
        public void close() throws IOException {
            if (delegate != null) {
                delegate.close();
            }
        }
    }
}
