package io.tapstate.cli;

import dev.tamboui.style.AnsiColor;
import dev.tamboui.style.Color;
import dev.tamboui.style.Modifier;
import dev.tamboui.layout.Rect;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.terminal.Frame;
import dev.tamboui.text.Line;
import dev.tamboui.tui.event.KeyCode;
import dev.tamboui.tui.event.KeyEvent;
import dev.tamboui.tui.event.KeyModifiers;
import dev.tamboui.tui.event.MouseButton;
import dev.tamboui.tui.event.MouseEvent;
import org.jline.utils.ScreenTerminal;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class WorkbenchShellPanelTest {

    @Test
    void convertsRealScreenTerminalAnsiCellsWithoutLosingTheirAttributes() {
        ScreenTerminal screen = new ScreenTerminal(12, 1);
        screen.write("\u001b[1;31;44mred\u001b[0m plain");
        long[] cells = new long[12];
        screen.dump(cells, new int[2]);

        Line line = WorkbenchShellPanel.convertRow(cells, 0, 12, -1);

        assertThat(line.rawContent()).isEqualTo("red plain   ");
        assertThat(line.spans()).hasSize(2);
        assertThat(line.spans().getFirst().content()).isEqualTo("red");
        assertThat(line.spans().getFirst().style().fg()).contains(Color.ansi(AnsiColor.RED));
        assertThat(line.spans().getFirst().style().bg()).contains(Color.ansi(AnsiColor.BLUE));
        assertThat(line.spans().getFirst().style().effectiveModifiers()).contains(Modifier.BOLD);
        assertThat(line.spans().get(1).content()).isEqualTo(" plain   ");
        assertThat(line.spans().get(1).style().fg()).isEmpty();
        assertThat(line.spans().get(1).style().bg()).isEmpty();
    }

    @Test
    void screenTerminalRetainsScrollableHistoryAfterTheVisibleRowsHaveRolledOver() {
        ScreenTerminal screen = new ScreenTerminal(8, 2);
        screen.write("first\r\nsecond\r\nthird");

        assertThat(screen.getHistorySize()).isPositive();
        assertThat(screen.getHistory())
                .extracting(row -> rowText(row, 8))
                .anyMatch(text -> text.startsWith("first"));
    }

    @Test
    void f6ClosesButShiftF6IsReservedForTheOuterWorkbenchRouter() {
        WorkbenchShellPanel panel = panel();
        panel.open();

        assertThat(panel.handle(KeyEvent.ofKey(KeyCode.F6, KeyModifiers.SHIFT))).isTrue();
        assertThat(panel.isOpen()).isFalse();

        panel.open();
        assertThat(panel.handle(KeyEvent.ofKey(KeyCode.F6))).isTrue();
        assertThat(panel.isOpen()).isFalse();
    }

    @Test
    void cyclesTheSameHeightSequenceAsCamel() {
        WorkbenchShellPanel panel = panel();

        assertThat(panel.heightPercent()).isEqualTo(50);
        panel.cycleHeight();
        assertThat(panel.heightPercent()).isEqualTo(75);
        panel.cycleHeight();
        assertThat(panel.heightPercent()).isEqualTo(100);
        panel.cycleHeight();
        assertThat(panel.heightPercent()).isEqualTo(25);
        panel.cycleHeight();
        assertThat(panel.heightPercent()).isEqualTo(50);
    }

    @Test
    void clickingOnlyTheFooterCloseRegionClosesThePanel() {
        WorkbenchShellPanel panel = panel();
        Rect area = new Rect(0, 23, 80, 1);
        panel.open();
        panel.renderFooter(Frame.forTesting(Buffer.empty(new Rect(0, 0, 80, 24))), area, WorkbenchTheme.dark());

        assertThat(panel.handle(MouseEvent.press(MouseButton.LEFT, 2, 23))).isTrue();
        assertThat(panel.isOpen()).isFalse();

        panel.open();
        assertThat(panel.handle(MouseEvent.press(MouseButton.LEFT, 25, 23))).isFalse();
        assertThat(panel.isOpen()).isTrue();
    }

    private static WorkbenchShellPanel panel() {
        return new WorkbenchShellPanel(new Repl(Cli.newCommandLine(), Path.of(".")), () -> {}, Runnable::run);
    }

    private static String rowText(long[] row, int width) {
        StringBuilder text = new StringBuilder(width);
        for (int column = 0; column < width; column++) {
            int codePoint = ScreenTerminal.cellCodePoint(row[column]);
            text.appendCodePoint(codePoint == 0 ? ' ' : codePoint);
        }
        return text.toString();
    }
}
