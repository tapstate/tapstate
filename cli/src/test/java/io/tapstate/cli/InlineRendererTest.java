package io.tapstate.cli;

import dev.tamboui.terminal.Frame;
import dev.tamboui.buffer.Buffer;
import dev.tamboui.layout.Rect;
import dev.tamboui.style.Color;
import dev.tamboui.text.Text;
import dev.tamboui.widgets.paragraph.Paragraph;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InlineRendererTest {

    @Test
    void renderAndUpdateKeepTheActiveRegionAtTheRequestedHeight() {
        RecordingSurface surface = new RecordingSurface();
        InlineRenderer renderer = new InlineRenderer(surface, 8);

        renderer.render(frame -> { }, 3);
        renderer.update(frame -> { }, 5);
        renderer.update(frame -> { }, 2);

        assertThat(surface.heights).containsExactly(3, 5, 2);
        assertThat(surface.renderCount).isEqualTo(3);
    }

    @Test
    void clearRemovesTheActiveRegionWithoutCommittingIt() {
        RecordingSurface surface = new RecordingSurface();
        InlineRenderer renderer = new InlineRenderer(surface, 8);

        renderer.render(frame -> { }, 4);
        renderer.clear();

        assertThat(surface.heights).containsExactly(4, 0);
        assertThat(surface.commitLines).isEmpty();
    }

    @Test
    void runningUpdatesStayTemporaryUntilTheFinalResultIsCommitted() {
        RecordingSurface surface = new RecordingSurface();
        InlineRenderer renderer = new InlineRenderer(surface, 8);

        renderer.render(frame -> { }, 4);
        renderer.update(frame -> { }, 4);
        renderer.update(frame -> { }, 4);
        renderer.clear();
        renderer.commit("valid: 1");

        assertThat(surface.heights).containsExactly(4, 4, 4, 0);
        assertThat(surface.commitLines).containsExactly("valid: 1");
    }

    @Test
    void commitPreservesMultipleScrollbackLinesAndDoesNotRenderThemAsActiveState() {
        RecordingSurface surface = new RecordingSurface();
        InlineRenderer renderer = new InlineRenderer(surface, 8);

        renderer.commit("first\nsecond\n\nthird");

        assertThat(surface.commitLines).containsExactly("first", "second", "", "third");
        assertThat(surface.heights).isEmpty();
    }

    @Test
    void commitRemovesAnsiSequencesAndPrintWriterTrailingNewline() {
        RecordingSurface surface = new RecordingSurface();
        InlineRenderer renderer = new InlineRenderer(surface, 8);

        renderer.commit("\u001b[31merror\u001b[0m\nnext\n");

        assertThat(surface.commitLines).containsExactly("error", "next");
    }

    @Test
    void heightIsClampedToTheRendererCapacity() {
        RecordingSurface surface = new RecordingSurface();
        InlineRenderer renderer = new InlineRenderer(surface, 4);

        renderer.render(frame -> { }, 0);
        renderer.update(frame -> { }, 9);

        assertThat(surface.heights).containsExactly(1, 4);
    }

    @Test
    void closeReleasesTheSurfaceOnce() {
        RecordingSurface surface = new RecordingSurface();
        InlineRenderer renderer = new InlineRenderer(surface, 4);

        renderer.close();
        renderer.close();

        assertThat(surface.releaseCount).isEqualTo(1);
    }

    @Test
    void onlyCommandsWithoutTerminalPromptsRunInTheBackground() {
        assertThat(InlineTui.canRunInBackground("ls")).isTrue();
        assertThat(InlineTui.canRunInBackground("pwd")).isTrue();
        assertThat(InlineTui.canRunInBackground("validate ./work")).isTrue();
        assertThat(InlineTui.canRunInBackground("connect localhost:8081")).isFalse();
        assertThat(InlineTui.canRunInBackground("login admin")).isFalse();
        assertThat(InlineTui.canRunInBackground(":ctx")).isFalse();
        assertThat(InlineTui.canRunInBackground("context list")).isFalse();
        assertThat(InlineTui.canRunInBackground("auth login admin")).isFalse();
        assertThat(InlineTui.canRunInBackground("exit")).isFalse();
    }

    @Test
    void contextArrowsStayInsideTheInlineSelectionSurface() {
        assertThat(InlinePrompter.selectionAfterKey(0, 2, InlinePrompter.DOWN)).isEqualTo(1);
        assertThat(InlinePrompter.selectionAfterKey(1, 2, InlinePrompter.DOWN)).isEqualTo(1);
        assertThat(InlinePrompter.selectionAfterKey(1, 2, InlinePrompter.UP)).isEqualTo(0);
        assertThat(InlinePrompter.selectionAfterKey(0, 2, InlinePrompter.UP)).isEqualTo(0);
    }

    @Test
    void borderlessInputSurfaceStillPaintsItsBackgroundAndCursor() {
        Color background = Color.rgb(35, 38, 43);
        Buffer buffer = Buffer.empty(Rect.of(20, 2));
        Frame frame = Frame.forTesting(buffer);

        InlineTui.fillBackground(frame, new Rect(0, 0, 20, 2), background);
        frame.renderWidget(Paragraph.builder()
                        .text(Text.from("▌").fg(Color.rgb(82, 166, 118)).bg(background))
                        .background(background)
                        .build(), new Rect(0, 0, 20, 2));

        assertThat(buffer.get(0, 0).symbol()).isEqualTo("▌");
        assertThat(buffer.get(19, 0).symbol()).isEqualTo("\u00a0");
        assertThat(buffer.get(19, 0).style().bg()).contains(background);
        assertThat(buffer.toAnsiStringTrimmed()).contains("\u00a0");
    }

    private static final class RecordingSurface implements InlineRenderer.Surface {
        private final List<Integer> heights = new ArrayList<>();
        private final List<String> commitLines = new ArrayList<>();
        private int renderCount;
        private int releaseCount;

        @Override
        public void render(java.util.function.Consumer<Frame> view, int height) {
            renderCount++;
            heights.add(height);
        }

        @Override
        public void commit(String line) {
            commitLines.add(line);
        }

        @Override
        public void release() {
            releaseCount++;
        }
    }
}
