package io.tapstate.cli;

import dev.tamboui.terminal.Frame;
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
