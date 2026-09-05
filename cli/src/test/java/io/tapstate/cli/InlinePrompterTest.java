package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InlinePrompterTest {

    @Test
    void arrowNavigationSelectsTheHighlightedContextAction() {
        RecordingView view = new RecordingView();
        InlinePrompter prompter = new InlinePrompter(
                keys(27, '[', 'B', 13), view);

        assertThat(prompter.choose("Context action", List.of("Create a context", "Quit")))
                .isEqualTo("Quit");
        assertThat(view.selections).containsExactly(0, 1);
    }

    @Test
    void verticalKeysDoNotTerminateATextPrompt() {
        RecordingView view = new RecordingView();
        InlinePrompter prompter = new InlinePrompter(
                keys(27, '[', 'A', 'n', 13), view);

        assertThat(prompter.ask("Context name", "")).isEqualTo("n");
        assertThat(view.inputs).contains("n");
    }

    @Test
    void multilinePromptPreservesBlankAndIndentedLines() {
        RecordingView view = new RecordingView();
        InlinePrompter prompter = new InlinePrompter(
                keys('a', 13, 13, ' ', ' ', 'b', 13, '.', 13), view);

        assertThat(prompter.lines("SQL")).isEqualTo("a\n\n  b");
    }

    private static InlinePrompter.KeyReader keys(int... values) {
        ArrayDeque<Integer> queue = new ArrayDeque<>();
        for (int value : values) {
            queue.add(value);
        }
        return timeout -> queue.isEmpty()
                ? org.jline.utils.NonBlockingReader.EOF
                : queue.removeFirst();
    }

    private static final class RecordingView implements InlinePrompter.View {
        private final List<Integer> selections = new ArrayList<>();
        private final List<String> inputs = new ArrayList<>();

        @Override
        public void showInput(String question, String value, int cursor,
                              String defaultValue, boolean secret) {
            inputs.add(value);
        }

        @Override
        public void showChoices(String question, List<String> options, int selected, String query) {
            selections.add(selected);
        }
    }
}
