package io.tapstate.cli;

import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The block-capture logic behind {@link JLinePrompter}'s multi-line {@code lines()} primitive, driven
 * by a fake line source so the sentinel / end-of-input / indentation rules are exercised directly. The
 * thin JLine adapter that turns a line reader into this source is not unit-tested (a real terminal is
 * needed); the rules that matter live in {@code captureBlock}. The one exception is the
 * default-carrying choice, driven over a dumb terminal on fixed streams: what an empty line means
 * there is the contract the guided first run relies on.
 */
class JLinePrompterTest {

    /** A line source that yields each line once, then {@code null} forever (end of input). */
    private static Supplier<String> source(String... lines) {
        Deque<String> queue = new ArrayDeque<>(List.of(lines));
        return () -> queue.isEmpty() ? null : queue.poll();
    }

    @Test
    void capturesAMultilineBlockUntilTheDotSentinel() {
        // the block ends at the lone '.'; lines after it are not consumed into the block
        assertThat(JLinePrompter.captureBlock(source("SELECT 1", "FROM t", ".", "ignored")))
                .isEqualTo("SELECT 1\nFROM t");
    }

    @Test
    void preservesLeadingIndentationOfBlockLines() {
        assertThat(JLinePrompter.captureBlock(source("def f():", "    return 1", ".")))
                .isEqualTo("def f():\n    return 1");
    }

    @Test
    void endsTheBlockAtEndOfInputWithoutASentinel() {
        assertThat(JLinePrompter.captureBlock(source("only line"))).isEqualTo("only line");
    }

    @Test
    void anImmediateSentinelYieldsAnEmptyBlock() {
        assertThat(JLinePrompter.captureBlock(source("."))).isEmpty();
    }

    @Test
    void immediateEndOfInputYieldsAnEmptyBlock() {
        assertThat(JLinePrompter.captureBlock(source())).isEmpty();
    }

    @Test
    void aBlankLineInsideTheBlockIsPreserved() {
        // only a lone '.' terminates; an empty line is real content (e.g. a paragraph break in SQL)
        assertThat(JLinePrompter.captureBlock(source("a", "", "b", ".")))
                .isEqualTo("a\n\nb");
    }

    @Test
    void aSecretReplyIsReturnedVerbatimWhileFreeTextIsTrimmed() {
        // a password / token must reach the wire exactly as typed; free-text answers drop surrounding
        // whitespace, and either way end-of-input (null) reads as empty
        assertThat(JLinePrompter.normalizeReply("  pass phrase  ", false)).isEqualTo("  pass phrase  ");
        assertThat(JLinePrompter.normalizeReply("  alice  ", true)).isEqualTo("alice");
        assertThat(JLinePrompter.normalizeReply(null, false)).isEmpty();
        assertThat(JLinePrompter.normalizeReply(null, true)).isEmpty();
    }

    /**
     * A prompter over a dumb terminal that reads {@code input} and writes to {@code output}. Built
     * directly rather than through the terminal builder, which even with streams and a dumb hint can
     * pick a native pty whose close blocks the test forever.
     */
    private static JLinePrompter over(String input, ByteArrayOutputStream output) throws IOException {
        DumbTerminal terminal = new DumbTerminal("test", "dumb",
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), output, StandardCharsets.UTF_8);
        return new JLinePrompter(terminal, true);
    }

    @Test
    void anEmptyLineTakesTheMarkedDefaultOfAChoice() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (JLinePrompter prompter = over("\n", output)) {
            // the default is the first option here, so this discriminates from the old "empty = last" rule
            assertThat(prompter.choose("Pick", List.of("first", "second", "third"), "first")).isEqualTo("first");
        }
        // the menu marks which option an empty reply takes
        assertThat(output.toString(StandardCharsets.UTF_8)).contains("first").contains("default");
    }

    @Test
    void aNumberStillPicksThatOptionWhenAChoiceCarriesADefault() throws IOException {
        try (JLinePrompter prompter = over("3\n", new ByteArrayOutputStream())) {
            assertThat(prompter.choose("Pick", List.of("first", "second", "third"), "first")).isEqualTo("third");
        }
    }
}
