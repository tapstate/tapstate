package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TuiCommandBarTest {

    @Test
    void appendsPrintableInputAndDeletesOneCodePointAtATime() {
        TuiCommandBar.Update typed = TuiCommandBar.accept("ls ", 'x');
        TuiCommandBar.Update deleted = TuiCommandBar.accept(typed.value(), TuiCommandBar.BACKSPACE);

        assertThat(typed.value()).isEqualTo("ls x");
        assertThat(typed.event()).isEqualTo(TuiCommandBar.Event.NONE);
        assertThat(deleted.value()).isEqualTo("ls ");
    }

    @Test
    void separatesSubmitPaletteCancelAndEofQuitFromTextInput() {
        assertThat(TuiCommandBar.accept("ls", TuiCommandBar.ENTER).event())
                .isEqualTo(TuiCommandBar.Event.SUBMIT);
        assertThat(TuiCommandBar.accept("ls", TuiCommandBar.CTRL_P).event())
                .isEqualTo(TuiCommandBar.Event.PALETTE);
        assertThat(TuiCommandBar.accept("ls", TuiCommandBar.CTRL_C).event())
                .isEqualTo(TuiCommandBar.Event.CANCEL);
        assertThat(TuiCommandBar.accept("", TuiCommandBar.CTRL_D).event())
                .isEqualTo(TuiCommandBar.Event.QUIT);
        assertThat(TuiCommandBar.accept("ls", TuiCommandBar.CTRL_D).event())
                .isEqualTo(TuiCommandBar.Event.NONE);
    }

    @Test
    void deletesAWholeSupplementaryCodePoint() {
        TuiCommandBar.Update update = TuiCommandBar.accept("hello 🚀", TuiCommandBar.BACKSPACE);

        assertThat(update.value()).isEqualTo("hello ");
        assertThat(update.event()).isEqualTo(TuiCommandBar.Event.NONE);
    }

    @Test
    void escapeClearsTheBufferWithoutSubmitting() {
        TuiCommandBar.Update update = TuiCommandBar.accept("connect http://localhost", TuiCommandBar.ESCAPE);

        assertThat(update.value()).isEmpty();
        assertThat(update.event()).isEqualTo(TuiCommandBar.Event.NONE);
    }

    @Test
    void projectsAnsiDiagnosticsAsReadableLines() {
        TuiCommandBar.ResultPane pane = TuiCommandBar.project(
                new CommandResult(false, Cli.EXIT_DIAGNOSTIC),
                "\u001b[1m\u001b[31merror:\u001b[39m\u001b[0m cli.server-required\n"
                        + "Connect to a server before running ls.");

        assertThat(pane.lines()).containsExactly("error: cli.server-required",
                "Connect to a server before running ls.");
    }

    @Test
    void removesResidualAnsiMarkersWhenTheEscapeByteWasLost() {
        TuiCommandBar.ResultPane pane = TuiCommandBar.project(
                new CommandResult(false, Cli.EXIT_DIAGNOSTIC),
                "[1m [31merror: [39m [0m cli.server-required");

        assertThat(pane.lines()).singleElement()
                .satisfies(line -> assertThat(line)
                        .contains("error:", "cli.server-required")
                        .doesNotContain("[1m", "[31m", "[39m", "[0m"));
    }
}
