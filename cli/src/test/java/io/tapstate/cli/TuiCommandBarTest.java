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
    void separatesSubmitPaletteAndQuitFromTextInput() {
        assertThat(TuiCommandBar.accept("ls", TuiCommandBar.ENTER).event())
                .isEqualTo(TuiCommandBar.Event.SUBMIT);
        assertThat(TuiCommandBar.accept("ls", TuiCommandBar.CTRL_P).event())
                .isEqualTo(TuiCommandBar.Event.PALETTE);
        assertThat(TuiCommandBar.accept("ls", TuiCommandBar.CTRL_C).event())
                .isEqualTo(TuiCommandBar.Event.QUIT);
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
}
