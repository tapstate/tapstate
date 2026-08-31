package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiPrompterTest {

    @Test
    void delegatesTextChoiceAndBlockPromptsToTheTuiDriver() {
        TuiPrompter prompter = new TuiPrompter(
                (question, defaultValue, secret) -> question + "/" + defaultValue + "/" + secret,
                (question, options) -> options.getFirst(),
                question -> "line one\nline two");

        assertThat(prompter.ask("Name", "dev")).isEqualTo("Name/dev/false");
        assertThat(prompter.secret("Password")).isEqualTo("Password//true");
        assertThat(prompter.choose("Mode", List.of("first", "last"))).isEqualTo("first");
        assertThat(prompter.lines("Body")).isEqualTo("line one\nline two");
    }
}
