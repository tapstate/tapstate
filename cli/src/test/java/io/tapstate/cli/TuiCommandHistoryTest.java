package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TuiCommandHistoryTest {

    @Test
    void walksBackAndReturnsTheUnfinishedDraft() {
        TuiCommandHistory history = new TuiCommandHistory();
        history.record("ls");
        history.record("pwd");

        assertThat(history.previous("sta")).isEqualTo("pwd");
        assertThat(history.previous("ignored")).isEqualTo("ls");
        assertThat(history.next()).isEqualTo("pwd");
        assertThat(history.next()).isEqualTo("sta");
    }

    @Test
    void boundsHistoryAndCollapsesImmediateDuplicates() {
        TuiCommandHistory history = new TuiCommandHistory(2);
        history.record("ls");
        history.record("ls");
        history.record("pwd");
        history.record("help");

        assertThat(history.entries()).containsExactly("pwd", "help");
        assertThat(history.previous("")).isEqualTo("help");
        assertThat(history.previous("")).isEqualTo("pwd");
        assertThat(history.previous("")).isEqualTo("pwd");
    }

    @Test
    void rejectsAnUnusableLimit() {
        assertThatThrownBy(() -> new TuiCommandHistory(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
