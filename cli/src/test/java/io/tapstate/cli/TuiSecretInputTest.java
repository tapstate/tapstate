package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TuiSecretInputTest {

    @Test
    void countsSupplementaryCodePointsAsOneMaskedCharacterAndDeletesThemAsOneUnit() {
        TuiSecretInput input = new TuiSecretInput();
        "a🔐b".codePoints().forEach(input::appendCodePoint);

        assertThat(input.codePointCount()).isEqualTo(3);
        assertThat(input.masked()).isEqualTo("•••");

        input.deleteLastCodePoint();
        input.deleteLastCodePoint();

        assertThat(input.codePointCount()).isEqualTo(1);
        assertThat(input.masked()).isEqualTo("•");
    }

    @Test
    void takeAndCloseClearTheBackingBuffer() {
        TuiSecretInput input = new TuiSecretInput();
        "hunter2".codePoints().forEach(input::appendCodePoint);

        assertThat(input.take()).isEqualTo("hunter2");
        assertThat(input.codePointCount()).isZero();
        assertThat(input.toString()).isEqualTo("TuiSecretInput[length=0]");

        "again".codePoints().forEach(input::appendCodePoint);
        input.close();

        assertThat(input.codePointCount()).isZero();
        assertThat(input.masked()).isEmpty();
    }
}
