package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TuiPasswordSheetTest {

    @Test
    void rendersLoginMetadataAndBulletsWithoutRetainingThePasswordInPresentationValues() {
        TuiPasswordSheet sheet = new TuiPasswordSheet(
                "https://control.example", "issuer-01J5", "alice@example.com");
        sheet.appendCodePoint('h');
        sheet.appendCodePoint('u');
        sheet.appendCodePoint('n');
        sheet.appendCodePoint('t');
        sheet.appendCodePoint('e');
        sheet.appendCodePoint('r');
        sheet.appendCodePoint('2');

        List<String> frame = sheet.frame();

        assertThat(frame).contains(
                "Server: https://control.example",
                "Issuer: issuer-01J5",
                "Username: alice@example.com",
                "Password: •••••••");
        assertThat(frame).noneMatch(line -> line.contains("hunter2"));
        assertThat(sheet.toString()).doesNotContain("hunter2");
    }

    @Test
    void submittedPasswordIsReturnedOnceAndTheSheetClearsItImmediately() {
        TuiPasswordSheet sheet = new TuiPasswordSheet("server", "issuer", "alice");
        "s3cr3t".codePoints().forEach(sheet::appendCodePoint);

        assertThat(sheet.submit()).isEqualTo("s3cr3t");
        assertThat(sheet.frame()).containsExactly(
                "Server: server",
                "Issuer: issuer",
                "Username: alice",
                "Password: "
        );
        assertThat(sheet.submit()).isEmpty();
    }

    @Test
    void passwordInputSupportsCodePointDeletionWithoutExposingRawText() {
        TuiPasswordSheet sheet = new TuiPasswordSheet("server", "issuer", "alice");
        "a🔐b".codePoints().forEach(sheet::appendCodePoint);

        sheet.deleteLastCodePoint();
        sheet.deleteLastCodePoint();

        assertThat(sheet.maskedPassword()).isEqualTo("•");
        assertThat(sheet.frame()).noneMatch(line -> line.contains("a🔐b"));
    }
}
