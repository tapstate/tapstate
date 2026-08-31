package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TuiActivityTest {

    @Test
    void compactsAndRedactsCommandArguments() {
        assertThat(TuiActivity.command("auth   login   admin@example.com   hunter2"))
                .isEqualTo("auth login admin@example.com [redacted]");
        assertThat(TuiActivity.command("token revoke tok_live"))
                .isEqualTo("token revoke [redacted]");
        assertThat(TuiActivity.command("connect --token tok_live"))
                .isEqualTo("connect --token [redacted]");
    }

    @Test
    void redactsStructuredAndBearerResults() {
        assertThat(TuiActivity.result("{\"accessToken\":\"abc\",\"refreshToken\":\"def\"}"))
                .isEqualTo("{\"accessToken\":\"[redacted]\",\"refreshToken\":\"[redacted]\"}");
        assertThat(TuiActivity.result("Authorization: Bearer abc123"))
                .isEqualTo("Authorization: Bearer [redacted]");
    }
}
