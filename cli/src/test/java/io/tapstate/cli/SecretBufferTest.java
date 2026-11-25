package io.tapstate.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecretBufferTest {

    @Test
    void masksUnicodeAndClearsStorageAfterConsumption() {
        SecretBuffer secret = new SecretBuffer();
        secret.append("p@ss");
        secret.append(0x1F511);

        assertThat(secret.length()).isEqualTo(5);
        assertThat(secret.mask()).isEqualTo("*****");
        assertThat(secret.toString()).doesNotContain("p@ss");
        String consumed = secret.consume(value -> value);
        assertThat(consumed).isEqualTo("p@ss\uD83D\uDD11");
        assertThat(secret.cleared()).isTrue();
        assertThatThrownBy(secret::length).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeClearsAnAbandonedSecret() {
        SecretBuffer secret = new SecretBuffer();
        secret.append("abandoned");

        secret.close();

        assertThat(secret.cleared()).isTrue();
        assertThat(secret.toString()).isEqualTo("SecretBuffer[redacted]");
    }
}
