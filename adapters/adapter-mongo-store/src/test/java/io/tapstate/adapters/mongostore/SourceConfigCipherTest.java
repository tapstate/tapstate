package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceConfigCipherTest {
    private static final String JSON = "{\"password\":\"whole-config-secret\",\"host\":\"example.invalid\","
            + "\"nested\":{\"tlsPassword\":\"another-secret\"},\"values\":[null,true,123,1.5]}";

    @Test
    void theWholeJsonRoundTripsAndEachEncryptionUsesDifferentBytes() {
        SourceConfigCipher cipher = new SourceConfigCipher(key((byte) 1));
        String first = cipher.encrypt("source-a", "arbitrary-connector", JSON);
        String second = cipher.encrypt("source-a", "arbitrary-connector", JSON);
        assertThat(first).startsWith("tscfg:1:").doesNotContain("password", "whole-config-secret", "example.invalid");
        assertThat(second).isNotEqualTo(first);
        assertThat(cipher.decrypt("source-a", "arbitrary-connector", first)).isEqualTo(JSON);
        assertThat(new SourceConfigCipher(key((byte) 1)).decrypt("source-a", "arbitrary-connector", second))
                .isEqualTo(JSON);
    }

    @Test
    void sourceAndConnectorSwapsAndCiphertextTamperingFailClosedWithoutSecretDiagnostics() {
        SourceConfigCipher cipher = new SourceConfigCipher(key((byte) 1));
        String envelope = cipher.encrypt("source-a", "connector-a", JSON);
        unreadable(() -> cipher.decrypt("source-b", "connector-a", envelope));
        unreadable(() -> cipher.decrypt("source-a", "connector-b", envelope));
        unreadable(() -> new SourceConfigCipher(key((byte) 2)).decrypt("source-a", "connector-a", envelope));
        String[] parts = envelope.split(":", -1);
        byte[] altered = Base64.getUrlDecoder().decode(parts[3]);
        altered[altered.length - 1] ^= 1;
        parts[3] = Base64.getUrlEncoder().withoutPadding().encodeToString(altered);
        unreadable(() -> cipher.decrypt("source-a", "connector-a", String.join(":", parts)));
    }

    @Test
    void unsupportedOrMalformedEnvelopesNeverBecomePlaintextConfig() {
        SourceConfigCipher cipher = new SourceConfigCipher(key((byte) 1));
        String envelope = cipher.encrypt("source-a", "connector-a", JSON);
        for (String value : new String[] {JSON, "", envelope.replace("tscfg:1:", "tscfg:2:"),
                envelope.substring(0, envelope.lastIndexOf(':') + 1) + "invalid-encoding@"}) {
            unreadable(() -> cipher.decrypt("source-a", "connector-a", value));
        }
    }

    @Test
    void invalidKeysAreCodedAndCallerKeyArraysCannotMutateTheCipher() {
        for (byte[] invalid : new byte[][] {null, new byte[0], new byte[16], new byte[31], new byte[33]}) {
            assertThatThrownBy(() -> new SourceConfigCipher(invalid))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
        }
        byte[] key = key((byte) 1);
        SourceConfigCipher cipher = new SourceConfigCipher(key);
        Arrays.fill(key, (byte) 2);
        String encrypted = cipher.encrypt("source-a", "connector-a", JSON);
        assertThat(new SourceConfigCipher(key((byte) 1)).decrypt("source-a", "connector-a", encrypted))
                .isEqualTo(JSON);
    }

    @Test
    void aRotatedKeyringReadsOldEnvelopesButWritesOnlyWithTheActiveKey() {
        byte[] oldKey = key((byte) 1);
        byte[] newKey = key((byte) 2);
        String oldKeyId = SourceConfigCipher.fingerprint(oldKey);
        String newKeyId = SourceConfigCipher.fingerprint(newKey);
        String oldEnvelope = new SourceConfigCipher(oldKey).encrypt("source-a", "connector-a", JSON);

        SourceConfigCipher rotated = new SourceConfigCipher(newKeyId, Map.of(oldKeyId, oldKey, newKeyId, newKey));

        assertThat(rotated.decrypt("source-a", "connector-a", oldEnvelope)).isEqualTo(JSON);
        assertThat(rotated.encrypt("source-a", "connector-a", JSON)).startsWith("tscfg:1:" + newKeyId + ":");
    }

    private static byte[] key(byte fill) {
        byte[] value = new byte[32];
        Arrays.fill(value, fill);
        return value;
    }

    private static void unreadable(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.getCause()).isNull();
            StringWriter printed = new StringWriter();
            error.printStackTrace(new PrintWriter(printed));
            assertThat(printed.toString()).doesNotContain("whole-config-secret", "another-secret", "example.invalid");
        });
    }
}
