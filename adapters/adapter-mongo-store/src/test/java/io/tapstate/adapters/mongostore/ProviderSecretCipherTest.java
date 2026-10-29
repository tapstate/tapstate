package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class ProviderSecretCipherTest {
    @Test void purposeAndOwnerAreAuthenticatedAndOldKeysRemainReadable() {
        byte[] first = new byte[32]; first[0] = 1;
        byte[] next = new byte[32]; next[0] = 2;
        var original = new SourceConfigCipher(first).providerSecrets();
        String encrypted = original.encrypt("workspace-a", "openai-compatible", "sk-sensitive");
        assertThat(encrypted).startsWith("tsai:1:").doesNotContain("sk-sensitive");
        assertThat(original.decrypt("workspace-a", "openai-compatible", encrypted)).isEqualTo("sk-sensitive");
        assertThatThrownBy(() -> original.decrypt("workspace-b", "openai-compatible", encrypted)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> original.decrypt("workspace-a", "other", encrypted)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new SourceConfigCipher(first).decrypt("workspace-a", "openai-compatible", encrypted)).isInstanceOf(TapstateException.class);
        var rotated = new SourceConfigCipher(SourceConfigCipher.fingerprint(next), Map.of(
                SourceConfigCipher.fingerprint(first), first, SourceConfigCipher.fingerprint(next), next)).providerSecrets();
        assertThat(rotated.decrypt("workspace-a", "openai-compatible", encrypted)).isEqualTo("sk-sensitive");
        assertThat(ProviderSecretCipher.keyId(rotated.encrypt("workspace-a", "openai-compatible", "new")))
                .isEqualTo(SourceConfigCipher.fingerprint(next));
        assertThatThrownBy(() -> new SourceConfigCipher(next).providerSecrets().decrypt("workspace-a", "openai-compatible", encrypted))
                .isInstanceOf(TapstateException.class);
    }
    @Test void malformedCiphertextFailsClosedWithoutSecretDiagnostics() {
        var cipher = new SourceConfigCipher(new byte[32]).providerSecrets();
        for (String value : java.util.List.of("sk-sensitive", "tsai:1:wrong:key", "tsai:1:x:!")) {
            assertThatThrownBy(() -> cipher.decrypt("owner", "openai-compatible", value))
                    .isInstanceOf(TapstateException.class).hasMessageNotContaining("sk-sensitive");
        }
    }
}
