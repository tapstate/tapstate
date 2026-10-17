package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** Separate versioned envelope and authenticated purpose for provider credentials. */
final class ProviderSecretCipher {
    private final String active;
    private final Map<String, SecretKeySpec> keys;
    ProviderSecretCipher(String active, Map<String, SecretKeySpec> keys) {
        this.active = active;
        this.keys = Map.copyOf(keys);
    }
    String encrypt(String owner, String provider, String secret) {
        byte[] nonce = new byte[12];
        new SecureRandom().nextBytes(nonce);
        byte[] plain = secret.getBytes(StandardCharsets.UTF_8);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, keys.get(active), new GCMParameterSpec(128, nonce));
            cipher.updateAAD(aad(active, owner, provider));
            byte[] encrypted = cipher.doFinal(plain);
            byte[] payload = new byte[nonce.length + encrypted.length];
            System.arraycopy(nonce, 0, payload, 0, nonce.length);
            System.arraycopy(encrypted, 0, payload, nonce.length, encrypted.length);
            return "tsai:1:" + active + ":" + Base64.getUrlEncoder().withoutPadding().encodeToString(payload);
        } catch (GeneralSecurityException error) {
            throw new IllegalStateException("Provider credential encryption is unavailable", error);
        } finally { Arrays.fill(plain, (byte) 0); }
    }
    String decrypt(String owner, String provider, String envelope) {
        byte[] plain = null;
        try {
            if (envelope == null) throw new IllegalArgumentException();
            String[] parts = envelope.split(":", -1);
            if (parts.length != 4 || !"tsai".equals(parts[0]) || !"1".equals(parts[1])
                    || !keys.containsKey(parts[2])) throw new IllegalArgumentException();
            byte[] payload = Base64.getUrlDecoder().decode(parts[3]);
            if (payload.length < 28) throw new IllegalArgumentException();
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, keys.get(parts[2]),
                    new GCMParameterSpec(128, Arrays.copyOf(payload, 12)));
            cipher.updateAAD(aad(parts[2], owner, provider));
            plain = cipher.doFinal(payload, 12, payload.length - 12);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException error) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", "ai-provider-settings", "field", "secret"), null);
        } finally { if (plain != null) Arrays.fill(plain, (byte) 0); }
    }
    static String keyId(String envelope) {
        String[] parts = envelope == null ? new String[0] : envelope.split(":", -1);
        return parts.length == 4 && "tsai".equals(parts[0]) && "1".equals(parts[1]) ? parts[2] : null;
    }
    private static byte[] aad(String key, String owner, String provider) {
        return JsonWriter.write(List.of("tsai", "1", key, owner, provider)).getBytes(StandardCharsets.UTF_8);
    }
}
