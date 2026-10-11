package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Authenticated encryption of one entire Source config JSON value, independent of connector and mode. */
public final class SourceConfigCipher {

    private static final String FORMAT = "tscfg";
    private static final String VERSION = "1";
    private static final int KEY_BYTES = 32;
    private static final int IV_BYTES = 12;
    private static final int TAG_BYTES = 16;
    private final Map<String, SecretKeySpec> keys;
    private final String activeKeyId;
    private final SecureRandom random = new SecureRandom();

    public SourceConfigCipher(byte[] keyBytes) {
        this(validatedFingerprint(keyBytes), singleKey(keyBytes));
    }

    SourceConfigCipher(String activeKeyId, Map<String, byte[]> keyBytesById) {
        this.activeKeyId = Objects.requireNonNull(activeKeyId, "activeKeyId");
        Objects.requireNonNull(keyBytesById, "keyBytesById");
        Map<String, SecretKeySpec> accepted = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> entry : keyBytesById.entrySet()) {
            byte[] bytes = entry.getValue();
            if (entry.getKey() == null || bytes == null || bytes.length != KEY_BYTES
                    || !entry.getKey().equals(fingerprint(bytes)) || accepted.containsKey(entry.getKey())) {
                throw invalidKeyring();
            }
            accepted.put(entry.getKey(), new SecretKeySpec(bytes.clone(), "AES"));
        }
        if (accepted.isEmpty() || !accepted.containsKey(activeKeyId)) {
            throw invalidKeyring();
        }
        keys = Map.copyOf(accepted);
    }

    public String encrypt(String sourceId, String connector, String configJson) {
        Objects.requireNonNull(configJson, "configJson");
        byte[] iv = new byte[IV_BYTES];
        random.nextBytes(iv);
        byte[] plaintext = configJson.getBytes(StandardCharsets.UTF_8);
        try {
            Cipher cipher = cipher();
            cipher.init(Cipher.ENCRYPT_MODE, keys.get(activeKeyId), new GCMParameterSpec(TAG_BYTES * 8, iv));
            cipher.updateAAD(context(activeKeyId, sourceId, connector));
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] payload = ByteBuffer.allocate(iv.length + ciphertext.length).put(iv).put(ciphertext).array();
            return FORMAT + ":" + VERSION + ":" + activeKeyId + ":"
                    + Base64.getUrlEncoder().withoutPadding().encodeToString(payload);
        } catch (GeneralSecurityException unavailable) {
            // A valid key and a mandated transformation must be usable in this runtime. Do not hide
            // a provider/programming defect as damaged user data or attach any config plaintext.
            throw new IllegalStateException("Source config encryption could not use the configured cipher", unavailable);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    public String decrypt(String sourceId, String connector, String envelope) {
        Objects.requireNonNull(envelope, "envelope");
        String[] parts = envelope.split(":", -1);
        if (parts.length != 4 || !FORMAT.equals(parts[0]) || !VERSION.equals(parts[1])) {
            throw unreadable(sourceId);
        }
        SecretKeySpec key = keys.get(parts[2]);
        if (key == null) throw unreadable(sourceId);
        byte[] payload;
        try {
            payload = Base64.getUrlDecoder().decode(parts[3]);
        } catch (IllegalArgumentException invalidEncoding) {
            throw unreadable(sourceId);
        }
        if (payload.length < IV_BYTES + TAG_BYTES) {
            throw unreadable(sourceId);
        }
        byte[] plaintext = null;
        try {
            Cipher cipher = cipher();
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BYTES * 8, Arrays.copyOfRange(payload, 0, IV_BYTES)));
            cipher.updateAAD(context(parts[2], sourceId, connector));
            plaintext = cipher.doFinal(payload, IV_BYTES, payload.length - IV_BYTES);
            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (AEADBadTagException invalidAuthentication) {
            // Wrong context, tampering, and wrong keys are all fail-closed stored-data refusals.
            // Crypto/parser messages and causes are not operator-safe diagnostic payloads.
            throw unreadable(sourceId);
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("Source config decryption could not use the configured cipher", unavailable);
        } finally {
            if (plaintext != null) Arrays.fill(plaintext, (byte) 0);
        }
    }

    String activeKeyId() {
        return activeKeyId;
    }

    static String envelopeKeyId(String envelope) {
        if (envelope == null) return null;
        String[] parts = envelope.split(":", -1);
        return parts.length == 4 && FORMAT.equals(parts[0]) && VERSION.equals(parts[1]) && !parts[2].isBlank()
                ? parts[2]
                : null;
    }

    private byte[] context(String keyId, String sourceId, String connector) {
        Objects.requireNonNull(sourceId, "sourceId");
        Objects.requireNonNull(connector, "connector");
        return JsonWriter.write(List.of(FORMAT, VERSION, keyId, sourceId, connector))
                .getBytes(StandardCharsets.UTF_8);
    }

    private static Cipher cipher() {
        try {
            return Cipher.getInstance("AES/GCM/NoPadding");
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("AES/GCM/NoPadding is required by the storage runtime", unavailable);
        }
    }

    static String fingerprint(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is required by the storage runtime", unavailable);
        }
    }

    private static TapstateException unreadable(String sourceId) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", sourceId, "field", "config"), null);
    }

    private static String validatedFingerprint(byte[] bytes) {
        if (bytes == null || bytes.length != KEY_BYTES) throw invalidKeyring();
        return fingerprint(bytes);
    }

    private static Map<String, byte[]> singleKey(byte[] bytes) {
        return Map.of(validatedFingerprint(bytes), bytes);
    }

    private static TapstateException invalidKeyring() {
        return new TapstateException(StoreError.SOURCE_CONFIG_KEYRING_INVALID, Map.of(), null);
    }
}
