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

/** Authenticated encryption of one entire configuration value, independent of connector and mode. */
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
        byte[] plaintext = configJson.getBytes(StandardCharsets.UTF_8);
        try {
            return encryptBytes(sourceId, connector, plaintext);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    /** Encrypts caller-owned binary input without changing or retaining it. */
    String encryptBytes(String sourceId, String connector, byte[] plaintext) {
        return seal(plaintext, keyId -> context(keyId, sourceId, connector));
    }

    /** Domain-separated use of the original CONFIG key; the legacy Source AAD remains unchanged. */
    String encryptBoundBytes(String domain, String binding, byte[] plaintext) {
        requireDomain(domain, binding);
        return seal(plaintext, keyId -> boundContext(keyId, domain, binding));
    }

    private String seal(byte[] plaintext, java.util.function.Function<String, byte[]> aad) {
        Objects.requireNonNull(plaintext, "plaintext");
        byte[] iv = new byte[IV_BYTES];
        random.nextBytes(iv);
        try {
            Cipher cipher = cipher();
            cipher.init(Cipher.ENCRYPT_MODE, keys.get(activeKeyId), new GCMParameterSpec(TAG_BYTES * 8, iv));
            cipher.updateAAD(aad.apply(activeKeyId));
            byte[] ciphertext = cipher.doFinal(plaintext);
            byte[] payload = ByteBuffer.allocate(iv.length + ciphertext.length).put(iv).put(ciphertext).array();
            return FORMAT + ":" + VERSION + ":" + activeKeyId + ":"
                    + Base64.getUrlEncoder().withoutPadding().encodeToString(payload);
        } catch (GeneralSecurityException unavailable) {
            // A valid key and a mandated transformation must be usable in this runtime. Do not hide
            // a provider/programming defect as damaged user data or attach any config plaintext.
            throw new IllegalStateException("Source config encryption could not use the configured cipher", unavailable);
        }
    }

    public String decrypt(String sourceId, String connector, String envelope) {
        byte[] plaintext = decryptBytes(sourceId, connector, envelope);
        try {
            return new String(plaintext, StandardCharsets.UTF_8);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    /** Returns caller-owned plaintext; the caller must erase it after decoding. */
    byte[] decryptBytes(String sourceId, String connector, String envelope) {
        return open(envelope, sourceId, keyId -> context(keyId, sourceId, connector));
    }

    byte[] decryptBoundBytes(String domain, String binding, String envelope) {
        requireDomain(domain, binding);
        return open(envelope, "alert-receiver-credentials", keyId -> boundContext(keyId, domain, binding));
    }

    private byte[] open(String envelope, String diagnosticId, java.util.function.Function<String, byte[]> aad) {
        Objects.requireNonNull(envelope, "envelope");
        String[] parts = envelope.split(":", -1);
        if (parts.length != 4 || !FORMAT.equals(parts[0]) || !VERSION.equals(parts[1])) {
            throw unreadable(diagnosticId);
        }
        SecretKeySpec key = keys.get(parts[2]);
        if (key == null) throw unreadable(diagnosticId);
        byte[] payload;
        try {
            payload = Base64.getUrlDecoder().decode(parts[3]);
        } catch (IllegalArgumentException invalidEncoding) {
            throw unreadable(diagnosticId);
        }
        if (payload.length < IV_BYTES + TAG_BYTES) {
            throw unreadable(diagnosticId);
        }
        try {
            Cipher cipher = cipher();
            cipher.init(Cipher.DECRYPT_MODE, key,
                    new GCMParameterSpec(TAG_BYTES * 8, Arrays.copyOfRange(payload, 0, IV_BYTES)));
            cipher.updateAAD(aad.apply(parts[2]));
            return cipher.doFinal(payload, IV_BYTES, payload.length - IV_BYTES);
        } catch (AEADBadTagException invalidAuthentication) {
            // Wrong context, tampering, and wrong keys are all fail-closed stored-data refusals.
            // Crypto/parser messages and causes are not operator-safe diagnostic payloads.
            throw unreadable(diagnosticId);
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("Source config decryption could not use the configured cipher", unavailable);
        }
    }

    /** Stable domain-separated authentication of bounded caller-owned bytes; no new persistent key is introduced. */
    String keyedDigest(String keyId, String domain, byte[] value) {
        requireDomain(domain, ""); Objects.requireNonNull(value, "value");
        SecretKeySpec key = keys.get(keyId); if (key == null) { throw unreadable("alert-candidate"); }
        byte[] encoded = key.getEncoded(); byte[] derived = null;
        try {
            var mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(encoded, "HmacSHA256"));
            derived = mac.doFinal(boundContext(keyId, domain, "keyed-digest-v1"));
            mac.init(new SecretKeySpec(derived, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(value));
        } catch (GeneralSecurityException unavailable) { throw new IllegalStateException("HmacSHA256 is required by the storage runtime", unavailable); }
        finally { Arrays.fill(encoded, (byte) 0); if (derived != null) { Arrays.fill(derived, (byte) 0); } }
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

    private static void requireDomain(String domain, String binding) {
        if (domain == null || !domain.matches("[a-z][a-z0-9.-]{0,63}") || binding == null || binding.length() > 8192) {
            throw new IllegalArgumentException("invalid bound cipher context");
        }
    }

    private static byte[] boundContext(String keyId, String domain, String binding) {
        return JsonWriter.write(List.of(FORMAT, VERSION, keyId, "domain", domain, binding)).getBytes(StandardCharsets.UTF_8);
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
