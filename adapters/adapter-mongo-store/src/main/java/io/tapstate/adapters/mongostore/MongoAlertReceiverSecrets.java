package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.AlertReceiver;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.ReceiverSecretMaterial;
import io.tapstate.spi.store.IoError;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Binary;

/** Pure preparation and decoding outside retried transactions, using the original CONFIG key. */
final class MongoAlertReceiverSecrets {
    static final int MAX_CIPHERTEXT_BYTES = 16 * 1024;
    private static final int MAX_PLAINTEXT_BYTES = 12 * 1024;
    private static final String DOMAIN = "alert-receiver-credentials-v1";
    private MongoAlertReceiverSecrets() { }
    record Prepared(AlertReceiver receiver, String ciphertext, String keyId) {
        @Override public String toString() { return "PreparedReceiver[redacted]"; }
    }

    static Prepared prepare(AlertReceiver receiver, ReceiverSecretMaterial material, SourceConfigCipher cipher) {
        if (receiver.credentials() == null) {
            require(material == null, "unexpected-material"); return new Prepared(receiver, null, cipher.activeKeyId());
        }
        require(material != null, "missing-material");
        byte[] authentication = material.authentication(); byte[] ca = material.privateCa();
        byte[] encoded = null;
        try {
            validate(receiver.transport(), authentication, ca, material.webhookEndpoint());
            Document value = new Document("formatVersion", 1).append("transportDigest", transportDigest(receiver))
                    .append("authentication", authentication == null ? null : new Binary(authentication))
                    .append("privateCa", ca == null ? null : new Binary(ca))
                    .append("webhookEndpoint", material.webhookEndpoint() == null ? null : AlertReceiver.canonicalEndpoint(material.webhookEndpoint()));
            try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
                new DocumentCodec().encode(writer, value, EncoderContext.builder().build()); encoded = output.toByteArray();
                // Clear the encoder's own buffer as well as the returned copy.
                for (var buffer : output.getByteBuffers()) { buffer.asNIO().clear(); while (buffer.hasRemaining()) { buffer.put((byte) 0); } }
            }
            require(encoded.length <= MAX_PLAINTEXT_BYTES, "material-too-large");
            String encrypted = cipher.encryptBoundBytes(DOMAIN, receiver.credentials().binding(), encoded);
            require(encrypted.getBytes(StandardCharsets.UTF_8).length <= MAX_CIPHERTEXT_BYTES, "ciphertext-too-large");
            return new Prepared(receiver, encrypted, cipher.activeKeyId());
        } finally { erase(authentication); erase(ca); erase(encoded); }
    }

    static ReceiverSecretMaterial resolve(AlertReceiver receiver, Document privateSlot, SourceConfigCipher cipher) {
        if (receiver.credentials() == null) {
            if (privateSlot != null) { throw unreadable(); }
            return new ReceiverSecretMaterial(null, null, null);
        }
        validateSlot(receiver, privateSlot);
        byte[] plaintext = cipher.decryptBoundBytes(DOMAIN, receiver.credentials().binding(), privateSlot.getString("ciphertext"));
        byte[] authentication = null; byte[] ca = null;
        try {
            if (plaintext.length > MAX_PLAINTEXT_BYTES) { throw unreadable(); }
            Document value;
            try (BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(plaintext))) {
                value = new DocumentCodec().decode(reader, DecoderContext.builder().build());
                if (reader.getBsonInput().getPosition() != plaintext.length) { throw unreadable(); }
            }
            if (!value.keySet().equals(Set.of("formatVersion", "transportDigest", "authentication", "privateCa", "webhookEndpoint"))
                    || !Integer.valueOf(1).equals(value.get("formatVersion"))
                    || !transportDigest(receiver).equals(value.get("transportDigest"))) { throw unreadable(); }
            authentication = binary(value.get("authentication")); ca = binary(value.get("privateCa"));
            if (value.get("webhookEndpoint") != null && !(value.get("webhookEndpoint") instanceof String)) { throw unreadable(); }
            URI endpoint = value.get("webhookEndpoint") == null ? null : URI.create(value.getString("webhookEndpoint"));
            validate(receiver.transport(), authentication, ca, endpoint);
            return new ReceiverSecretMaterial(authentication, ca, endpoint);
        } catch (TapstateException | IllegalArgumentException | org.bson.BSONException malformed) { throw unreadable(); }
        finally { erase(plaintext); erase(authentication); erase(ca); }
    }

    static Document slot(Prepared prepared) {
        if (prepared.ciphertext() == null) { return null; }
        AlertReceiver.CredentialReference reference = prepared.receiver().credentials();
        return new Document("formatVersion", 1).append("credentialId", reference.credentialId())
                .append("authenticationIdentity", reference.authenticationIdentity()).append("keyId", prepared.keyId())
                .append("ciphertext", prepared.ciphertext());
    }
    static void validateSlot(AlertReceiver receiver, Document slot) {
        if (receiver.credentials() == null) { if (slot != null) { throw unreadable(); } return; }
        if (slot == null || !slot.keySet().equals(Set.of("formatVersion", "credentialId", "authenticationIdentity", "keyId", "ciphertext"))
                || !Integer.valueOf(1).equals(slot.get("formatVersion"))
                || !receiver.credentials().credentialId().equals(slot.get("credentialId"))
                || !receiver.credentials().authenticationIdentity().equals(slot.get("authenticationIdentity"))
                || !(slot.get("keyId") instanceof String keyId) || !keyId.matches("[a-f0-9]{64}")
                || !(slot.get("ciphertext") instanceof String ciphertext) || ciphertext.length() > MAX_CIPHERTEXT_BYTES
                || !ciphertext.matches("tscfg:1:[a-f0-9]{64}:[A-Za-z0-9_-]+")
                || !keyId.equals(SourceConfigCipher.envelopeKeyId(ciphertext))) { throw unreadable(); }
        String payload = slot.getString("ciphertext").substring(slot.getString("ciphertext").lastIndexOf(':') + 1);
        try {
            byte[] decoded = java.util.Base64.getUrlDecoder().decode(payload);
            if (decoded.length < 28 || !java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(payload)) { throw unreadable(); }
        } catch (IllegalArgumentException malformed) { throw unreadable(); }
    }

    static void validate(AlertReceiver.Transport transport, byte[] authentication, byte[] ca, URI endpoint) {
        boolean authenticated = transport.authentication() != AlertReceiver.Authentication.NONE;
        require(authenticated ? authentication != null && authentication.length > 0 && authentication.length <= 8192 : authentication == null,
                "authentication-material-mismatch");
        if (authenticated && transport.authentication() != AlertReceiver.Authentication.HMAC_SHA256) {
            java.nio.CharBuffer text = null;
            try {
                text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(authentication));
                require(text.length() <= 4096 && text.length() > 0, "authentication-material-size");
                boolean nonblank = false;
                for (int i = 0; i < text.length(); i++) {
                    char c = text.charAt(i);
                    require(c != 0 && c != '\r' && c != '\n', "authentication-material-control");
                    if (transport.authentication() == AlertReceiver.Authentication.BEARER) { require(!Character.isISOControl(c), "bearer-control"); }
                    nonblank |= !Character.isWhitespace(c);
                }
                require(transport.authentication() != AlertReceiver.Authentication.BEARER || nonblank, "blank-bearer");
            } catch (CharacterCodingException malformed) { throw invalid("authentication-encoding"); }
            finally { if (text != null && text.hasArray()) { Arrays.fill(text.array(), '\0'); } }
        }
        if (transport.authentication() == AlertReceiver.Authentication.HMAC_SHA256) {
            require(authentication.length >= 16 && authentication.length <= 4096, "signing-key-size");
        }
        require(transport.trust() == AlertReceiver.Trust.PRIVATE_CA ? ca != null && ca.length > 0 && ca.length <= 8192 : ca == null, "trust-material-mismatch");
        if (ca != null) {
            try {
                var certificates = CertificateFactory.getInstance("X.509").generateCertificates(new java.io.ByteArrayInputStream(ca));
                require(!certificates.isEmpty() && certificates.size() <= 16 && certificates.stream()
                        .allMatch(certificate -> certificate instanceof java.security.cert.X509Certificate x509 && x509.getBasicConstraints() >= 0), "invalid-private-ca");
            } catch (java.security.cert.CertificateException malformed) { throw invalid("invalid-private-ca"); }
        }
        if (!(transport instanceof AlertReceiver.Email)) { require(endpoint != null && AlertReceiver.https(transport).matchesEndpoint(endpoint), "endpoint-mismatch"); }
        else { require(endpoint == null, "unexpected-endpoint"); }
    }
    private static byte[] binary(Object value) {
        if (value == null) { return null; }
        if (!(value instanceof Binary binary) || binary.getType() != 0) { throw unreadable(); }
        return binary.getData();
    }
    private static String transportDigest(AlertReceiver receiver) {
        return SourceConfigCipher.fingerprint(io.tapstate.core.common.JsonWriter.write(
                MongoAlertReceiverCodec.body(receiver).get("transport", Document.class)).getBytes(StandardCharsets.UTF_8));
    }
    private static void erase(byte[] value) { if (value != null) { Arrays.fill(value, (byte) 0); } }
    private static void require(boolean condition, String reason) { if (!condition) { throw invalid(reason); } }
    private static TapstateException invalid(String reason) {
        return new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", "receiverCredentials", "reason", reason), null);
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-receiver", "field", "credentials"), null);
    }
}
