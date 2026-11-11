package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.ReceiverSecretMaterial;
import io.tapstate.spi.store.IoError;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import org.bson.*;
import org.bson.codecs.*;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Binary;

/** Bounded unsaved form preparation. Encryption and material decoding stay outside retried CONFIG callbacks. */
final class MongoAlertCandidateSecrets {
    private static final String DOMAIN = "alert-test-candidate-v1";
    record Prepared(AlertTestCommand.Parameters parameters, AlertIntent.CandidateTarget target, AlertIntent.EncryptedCandidate encrypted) {
        @Override public String toString() { return "PreparedAlertCandidate[redacted]"; }
    }
    private MongoAlertCandidateSecrets() { }
    static Prepared prepare(AlertTestCommand.Key key, AlertTestCommand.Parameters form, AlertReceiver.Transport transport,
            AlertConfiguration.Identity existingReceiver, long bindingRevision, ReceiverSecretMaterial material, SourceConfigCipher cipher) {
        byte[] bytes = encode(transport, material); byte[] digestInput = null;
        try {
            digestInput = digestInput(key, form, transport, existingReceiver, bindingRevision, bytes);
            String digest = cipher.keyedDigest(cipher.activeKeyId(), DOMAIN, digestInput);
            var p = new AlertTestCommand.Parameters(form.channelType(), new AlertTestCommand.Candidate(form.candidate().id(), digest),
                    form.receiver(), form.targetId(), form.bindingId(), form.route(), form.payloadSha256());
            AlertIntent.EncryptedCandidate encrypted = null;
            if (AlertIntent.needsPrivate(transport)) {
                encrypted = new AlertIntent.EncryptedCandidate(cipher.activeKeyId(), cipher.encryptBoundBytes(DOMAIN, binding(key, p), bytes));
            }
            return new Prepared(p, new AlertIntent.CandidateTarget(transport, existingReceiver, bindingRevision, encrypted == null ? null : encrypted.sha256()), encrypted);
        } finally { erase(bytes); erase(digestInput); }
    }
    static ReceiverSecretMaterial resolve(AlertTestCommand.Accepted command, AlertIntent.CandidateTarget target,
            AlertIntent.EncryptedCandidate encrypted, SourceConfigCipher cipher) {
        byte[] bytes = null; byte[] digestInput = null;
        try {
            boolean required = AlertIntent.needsPrivate(target.transport());
            if (required != (encrypted != null) || encrypted != null && (!encrypted.keyId().equals(SourceConfigCipher.envelopeKeyId(encrypted.ciphertext()))
                    || !encrypted.sha256().equals(target.encryptedMaterialSha256()))) { throw unreadable(); }
            bytes = encrypted == null ? encode(target.transport(), null) : cipher.decryptBoundBytes(DOMAIN, binding(command.key(), command.parameters()), encrypted.ciphertext());
            if (bytes.length > 12 * 1024) { throw unreadable(); }
            digestInput = digestInput(command.key(), command.parameters(), target.transport(), target.existingReceiver(), target.receiverBindingRevision(), bytes);
            if (!command.parameters().candidate().digest().equals(cipher.keyedDigest(encrypted == null ? cipher.activeKeyId() : encrypted.keyId(), DOMAIN, digestInput))) { throw unreadable(); }
            return decode(target.transport(), bytes);
        } catch (IllegalArgumentException | org.bson.BSONException | TapstateException invalid) { throw unreadable(); }
        finally { erase(bytes); erase(digestInput); }
    }
    private static byte[] digestInput(AlertTestCommand.Key key, AlertTestCommand.Parameters p, AlertReceiver.Transport transport,
            AlertConfiguration.Identity existingReceiver, long revision, byte[] material) {
        var dummy = new AlertTestCommand.Accepted(key, p, key.clientId().createdAt(), key.clientId().createdAt().plus(AlertTestCommand.ADMISSION_AGE));
        Document command = MongoAlertDeliveryCodec.command(dummy); command.remove("acceptedAt"); command.remove("deadline");
        command.get("parameters", Document.class).remove("candidateDigest");
        return bson(new Document("domain", DOMAIN).append("command", command).append("transport", MongoAlertReceiverCodec.transport(transport))
                .append("existingReceiver", existingReceiver == null ? null : MongoAlertConfigurationCodec.identity(existingReceiver))
                .append("bindingRevision", revision).append("material", new Binary(material)));
    }
    private static String binding(AlertTestCommand.Key key, AlertTestCommand.Parameters p) {
        return io.tapstate.core.common.JsonWriter.write(List.of(key.scope().authorityId(), key.scope().clusterId(), key.actorPrincipal(),
                key.clientId().value(), p.candidate().id(), p.candidate().digest()));
    }
    private static byte[] encode(AlertReceiver.Transport transport, ReceiverSecretMaterial material) {
        byte[] auth = material == null ? null : material.authentication(); byte[] ca = material == null ? null : material.privateCa();
        try {
            URI endpoint = material == null ? null : material.webhookEndpoint();
            MongoAlertReceiverSecrets.validate(transport, auth, ca, endpoint);
            byte[] value = bson(new Document("formatVersion", 1).append("authentication", auth == null ? null : new Binary(auth))
                    .append("privateCa", ca == null ? null : new Binary(ca)).append("webhookEndpoint", endpoint == null ? null : AlertReceiver.canonicalEndpoint(endpoint)));
            if (value.length > 12 * 1024) { erase(value); throw invalid(); } return value;
        } finally { erase(auth); erase(ca); }
    }
    private static ReceiverSecretMaterial decode(AlertReceiver.Transport transport, byte[] bytes) {
        byte[] auth = null; byte[] ca = null;
        try (var reader = new BsonBinaryReader(ByteBuffer.wrap(bytes))) {
            Document row = new DocumentCodec().decode(reader, DecoderContext.builder().build());
            if (reader.getBsonInput().getPosition() != bytes.length || !row.keySet().equals(Set.of("formatVersion", "authentication", "privateCa", "webhookEndpoint"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion"))) { throw unreadable(); }
            auth = binary(row.get("authentication")); ca = binary(row.get("privateCa"));
            URI endpoint = row.get("webhookEndpoint") == null ? null : URI.create(MongoAlertDeliveryCodec.text(row, "webhookEndpoint"));
            MongoAlertReceiverSecrets.validate(transport, auth, ca, endpoint); return new ReceiverSecretMaterial(auth, ca, endpoint);
        } finally { erase(auth); erase(ca); }
    }
    private static byte[] binary(Object value) { if (value == null) { return null; } if (!(value instanceof Binary b) || b.getType() != 0) { throw unreadable(); } return b.getData(); }
    private static byte[] bson(Document value) {
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, value, EncoderContext.builder().build()); byte[] result = output.toByteArray();
            for (var buffer : output.getByteBuffers()) { buffer.asNIO().clear(); while (buffer.hasRemaining()) { buffer.put((byte) 0); } }
            return result;
        }
    }
    private static void erase(byte[] bytes) { if (bytes != null) { Arrays.fill(bytes, (byte) 0); } }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "alert-candidate", "field", "material"), null); }
    private static TapstateException invalid() { return new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", "candidateMaterial", "reason", "material-too-large"), null); }
}
