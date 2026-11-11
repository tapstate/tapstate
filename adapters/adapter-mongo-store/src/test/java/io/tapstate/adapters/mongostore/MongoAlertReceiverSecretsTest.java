package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.alerting.AlertReceiver.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.ReceiverSecretMaterial;
import io.tapstate.spi.store.IoError;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static io.tapstate.adapters.mongostore.AlertConfigurationFixtures.*;
import static io.tapstate.adapters.mongostore.AlertReceiverFixtures.*;
import static org.assertj.core.api.Assertions.*;

class MongoAlertReceiverSecretsTest {
    @Test void publicEnvelopeAndIndependentCipherAadMatchTheAuthoredGolden() throws Exception {
        Document golden;
        try (var input = getClass().getResourceAsStream("/alert-receiver-v1.golden.json")) {
            assertThat(input).isNotNull(); golden = Document.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
        }
        var receiver = email(); var cipher = cipher();
        // The envelope golden uses opaque syntactic bytes; actual GCM validation is independent below.
        Document slot = golden.get("receiver", Document.class).get("privateSlot", Document.class);
        var shape = new MongoAlertReceiverSecrets.Prepared(receiver, slot.getString("ciphertext"), slot.getString("keyId"));
        assertThat(MongoAlertConfigurationCodec.write(receiver, 0).append("privateSlot", MongoAlertReceiverSecrets.slot(shape)))
                .isEqualTo(golden.get("receiver"));
        try (var material = material(PASSWORD)) {
            var actual = MongoAlertReceiverSecrets.prepare(receiver, material, cipher);
            byte[] key = new byte[32]; java.util.Arrays.fill(key, (byte) 3);
            byte[] bytes = java.util.Base64.getUrlDecoder().decode(actual.ciphertext().split(":")[3]);
            javax.crypto.Cipher independent = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
            independent.init(javax.crypto.Cipher.DECRYPT_MODE, new javax.crypto.spec.SecretKeySpec(key, "AES"),
                    new javax.crypto.spec.GCMParameterSpec(128, java.util.Arrays.copyOf(bytes, 12)));
            independent.updateAAD(golden.getString("aad").getBytes(StandardCharsets.UTF_8));
            byte[] plaintext = independent.doFinal(bytes, 12, bytes.length - 12);
            try (var reader = new org.bson.BsonBinaryReader(java.nio.ByteBuffer.wrap(plaintext))) {
                Document payload = new org.bson.codecs.DocumentCodec().decode(reader, org.bson.codecs.DecoderContext.builder().build());
                assertThat(payload.getString("transportDigest")).isEqualTo(golden.getString("transportDigest"));
            } finally { java.util.Arrays.fill(key, (byte) 0); java.util.Arrays.fill(plaintext, (byte) 0); }
        }
    }
    @Test void originalKeyAndBindingRoundTripWithoutPuttingMaterialInThePublicDefinition() {
        var receiver = email(); var cipher = cipher();
        try (var input = material(PASSWORD)) {
            var prepared = MongoAlertReceiverSecrets.prepare(receiver, input, cipher);
            Document row = MongoAlertConfigurationCodec.write(receiver, 0).append("privateSlot", MongoAlertReceiverSecrets.slot(prepared));
            assertThat(row.toJson()).doesNotContain(PASSWORD);
            assertThat(prepared.toString()).doesNotContain(prepared.ciphertext(), PASSWORD);
            assertThat(MongoAlertConfigurationCodec.read(Kind.RECEIVER, SCOPE, "receiver-a", row).definition()).isEqualTo(receiver);
            try (var restored = MongoAlertReceiverSecrets.resolve(receiver, MongoAlertReceiverSecrets.slot(prepared), cipher)) {
                assertThat(restored.authentication()).isEqualTo(PASSWORD.getBytes(StandardCharsets.UTF_8));
                assertThat(restored.toString()).doesNotContain(PASSWORD);
            }
            assertThat(prepared.ciphertext().length()).isLessThanOrEqualTo(16384);
        }
    }

    @Test void credentialPayloadCannotCrossScopeLifetimeOrSourceCipherDomain() {
        var original = email(); var cipher = cipher();
        try (var input = material(PASSWORD)) {
            var prepared = MongoAlertReceiverSecrets.prepare(original, input, cipher);
            Header h = Header.create(Identity.create(Kind.RECEIVER, new AlarmKey.Scope("other-org", SCOPE.clusterId()), "receiver-a"),
                    AlertChannelPolicy.Deployment.ON_PREM, AT, "actor-a");
            var foreign = new AlertReceiver(h, original.name(), true, 1, original.transport(),
                    new CredentialReference(h.identity(), CREDENTIAL_ID, original.credentials().authenticationIdentity()));
            assertUnreadable(() -> MongoAlertReceiverSecrets.resolve(foreign, MongoAlertReceiverSecrets.slot(prepared), cipher));
            assertUnreadable(() -> cipher.decrypt("alert-receiver-credentials-v1", original.credentials().binding(), prepared.ciphertext()));
            String source = cipher.encrypt("alert-receiver-credentials-v1", original.credentials().binding(), "legacy-source-value");
            Document substituted = MongoAlertReceiverSecrets.slot(prepared); substituted.put("ciphertext", source);
            assertUnreadable(() -> MongoAlertReceiverSecrets.resolve(original, substituted, cipher));
        }
    }

    @Test void changingPublicTransportCannotReuseOldEncryptedMaterial() {
        var original = email(); var old = (Email) original.transport();
        var changed = new Email("another-smtp.example.invalid", old.port(), old.encryption(), old.authentication(), old.username(), old.from(), old.recipients(), old.trust());
        var replacement = original.edit("New host", true, changed, original.credentials(), AT.plusSeconds(1), "actor-a");
        try (var input = material(PASSWORD)) {
            var prepared = MongoAlertReceiverSecrets.prepare(original, input, cipher());
            assertUnreadable(() -> MongoAlertReceiverSecrets.resolve(replacement, MongoAlertReceiverSecrets.slot(prepared), cipher()));
        }
    }

    @Test void privateWebhookEndpointAndBearerRemainEncryptedAndMustMatchTheFrozenDigest() {
        var receiver = webhook();
        try (var input = webhookMaterial()) {
            var prepared = MongoAlertReceiverSecrets.prepare(receiver, input, cipher());
            Document row = MongoAlertConfigurationCodec.write(receiver, 0).append("privateSlot", MongoAlertReceiverSecrets.slot(prepared));
            assertThat(row.toJson()).doesNotContain("private-endpoint-token", "private-query-token", "test-bearer-token");
            try (var restored = MongoAlertReceiverSecrets.resolve(receiver, MongoAlertReceiverSecrets.slot(prepared), cipher())) {
                assertThat(restored.webhookEndpoint()).isEqualTo(ENDPOINT);
            }
        }
        try (var wrong = new ReceiverSecretMaterial("token".getBytes(StandardCharsets.UTF_8), null, java.net.URI.create("https://other.example.invalid/"))) {
            assertThatThrownBy(() -> MongoAlertReceiverSecrets.prepare(receiver, wrong, cipher())).isInstanceOf(TapstateException.class);
        }
    }

    @Test void missingMalformedOversizedOrWrongKeySlotsAreRefusedWithoutEchoingMaterial() {
        var receiver = email();
        try (var input = material(PASSWORD)) {
            var prepared = MongoAlertReceiverSecrets.prepare(receiver, input, cipher());
            for (String field : List.of("credentialId", "keyId", "authenticationIdentity", "ciphertext")) {
                Document slot = MongoAlertReceiverSecrets.slot(prepared); slot.put(field, "invalid");
                assertUnreadable(() -> MongoAlertReceiverSecrets.resolve(receiver, slot, cipher()));
            }
            assertUnreadable(() -> MongoAlertReceiverSecrets.resolve(receiver, null, cipher()));
            Document huge = MongoAlertReceiverSecrets.slot(prepared); huge.put("ciphertext", "x".repeat(16385));
            assertUnreadable(() -> MongoAlertReceiverSecrets.resolve(receiver, huge, cipher()));
            byte[] other = new byte[32]; java.util.Arrays.fill(other, (byte) 4);
            assertUnreadable(() -> MongoAlertReceiverSecrets.resolve(receiver, MongoAlertReceiverSecrets.slot(prepared), new SourceConfigCipher(other)));
        }
        try (var huge = material("x".repeat(9000))) {
            assertThatThrownBy(() -> MongoAlertReceiverSecrets.prepare(receiver, huge, cipher())).isInstanceOf(TapstateException.class)
                    .hasMessageNotContaining("x".repeat(100));
        }
    }

    @Test void anonymousSmtpRequiresNoPrivateSlotButAuthenticatedModesNeverGuessMissingMaterial() {
        var original = email(); var old = (Email) original.transport();
        var anonymous = new Email(old.host(), old.port(), old.encryption(), Authentication.NONE, null, old.from(), old.recipients(), Trust.SYSTEM);
        var receiver = new AlertReceiver(original.header(), "Anonymous", true, 1, anonymous, null);
        assertThat(MongoAlertReceiverSecrets.prepare(receiver, null, cipher()).ciphertext()).isNull();
        assertThatThrownBy(() -> MongoAlertReceiverSecrets.prepare(original, null, cipher())).isInstanceOf(TapstateException.class);
        try (var material = material(PASSWORD)) {
            material.close();
            assertThatThrownBy(material::authentication).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test void privateCaAndBinaryHmacMaterialRoundTripWithoutBecomingPublicFields() throws Exception {
        byte[] ca;
        try (var input = getClass().getResourceAsStream("/alert-receiver-test-ca.pem")) { assertThat(input).isNotNull(); ca = input.readAllBytes(); }
        var old = webhook();
        var transport = Webhook.fromEndpoint(ENDPOINT, Authentication.HMAC_SHA256, "signing-identity", Trust.PRIVATE_CA);
        var receiver = new AlertReceiver(old.header(), old.name(), true, 1, transport,
                new CredentialReference(old.header().identity(), CREDENTIAL_ID, AlertReceiver.authenticationIdentity(transport)));
        byte[] signingKey = new byte[32]; java.util.Arrays.fill(signingKey, (byte) 0xff);
        try (var input = new ReceiverSecretMaterial(signingKey, ca, ENDPOINT)) {
            var prepared = MongoAlertReceiverSecrets.prepare(receiver, input, cipher());
            assertThat(MongoAlertReceiverSecrets.slot(prepared).toJson()).doesNotContain("BEGIN CERTIFICATE", "private-query-token");
            try (var resolved = MongoAlertReceiverSecrets.resolve(receiver, MongoAlertReceiverSecrets.slot(prepared), cipher())) {
                assertThat(resolved.authentication()).isEqualTo(signingKey);
                assertThat(resolved.privateCa()).isEqualTo(ca);
            }
        }
        try (var bad = new ReceiverSecretMaterial(signingKey, "not-a-certificate".getBytes(StandardCharsets.UTF_8), ENDPOINT)) {
            assertThatThrownBy(() -> MongoAlertReceiverSecrets.prepare(receiver, bad, cipher())).isInstanceOf(TapstateException.class);
        }
    }

    @Test void totalEncryptedBudgetRejectsOtherwiseValidCombinedMaterial() throws Exception {
        byte[] ca;
        try (var input = getClass().getResourceAsStream("/alert-receiver-test-ca.pem")) { assertThat(input).isNotNull(); ca = input.readAllBytes(); }
        byte[] chain = new byte[ca.length * 4];
        for (int i = 0; i < 4; i++) { System.arraycopy(ca, 0, chain, i * ca.length, ca.length); }
        var original = email(); var old = (Email) original.transport();
        var transport = new Email(old.host(), old.port(), old.encryption(), old.authentication(), old.username(), old.from(), old.recipients(), Trust.PRIVATE_CA);
        var receiver = new AlertReceiver(original.header(), original.name(), true, 1, transport, original.credentials());
        byte[] password = "\uD83D\uDD12".repeat(2048).getBytes(StandardCharsets.UTF_8);
        try (var material = new ReceiverSecretMaterial(password, chain, null)) {
            assertThatThrownBy(() -> MongoAlertReceiverSecrets.prepare(receiver, material, cipher()))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(AlertError.INVALID_CONFIGURATION));
        }
    }

    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(failure.getMessage()).doesNotContain(PASSWORD, "test-bearer-token", "private-query-token");
        });
    }
}
