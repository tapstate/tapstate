package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.alerting.AlertReceiver.*;
import io.tapstate.spi.alerting.ReceiverSecretMaterial;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

final class AlertReceiverFixtures {
    static final String CREDENTIAL_ID = "22222222-2222-4222-8222-222222222222";
    static final String PASSWORD = "isolated-test-password";
    static final URI ENDPOINT = URI.create("https://hooks.example.invalid/private-endpoint-token?token=private-query-token");
    private AlertReceiverFixtures() { }
    static AlertReceiver email() {
        Header header = AlertConfigurationFixtures.header(Kind.RECEIVER, "receiver-a");
        Email transport = new Email("smtp.example.invalid", 465, Encryption.IMPLICIT_TLS, Authentication.PLAIN,
                "account-a", "sender@example.invalid", List.of("recipient@example.invalid"), Trust.SYSTEM);
        return new AlertReceiver(header, "Email", true, 1, transport,
                new CredentialReference(header.identity(), CREDENTIAL_ID, AlertReceiver.authenticationIdentity(transport)));
    }
    static AlertReceiver webhook() {
        Header header = AlertConfigurationFixtures.header(Kind.RECEIVER, "receiver-webhook");
        Webhook transport = Webhook.fromEndpoint(ENDPOINT, Authentication.BEARER, "account-a", Trust.SYSTEM);
        return new AlertReceiver(header, "Webhook", true, 1, transport,
                new CredentialReference(header.identity(), CREDENTIAL_ID, AlertReceiver.authenticationIdentity(transport)));
    }
    static ReceiverSecretMaterial material(String password) { return new ReceiverSecretMaterial(password.getBytes(StandardCharsets.UTF_8), null, null); }
    static ReceiverSecretMaterial webhookMaterial() { return new ReceiverSecretMaterial("test-bearer-token".getBytes(StandardCharsets.UTF_8), null, ENDPOINT); }
    static SourceConfigCipher cipher() { byte[] key = new byte[32]; java.util.Arrays.fill(key, (byte) 3); return new SourceConfigCipher(key); }
}
