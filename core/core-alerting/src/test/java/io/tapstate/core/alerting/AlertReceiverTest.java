package io.tapstate.core.alerting;

import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.alerting.AlertReceiver.*;
import io.tapstate.core.common.TapstateException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlertReceiverTest {
    private static final Instant AT = Instant.parse("2026-10-27T00:00:00.123456789Z");
    private static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org-a", "cluster-a");
    @Test void publicWebhookDefinitionNeverContainsItsPrivatePathQueryOrMaterial() {
        URI endpoint = URI.create("https://HOOKS.example.invalid:443/private-token?secret=secret-query");
        Webhook webhook = Webhook.fromEndpoint(endpoint, Authentication.BEARER, "account-a", Trust.SYSTEM);
        Header header = header();
        var receiver = new AlertReceiver(header, "Webhook", true, 1, webhook,
                CredentialReference.create(header.identity(), AlertReceiver.authenticationIdentity(webhook)));
        assertThat(receiver.toString()).doesNotContain("private-token", "secret-query");
        assertThat(webhook.origin()).isEqualTo(URI.create("https://hooks.example.invalid"));
        assertThat(webhook.matchesEndpoint(URI.create("https://hooks.example.invalid/private-token?secret=secret-query"))).isTrue();
        assertThat(webhook.matchesEndpoint(URI.create("https://hooks.example.invalid/new-target"))).isFalse();
    }
    @Test void metadataAndEquivalentMaterialRotationPreserveTheOriginalBinding() {
        var original = email();
        var renamed = original.edit("Renamed", true, original.transport(), original.credentials(), AT.plusSeconds(1), "actor-b");
        assertThat(renamed.header().revision()).isEqualTo(2);
        assertThat(renamed.bindingRevision()).isOne();
        assertThat(renamed.credentials()).isEqualTo(original.credentials());
        assertThat(renamed.succeeds(original)).isTrue();
        var transport = (Email) original.transport();
        var changed = new Email(transport.host(), 587, Encryption.STARTTLS_REQUIRED, transport.authentication(), transport.username(),
                transport.from(), transport.recipients(), transport.trust());
        var newTarget = renamed.edit("Changed", true, changed, renamed.credentials(), AT.plusSeconds(2), "actor-b");
        assertThat(newTarget.bindingRevision()).isEqualTo(2);
        var aba = newTarget.edit("Changed back", true, original.transport(), original.credentials(), AT.plusSeconds(3), "actor-b");
        assertThat(aba.bindingRevision()).isEqualTo(3);
        assertThat(aba.header().identity()).isEqualTo(original.header().identity());
    }
    @Test void disablingAndReenablingCannotMakeAnOldBindingCurrentAgain() {
        var original = email();
        var disabled = original.edit(original.name(), false, original.transport(), original.credentials(), AT.plusSeconds(1), "actor-a");
        var enabled = disabled.edit(original.name(), true, original.transport(), original.credentials(), AT.plusSeconds(2), "actor-a");
        assertThat(disabled.bindingRevision()).isEqualTo(2);
        assertThat(enabled.bindingRevision()).isEqualTo(3);
        assertThat(enabled.retire(AT.plusSeconds(3), "actor-a").header().active()).isFalse();
    }
    @Test void credentialsCannotMoveBetweenScopesLifetimesOrAuthenticationIdentities() {
        var original = email(); var transport = (Email) original.transport();
        Header other = header();
        assertThatThrownBy(() -> new AlertReceiver(other, "Wrong lifetime", true, 1, transport, original.credentials()))
                .isInstanceOf(TapstateException.class);
        var different = new Email(transport.host(), transport.port(), transport.encryption(), Authentication.LOGIN, "account-b", transport.from(),
                transport.recipients(), transport.trust());
        assertThatThrownBy(() -> original.edit("Wrong account", true, different, original.credentials(), AT.plusSeconds(1), "actor-a"))
                .isInstanceOf(TapstateException.class);
        var changedReference = CredentialReference.create(original.header().identity(), AlertReceiver.authenticationIdentity(different));
        assertThat(original.edit("New account", true, different, changedReference, AT.plusSeconds(1), "actor-a").bindingRevision()).isEqualTo(2);
    }
    @Test void emailNormalizationPreservesLocalPartCaseAndOnlyTlsModesExist() {
        var transport = (Email) email().transport();
        assertThat(transport.host()).isEqualTo("smtp.example.invalid");
        assertThat(transport.recipients()).containsExactly("A@example.invalid", "a@example.invalid");
        assertThat(Encryption.values()).containsExactly(Encryption.IMPLICIT_TLS, Encryption.STARTTLS_REQUIRED);
        assertThatThrownBy(() -> new Email(transport.host(), 0, transport.encryption(), transport.authentication(), transport.username(),
                transport.from(), transport.recipients(), Trust.SYSTEM)).isInstanceOf(TapstateException.class);
    }
    @Test void cloudReceiversAndInlineUrlCredentialsAreRefused() {
        var original = email(); Header h = Header.create(original.header().identity(), AlertChannelPolicy.Deployment.CLOUD_MANAGED, AT, "actor-a");
        assertThatThrownBy(() -> new AlertReceiver(h, "Cloud", true, 1, original.transport(), original.credentials())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> Webhook.fromEndpoint(URI.create("https://user:password@hooks.example.invalid/"), Authentication.NONE, null, Trust.SYSTEM))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> Webhook.fromEndpoint(URI.create("http://hooks.example.invalid/"), Authentication.NONE, null, Trust.SYSTEM))
                .isInstanceOf(TapstateException.class);
    }
    private static Header header() { return Header.create(Identity.create(Kind.RECEIVER, SCOPE, "receiver-a"), AlertChannelPolicy.Deployment.ON_PREM, AT, "actor-a"); }
    private static AlertReceiver email() {
        Header h = header();
        Email transport = new Email("SMTP.example.invalid", 465, Encryption.IMPLICIT_TLS, Authentication.PLAIN, "account-a", "sender@example.invalid",
                List.of("A@EXAMPLE.invalid", "a@example.invalid"), Trust.SYSTEM);
        return new AlertReceiver(h, "Email", true, 1, transport, CredentialReference.create(h.identity(), AlertReceiver.authenticationIdentity(transport)));
    }
}
