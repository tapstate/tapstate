package io.tapstate.core.alerting;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Semantic target identity excludes receiver UUID, candidate ID and secret bytes. */
public final class AlertTargetIdentity {
    private AlertTargetIdentity() { }
    public static Map<String, AlertTargetLimit.Domain> domains(AlarmKey.Scope scope, AlertReceiver.Transport transport) {
        String auth = DeliveryKey.fingerprint(List.of("target-auth-v1",AlertReceiver.authenticationIdentity(transport)));
        if (transport instanceof AlertReceiver.Email email) {
            return email.recipients().stream().collect(Collectors.toUnmodifiableMap(recipient -> recipient,
                    recipient -> new AlertTargetLimit.Domain(scope,"email",emailTarget(email,recipient),auth)));
        }
        var webhook = AlertReceiver.https(transport);
        String id = "target-" + DeliveryKey.fingerprint(transport instanceof AlertReceiver.Webhook
                ?List.of("webhook-target-v1",webhook.endpointDigest()):List.of("https-target-v1",transport.channelType(),webhook.endpointDigest()));
        return Map.of(AlertReceiver.httpsTargetId(transport),new AlertTargetLimit.Domain(scope,transport.channelType(),id,auth));
    }
    private static String emailTarget(AlertReceiver.Email email,String recipient) {
        return "target-" + DeliveryKey.fingerprint(List.of("smtp-target-v1",email.host(),Integer.toString(email.port()),email.encryption().name(),email.from(),recipient));
    }
}
