package io.tapstate.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapstate.adapters.alerting.NotificationRenderer;
import io.tapstate.cloud.sdk.model.NotificationSendRequest;
import java.io.IOException;
import java.util.Map;

/** Freezes the direct email request without a recipient; Cloud resolves the organization mailbox. */
final class CloudAlertNotificationWire {
    private static final ObjectMapper JSON = new ObjectMapper();

    private CloudAlertNotificationWire() { }

    static byte[] encode(NotificationRenderer.Snapshot message, String dedupeKey) {
        try {
            byte[] body = JSON.writeValueAsBytes(Map.of("channel", "EMAIL", "subject", message.subject(),
                    "textBody", message.text(), "htmlBody", message.html(), "dedupeKey", dedupeKey));
            return body.length <= CloudAlertEventWire.MAX_BYTES ? body : null;
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("fixed Cloud email encoding failed", failure);
        }
    }

    static NotificationSendRequest decode(byte[] payload) {
        try {
            var tree = JSON.readTree(payload);
            if (tree == null || !tree.isObject() || tree.size() != 5 || !"EMAIL".equals(tree.path("channel").asText())
                    || !tree.path("dedupeKey").isTextual() || tree.path("dedupeKey").asText().isBlank()
                    || !tree.path("subject").isTextual() || tree.path("subject").asText().isBlank()
                    || !tree.path("textBody").isTextual() || !tree.path("htmlBody").isTextual()) {
                throw CloudAlertEventWire.invalid("notificationPayload");
            }
            return new NotificationSendRequest().channel("EMAIL").subject(tree.path("subject").asText())
                    .textBody(tree.path("textBody").asText()).htmlBody(tree.path("htmlBody").asText())
                    .dedupeKey(tree.path("dedupeKey").asText());
        } catch (IOException failure) {
            throw CloudAlertEventWire.invalid("notificationPayload");
        }
    }

}
