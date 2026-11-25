package io.tapstate.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapstate.adapters.alerting.NotificationRenderer;
import io.tapstate.cloud.sdk.model.NotificationSendRequest;
import java.io.IOException;
import java.util.Map;

/** Freezes the direct email request without a recipient; Cloud resolves the organization mailbox. */
final class CloudAlertNotificationWire {
    private static final String EMAIL = "EMAIL";
    private static final String SUBJECT = "subject";
    private static final String TEXT_BODY = "textBody";
    private static final String HTML_BODY = "htmlBody";
    private static final String DEDUPE_KEY = "dedupeKey";
    private static final ObjectMapper JSON = new ObjectMapper();

    private CloudAlertNotificationWire() { }

    static byte[] encode(NotificationRenderer.Snapshot message, String dedupeKey) {
        try {
            byte[] body = JSON.writeValueAsBytes(Map.of("channel", EMAIL, SUBJECT, message.subject(),
                    TEXT_BODY, message.text(), HTML_BODY, message.html(), DEDUPE_KEY, dedupeKey));
            return body.length <= CloudAlertEventWire.MAX_BYTES ? body : null;
        } catch (JsonProcessingException failure) {
            throw new IllegalStateException("fixed Cloud email encoding failed", failure);
        }
    }

    static NotificationSendRequest decode(byte[] payload) {
        try {
            var tree = JSON.readTree(payload);
            if (tree == null || !tree.isObject() || tree.size() != 5 || !EMAIL.equals(tree.path("channel").asText())
                    || !tree.path(DEDUPE_KEY).isTextual() || tree.path(DEDUPE_KEY).asText().isBlank()
                    || !tree.path(SUBJECT).isTextual() || tree.path(SUBJECT).asText().isBlank()
                    || !tree.path(TEXT_BODY).isTextual() || !tree.path(HTML_BODY).isTextual()) {
                throw CloudAlertEventWire.invalid("notificationPayload");
            }
            return new NotificationSendRequest().channel(EMAIL).subject(tree.path(SUBJECT).asText())
                    .textBody(tree.path(TEXT_BODY).asText()).htmlBody(tree.path(HTML_BODY).asText())
                    .dedupeKey(tree.path(DEDUPE_KEY).asText());
        } catch (IOException failure) {
            throw CloudAlertEventWire.invalid("notificationPayload");
        }
    }

}
