package io.tapstate.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.tapstate.adapters.alerting.NotificationRenderer;
import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.common.TapstateException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudAlertNotificationWireTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
            channel   | "SMS"
            dedupeKey | 7
            dedupeKey | " "
            subject   | 7
            subject   | " "
            textBody  | 7
            htmlBody  | 7
            """)
    void directEmailRequiresTheOriginalChannelAndMessageFields(String field, String replacement) throws Exception {
        var payload = CloudAlertNotificationWire.encode(snapshot("Failure details"), "delivery-1");
        var envelope = (ObjectNode) JSON.readTree(payload);
        envelope.set(field, JSON.readTree(replacement));
        assertInvalid(JSON.writeValueAsBytes(envelope));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "[]", "{"})
    void unreadableOrNonObjectPayloadCannotBecomeAnEmail(String payload) {
        assertInvalid(payload.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void oversizedRenderedEmailCannotBeStaged() {
        var message = snapshot("x".repeat(CloudAlertEventWire.MAX_BYTES + 1));
        assertThat(CloudAlertNotificationWire.encode(message, "delivery-1")).isNull();
    }

    private static NotificationRenderer.Snapshot snapshot(String text) {
        return new NotificationRenderer.Snapshot(1, "en", "Pipeline failure", text, "<p>Failure</p>", new byte[0]);
    }

    private static void assertInvalid(byte[] payload) {
        assertThatThrownBy(() -> CloudAlertNotificationWire.decode(payload))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(AlertError.INVALID_CONFIGURATION);
                    assertThat(error.args()).containsEntry("field", "notificationPayload");
                });
    }
}
