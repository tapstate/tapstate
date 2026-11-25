package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.spi.alerting.NotificationChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NotificationPayloadValueTest {
    private static final String PRIVATE_TEXT = "private-notification-body";

    @Test void frozenRequestsCompareTheirPayloadContentsWithoutSharingOrPrintingThem() {
        byte[] input = PRIVATE_TEXT.getBytes(StandardCharsets.UTF_8);
        var destination = new NotificationChannel.Destination(new AlarmKey.Scope("installation", "cluster"),
                "email", "receiver", "target", "binding");
        var deadline = Instant.parse("2026-10-27T00:01:00Z");
        var first = NotificationChannel.Request.channelTest(destination, "delivery-a", "attempt-a",
                deadline, "application/json", input);
        var same = NotificationChannel.Request.channelTest(destination, "delivery-a", "attempt-a",
                deadline, "application/json", input.clone());
        input[0] = 0;
        first.payload()[0] = 0;
        assertThat(first.payload()).isEqualTo(PRIVATE_TEXT.getBytes(StandardCharsets.UTF_8));
        assertThat(first).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(first).isNotEqualTo(NotificationChannel.Request.channelTest(destination, "delivery-a", "attempt-b",
                deadline, "application/json", same.payload()));
        assertThat(first.toString()).doesNotContain(PRIVATE_TEXT);
    }

    @Test void mimeResultsKeepContentEqualityAndFailureIdentityWithDefensiveBytes() {
        byte[] input = PRIVATE_TEXT.getBytes(StandardCharsets.UTF_8);
        var first = new EmailMime.Result(input, null);
        var same = new EmailMime.Result(input.clone(), null);
        input[0] = 0;
        first.bytes()[0] = 0;
        assertThat(first.bytes()).isEqualTo(PRIVATE_TEXT.getBytes(StandardCharsets.UTF_8));
        assertThat(first).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(first).isNotEqualTo(new EmailMime.Result(same.bytes(), NotificationRenderer.Failure.PAYLOAD_BUDGET));
        assertThat(new EmailMime.Result(null, NotificationRenderer.Failure.INVALID_FACTS))
                .isEqualTo(new EmailMime.Result(null, NotificationRenderer.Failure.INVALID_FACTS));
        assertThat(first.toString()).doesNotContain(PRIVATE_TEXT);
    }

    @Test void renderingSnapshotsKeepEveryFieldInEqualityAndRedactTheirDiagnosticText() {
        byte[] input = PRIVATE_TEXT.getBytes(StandardCharsets.UTF_8);
        var first = new NotificationRenderer.Snapshot(1, "en", "private-subject", PRIVATE_TEXT, "private-html", input);
        var same = new NotificationRenderer.Snapshot(1, "en", "private-subject", PRIVATE_TEXT, "private-html", input.clone());
        input[0] = 0;
        first.webhook()[0] = 0;
        assertThat(first.webhook()).isEqualTo(PRIVATE_TEXT.getBytes(StandardCharsets.UTF_8));
        assertThat(first).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(first).isNotEqualTo(new NotificationRenderer.Snapshot(1, "en", "different", PRIVATE_TEXT, "private-html", same.webhook()));
        assertThat(first.toString()).doesNotContain(PRIVATE_TEXT, "private-subject", "private-html");
    }
}
