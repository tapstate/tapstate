package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.adapters.alerting.NotificationRenderer.*;

class NotificationTemplateRenderTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final AlarmKey KEY = new AlarmKey(new AlarmKey.Scope("org", "cluster"), new AlarmKey.Resource("pipeline", "orders", "resource-1"), "pipeline.failed", "rule");
    static Facts facts(Kind kind) {
        boolean recovery = kind == Kind.RECOVERY || kind == Kind.RECOVERY_SUMMARY;
        boolean event = kind == Kind.EVENT;
        var item = new Item(KEY, event ? null : 1L, event ? State.EVENT : recovery ? State.RESOLVED : State.FIRING,
                AlarmSeverity.MAJOR, "orders <img>", "bad <script>&\"", T, null, recovery ? T.plusSeconds(10) : null);
        return new Facts(kind, "delivery-1", 1, T, List.of(item), 1, null,
                List.of(new Previous("delivery-previous", DeliveryEvidence.Outcome.UNKNOWN)), null, null);
    }
    @Test void everyBusinessLayoutKeepsMachineFactsAndEscapesLiteralCustomText() {
        var renderer = new NotificationRenderer();
        var settings = new MessageSettings("[prod] ", " (team)", "${secret} <script>\n{{literal}}", "footer & text");
        for (Kind kind : List.of(Kind.FAULT, Kind.REMINDER, Kind.RECOVERY, Kind.RECOVERY_SUMMARY, Kind.EVENT)) {
            var result = renderer.render(facts(kind), settings);
            assertThat(result.successful()).isTrue();
            var snapshot = result.snapshot();
            assertThat(snapshot.subject()).startsWith("[prod] [").endsWith(" (team)");
            assertThat(snapshot.html()).doesNotContain("<script>", "<img>", "<pre>")
                    .contains("<!doctype html>", "<table", "<th scope=\"row\"", "&lt;script&gt;", "&lt;img&gt;",
                            "${secret}", "{{literal}}", "Supplemental information", "User supplied footer", "footer &amp; text");
            assertThat(snapshot.html()).contains("[" + kindDisplay(kind) + "]", "Cluster", "Occurred at", "Reason");
            assertThat(snapshot.html()).contains(">" + stateDisplay(kind) + "</td>");
            assertThat(snapshot.html()).contains(kind == Kind.RECOVERY || kind == Kind.RECOVERY_SUMMARY
                    ? "Recovered at" : "Episode");
            assertThat(snapshot.text()).contains("User supplied note", "User supplied footer", "Unknown", "org/cluster");
            var json = (java.util.Map<?, ?>) JsonReader.parse(new String(snapshot.webhook(), StandardCharsets.UTF_8));
            assertThat(json.get("purpose")).isEqualTo("alert");
            assertThat(json.get("deliveryId")).isEqualTo("delivery-1");
            assertThat(json.get("items")).isInstanceOf(List.class);
            assertThat(renderer.render(facts(kind), settings).snapshot().webhook()).isEqualTo(snapshot.webhook());
            byte[] changed = snapshot.webhook(); changed[0] = 0;
            assertThat(snapshot.webhook()[0]).isEqualTo((byte) '{');
        }
    }
    private static String kindDisplay(Kind kind) {
        return switch (kind) {
            case FAULT -> "Alert";
            case REMINDER -> "Reminder";
            case RECOVERY -> "Recovery";
            case RECOVERY_SUMMARY -> "Recovery summary";
            case EVENT -> "Event";
            case CHANNEL_TEST -> "TEST";
        };
    }
    private static String stateDisplay(Kind kind) {
        return switch (kind) {
            case FAULT, REMINDER -> "FIRING";
            case RECOVERY, RECOVERY_SUMMARY -> "RESOLVED";
            case EVENT -> "EVENT";
            case CHANNEL_TEST -> throw new IllegalArgumentException("channel tests have no event state");
        };
    }
    @Test void everyFactValueIsEscapedInHtmlTextAndLinkContexts() {
        var key = new AlarmKey(new AlarmKey.Scope("org&owner", "cluster<svg>"),
                new AlarmKey.Resource("pipeline", "orders<img src=x>", "resource-1"),
                "pipeline.failed", "rule'\"><svg>");
        var item = new Item(key, 1L, State.FIRING, AlarmSeverity.MAJOR,
                "<img src=x onerror=alert(1)>", "failure </td><script>alert(1)</script>", T, T, null);
        var f = new Facts(Kind.FAULT, "delivery-1", 1, T, List.of(item), 1,
                java.net.URI.create("https://tapstate.example/alerts/a&b"), List.of(), null, null);
        var settings = new MessageSettings("", "", "note <script>alert(1)</script>", "footer </td>");
        var html = new NotificationRenderer().render(f, settings).snapshot().html();
        assertThat(html).doesNotContain("<img", "<svg", "<script>", "</td><script>")
                .contains("org&amp;owner", "cluster&lt;svg&gt;", "orders&lt;img src=x&gt;",
                        "rule&#39;&quot;&gt;&lt;svg&gt;", "&lt;img src=x onerror=alert(1)&gt;",
                        "failure &lt;/td&gt;&lt;script&gt;alert(1)&lt;/script&gt;",
                        "href=\"https://tapstate.example/alerts/a&amp;b\"",
                        "note &lt;script&gt;alert(1)&lt;/script&gt;", "footer &lt;/td&gt;");
    }
    @Test void testsHaveAnUnremovableMarkerAndNoRealEpisodeOrReadState() {
        Facts f = new Facts(Kind.CHANNEL_TEST, "test-delivery", 0, T, List.of(), 0, null, List.of(), "test-1", "Email");
        var snapshot = new NotificationRenderer().render(f, new MessageSettings("prefix ", " suffix", "note", "footer")).snapshot();
        assertThat(snapshot.subject()).contains("[TEST]");
        String json = new String(snapshot.webhook(), StandardCharsets.UTF_8);
        assertThat(json).contains("\"purpose\":\"channel-test\"", "\"isTest\":true", "test-1").doesNotContain("episode", "resource", "firing", "readAt");
        assertThat(snapshot.text()).contains("does not represent a real fault");
        assertThat(snapshot.html()).contains("[TEST]", "This is a notification configuration test. It does not represent a real fault.",
                "Test ID", "test-1", "Channel", "Email", "Initiated at").doesNotContain("<pre>");
    }
    @Test void codePointLimitsAndHeaderInjectionAreValidatedBeforeRendering() {
        String point = new String(Character.toChars(0x1f600));
        new MessageSettings(point.repeat(64), "", point.repeat(1024), "");
        assertThatThrownBy(() -> new MessageSettings(point.repeat(65), "", "", "")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new MessageSettings("ok\r\nBcc: hidden@example.test", "", "", "")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new MessageSettings("", "", "bad\u0000", "")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new MessageSettings("\ud800", "", "", "")).isInstanceOf(TapstateException.class);
        var snapshot = new NotificationRenderer().render(facts(Kind.FAULT), new MessageSettings("x".repeat(64), "y".repeat(64), "", "")).snapshot();
        assertThat(snapshot.subject().codePointCount(0, snapshot.subject().length())).isLessThanOrEqualTo(256);
        assertThat(snapshot.subject()).contains("[Alert]", "[MAJOR]");
    }
    @Test void oversizedGroupsAreVisibleSummariesAndUnrenderablePayloadsHaveNoSnapshot() {
        var first = facts(Kind.FAULT).items().getFirst();
        var members = new ArrayList<Item>();
        for (int i = 0; i < 25; i++) { members.add(first); }
        var f = new Facts(Kind.FAULT, "group", 1, T, members, 30, null, List.of(), null, null);
        var snapshot = new NotificationRenderer().render(f, MessageSettings.empty()).snapshot();
        assertThat(new String(snapshot.webhook(), StandardCharsets.UTF_8)).contains("\"omittedItems\":10");
        String wide = "x".repeat(256);
        AlarmKey key = new AlarmKey(new AlarmKey.Scope(wide, wide), new AlarmKey.Resource("pipeline", wide, wide), "pipeline.failed", wide);
        String reason = new String(Character.toChars(0x1f600)).repeat(8192);
        var giant = new Item(key, 1L, State.FIRING, AlarmSeverity.MAJOR, wide, reason, T, T, null);
        members.clear(); for (int i = 0; i < 20; i++) { members.add(giant); }
        var result = new NotificationRenderer().render(new Facts(Kind.FAULT, "wide", 1, T, members, 20, null, List.of(), null, null), MessageSettings.empty());
        assertThat(result.failure()).isEqualTo(Failure.PAYLOAD_BUDGET);
        assertThat(result.snapshot()).isNull();
    }
    @Test void mimeIsStableBoundedAndContainsExactlyOneConfiguredTarget() {
        var snapshot = new NotificationRenderer().render(facts(Kind.FAULT), MessageSettings.empty()).snapshot();
        var frozen = EmailMime.freeze("delivery-1", T, "alerts@example.test", "Case.Sensitive@example.test", snapshot);
        assertThat(frozen.successful()).isTrue();
        assertThat(frozen.bytes()).isEqualTo(EmailMime.freeze("delivery-1", T, "alerts@example.test", "Case.Sensitive@example.test", snapshot).bytes());
        String mime = new String(frozen.bytes(), StandardCharsets.US_ASCII);
        assertThat(mime).contains("To: <Case.Sensitive@example.test>", "Message-ID: <", "multipart/alternative", "text/plain", "text/html").doesNotContain("Bcc:");
        assertThat(frozen.bytes().length).isLessThanOrEqualTo(EmailMime.MESSAGE_LIMIT);
        assertThat(EmailMime.freeze("delivery-1", T, "alerts@example.test", "target@example.test\r\nBcc: other@example.test", snapshot).failure()).isEqualTo(Failure.INVALID_FACTS);
    }
    @Test void everyGroupMemberNameAppearsInTheVisibleEmailFacts() {
        AlarmKey secondKey = new AlarmKey(KEY.scope(), new AlarmKey.Resource("pipeline", "technical-002", "resource-2"), "pipeline.failed", "rule");
        var first = new Item(KEY, 1L, State.FIRING, AlarmSeverity.MAJOR, "orders-east", "failed", T, T, null);
        var second = new Item(secondKey, 2L, State.FIRING, AlarmSeverity.CRITICAL, "orders-west", "failed", T, T, null);
        var snapshot = new NotificationRenderer().render(new Facts(Kind.FAULT, "group-names", 1, T,
                List.of(first, second), 2, null, List.of(), null, null), MessageSettings.empty()).snapshot();
        assertThat(snapshot.text()).contains("orders-east", "orders-west");
        assertThat(snapshot.html()).contains("orders-east", "orders-west");
        assertThat(snapshot.subject()).contains("CRITICAL");
    }
    @Test void fixedTestInitiationTimeIsVisibleInsideTheDecodedEmailBody() {
        var f = new Facts(Kind.CHANNEL_TEST, "test-delivery", 0, T, List.of(), 0, null, List.of(), "test-1", "Email");
        var snapshot = new NotificationRenderer().render(f, MessageSettings.empty()).snapshot();
        var frozen = EmailMime.freeze("test-delivery", T, "alerts@example.test", "user@example.test", snapshot);
        String mime = new String(frozen.bytes(), StandardCharsets.US_ASCII);
        String part = mime.split("Content-Transfer-Encoding: base64\r\n\r\n", -1)[1].split("\r\n--", -1)[0];
        String text = new String(java.util.Base64.getMimeDecoder().decode(part), StandardCharsets.UTF_8);
        assertThat(text).contains("test-1", "Email", T.toString());
    }
    @Test void completeVersionOneWireContractsMatchReviewedBusinessAndTestGoldens() throws Exception {
        var renderer = new NotificationRenderer();
        var business = renderer.render(facts(Kind.FAULT), MessageSettings.empty()).snapshot();
        var test = renderer.render(new Facts(Kind.CHANNEL_TEST, "test-delivery", 0, T, List.of(), 0, null, List.of(), "test-1", "Email"), MessageSettings.empty()).snapshot();
        try (var in = getClass().getResourceAsStream("/notification-business-v1.golden.json")) {
            assertThat(in).isNotNull();
            assertThat(new String(business.webhook(), StandardCharsets.UTF_8)).isEqualTo(new String(in.readAllBytes(), StandardCharsets.UTF_8).stripTrailing());
        }
        try (var in = getClass().getResourceAsStream("/notification-test-v1.golden.json")) {
            assertThat(in).isNotNull();
            assertThat(new String(test.webhook(), StandardCharsets.UTF_8)).isEqualTo(new String(in.readAllBytes(), StandardCharsets.UTF_8).stripTrailing());
        }
    }
}
