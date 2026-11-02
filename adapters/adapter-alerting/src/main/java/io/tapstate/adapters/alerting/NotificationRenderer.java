package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlarmSeverity;
import io.tapstate.core.alerting.DeliveryEvidence;
import io.tapstate.core.alerting.MessageSettings;
import io.tapstate.core.common.JsonWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/** Fixed layouts over already authorized, redacted facts. This class never performs I/O to a target. */
public final class NotificationRenderer {
    public static final int TEMPLATE_VERSION = 1;
    public static final int WEBHOOK_LIMIT = 64 * 1024;
    public static final int MAX_ITEMS = 20;
    private static final DateTimeFormatter TEST_TIME = new DateTimeFormatterBuilder().appendInstant(0).toFormatter(Locale.ROOT);
    public enum Kind { FAULT, REMINDER, RECOVERY, RECOVERY_SUMMARY, EVENT, CHANNEL_TEST }
    public enum State { FIRING, RESOLVED, EVENT }
    public enum Failure { INVALID_FACTS, PAYLOAD_BUDGET }
    public record Item(AlarmKey key, Long episode, State state, AlarmSeverity severity,
            String displayName, String reason, Instant occurredAt, Instant observedAt, Instant recoveredAt) { }
    public record Previous(String deliveryId, DeliveryEvidence.Outcome outcome) { }
    public record Facts(Kind kind, String deliveryId, long logicalSequence, Instant createdAt,
            List<Item> items, int totalItems, URI detailsUrl, List<Previous> previous,
            String testId, String channelName) {
        public Facts { items = List.copyOf(items); previous = List.copyOf(previous); }
    }
    public record Snapshot(int templateVersion, String locale, String subject, String text, String html, byte[] webhook) {
        public Snapshot { webhook = webhook.clone(); }
        @Override public byte[] webhook() { return webhook.clone(); }
        @Override public boolean equals(Object value) {
            return this == value || value instanceof Snapshot other && templateVersion == other.templateVersion
                    && Objects.equals(locale, other.locale) && Objects.equals(subject, other.subject)
                    && Objects.equals(text, other.text) && Objects.equals(html, other.html)
                    && java.util.Arrays.equals(webhook, other.webhook);
        }
        @Override public int hashCode() {
            return 31 * Objects.hash(templateVersion, locale, subject, text, html) + java.util.Arrays.hashCode(webhook);
        }
        @Override public String toString() {
            return "NotificationSnapshot[templateVersion=" + templateVersion + ", webhookBytes=" + webhook.length + "]";
        }
    }
    public record Result(Snapshot snapshot, Failure failure) {
        public boolean successful() { return snapshot != null; }
    }
    private final Properties words;
    private final boolean allowInsecureLoopback;
    public NotificationRenderer() { this(false); }
    public NotificationRenderer(boolean allowInsecureLoopback) {
        this.allowInsecureLoopback = allowInsecureLoopback;
        words = new Properties();
        try (var stream = getClass().getResourceAsStream("/notifications/en.properties")) {
            if (stream == null) { throw new IllegalStateException("missing fixed notification vocabulary"); }
            words.load(new InputStreamReader(stream, StandardCharsets.UTF_8));
        } catch (IOException ex) { throw new IllegalStateException("cannot read fixed notification vocabulary", ex); }
    }
    public Result render(Facts facts, MessageSettings settings) {
        Objects.requireNonNull(settings, "settings");
        if (!valid(facts)) { return new Result(null, Failure.INVALID_FACTS); }
        boolean test = facts.kind() == Kind.CHANNEL_TEST;
        List<Item> items = facts.items().subList(0, Math.min(MAX_ITEMS, facts.items().size()));
        String severity = test || items.isEmpty() ? "" : "[" + facts.items().stream().map(Item::severity).min(java.util.Comparator.comparingInt(Enum::ordinal)).orElseThrow().name() + "] ";
        String system = word("kind." + wire(facts.kind())) + " " + severity
                + (test ? facts.channelName() : cleanTitle(items.getFirst().displayName()));
        int available = 256 - count(settings.titlePrefix()) - count(settings.titleSuffix());
        String subject = settings.titlePrefix() + shorten(system, available) + settings.titleSuffix();
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("schemaVersion", 1);
        root.put("purpose", test ? "channel-test" : "alert");
        root.put("isTest", test);
        root.put("deliveryId", facts.deliveryId());
        root.put("logicalSequence", facts.logicalSequence());
        root.put("kind", wire(facts.kind()));
        root.put("createdAt", facts.createdAt().toString());
        root.put("templateVersion", TEMPLATE_VERSION);
        root.put("locale", "en");
        root.put("title", subject);
        root.put("custom", ordered("messageNote", settings.messageNote(), "footerText", settings.footerText()));
        StringBuilder text = new StringBuilder(subject).append('\n').append(word("source")).append('\n');
        if (test) {
            root.put("testId", facts.testId());
            root.put("channelName", facts.channelName());
            root.put("summary", word("test-summary"));
            text.append(word("test-summary")).append('\n');
            text.append("Channel: ").append(facts.channelName()).append('\n');
            text.append(word("created")).append(": ").append(testTime(facts.createdAt())).append('\n');
            text.append(word("test-save-note")).append('\n');
        } else {
            List<Map<String, Object>> renderedItems = new ArrayList<>();
            for (Item item : items) {
                String reason = item.reason() == null ? word("unknown") : shorten(item.reason(), 512);
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("scope", ordered("authorityId", item.key().scope().authorityId(), "clusterId", item.key().scope().clusterId()));
                value.put("resource", ordered("kind", item.key().resource().kind(), "id", item.key().resource().id(), "incarnation", item.key().resource().incarnation()));
                value.put("alarmTypeId", item.key().typeId()); value.put("qualifier", item.key().qualifier());
                value.put("episode", item.episode()); value.put("state", wire(item.state()));
                value.put("severity", wire(item.severity()));
                value.put("occurredAt", item.occurredAt() == null ? null : item.occurredAt().toString());
                value.put("observedAt", item.observedAt() == null ? null : item.observedAt().toString());
                value.put("recoveredAt", item.recoveredAt() == null ? null : item.recoveredAt().toString());
                value.put("displayName", shorten(item.displayName(), 256)); value.put("reason", reason);
                renderedItems.add(value);
                text.append(word("name")).append(": ").append(shorten(item.displayName(), 256)).append('\n');
                appendTextRow(text, word("alarm-type"), alarmType(item));
                appendTextRow(text, word("state"), word("state." + wire(item.state())));
                appendTextRow(text, word("severity"), word("severity." + wire(item.severity())));
                if (item.occurredAt() != null) appendTextRow(text, word("occurred"), testTime(item.occurredAt()));
                if (item.recoveredAt() != null) appendTextRow(text, word("recovered"), testTime(item.recoveredAt()));
                if (item.reason() != null && !item.reason().isBlank()) appendTextRow(text, word("reason"), shorten(item.reason(), 512));
            }
            root.put("items", renderedItems); root.put("totalItems", facts.totalItems());
            root.put("omittedItems", facts.totalItems() - items.size());
            if (facts.totalItems() > items.size()) { text.append(word("omitted")).append(": ").append(facts.totalItems() - items.size()).append('\n'); }
            List<Map<String, Object>> previous = new ArrayList<>();
            for (Previous p : facts.previous()) {
                previous.add(ordered("deliveryId", p.deliveryId(), "outcome", wire(p.outcome())));
            }
            root.put("previous", previous);
        }
        if (!settings.messageNote().isEmpty()) { text.append(word("user-note")).append(":\n").append(settings.messageNote()).append('\n'); }
        if (facts.detailsUrl() != null) { root.put("detailsUrl", facts.detailsUrl().toASCIIString()); text.append(word("details")).append(": ").append(facts.detailsUrl().toASCIIString()).append('\n'); }
        if (!settings.footerText().isEmpty()) { text.append(word("user-footer")).append(":\n").append(settings.footerText()).append('\n'); }
        if (!test) {
            text.append('\n').append(word("technical-details")).append('\n');
            appendTextRow(text, word("delivery-id"), facts.deliveryId());
            appendTextRow(text, "Logical sequence", Long.toString(facts.logicalSequence()));
            appendTextRow(text, word("created"), facts.createdAt().toString());
            for (Item item : items) {
                appendTextRow(text, word("name"), shorten(item.displayName(), 256));
                appendTextRow(text, word("resource"), item.key().resource().kind() + "/" + item.key().resource().id());
                appendTextRow(text, "Incarnation", item.key().resource().incarnation());
                appendTextRow(text, word("scope"), item.key().scope().authorityId() + "/" + item.key().scope().clusterId());
                appendTextRow(text, "Alarm type ID", item.key().typeId());
                appendTextRow(text, "Qualifier", item.key().qualifier());
                appendTextRow(text, word("episode"), item.episode() == null ? word("not-applicable") : item.episode().toString());
                if (item.observedAt() != null) appendTextRow(text, word("observed"), item.observedAt().toString());
            }
            for (Previous previous : facts.previous()) appendTextRow(text, word("previous"), previous.deliveryId() + " " + wire(previous.outcome()));
        }
        String plain = text.toString();
        String html = html(facts, settings, subject, items, test);
        byte[] json = JsonWriter.write(root).getBytes(StandardCharsets.UTF_8);
        if (json.length > WEBHOOK_LIMIT || plain.getBytes(StandardCharsets.UTF_8).length + html.getBytes(StandardCharsets.UTF_8).length > 160 * 1024) {
            return new Result(null, Failure.PAYLOAD_BUDGET);
        }
        return new Result(new Snapshot(TEMPLATE_VERSION, "en", subject, plain, html, json), null);
    }
    private String html(Facts facts, MessageSettings settings, String subject, List<Item> items, boolean test) {
        StringBuilder out = new StringBuilder(2048);
        out.append("<!doctype html><html lang=\"en\"><body style=\"margin:0;padding:24px;background:#1b1c1e;color:#f3f4f5;font-family:Arial,Helvetica,sans-serif\">")
                .append("<table role=\"presentation\" cellspacing=\"0\" cellpadding=\"0\" border=\"0\" width=\"100%\"><tr><td align=\"center\">")
                .append("<table role=\"presentation\" cellspacing=\"0\" cellpadding=\"0\" border=\"0\" width=\"840\" style=\"max-width:100%;background:#262729;border:1px solid #414347;border-radius:8px\">")
                .append("<tr><td style=\"padding:24px 28px 12px\"><div style=\"font-size:12px;color:#aeb2b8\">")
                .append(escape(word("source"))).append("</div><h1 style=\"margin:8px 0 0;font-size:22px;line-height:1.35;color:#f3f4f5\">")
                .append(escape(subject)).append("</h1></td></tr>");

        if (test) {
            out.append("<tr><td style=\"padding:0 28px 20px\"><div style=\"padding:14px 16px;border:1px solid #8b702e;background:#3a3322;border-radius:4px\">")
                    .append("<strong style=\"display:block;margin-bottom:8px;color:#f3f4f5\">[TEST] ").append(escape(word("test-summary"))).append("</strong>")
                    .append("<table cellspacing=\"0\" cellpadding=\"0\" border=\"0\" width=\"100%\">");
            appendHtmlRow(out, "Channel", facts.channelName());
            appendHtmlRow(out, word("created"), testTime(facts.createdAt()));
            out.append("</table><p style=\"margin:12px 0 0;color:#f3f4f5;font-size:13px;line-height:1.5\">")
                    .append(escape(word("test-save-note"))).append("</p></div></td></tr>");
        } else {
            for (Item item : items) {
                out.append("<tr><td style=\"padding:0 28px 20px\"><table cellspacing=\"0\" cellpadding=\"0\" border=\"0\" width=\"100%\" style=\"border:1px solid #414347;border-radius:4px;border-collapse:separate\">")
                        .append("<tr><th colspan=\"2\" align=\"left\" style=\"padding:13px 16px;background:#303235;color:#f3f4f5;border-bottom:1px solid #414347;font-size:16px\">")
                        .append(escape(shorten(item.displayName(), 256))).append("</th></tr>");
                appendHtmlRow(out, word("alarm-type"), alarmType(item));
                appendHtmlRow(out, word("state"), word("state." + wire(item.state())));
                appendHtmlRow(out, word("severity"), word("severity." + wire(item.severity())));
                if (item.occurredAt() != null) appendHtmlRow(out, word("occurred"), testTime(item.occurredAt()));
                if (item.recoveredAt() != null) appendHtmlRow(out, word("recovered"), testTime(item.recoveredAt()));
                if (item.reason() != null && !item.reason().isBlank()) appendHtmlRow(out, word("reason"), shorten(item.reason(), 512));
                out.append("</table></td></tr>");
            }
            if (facts.totalItems() > items.size()) {
                out.append("<tr><td style=\"padding:0 28px 20px;color:#aeb2b8\">")
                        .append(escape(word("omitted"))).append(": ").append(facts.totalItems() - items.size()).append("</td></tr>");
            }
        }

        appendSupplemental(out, facts, settings, test);
        if (!settings.footerText().isEmpty()) {
            out.append("<tr><td style=\"padding:0 28px 20px\"><div style=\"border-top:1px solid #414347;padding-top:14px;color:#aeb2b8;font-size:13px\"><strong style=\"color:#aeb2b8\">")
                    .append(escape(word("user-footer"))).append("</strong><div style=\"margin-top:5px;white-space:pre-wrap;color:#aeb2b8\">")
                    .append(escape(settings.footerText())).append("</div></div></td></tr>");
        }
        if (!test) appendTechnicalDetails(out, facts, items);
        out.append("</table></td></tr></table></body></html>");
        return out.toString();
    }
    private void appendSupplemental(StringBuilder out, Facts facts, MessageSettings settings, boolean test) {
        if (test && facts.detailsUrl() == null && settings.messageNote().isEmpty()) { return; }
        out.append("<tr><td style=\"padding:0 28px\"><div style=\"border-top:1px solid #414347;padding:16px 0\">");
        if (facts.detailsUrl() != null) {
            String href = escape(facts.detailsUrl().toASCIIString());
            out.append("<div style=\"margin-top:10px\"><a style=\"color:#8ab4f8;text-decoration:underline\" href=\"").append(href).append("\">")
                    .append(escape(word("details"))).append("</a></div>");
        }
        if (!settings.messageNote().isEmpty()) {
            out.append("<div style=\"margin-top:14px\"><strong style=\"color:#f3f4f5\">").append(escape(word("user-note")))
                    .append("</strong><div style=\"margin-top:5px;white-space:pre-wrap;color:#f3f4f5\">")
                    .append(escape(settings.messageNote())).append("</div></div>");
        }
        out.append("</div></td></tr>");
    }
    private void appendTechnicalDetails(StringBuilder out, Facts facts, List<Item> items) {
        out.append("<tr><td style=\"padding:0 28px 20px\"><h2 style=\"color:#f3f4f5;font-size:14px\">")
                .append(escape(word("technical-details"))).append("</h2><table width=\"100%\" cellspacing=\"0\" cellpadding=\"0\" border=\"0\">");
        appendHtmlRow(out, word("delivery-id"), facts.deliveryId());
        appendHtmlRow(out, "Logical sequence", Long.toString(facts.logicalSequence()));
        appendHtmlRow(out, word("created"), facts.createdAt().toString());
        for (Item item : items) {
            appendHtmlRow(out, word("name"), shorten(item.displayName(), 256));
            appendHtmlRow(out, word("resource"), item.key().resource().kind() + "/" + item.key().resource().id());
            appendHtmlRow(out, "Incarnation", item.key().resource().incarnation());
            appendHtmlRow(out, "Cluster", item.key().scope().clusterId());
            appendHtmlRow(out, word("scope"), item.key().scope().authorityId());
            appendHtmlRow(out, "Alarm type ID", item.key().typeId());
            appendHtmlRow(out, "Qualifier", item.key().qualifier());
            appendHtmlRow(out, word("episode"), item.episode() == null ? word("not-applicable") : item.episode().toString());
            if (item.observedAt() != null) appendHtmlRow(out, word("observed"), item.observedAt().toString());
        }
        for (Previous previous : facts.previous()) appendHtmlRow(out, word("previous"), previous.deliveryId() + " " + wire(previous.outcome()));
        out.append("</table></td></tr>");
    }
    private String alarmType(Item item) {
        String id = item.key().typeId();
        String localized = words.getProperty("alarm-type." + id);
        if (localized != null) return localized;
        String human = id.replaceAll("[._-]+", " ").trim();
        return human.isEmpty() ? word("alarm-type") : human.substring(0, 1).toUpperCase(Locale.ROOT) + human.substring(1);
    }
    private static void appendTextRow(StringBuilder out, String label, String value) {
        out.append(label).append(": ").append(value).append('\n');
    }
    private static void appendHtmlRow(StringBuilder out, String label, String value) {
        out.append("<tr><th scope=\"row\" align=\"left\" valign=\"top\" style=\"width:128px;white-space:nowrap;padding:7px 16px;color:#aeb2b8;font-size:13px;font-weight:normal;border-bottom:1px solid #414347\">")
                .append(escape(label)).append("</th><td style=\"padding:7px 16px;color:#f3f4f5;font-size:13px;white-space:pre-wrap;overflow-wrap:anywhere;border-bottom:1px solid #414347\">")
                .append(escape(value)).append("</td></tr>");
    }
    private static String testTime(Instant value) {
        return TEST_TIME.format(value).replace('T', ' ').replace("Z", " UTC");
    }
    private boolean valid(Facts f) {
        if (f == null || f.kind() == null || !identifier(f.deliveryId()) || f.createdAt() == null
                || f.items().size() > 100 || f.previous().size() > 32 || f.totalItems() < f.items().size()) { return false; }
        if (f.detailsUrl() != null && !validDetailsUrl(f.detailsUrl())) return false;
        if (f.kind() == Kind.CHANNEL_TEST) {
            if (!f.items().isEmpty() || f.totalItems() != 0 || !f.previous().isEmpty() || !identifier(f.testId())
                    || f.channelName() == null || count(f.channelName()) > 64 || !f.channelName().matches("[A-Za-z0-9 ._-]+") || f.logicalSequence() != 0) { return false; }
        } else if (f.items().isEmpty() || f.logicalSequence() <= 0 || f.testId() != null || f.channelName() != null) { return false; }
        for (Item item : f.items()) {
            if (item.key() == null || item.state() == null || item.severity() == null || item.displayName() == null
                    || !validPlain(item.displayName(), 2048) || item.reason() != null && !validPlain(item.reason(), 8192)) { return false; }
            if (f.kind() == Kind.EVENT) {
                if (item.state() != State.EVENT || item.episode() != null || item.recoveredAt() != null) { return false; }
            } else if (item.episode() == null || item.episode() <= 0) { return false; }
            if (f.kind() == Kind.FAULT || f.kind() == Kind.REMINDER) {
                if (item.state() != State.FIRING || item.recoveredAt() != null) { return false; }
            }
            if (f.kind() == Kind.RECOVERY || f.kind() == Kind.RECOVERY_SUMMARY) {
                if (item.state() != State.RESOLVED || item.recoveredAt() == null) { return false; }
            }
        }
        return f.previous().stream().allMatch(p -> identifier(p.deliveryId()) && p.outcome() != null);
    }
    private boolean validDetailsUrl(URI url) {
        if (url.getHost() == null || url.getRawUserInfo() != null || url.getRawFragment() != null
                || url.toASCIIString().length() > 2048) return false;
        String host = url.getHost();
        boolean loopback = host.equalsIgnoreCase("localhost") || host.equals("127.0.0.1") || host.equals("[::1]");
        if (!"https".equalsIgnoreCase(url.getScheme())
                && !(allowInsecureLoopback && "http".equalsIgnoreCase(url.getScheme()) && loopback)) return false;
        String query = url.getRawQuery();
        if (query == null) return true;
        if (!"/alerts".equals(url.getRawPath()) || !query.matches("view=(active|history)&key=[A-Za-z0-9_-]{1,1800}&episode=[1-9][0-9]*")) return false;
        try {
            return Long.parseLong(query.substring(query.lastIndexOf('=') + 1)) > 0;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
    private static Map<String, Object> ordered(Object... pairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) { result.put((String) pairs[i], pairs[i + 1]); }
        return result;
    }
    private static boolean validPlain(String value, int limit) {
        if (count(value) > limit || value.codePoints().anyMatch(c -> Character.isISOControl(c) && c != '\n' && c != '\r' && c != '\t')) { return false; }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(++i))) { return false; }
            } else if (Character.isLowSurrogate(c)) { return false; }
        }
        return true;
    }
    private String word(String key) {
        String value = words.getProperty(key);
        if (value == null) { throw new IllegalStateException("missing notification vocabulary key: " + key); }
        return value;
    }
    private static boolean identifier(String value) { return value != null && value.matches("[A-Za-z0-9._-]{1,256}"); }
    private static String wire(Enum<?> value) { return value.name().toLowerCase(Locale.ROOT).replace('_', '-'); }
    private static int count(String value) { return value.codePointCount(0, value.length()); }
    private static String cleanTitle(String value) {
        StringBuilder safe = new StringBuilder();
        value.codePoints().forEach(c -> safe.appendCodePoint(Character.isISOControl(c) ? ' ' : c));
        return safe.toString();
    }
    private static String shorten(String value, int max) {
        if (count(value) <= max) { return value; }
        return value.substring(0, value.offsetByCodePoints(0, max - 3)) + "...";
    }
    private static String escape(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&#39;");
    }
}
