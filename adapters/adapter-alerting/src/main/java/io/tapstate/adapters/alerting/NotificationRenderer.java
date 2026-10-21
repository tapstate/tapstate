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
    }
    public record Result(Snapshot snapshot, Failure failure) {
        public boolean successful() { return snapshot != null; }
    }
    private final Properties words;
    public NotificationRenderer() {
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
        text.append(word("delivery-id")).append(": ").append(facts.deliveryId()).append('\n');
        if (test) {
            root.put("testId", facts.testId());
            root.put("channelName", facts.channelName());
            root.put("summary", word("test-summary"));
            text.append(word("created")).append(": ").append(facts.createdAt()).append('\n');
            text.append(word("test-summary")).append('\n').append(word("test-id")).append(": ").append(facts.testId()).append('\n');
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
                text.append(word("resource")).append(": ").append(item.key().resource().kind()).append('/')
                        .append(item.key().resource().id()).append(" [").append(item.key().resource().incarnation()).append("]\n")
                        .append(word("scope")).append(": ").append(item.key().scope().authorityId()).append('/')
                        .append(item.key().scope().clusterId()).append('\n')
                        .append(word("alarm-type")).append(": ").append(item.key().typeId()).append('\n')
                        .append(word("state")).append(": ").append(wire(item.state())).append('\n')
                        .append(word("severity")).append(": ").append(wire(item.severity())).append('\n')
                        .append(word("episode")).append(": ").append(item.episode() == null ? word("not-applicable") : item.episode()).append('\n')
                        .append(word("occurred")).append(": ").append(item.occurredAt() == null ? word("unknown") : item.occurredAt()).append('\n')
                        .append(word("observed")).append(": ").append(item.observedAt() == null ? word("unknown") : item.observedAt()).append('\n')
                        .append(word("reason")).append(": ").append(reason).append('\n');
                if (item.recoveredAt() != null) { text.append(word("recovered")).append(": ").append(item.recoveredAt()).append('\n'); }
            }
            root.put("items", renderedItems); root.put("totalItems", facts.totalItems());
            root.put("omittedItems", facts.totalItems() - items.size());
            if (facts.totalItems() > items.size()) { text.append(word("omitted")).append(": ").append(facts.totalItems() - items.size()).append('\n'); }
            List<Map<String, Object>> previous = new ArrayList<>();
            for (Previous p : facts.previous()) {
                previous.add(ordered("deliveryId", p.deliveryId(), "outcome", wire(p.outcome())));
                text.append(word("previous")).append(": ").append(p.deliveryId()).append(' ').append(wire(p.outcome())).append('\n');
            }
            root.put("previous", previous);
        }
        if (!settings.messageNote().isEmpty()) { text.append(word("user-note")).append(":\n").append(settings.messageNote()).append('\n'); }
        if (facts.detailsUrl() != null) { root.put("detailsUrl", facts.detailsUrl().toASCIIString()); text.append(word("details")).append(": ").append(facts.detailsUrl().toASCIIString()).append('\n'); }
        if (!settings.footerText().isEmpty()) { text.append(word("user-footer")).append(":\n").append(settings.footerText()).append('\n'); }
        String plain = text.toString();
        String html = "<!doctype html><html lang=\"en\"><body><pre>" + escape(plain) + "</pre></body></html>";
        byte[] json = JsonWriter.write(root).getBytes(StandardCharsets.UTF_8);
        if (json.length > WEBHOOK_LIMIT || plain.getBytes(StandardCharsets.UTF_8).length + html.getBytes(StandardCharsets.UTF_8).length > 160 * 1024) {
            return new Result(null, Failure.PAYLOAD_BUDGET);
        }
        return new Result(new Snapshot(TEMPLATE_VERSION, "en", subject, plain, html, json), null);
    }
    private static boolean valid(Facts f) {
        if (f == null || f.kind() == null || !identifier(f.deliveryId()) || f.createdAt() == null
                || f.items().size() > 100 || f.previous().size() > 32 || f.totalItems() < f.items().size()) { return false; }
        if (f.detailsUrl() != null && (!"https".equalsIgnoreCase(f.detailsUrl().getScheme())
                || f.detailsUrl().getHost() == null || f.detailsUrl().getUserInfo() != null
                || f.detailsUrl().getRawQuery() != null || f.detailsUrl().getRawFragment() != null
                || f.detailsUrl().toASCIIString().length() > 2048)) { return false; }
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
