package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.store.IoError;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

/** Closed, versioned definitions; no arbitrary object graphs or Java class names are persisted. */
final class MongoAlertConfigurationCodec {
    static final int MAX_BYTES = 256 * 1024;
    private static final Set<String> FIELDS = Set.of("_id", "formatVersion", "kind", "incarnation", "recordRevision",
            "writeFence", "deployment", "createdAt", "createdBy", "updatedAt", "updatedBy", "retiredAt", "body");
    record Stored(Definition definition, long writeFence) { }
    private MongoAlertConfigurationCodec() { }

    static Document key(AlarmKey.Scope scope, String id) {
        return new Document("authorityId", scope.authorityId()).append("clusterId", scope.clusterId()).append("id", id);
    }

    static Document write(Definition definition, long fence) {
        Header h = definition.header(); Identity id = h.identity();
        require(fence >= 0);
        Document body = switch (definition) {
            case Rule rule -> new Document("name", rule.name()).append("typeId", rule.typeId())
                    .append("typeVersion", rule.typeVersion()).append("resources", resources(rule.resources()))
                    .append("parameters", decimals(rule.parameters()))
                    .append("severityOverride", rule.severityOverride() == null ? null : rule.severityOverride().name())
                    .append("enabled", rule.enabled()).append("metric", rule.metric() == null ? null : metric(rule.metric()));
            case Subscription route -> new Document("name", route.name()).append("match", match(route.match()))
                    .append("enabled", route.enabled()).append("channels", route.channels().stream().map(MongoAlertConfigurationCodec::channel).toList())
                    .append("groupBy", route.groupBy().stream().map(Enum::name).sorted().toList())
                    .append("grouping", new Document("initialNanos", route.grouping().initialWait().toNanos())
                            .append("changeNanos", route.grouping().changeInterval().toNanos()).append("repeatNanos", route.grouping().repeatInterval().toNanos()))
                    .append("inhibition", route.inhibition().stream().map(edge -> new Document("root", edge.rootType()).append("dependent", edge.dependentType())).toList());
            case Silence silence -> new Document("reason", silence.reason()).append("window", window(silence.window()));
        };
        Document row = new Document("_id", key(id.scope(), id.id())).append("formatVersion", AlertConfiguration.FORMAT_VERSION)
                .append("kind", id.kind().name()).append("incarnation", id.incarnation()).append("recordRevision", h.revision())
                .append("writeFence", fence).append("deployment", h.deployment().name()).append("createdAt", h.createdAt().toString())
                .append("createdBy", h.createdBy()).append("updatedAt", h.updatedAt().toString()).append("updatedBy", h.updatedBy())
                .append("retiredAt", h.retiredAt() == null ? null : h.retiredAt().toString()).append("body", body);
        if (size(row) > MAX_BYTES) {
            throw new TapstateException(AlertError.INVALID_CONFIGURATION,
                    Map.of("field", "configurationBytes", "reason", "encoded-record-too-large"), null);
        }
        return row;
    }

    static Stored read(Kind expectedKind, AlarmKey.Scope expectedScope, String expectedId, Document row) {
        try {
            fields(row, FIELDS); require(size(row) <= MAX_BYTES);
            require(row.get("formatVersion") instanceof Integer version && version == AlertConfiguration.FORMAT_VERSION);
            Document key = doc(row, "_id"); fields(key, Set.of("authorityId", "clusterId", "id"));
            require(key(expectedScope, expectedId).equals(key));
            require(text(row, "kind").equals(expectedKind.name()));
            Identity identity = new Identity(expectedKind, expectedScope, expectedId, text(row, "incarnation"));
            Header header = new Header(identity, number(row, "recordRevision", 1),
                    enumeration(AlertChannelPolicy.Deployment.values(), text(row, "deployment")),
                    time(row, "createdAt", false), text(row, "createdBy"), time(row, "updatedAt", false),
                    text(row, "updatedBy"), time(row, "retiredAt", true));
            Document body = doc(row, "body");
            Definition definition = switch (expectedKind) {
                case RULE -> readRule(header, body);
                case SUBSCRIPTION -> readSubscription(header, body);
                case SILENCE -> {
                    fields(body, Set.of("reason", "window"));
                    yield new Silence(header, text(body, "reason"), readWindow(expectedScope, doc(body, "window")));
                }
                case RECEIVER -> throw new IllegalArgumentException("unsupported receiver format");
            };
            return new Stored(definition, number(row, "writeFence", 0));
        } catch (IllegalArgumentException | NullPointerException | ArithmeticException | java.time.DateTimeException | TapstateException invalid) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", "alert-configuration", "field", "format-or-identity"), null);
        }
    }

    private static Rule readRule(Header header, Document body) {
        fields(body, Set.of("name", "typeId", "typeVersion", "resources", "parameters", "severityOverride", "enabled", "metric"));
        require(body.get("typeVersion") instanceof Integer);
        Map<String, BigDecimal> parameters = new LinkedHashMap<>();
        Document supplied = doc(body, "parameters"); require(supplied.size() <= 16);
        supplied.forEach((key, value) -> parameters.put(key, decimal(value)));
        return new Rule(header, text(body, "name"), text(body, "typeId"), body.getInteger("typeVersion"),
                readResources(doc(body, "resources")), parameters, body.get("severityOverride") == null ? null
                        : enumeration(AlarmSeverity.values(), text(body, "severityOverride")),
                bool(body, "enabled"), body.get("metric") == null ? null : readMetric(doc(body, "metric")));
    }

    private static Subscription readSubscription(Header header, Document body) {
        fields(body, Set.of("name", "match", "enabled", "channels", "groupBy", "grouping", "inhibition"));
        Document grouping = doc(body, "grouping"); fields(grouping, Set.of("initialNanos", "changeNanos", "repeatNanos"));
        List<RouteChannel> channels = new ArrayList<>();
        for (Object value : list(body, "channels", 16)) { channels.add(readChannel(document(value))); }
        Set<GroupBy> groups = new HashSet<>();
        for (Object value : list(body, "groupBy", GroupBy.values().length)) {
            require(value instanceof String); require(groups.add(enumeration(GroupBy.values(), (String) value)));
        }
        List<AlertInhibitionPolicy.Edge> inhibition = new ArrayList<>();
        for (Object value : list(body, "inhibition", 64)) {
            Document edge = document(value); fields(edge, Set.of("root", "dependent"));
            inhibition.add(new AlertInhibitionPolicy.Edge(text(edge, "root"), text(edge, "dependent")));
        }
        return new Subscription(header, text(body, "name"), readMatch(doc(body, "match")), bool(body, "enabled"),
                channels, groups, new AlertGroupingClock.Policy(Duration.ofNanos(number(grouping, "initialNanos", 0)),
                Duration.ofNanos(number(grouping, "changeNanos", 1)), Duration.ofNanos(number(grouping, "repeatNanos", 1))), inhibition);
    }

    private static Document resource(AlarmKey.Resource value) {
        return new Document("kind", value.kind()).append("id", value.id()).append("incarnation", value.incarnation());
    }
    private static AlarmKey.Resource readResource(Document value) {
        fields(value, Set.of("kind", "id", "incarnation"));
        return new AlarmKey.Resource(text(value, "kind"), text(value, "id"), text(value, "incarnation"));
    }
    private static List<Document> resources(Set<AlarmKey.Resource> values) {
        return values.stream().sorted(java.util.Comparator.comparing(AlarmKey.Resource::kind)
                .thenComparing(AlarmKey.Resource::id).thenComparing(AlarmKey.Resource::incarnation))
                .map(MongoAlertConfigurationCodec::resource).toList();
    }
    private static Set<AlarmKey.Resource> readResourcesList(Document row, String field) {
        Set<AlarmKey.Resource> result = new HashSet<>();
        for (Object value : list(row, field, 100)) { require(result.add(readResource(document(value)))); }
        return result;
    }
    private static Document resources(Resources selector) {
        return new Document("kinds", selector.kinds().stream().sorted().toList()).append("all", selector.all())
                .append("selected", resources(selector.selected()));
    }
    private static Resources readResources(Document selector) {
        fields(selector, Set.of("kinds", "all", "selected"));
        return new Resources(strings(selector, "kinds", 8), bool(selector, "all"), readResourcesList(selector, "selected"));
    }
    private static Document match(Match match) {
        return new Document("resources", resources(match.resources())).append("typeIds", match.typeIds().stream().sorted().toList())
                .append("severities", match.severities().stream().map(Enum::name).sorted().toList());
    }
    private static Match readMatch(Document match) {
        fields(match, Set.of("resources", "typeIds", "severities"));
        Set<AlarmSeverity> severities = new HashSet<>();
        for (String value : strings(match, "severities", AlarmSeverity.values().length)) {
            severities.add(enumeration(AlarmSeverity.values(), value));
        }
        return new Match(readResources(doc(match, "resources")), strings(match, "typeIds", 16), severities);
    }
    private static Document identity(Identity value) {
        return key(value.scope(), value.id()).append("kind", value.kind().name()).append("incarnation", value.incarnation());
    }
    private static Identity readIdentity(Document value) {
        fields(value, Set.of("authorityId", "clusterId", "id", "kind", "incarnation"));
        return new Identity(enumeration(Kind.values(), text(value, "kind")),
                new AlarmKey.Scope(text(value, "authorityId"), text(value, "clusterId")), text(value, "id"), text(value, "incarnation"));
    }
    private static Document channel(RouteChannel channel) {
        MessageSettings m = channel.messages();
        return new Document("type", channel.channelType()).append("receivers", channel.receivers().stream().map(MongoAlertConfigurationCodec::identity).toList())
                .append("messages", new Document("titlePrefix", m.titlePrefix()).append("titleSuffix", m.titleSuffix())
                        .append("messageNote", m.messageNote()).append("footerText", m.footerText()));
    }
    private static RouteChannel readChannel(Document channel) {
        fields(channel, Set.of("type", "receivers", "messages"));
        List<Identity> receivers = new ArrayList<>();
        for (Object value : list(channel, "receivers", 32)) { receivers.add(readIdentity(document(value))); }
        Document m = doc(channel, "messages"); fields(m, Set.of("titlePrefix", "titleSuffix", "messageNote", "footerText"));
        return new RouteChannel(text(channel, "type"), receivers, new MessageSettings(text(m, "titlePrefix"), text(m, "titleSuffix"),
                text(m, "messageNote"), text(m, "footerText")));
    }
    private static Document metric(MetricAlertEvaluator.Rule metric) {
        return new Document("name", metric.name()).append("type", metric.type().name()).append("unit", metric.unit())
                .append("attributes", new Document(new java.util.TreeMap<>(metric.attributes())))
                .append("direction", metric.direction().name()).append("trigger", metric.trigger().toString()).append("recover", metric.recover().toString());
    }
    private static MetricAlertEvaluator.Rule readMetric(Document metric) {
        fields(metric, Set.of("name", "type", "unit", "attributes", "direction", "trigger", "recover"));
        Map<String, String> attributes = new LinkedHashMap<>();
        Document source = doc(metric, "attributes"); require(source.size() <= 16);
        source.forEach((key, value) -> { require(value instanceof String); attributes.put(key, (String) value); });
        return new MetricAlertEvaluator.Rule(text(metric, "name"), enumeration(MetricType.values(), text(metric, "type")),
                text(metric, "unit"), attributes, enumeration(MetricAlertEvaluator.Direction.values(), text(metric, "direction")),
                decimal(metric.get("trigger")), decimal(metric.get("recover")));
    }
    private static Document window(AlertSilenceWindow window) {
        return new Document("allResources", window.allResources()).append("resources", resources(window.resources()))
                .append("typeIds", window.typeIds().stream().sorted().toList()).append("startsAt", window.startsAt().toString())
                .append("durationSeconds", window.duration().getSeconds())
                .append("periodSeconds", window.period() == null ? null : window.period().getSeconds())
                .append("endsAt", window.endsAt() == null ? null : window.endsAt().toString());
    }
    private static AlertSilenceWindow readWindow(AlarmKey.Scope scope, Document window) {
        fields(window, Set.of("allResources", "resources", "typeIds", "startsAt", "durationSeconds", "periodSeconds", "endsAt"));
        return new AlertSilenceWindow(scope, bool(window, "allResources"), readResourcesList(window, "resources"), strings(window, "typeIds", 16),
                time(window, "startsAt", false), Duration.ofSeconds(number(window, "durationSeconds", 1)),
                window.get("periodSeconds") == null ? null : Duration.ofSeconds(number(window, "periodSeconds", 1)), time(window, "endsAt", true));
    }
    private static Document decimals(Map<String, BigDecimal> source) {
        Document result = new Document(); new java.util.TreeMap<>(source).forEach((key, value) -> result.append(key, value.toString())); return result;
    }
    private static BigDecimal decimal(Object value) {
        require(value instanceof String text && text.length() <= 160);
        BigDecimal parsed = new BigDecimal((String) value);
        require(parsed.precision() <= 64 && Math.abs((long) parsed.scale()) <= 64); return parsed;
    }
    private static long number(Document row, String field, long minimum) {
        require(row.get(field) instanceof Long value && value >= minimum); return row.getLong(field);
    }
    private static Instant time(Document row, String field, boolean optional) {
        if (optional && row.get(field) == null) { return null; }
        String encoded = text(row, field); require(encoded.length() <= 40);
        Instant instant = Instant.parse(encoded); require(instant.toString().equals(encoded)); return instant;
    }
    private static boolean bool(Document row, String field) { require(row.get(field) instanceof Boolean); return row.getBoolean(field); }
    private static String text(Document row, String field) { require(row.get(field) instanceof String); return row.getString(field); }
    private static Document doc(Document row, String field) { return document(row.get(field)); }
    private static Document document(Object value) { require(value instanceof Document); return (Document) value; }
    private static List<?> list(Document row, String field, int maximum) {
        require(row.get(field) instanceof List<?> values && values.size() <= maximum); return (List<?>) row.get(field);
    }
    private static Set<String> strings(Document row, String field, int maximum) {
        Set<String> result = new HashSet<>();
        for (Object value : list(row, field, maximum)) { require(value instanceof String); require(result.add((String) value)); }
        return result;
    }
    private static <E extends Enum<E>> E enumeration(E[] known, String value) {
        return Arrays.stream(known).filter(item -> item.name().equals(value)).findFirst().orElseThrow(IllegalArgumentException::new);
    }
    private static void fields(Document value, Set<String> expected) { require(value.keySet().equals(expected)); }
    private static void require(boolean condition) { if (!condition) { throw new IllegalArgumentException("invalid alert configuration encoding"); } }
    static int size(Document document) {
        try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, document, EncoderContext.builder().build()); return output.getSize();
        }
    }
}
