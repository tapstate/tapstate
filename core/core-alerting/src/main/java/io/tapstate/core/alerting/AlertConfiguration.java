package io.tapstate.core.alerting;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Bounded authoring definitions, separate from per-resource evaluation and delivery snapshots. */
public final class AlertConfiguration {
    public static final int FORMAT_VERSION = 1;
    private AlertConfiguration() { }

    public enum Kind { RULE, RECEIVER, SUBSCRIPTION, SILENCE }
    public enum GroupBy { RESOURCE, TYPE, RULE }

    public record Identity(Kind kind, AlarmKey.Scope scope, String id, String incarnation) {
        public Identity {
            Objects.requireNonNull(kind, "kind"); Objects.requireNonNull(scope, "scope");
            id = AlertValidation.text(id, "configurationId", 256);
            AlertValidation.require(incarnation != null, "configurationIncarnation", "required");
            UUID parsed;
            try { parsed = UUID.fromString(incarnation); }
            catch (IllegalArgumentException ex) {
                AlertValidation.require(false, "configurationIncarnation", "invalid-uuid");
                throw new IllegalStateException("unreachable", ex);
            }
            AlertValidation.require(parsed.version() == 4 && parsed.variant() == 2
                    && parsed.toString().equals(incarnation), "configurationIncarnation", "invalid-uuid");
        }
        public static Identity create(Kind kind, AlarmKey.Scope scope, String id) {
            return new Identity(kind, scope, id, UUID.randomUUID().toString());
        }
    }

    /** Host supplied identity and audit facts; revisions are internal concurrency data. */
    public record Header(Identity identity, long revision, AlertChannelPolicy.Deployment deployment,
            Instant createdAt, String createdBy, Instant updatedAt, String updatedBy, Instant retiredAt) {
        public Header {
            Objects.requireNonNull(identity, "identity"); Objects.requireNonNull(deployment, "deployment");
            AlertValidation.require(deployment != AlertChannelPolicy.Deployment.UNRESOLVED, "deployment", "unresolved");
            AlertValidation.require(revision > 0, "configurationRevision", "must-be-positive");
            Objects.requireNonNull(createdAt, "createdAt"); Objects.requireNonNull(updatedAt, "updatedAt");
            createdBy = AlertValidation.text(createdBy, "createdBy", 256);
            updatedBy = AlertValidation.text(updatedBy, "updatedBy", 256);
            AlertValidation.require(!updatedAt.isBefore(createdAt), "updatedAt", "before-creation");
            AlertValidation.require(retiredAt == null || retiredAt.equals(updatedAt), "retiredAt", "must-be-final-update");
            AlertValidation.require(revision != 1 || createdAt.equals(updatedAt) && createdBy.equals(updatedBy)
                    && retiredAt == null, "configurationRevision", "invalid-first-revision");
        }
        public static Header create(Identity identity, AlertChannelPolicy.Deployment deployment,
                Instant at, String actor) {
            return new Header(identity, 1, deployment, at, actor, at, actor, null);
        }
        public Header next(Instant at, String actor) { return advance(at, actor, false); }
        public Header retire(Instant at, String actor) { return advance(at, actor, true); }
        private Header advance(Instant at, String actor, boolean retire) {
            AlertValidation.require(retiredAt == null, "configuration", "already-retired");
            AlertValidation.require(revision < Long.MAX_VALUE, "configurationRevision", "exhausted");
            AlertValidation.require(at != null && !at.isBefore(updatedAt), "updatedAt", "before-previous-update");
            return new Header(identity, revision + 1, deployment, createdAt, createdBy, at, actor, retire ? at : null);
        }
        public boolean active() { return retiredAt == null; }
        public boolean succeeds(Header previous) {
            return previous != null && previous.active() && previous.revision < Long.MAX_VALUE
                    && revision == previous.revision + 1 && identity.equals(previous.identity)
                    && deployment == previous.deployment && createdAt.equals(previous.createdAt)
                    && createdBy.equals(previous.createdBy) && !updatedAt.isBefore(previous.updatedAt);
        }
    }

    public sealed interface Definition permits Rule, Subscription, Silence {
        Header header();
    }

    /** Explicit resource incarnations or all current resources of the named kinds; no label authority. */
    public record Resources(Set<String> kinds, boolean all, Set<AlarmKey.Resource> selected) {
        public Resources {
            kinds = checkedSet(kinds, "resourceKinds", 8, false);
            kinds.forEach(value -> typeId(value, "resourceKind"));
            selected = checkedSet(selected, "selectedResources", 100, true);
            AlertValidation.require(all ? selected.isEmpty() : !selected.isEmpty(), "selectedResources", "explicit-scope-required");
            for (AlarmKey.Resource resource : selected) {
                AlertValidation.require(kinds.contains(resource.kind()), "selectedResources", "wrong-kind");
            }
        }
        public boolean matches(AlarmKey.Resource resource) {
            return kinds.contains(resource.kind()) && (all || selected.contains(resource));
        }
    }

    public record Match(Resources resources, Set<String> typeIds, Set<AlarmSeverity> severities) {
        public Match {
            Objects.requireNonNull(resources, "resources");
            typeIds = checkedSet(typeIds, "matchTypes", 16, true);
            typeIds.forEach(value -> typeId(value, "matchType"));
            severities = checkedSet(severities, "matchSeverities", AlarmSeverity.values().length, true);
        }
        public boolean matches(AlarmKey key, AlarmSeverity severity) {
            return resources.matches(key.resource()) && (typeIds.isEmpty() || typeIds.contains(key.typeId()))
                    && (severities.isEmpty() || severities.contains(severity));
        }
    }

    public record Rule(Header header, String name, String typeId, int typeVersion, Resources resources,
            Map<String, BigDecimal> parameters, AlarmSeverity severityOverride, boolean enabled,
            MetricAlertEvaluator.Rule metric) implements Definition {
        public Rule {
            requireKind(header, Kind.RULE);
            name = AlertValidation.text(name, "ruleName", 128);
            typeId = AlertConfiguration.typeId(typeId, "ruleType");
            AlertValidation.require(typeVersion > 0, "typeVersion", "must-be-positive");
            Objects.requireNonNull(resources, "resources");
            AlertValidation.require(parameters != null && parameters.size() <= 16, "parameters", "too-many-or-missing");
            parameters.forEach((key, value) -> {
                AlertConfiguration.typeId(key, "parameterName");
                AlertValidation.require(value != null && value.precision() <= 64 && Math.abs((long) value.scale()) <= 64,
                        "parameterValue", "decimal-too-large");
            });
            parameters = Map.copyOf(parameters);
            AlertValidation.require(severityOverride != AlarmSeverity.INDETERMINATE, "severityOverride", "not-configurable");
            if (metric != null) {
                for (BigDecimal threshold : List.of(metric.trigger(), metric.recover())) {
                    AlertValidation.require(threshold.precision() <= 64 && Math.abs((long) threshold.scale()) <= 64,
                            "metricThreshold", "decimal-too-large");
                }
            }
        }
        /** Used for authoring only; decoding does not turn an unknown stored type into a default. */
        public Rule validated(AlarmTypeCatalog catalog) {
            AlarmTypeCatalog.Type type = requireType(catalog);
            AlertValidation.require(type.resourceKinds().containsAll(resources.kinds()), "resourceKinds", "unsupported-by-type");
            AlertValidation.require((metric != null) == type.evaluatorId().equals("threshold"), "metric", "evaluator-contract-mismatch");
            return new Rule(header, name, typeId, typeVersion, resources,
                    type.validatedParameters(parameters), severityOverride, enabled, metric);
        }
        public AlarmSeverity effectiveSeverity(AlarmTypeCatalog catalog) {
            AlarmTypeCatalog.Type type = requireType(catalog);
            return severityOverride == null ? type.defaultSeverity() : severityOverride;
        }
        public boolean matches(AlarmKey key) {
            return header.active() && enabled && header.identity().scope().equals(key.scope())
                    && typeId.equals(key.typeId()) && resources.matches(key.resource());
        }
        private AlarmTypeCatalog.Type requireType(AlarmTypeCatalog catalog) {
            Objects.requireNonNull(catalog, "catalog");
            AlarmTypeCatalog.Type type = catalog.types().stream().filter(value -> value.id().equals(typeId)).findFirst().orElse(null);
            AlertValidation.require(type != null && type.version() == typeVersion, "ruleType", "unsupported-type-or-version");
            return type;
        }
    }

    /** A channel owns one message setting set; each receiver keeps its original creation identity. */
    public record RouteChannel(String channelType, List<Identity> receivers, MessageSettings messages) {
        public RouteChannel {
            channelType = typeId(channelType, "channelType");
            AlertValidation.require(receivers != null && !receivers.isEmpty() && receivers.size() <= 32
                    && receivers.stream().allMatch(value -> value != null && value.kind() == Kind.RECEIVER),
                    "receivers", "invalid-references");
            receivers = List.copyOf(receivers);
            AlertValidation.require(receivers.stream().distinct().count() == receivers.size(), "receivers", "duplicate-reference");
            AlertValidation.require(receivers.stream().map(Identity::id).distinct().count() == receivers.size(),
                    "receivers", "duplicate-id");
            Objects.requireNonNull(messages, "messages");
        }
    }

    public record Subscription(Header header, String name, Match match, boolean enabled,
            List<RouteChannel> channels, Set<GroupBy> groupBy, AlertGroupingClock.Policy grouping,
            List<AlertInhibitionPolicy.Edge> inhibition) implements Definition {
        public Subscription {
            requireKind(header, Kind.SUBSCRIPTION);
            name = AlertValidation.text(name, "subscriptionName", 128);
            Objects.requireNonNull(match, "match"); Objects.requireNonNull(grouping, "grouping");
            AlertValidation.require(channels != null && !channels.isEmpty() && channels.size() <= 16
                    && channels.stream().allMatch(Objects::nonNull), "channels", "invalid-channels");
            channels = List.copyOf(channels);
            AlertValidation.require(channels.stream().map(RouteChannel::channelType).distinct().count() == channels.size(),
                    "channels", "duplicate-channel");
            for (RouteChannel channel : channels) {
                AlertValidation.require(channel.receivers().stream().allMatch(value -> value.scope().equals(header.identity().scope())),
                        "receivers", "cross-scope-reference");
            }
            groupBy = checkedSet(groupBy, "groupBy", GroupBy.values().length, true);
            new AlertInhibitionPolicy(header.identity().scope(), inhibition);
            inhibition = List.copyOf(inhibition);
        }
        public boolean matches(AlarmKey key, AlarmSeverity severity) {
            return header.active() && enabled && header.identity().scope().equals(key.scope()) && match.matches(key, severity);
        }
    }

    public record Silence(Header header, String reason, AlertSilenceWindow window) implements Definition {
        public Silence {
            requireKind(header, Kind.SILENCE);
            reason = AlertValidation.text(reason, "silenceReason", 1024);
            Objects.requireNonNull(window, "window");
            AlertValidation.require(header.identity().scope().equals(window.scope()), "silenceScope", "cross-scope-window");
        }
        public boolean matches(AlarmKey key, Instant now) { return header.active() && window.matches(key, now); }
    }

    private static void requireKind(Header header, Kind kind) {
        Objects.requireNonNull(header, "header");
        AlertValidation.require(header.identity().kind() == kind, "configurationKind", "wrong-kind");
    }
    private static String typeId(String value, String field) {
        AlertValidation.text(value, field, 128);
        AlertValidation.require(value.matches("[a-z][a-z0-9]*([.-][a-z0-9]+)*"), field, "invalid-id");
        return value;
    }
    private static <T> Set<T> checkedSet(Set<T> values, String field, int maximum, boolean emptyAllowed) {
        AlertValidation.require(values != null && values.size() <= maximum && (emptyAllowed || !values.isEmpty())
                && values.stream().allMatch(Objects::nonNull), field, "invalid-members-or-size");
        return Set.copyOf(values);
    }
}
