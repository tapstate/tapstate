package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.MetricType;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.alerting.AlertConfiguration.*;
import static org.assertj.core.api.Assertions.*;

class AlertConfigurationTest {
    private static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("organization", "cluster");
    private static final Instant AT = Instant.parse("2026-10-27T00:00:00.123456789Z");
    private static final AlarmKey.Resource SOURCE = new AlarmKey.Resource("source", "orders", "source-lifetime-1");

    @Test void editingAndRetirementKeepCreationIdentityAndCannotReopenAnOldObject() {
        Header first = header(Kind.RULE, "rule");
        Header edit = first.next(AT.plusNanos(1), "second-actor");
        Header retired = edit.retire(AT.plusNanos(2), "third-actor");
        assertThat(edit.succeeds(first)).isTrue();
        assertThat(retired.succeeds(edit)).isTrue();
        assertThat(edit.identity()).isEqualTo(first.identity());
        assertThat(edit.createdBy()).isEqualTo("creator");
        assertThat(edit.updatedAt()).isEqualTo(AT.plusNanos(1));
        assertThatThrownBy(() -> retired.next(AT.plusSeconds(1), "creator")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> edit.next(AT, "creator")).isInstanceOf(TapstateException.class);
        Header recreated = header(Kind.RULE, "rule");
        assertThat(recreated.identity().id()).isEqualTo(first.identity().id());
        assertThat(recreated.identity().incarnation()).isNotEqualTo(first.identity().incarnation());
        assertThat(recreated.succeeds(first)).isFalse();
    }

    @Test void onlyTheTrustedResolvedDeploymentCanCreateTheInternalHeader() {
        Identity identity = Identity.create(Kind.RULE, SCOPE, "rule");
        assertThatThrownBy(() -> Header.create(identity, AlertChannelPolicy.Deployment.UNRESOLVED, AT, "creator"))
                .isInstanceOf(TapstateException.class);
        Header initial = header(Kind.RULE, "rule");
        Header exhausted = new Header(initial.identity(), Long.MAX_VALUE, initial.deployment(), AT, "creator", AT, "creator", null);
        assertThatThrownBy(() -> exhausted.next(AT, "creator")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Identity(Kind.RULE, SCOPE, "rule", "00000000-0000-0000-0000-000000000000"))
                .isInstanceOf(TapstateException.class);
    }

    @Test void explicitResourcesNeverFollowSameIdRecreationOrAnotherScope() {
        Rule rule = rule(header(Kind.RULE, "rule"), Map.of(), null).validated(BuiltinAlarmTypes.catalog());
        assertThat(rule.matches(key(SCOPE, SOURCE))).isTrue();
        assertThat(rule.matches(key(SCOPE, new AlarmKey.Resource("source", "orders", "source-lifetime-2")))).isFalse();
        assertThat(rule.matches(key(new AlarmKey.Scope("another-org", "cluster"), SOURCE))).isFalse();
        assertThat(rule.parameters()).containsEntry("trigger-for", BigDecimal.ZERO).containsEntry("resolve-for", BigDecimal.ZERO);
        assertThat(rule.effectiveSeverity(BuiltinAlarmTypes.catalog())).isEqualTo(AlarmSeverity.MAJOR);
        Rule override = rule(rule.header(), Map.of(), AlarmSeverity.MINOR).validated(BuiltinAlarmTypes.catalog());
        assertThat(override.header().identity()).isEqualTo(rule.header().identity());
        assertThat(override.effectiveSeverity(BuiltinAlarmTypes.catalog())).isEqualTo(AlarmSeverity.MINOR);
    }

    @Test void authoringRejectsUnknownFieldsVersionsKindsAndManualIndeterminateAsAWhole() {
        Header header = header(Kind.RULE, "rule");
        assertThatThrownBy(() -> rule(header, Map.of("unknown", BigDecimal.ONE), null).validated(BuiltinAlarmTypes.catalog()))
                .isInstanceOf(TapstateException.class);
        Rule unknownVersion = new Rule(header, "Rule", "source.connection-failed", 2, sources(), Map.of(), null, true, null);
        assertThat(unknownVersion.typeVersion()).isEqualTo(2);
        assertThatThrownBy(() -> unknownVersion.validated(BuiltinAlarmTypes.catalog())).isInstanceOf(TapstateException.class);
        Rule wrongKind = new Rule(header, "Rule", "source.connection-failed", 1,
                new Resources(Set.of("pipeline"), true, Set.of()), Map.of(), null, true, null);
        assertThatThrownBy(() -> wrongKind.validated(BuiltinAlarmTypes.catalog())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> rule(header, Map.of(), AlarmSeverity.INDETERMINATE)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> rule(header, Map.of("trigger-for", new BigDecimal("1E+1000000")), null))
                .isInstanceOf(TapstateException.class);
    }

    @Test void metricConditionsBelongOnlyToAnExplicitThresholdType() {
        var metric = new MetricAlertEvaluator.Rule("metric", MetricType.GAUGE, "s", Map.of(),
                MetricAlertEvaluator.Direction.ABOVE, BigDecimal.TEN, BigDecimal.ONE);
        Rule invalid = new Rule(header(Kind.RULE, "rule"), "Rule", "source.connection-failed", 1,
                sources(), Map.of(), null, true, metric);
        assertThatThrownBy(() -> invalid.validated(BuiltinAlarmTypes.catalog())).isInstanceOf(TapstateException.class);
        var type = new AlarmTypeCatalog.Type("test.threshold", 1, AlarmTypeCatalog.Kind.STATEFUL, Set.of("source"),
                "metrics", "threshold", List.of(), AlarmSeverity.WARNING, true, "test.condition", "test.recovery", "test.guide");
        Rule valid = new Rule(invalid.header(), "Metric rule", type.id(), 1, sources(), Map.of(), null, true, metric);
        assertThat(valid.validated(new AlarmTypeCatalog(List.of(type))).metric()).isEqualTo(metric);
    }

    @Test void routesKeepPerChannelTextAndOriginalReceiverIdentityAndCannotCrossScope() {
        Identity receiver = Identity.create(Kind.RECEIVER, SCOPE, "receiver");
        var messages = new MessageSettings("[team] ", "", "Runbook", "");
        RouteChannel email = new RouteChannel("email", List.of(receiver), messages);
        Subscription route = route(header(Kind.SUBSCRIPTION, "route"), List.of(email), List.of());
        assertThat(route.matches(key(SCOPE, SOURCE), AlarmSeverity.MAJOR)).isTrue();
        assertThat(route.channels().getFirst().messages()).isEqualTo(messages);
        assertThat(route.channels().getFirst().receivers().getFirst()).isEqualTo(receiver);
        Identity recreated = Identity.create(Kind.RECEIVER, SCOPE, "receiver");
        assertThat(route.channels().getFirst().receivers()).doesNotContain(recreated);
        assertThatThrownBy(() -> route(route.header(), List.of(email, email), List.of())).isInstanceOf(TapstateException.class);
        Identity foreign = Identity.create(Kind.RECEIVER, new AlarmKey.Scope("foreign", "cluster"), "receiver");
        assertThatThrownBy(() -> route(route.header(), List.of(new RouteChannel("email", List.of(foreign), messages)), List.of()))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new RouteChannel("email", List.of(receiver, recreated), messages)).isInstanceOf(TapstateException.class);
    }

    @Test void stoppedRoutesAndRetiredSilencesOnlyChangeNotificationEligibility() {
        RouteChannel email = new RouteChannel("email", List.of(Identity.create(Kind.RECEIVER, SCOPE, "receiver")), MessageSettings.empty());
        Subscription route = route(header(Kind.SUBSCRIPTION, "route"), List.of(email), List.of());
        Subscription stopped = new Subscription(route.header().next(AT, "creator"), route.name(), route.match(), false,
                route.channels(), route.groupBy(), route.grouping(), route.inhibition());
        assertThat(stopped.matches(key(SCOPE, SOURCE), AlarmSeverity.MAJOR)).isFalse();
        AlertSilenceWindow window = new AlertSilenceWindow(SCOPE, false, Set.of(SOURCE), Set.of(), AT,
                Duration.ofMinutes(10), null, null);
        Silence silence = new Silence(header(Kind.SILENCE, "silence"), "Maintenance", window);
        assertThat(silence.matches(key(SCOPE, SOURCE), AT.plusSeconds(1))).isTrue();
        Silence ended = new Silence(silence.header().retire(AT.plusSeconds(2), "creator"), silence.reason(), window);
        assertThat(ended.matches(key(SCOPE, SOURCE), AT.plusSeconds(3))).isFalse();
        assertThat(silence.matches(key(SCOPE, SOURCE), AT.plusSeconds(600))).isFalse();
        assertThat(silence.header().revision()).isEqualTo(1);
    }

    @Test void inhibitionCyclesAndMalformedExplicitSelectorsAreRejected() {
        Header header = header(Kind.SUBSCRIPTION, "route");
        RouteChannel email = new RouteChannel("email", List.of(Identity.create(Kind.RECEIVER, SCOPE, "receiver")), MessageSettings.empty());
        assertThatThrownBy(() -> route(header, List.of(email), List.of(new AlertInhibitionPolicy.Edge("test.a", "test.b"),
                new AlertInhibitionPolicy.Edge("test.b", "test.a")))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Resources(Set.of("source"), false, Set.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Resources(Set.of("pipeline"), false, Set.of(SOURCE))).isInstanceOf(TapstateException.class);
    }

    @Test void routeTextEditsKeepSemanticBindingButChangeAwayAndBackNeverRestoresIt() {
        Identity a = Identity.create(Kind.RECEIVER, SCOPE, "a"), b = Identity.create(Kind.RECEIVER, SCOPE, "b");
        Subscription original = route(header(Kind.SUBSCRIPTION, "route"),
                List.of(new RouteChannel("email", List.of(a), MessageSettings.empty())), List.of());
        Subscription text = original.edit("Renamed", original.match(), true,
                List.of(new RouteChannel("email", List.of(a), new MessageSettings("[team] ", "", "", ""))),
                original.groupBy(), original.grouping(), original.inhibition(), AT.plusSeconds(1), "creator");
        assertThat(text.header().revision()).isEqualTo(2);
        assertThat(text.bindingRevision()).isEqualTo(1);
        assertThat(text.succeeds(original)).isTrue();
        assertThat(text.hasBinding(original.header().identity(), original.bindingRevision())).isTrue();
        Subscription changed = text.edit(text.name(), text.match(), true,
                List.of(new RouteChannel("email", List.of(b), MessageSettings.empty())), text.groupBy(), text.grouping(), text.inhibition(),
                AT.plusSeconds(2), "creator");
        Subscription returned = changed.edit(text.name(), text.match(), true, text.channels(), text.groupBy(), text.grouping(), text.inhibition(),
                AT.plusSeconds(3), "creator");
        assertThat(returned.channels()).isEqualTo(text.channels());
        assertThat(returned.bindingRevision()).isEqualTo(3);
        assertThat(returned.hasBinding(original.header().identity(), original.bindingRevision())).isFalse();
        assertThat(returned.retire(AT.plusSeconds(4), "creator").hasBinding(returned.header().identity(), returned.bindingRevision())).isFalse();
    }

    @Test void routeSemanticRevisionIgnoresOrderingButTracksEnableMatchGroupingAndInhibition() {
        Identity a = Identity.create(Kind.RECEIVER, SCOPE, "a"), b = Identity.create(Kind.RECEIVER, SCOPE, "b");
        Subscription original = route(header(Kind.SUBSCRIPTION, "route"),
                List.of(new RouteChannel("email", List.of(a, b), MessageSettings.empty())), List.of());
        Subscription reordered = original.edit(original.name(), original.match(), true,
                List.of(new RouteChannel("email", List.of(b, a), MessageSettings.empty())), original.groupBy(), original.grouping(), original.inhibition(), AT, "creator");
        assertThat(reordered.bindingRevision()).isEqualTo(1);
        List<Subscription> changes = List.of(
                original.edit(original.name(), original.match(), false, original.channels(), original.groupBy(), original.grouping(), original.inhibition(), AT, "creator"),
                original.edit(original.name(), new Match(sources(), Set.of("source.connection-failed"), Set.of()), true,
                        original.channels(), original.groupBy(), original.grouping(), original.inhibition(), AT, "creator"),
                original.edit(original.name(), original.match(), true, original.channels(), Set.of(GroupBy.RESOURCE), original.grouping(), original.inhibition(), AT, "creator"),
                original.edit(original.name(), original.match(), true, original.channels(), original.groupBy(),
                        new AlertGroupingClock.Policy(Duration.ZERO, Duration.ofSeconds(1), Duration.ofSeconds(2)), original.inhibition(), AT, "creator"),
                original.edit(original.name(), original.match(), true, original.channels(), original.groupBy(), original.grouping(),
                        List.of(new AlertInhibitionPolicy.Edge("test.a", "test.b")), AT, "creator"));
        assertThat(changes).allSatisfy(value -> { assertThat(value.bindingRevision()).isEqualTo(2); assertThat(value.succeeds(original)).isTrue(); });
        Subscription invalid = new Subscription(changes.getFirst().header(), original.name(), original.match(), false,
                original.channels(), original.groupBy(), original.grouping(), original.inhibition(), 1);
        assertThat(invalid.succeeds(original)).isFalse();
    }

    @Test void legacyRouteHasNoSendBindingAndOnlyAnExplicitEditEnrollsItAtTheNewRecordRevision() {
        Subscription original = route(header(Kind.SUBSCRIPTION, "route"), List.of(new RouteChannel("email",
                List.of(Identity.create(Kind.RECEIVER, SCOPE, "a")), MessageSettings.empty())), List.of());
        Subscription legacy = new Subscription(original.header(), original.name(), original.match(), original.enabled(), original.channels(),
                original.groupBy(), original.grouping(), original.inhibition(), 0);
        assertThat(legacy.hasBinding(legacy.header().identity(), 0)).isFalse();
        Subscription enrolled = legacy.edit(legacy.name(), legacy.match(), legacy.enabled(), legacy.channels(),
                legacy.groupBy(), legacy.grouping(), legacy.inhibition(), AT, "creator");
        assertThat(enrolled.header().revision()).isEqualTo(2);
        assertThat(enrolled.bindingRevision()).isEqualTo(2);
        assertThat(enrolled.succeeds(legacy)).isTrue();
    }

    private static Header header(Kind kind, String id) {
        return Header.create(Identity.create(kind, SCOPE, id), AlertChannelPolicy.Deployment.ON_PREM, AT, "creator");
    }
    private static Resources sources() { return new Resources(Set.of("source"), false, Set.of(SOURCE)); }
    private static Rule rule(Header header, Map<String, BigDecimal> parameters, AlarmSeverity severity) {
        return new Rule(header, "Source rule", "source.connection-failed", 1, sources(), parameters, severity, true, null);
    }
    private static AlarmKey key(AlarmKey.Scope scope, AlarmKey.Resource resource) {
        return new AlarmKey(scope, resource, "source.connection-failed", "rule");
    }
    private static Subscription route(Header header, List<RouteChannel> channels, List<AlertInhibitionPolicy.Edge> inhibition) {
        return new Subscription(header, "Route", new Match(sources(), Set.of(), Set.of()), true, channels, Set.of(GroupBy.TYPE),
                new AlertGroupingClock.Policy(Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(120)), inhibition);
    }
}
