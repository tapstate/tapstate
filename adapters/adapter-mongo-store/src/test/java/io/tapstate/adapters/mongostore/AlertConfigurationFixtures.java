package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class AlertConfigurationFixtures {
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org-a", "cluster-a");
    static final Instant AT = Instant.parse("2026-10-27T00:00:00.123456789Z");
    static final String INCARNATION = "11111111-1111-4111-8111-111111111111";
    private AlertConfigurationFixtures() { }
    static Header header(Kind kind, String id) {
        return Header.create(new Identity(kind, SCOPE, id, INCARNATION), AlertChannelPolicy.Deployment.ON_PREM, AT, "actor-a");
    }
    static Resources resources() { return new Resources(Set.of("source"), true, Set.of()); }
    static Rule rule() {
        return new Rule(header(Kind.RULE, "rule-a"), "Source health", "source.connection-failed", 1,
                resources(), Map.of(), null, true, null).validated(BuiltinAlarmTypes.catalog());
    }
    static Subscription route() {
        Identity receiver = new Identity(Kind.RECEIVER, SCOPE, "receiver-a", INCARNATION);
        return new Subscription(header(Kind.SUBSCRIPTION, "route-a"), "Team route", new Match(resources(), Set.of(), Set.of()), true,
                List.of(new RouteChannel("email", List.of(receiver), new MessageSettings("[team] ", "", "Runbook", ""))),
                Set.of(GroupBy.TYPE), new AlertGroupingClock.Policy(Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(120)), List.of());
    }
    static Subscription legacyRoute() {
        Subscription value = route();
        return new Subscription(value.header(), value.name(), value.match(), value.enabled(), value.channels(),
                value.groupBy(), value.grouping(), value.inhibition(), 0);
    }
    static Silence silence() {
        return new Silence(header(Kind.SILENCE, "silence-a"), "Maintenance",
                new AlertSilenceWindow(SCOPE, true, Set.of(), Set.of("source.connection-failed"), AT, Duration.ofSeconds(600), null, null));
    }
    static Rule edit(Rule rule, String name) {
        return new Rule(rule.header().next(AT.plusSeconds(1), "actor-b"), name, rule.typeId(), rule.typeVersion(),
                rule.resources(), rule.parameters(), rule.severityOverride(), rule.enabled(), rule.metric());
    }
    static Rule retire(Rule rule) {
        return new Rule(rule.header().retire(AT.plusSeconds(2), "actor-b"), rule.name(), rule.typeId(), rule.typeVersion(),
                rule.resources(), rule.parameters(), rule.severityOverride(), rule.enabled(), rule.metric());
    }
}
