package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.AlertSignal.Condition.*;

class AlertNoisePolicyTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final AlarmKey KEY = AlertLifecycleTest.key("org-a", "resource-1", "rule-a");
    @Test void conditionGroupsUseThreeValuedLogicAndRejectUnboundedOrEmptyGroups() {
        assertThat(AlertConditionGroup.evaluate(AlertConditionGroup.Operator.ALL, List.of(BAD, UNKNOWN))).isEqualTo(UNKNOWN);
        assertThat(AlertConditionGroup.evaluate(AlertConditionGroup.Operator.ALL, List.of(GOOD, UNKNOWN))).isEqualTo(GOOD);
        assertThat(AlertConditionGroup.evaluate(AlertConditionGroup.Operator.ANY, List.of(BAD, UNKNOWN))).isEqualTo(BAD);
        assertThat(AlertConditionGroup.evaluate(AlertConditionGroup.Operator.ANY, List.of(GOOD, UNKNOWN))).isEqualTo(UNKNOWN);
        assertThatThrownBy(() -> AlertConditionGroup.evaluate(AlertConditionGroup.Operator.ALL, List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> AlertConditionGroup.evaluate(AlertConditionGroup.Operator.ALL, java.util.Collections.nCopies(9, BAD))).isInstanceOf(TapstateException.class);
    }
    @Test void silenceUsesHalfOpenWindowsAndStableScopeResourceIncarnations() {
        var window = new AlertSilenceWindow(KEY.scope(), false, Set.of(KEY.resource()), Set.of(), T,
                Duration.ofSeconds(10), Duration.ofSeconds(60), T.plusSeconds(125));
        for (int second : new int[]{0, 9, 60, 69, 120, 124}) { assertThat(window.matches(KEY, T.plusSeconds(second))).isTrue(); }
        for (int second : new int[]{-1, 10, 59, 70, 125, 180}) { assertThat(window.matches(KEY, T.plusSeconds(second))).isFalse(); }
        assertThat(window.matches(AlertLifecycleTest.key("org-b", "resource-1", "rule-a"), T)).isFalse();
        assertThat(window.matches(AlertLifecycleTest.key("org-a", "resource-2", "rule-a"), T)).isFalse();
        var once = new AlertSilenceWindow(KEY.scope(), true, Set.of(), Set.of(KEY.typeId()), T, Duration.ofSeconds(10), null, null);
        assertThat(once.matches(KEY, T.plusSeconds(10))).isFalse();
        assertThatThrownBy(() -> new AlertSilenceWindow(KEY.scope(), false, Set.of(), Set.of(), T, Duration.ofSeconds(1), null, null)).isInstanceOf(TapstateException.class);
    }
    @Test void inhibitionDoesNotClearAlertsOrTrustAnotherScopeAnUnknownRootOrACycle() {
        AlarmKey rootKey = new AlarmKey(KEY.scope(), new AlarmKey.Resource("cluster-node", "node", "boot-1"), "cluster.node-unavailable", "node-rule");
        AlertSnapshot root = firing(rootKey);
        AlertSnapshot dependent = firing(KEY);
        var policy = new AlertInhibitionPolicy(KEY.scope(), List.of(new AlertInhibitionPolicy.Edge(rootKey.typeId(), KEY.typeId())));
        assertThat(policy.suppresses(root, dependent, true)).isTrue();
        assertThat(policy.suppresses(root, dependent, false)).isFalse();
        assertThat(dependent.status()).isEqualTo(AlertSnapshot.Status.FIRING);
        AlertSnapshot staleRoot = AlertLifecycle.tick(root, T.plusSeconds(20)).snapshot();
        assertThat(staleRoot.status()).isEqualTo(AlertSnapshot.Status.FIRING);
        assertThat(policy.suppresses(staleRoot, dependent, true)).isFalse();
        assertThat(policy.suppresses(root, firing(AlertLifecycleTest.key("other-org", "resource-1", "rule-a")), true)).isFalse();
        assertThatThrownBy(() -> new AlertInhibitionPolicy(KEY.scope(), List.of(new AlertInhibitionPolicy.Edge("type.a", "type.b"),
                new AlertInhibitionPolicy.Edge("type.b", "type.c"), new AlertInhibitionPolicy.Edge("type.c", "type.a")))).isInstanceOf(TapstateException.class);
    }
    @Test void unrelatedAlertsOfTheSameTypeAreAPlainNonMatch() {
        var policy = new AlertInhibitionPolicy(KEY.scope(), List.of(new AlertInhibitionPolicy.Edge("cluster.node-unavailable", KEY.typeId())));
        var other = new AlarmKey(KEY.scope(), new AlarmKey.Resource("pipeline", "another-pipeline", "resource-2"), KEY.typeId(), "rule-a");
        assertThatCode(() -> assertThat(policy.suppresses(firing(KEY), firing(other), true)).isFalse()).doesNotThrowAnyException();
    }
    @Test void notificationSubjectsIncludeTheWholeResourceAndSeparateGroupNamespaces() {
        var a = DeliveryKey.NotificationRound.forEpisode(KEY, 1, "route", "route-1", DeliveryKey.Kind.FAULT, 1);
        var b = DeliveryKey.NotificationRound.forEpisode(AlertLifecycleTest.key("org-a", "resource-2", "rule-a"), 1, "route", "route-1", DeliveryKey.Kind.FAULT, 1);
        assertThat(a).isNotEqualTo(b);
        assertThat(a).isEqualTo(DeliveryKey.NotificationRound.forEpisode(KEY, 1, "route", "route-1", DeliveryKey.Kind.FAULT, 1));
        assertThat(a.subjectId()).isEqualTo("episode-307c12ad2c47e68d2886709ed4014e156ec4b854691781f990c6222844656a69");
        var group = DeliveryKey.NotificationRound.forGroup(KEY.scope(), a.subjectId(), "group-1", 1, "route", "route-1", DeliveryKey.Kind.FAULT, 1);
        assertThat(group.subjectId()).startsWith("group-").isNotEqualTo(a.subjectId());
    }
    static AlertSnapshot firing(AlarmKey key) {
        AlertRule rule = new AlertRule(key, 1, 1, "producer", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(15), Duration.ofSeconds(10));
        return AlertLifecycle.evaluate(AlertSnapshot.initial(rule), new AlertSignal(key, 1, 1, "producer", 1, T, BAD, AlarmSeverity.MAJOR), T).snapshot();
    }
}
