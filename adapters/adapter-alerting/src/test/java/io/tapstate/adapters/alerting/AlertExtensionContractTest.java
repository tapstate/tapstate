package io.tapstate.adapters.alerting;

import static org.assertj.core.api.Assertions.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.alerting.NotificationChannel;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Exercises test-only extensions through the ordinary pure contracts, without implying host integration or durable acceptance. */
class AlertExtensionContractTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org", "cluster");
    static AlarmTypeCatalog.Type type(String id, AlarmTypeCatalog.Kind kind, String provider, String evaluator) {
        return new AlarmTypeCatalog.Type(id, 1, kind, Set.of("pipeline"), provider, evaluator, List.of(),
                AlarmSeverity.MINOR, false, id + ".condition", kind == AlarmTypeCatalog.Kind.STATEFUL ? id + ".recovery" : null, id + ".guidance");
    }
    static AlarmKey key(AlarmTypeCatalog.Type type) {
        return new AlarmKey(SCOPE, new AlarmKey.Resource("pipeline", "orders", "resource-1"), type.id(), "rule");
    }
    @Test void newMetricTypeUsesOriginalFactsAndTheUnmodifiedLifecycleAndChannelContracts() throws Exception {
        var type = type("test.lag-high", AlarmTypeCatalog.Kind.STATEFUL, "raw-metric", "threshold");
        var catalog = new AlarmTypeCatalog(List.of(type));
        assertThat(catalog.availability(type.id(), 1, 1, Set.of("raw-metric"), Set.of("threshold"))).isEqualTo(AlarmTypeCatalog.Availability.AVAILABLE);
        var metricRule = new MetricAlertEvaluator.Rule("tapstate.pipeline.lag", MetricType.GAUGE, "s", Map.of(MetricAttributes.TABLE_ID, "orders"),
                MetricAlertEvaluator.Direction.ABOVE, BigDecimal.TEN, BigDecimal.valueOf(5));
        var lifecycleRule = new AlertRule(key(type), 1, 1, "metric-producer-1", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(30), Duration.ofSeconds(10));
        var state = AlertSnapshot.initial(lifecycleRule);
        try (var channel = new RecordingChannel()) {
            for (int i = 0; i < 2; i++) {
                Instant now = T.plusSeconds(i);
                MetricFact fact = MetricFact.single(metricRule.name(), MetricType.GAUGE, "s", MetricPoint.reading(Map.of(MetricAttributes.TABLE_ID, "orders"), now, i == 0 ? 11 : 4));
                var measurement = MetricAlertEvaluator.evaluate(metricRule, fact, null, state.status() == AlertSnapshot.Status.FIRING,
                        now, Duration.ofSeconds(30), Duration.ofSeconds(10));
                var signal = new AlertSignal(key(type), 1, 1, "metric-producer-1", i + 1, now, measurement.condition(), AlarmSeverity.MINOR);
                state = AlertLifecycle.evaluate(state, signal, now).snapshot();
                sendState(channel, state, i == 1, now);
            }
            assertThat(state.status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
            assertThat(state.episode().number()).isEqualTo(1);
            assertThat(channel.received).hasSize(2);
            assertThat(body(channel.received.getFirst())).contains("test.lag-high", "\"state\":\"firing\"");
            assertThat(body(channel.received.getLast())).contains("\"state\":\"resolved\"");
        }
    }
    @Test void aNewTypedProviderTypeNeedsNoSpecialLifecycleOrNotificationBranch() throws Exception {
        var type = type("test.replica-unavailable", AlarmTypeCatalog.Kind.STATEFUL, "test-replica-health", "condition");
        var catalog = new AlarmTypeCatalog(List.of(type));
        assertThat(catalog.availability(type.id(), 1, 1, Set.of(), Set.of("condition"))).isEqualTo(AlarmTypeCatalog.Availability.INPUT_UNAVAILABLE);
        var rule = new AlertRule(key(type), 1, 1, "replica-producer-1", Duration.ZERO, Duration.ZERO, Duration.ofSeconds(30), Duration.ofSeconds(10));
        var state = AlertSnapshot.initial(rule);
        try (var channel = new RecordingChannel()) {
            for (int i = 0; i < 2; i++) {
                Instant now = T.plusSeconds(i);
                var signal = new AlertSignal(key(type), 1, 1, "replica-producer-1", i + 1, now,
                        i == 0 ? AlertSignal.Condition.BAD : AlertSignal.Condition.GOOD, AlarmSeverity.MAJOR);
                state = AlertLifecycle.evaluate(state, signal, now).snapshot();
                sendState(channel, state, i == 1, now);
            }
            assertThat(state.status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
            assertThat(channel.received).hasSize(2);
            assertThat(body(channel.received.getFirst())).contains("test.replica-unavailable", "\"severity\":\"major\"");
            assertThat(channel.received.getFirst().destination().channelType()).isEqualTo("test.sink");
        }
    }
    @Test void aOneShotEventUsesAnEventRoundWithoutAnEpisodeOrRecoveryLifecycle() throws Exception {
        var type = type("test.maintenance-completed", AlarmTypeCatalog.Kind.EVENT, "test-maintenance-events", "event");
        var catalog = new AlarmTypeCatalog(List.of(type));
        assertThat(catalog.availability(type.id(), 1, 1, Set.of("test-maintenance-events"), Set.of("event"))).isEqualTo(AlarmTypeCatalog.Availability.AVAILABLE);
        var round = new DeliveryKey.NotificationRound(SCOPE, "producer-event-7", "producer-1", "route", "route-1", DeliveryKey.Kind.EVENT, 1);
        var item = new NotificationRenderer.Item(key(type), null, NotificationRenderer.State.EVENT, AlarmSeverity.WARNING,
                "Maintenance", "Test event", T, T, null);
        var facts = new NotificationRenderer.Facts(NotificationRenderer.Kind.EVENT, "event-delivery", 1, T,
                List.of(item), 1, null, List.of(), null, null);
        try (var channel = new RecordingChannel()) {
            send(channel, round, facts);
            assertThat(body(channel.received.getFirst())).contains("\"kind\":\"event\"", "\"episode\":null", "\"state\":\"event\"", "\"recoveredAt\":null")
                    .doesNotContain("readAt", "acknowledgement");
        }
    }
    private static void sendState(RecordingChannel channel, AlertSnapshot state, boolean recovery, Instant now) throws Exception {
        var round = DeliveryKey.NotificationRound.forEpisode(state.rule().key(), state.episode().number(), "route", "route-1",
                recovery ? DeliveryKey.Kind.RECOVERY : DeliveryKey.Kind.FAULT, recovery ? 2 : 1);
        var item = new NotificationRenderer.Item(state.rule().key(), state.episode().number(),
                recovery ? NotificationRenderer.State.RESOLVED : NotificationRenderer.State.FIRING, state.episode().severity(),
                "Orders", "Test condition", state.episode().firedAt(), now, state.episode().recoveredAt());
        send(channel, round, new NotificationRenderer.Facts(recovery ? NotificationRenderer.Kind.RECOVERY : NotificationRenderer.Kind.FAULT,
                recovery ? "recovery-delivery" : "fault-delivery", round.round(), now, List.of(item), 1, null, List.of(), null, null));
    }
    private static void send(RecordingChannel channel, DeliveryKey.NotificationRound round, NotificationRenderer.Facts facts) throws Exception {
        AlertChannelPolicy.requireDeliverable(AlertChannelPolicy.Deployment.ON_PREM, channel.capability());
        var rendered = new NotificationRenderer().render(facts, MessageSettings.empty());
        assertThat(rendered.successful()).isTrue();
        var delivery = new DeliveryKey(round, channel.typeId(), "receiver-1", "target-1");
        var request = NotificationChannel.Request.business(delivery, facts.deliveryId(), "attempt-1", "binding-1",
                T.plusSeconds(30), "application/json", rendered.snapshot().webhook());
        assertThat(channel.send(request).outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
    }
    private static String body(NotificationChannel.Request request) { return new String(request.payload(), StandardCharsets.UTF_8); }
    static final class RecordingChannel implements NotificationChannel {
        final List<Request> received = new ArrayList<>();
        final TargetBinding target = new TargetBinding(SCOPE, "receiver-1", "target-1", "binding-1");
        @Override public AlertChannelPolicy.Capability capability() {
            return new AlertChannelPolicy.Capability("test.sink", 1, AlertChannelPolicy.Family.OP_EXTERNAL);
        }
        @Override public Result send(Request request) {
            assertThat(target.matches(request)).isTrue(); assertThat(request.purpose()).isEqualTo(Purpose.BUSINESS);
            received.add(request);
            return new Result(DeliveryEvidence.Outcome.ACCEPTED, Action.STOP, null, Phase.LOCAL, 0, "test-sink");
        }
    }
}
