package io.tapstate.core.alerting;

import static io.tapstate.core.alerting.AlertChannelPolicy.Deployment.*;
import static io.tapstate.core.alerting.AlertRetentionPolicy.Setting.*;
import static org.assertj.core.api.Assertions.*;
import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AlertRetentionPolicyTest {
    private static final Instant C = Instant.parse("2026-10-21T00:00:00Z");
    private static Duration hours(long value) { return Duration.ofHours(value); }
    private static Map<AlertRetentionPolicy.Setting, Duration> copy(AlertRetentionPolicy policy) {
        return new EnumMap<>(policy.values());
    }
    @Test void defaultsAndAllowedFieldsAreExplicitForBothDeployments() {
        var op = AlertRetentionPolicy.defaults(ON_PREM); var cloud = AlertRetentionPolicy.defaults(CLOUD_MANAGED);
        assertThat(op.values()).containsExactlyInAnyOrderEntriesOf(Map.ofEntries(
                Map.entry(ACCEPTED_PAYLOAD, hours(24)), Map.entry(ACCEPTED_RESULT, hours(168)),
                Map.entry(FAILED_PAYLOAD, hours(72)), Map.entry(FAILED_RESULT, hours(336)),
                Map.entry(UNREAD_NOTIFICATION, hours(720)), Map.entry(READ_NOTIFICATION, hours(72)),
                Map.entry(TEST_PAYLOAD, hours(1)), Map.entry(TEST_ACCEPTED_RESULT, hours(24)),
                Map.entry(TEST_FAILED_RESULT, hours(72)), Map.entry(ENDED_EPISODE, hours(720)), Map.entry(TRANSITION, hours(336))));
        assertThat(cloud.values()).containsExactlyInAnyOrderEntriesOf(Map.of(
                ACCEPTED_RESULT, hours(24), FAILED_PAYLOAD, hours(24), FAILED_RESULT, hours(72),
                UNREAD_NOTIFICATION, hours(72), READ_NOTIFICATION, hours(12), ENDED_EPISODE, hours(168), TRANSITION, hours(72)));
        assertThat(cloud.acceptedPayload()).isEqualTo(hours(1));
        assertThatThrownBy(() -> AlertRetentionPolicy.defaults(UNRESOLVED)).isInstanceOf(TapstateException.class);
        var forbidden = copy(cloud); forbidden.put(ACCEPTED_PAYLOAD, hours(1));
        assertThatThrownBy(() -> AlertRetentionPolicy.configure(CLOUD_MANAGED, forbidden)).isInstanceOf(TapstateException.class);
        var missing = copy(op); missing.remove(TEST_PAYLOAD);
        assertThatThrownBy(() -> AlertRetentionPolicy.configure(ON_PREM, missing)).isInstanceOf(TapstateException.class);
    }
    @Test void everyEndpointIsInclusiveAndEveryFieldRejectsOutOfRangeOrFractionalMinutes() {
        for (var deployment : new AlertChannelPolicy.Deployment[]{ON_PREM, CLOUD_MANAGED}) {
            var ranges = AlertRetentionPolicy.ranges(deployment);
            for (boolean maximum : new boolean[]{false, true}) {
                var values = new EnumMap<AlertRetentionPolicy.Setting, Duration>(AlertRetentionPolicy.Setting.class);
                ranges.forEach((field, range) -> values.put(field, maximum ? range.maximum() : range.minimum()));
                assertThat(AlertRetentionPolicy.configure(deployment, values).values()).isEqualTo(values);
            }
            for (var field : ranges.keySet()) {
                var range = ranges.get(field);
                for (Duration invalid : new Duration[]{Duration.ZERO, Duration.ofMinutes(-1), range.minimum().minusMinutes(1),
                        range.maximum().plusMinutes(1), range.defaultValue().plusSeconds(1), range.defaultValue().plusNanos(1),
                        Duration.ofSeconds(Long.MAX_VALUE)}) {
                    var values = copy(AlertRetentionPolicy.defaults(deployment)); values.put(field, invalid);
                    assertThatThrownBy(() -> AlertRetentionPolicy.configure(deployment, values)).as("%s %s %s", deployment, field, invalid)
                            .isInstanceOf(TapstateException.class);
                }
            }
        }
    }
    @Test void crossFieldRelationsRejectTheWholeCandidateWithoutChangingTheFrozenPolicy() {
        var op = AlertRetentionPolicy.defaults(ON_PREM); var cloud = AlertRetentionPolicy.defaults(CLOUD_MANAGED);
        var values = copy(op); values.put(ACCEPTED_PAYLOAD, hours(168)); values.put(ACCEPTED_RESULT, hours(24));
        assertThatThrownBy(() -> AlertRetentionPolicy.configure(ON_PREM, values)).isInstanceOf(TapstateException.class);
        assertThat(op.duration(ACCEPTED_PAYLOAD)).isEqualTo(hours(24));
        var cloudValues = copy(cloud); cloudValues.put(ACCEPTED_RESULT, hours(48)); cloudValues.put(FAILED_RESULT, hours(24));
        assertThatThrownBy(() -> AlertRetentionPolicy.configure(CLOUD_MANAGED, cloudValues)).isInstanceOf(TapstateException.class);
        var read = copy(op); read.put(UNREAD_NOTIFICATION, hours(24));
        assertThatThrownBy(() -> AlertRetentionPolicy.configure(ON_PREM, read)).isInstanceOf(TapstateException.class);
        var test = copy(op); test.put(TEST_PAYLOAD, hours(24)); test.put(TEST_ACCEPTED_RESULT, hours(1));
        assertThatThrownBy(() -> AlertRetentionPolicy.configure(ON_PREM, test)).isInstanceOf(TapstateException.class);
        var valid = copy(op); var frozen = AlertRetentionPolicy.configure(ON_PREM, valid); valid.put(ACCEPTED_PAYLOAD, hours(1));
        assertThat(frozen.duration(ACCEPTED_PAYLOAD)).isEqualTo(hours(24));
        assertThatThrownBy(() -> frozen.values().put(ACCEPTED_PAYLOAD, hours(1))).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void cutoffCandidatesOnlyTightenAndKeepTheOriginalPolicyAndDeadline() {
        var policy = AlertRetentionPolicy.defaults(ON_PREM); Instant deadline = C.plus(hours(24));
        var initial = DeliveryRetention.begin(policy, DeliveryRetention.Kind.OP_EXTERNAL, C, deadline);
        assertThat(initial.payloadUntil()).isEqualTo(C.plus(hours(96)));
        var stopped = initial.stop(C.plus(hours(2)), C.plusSeconds(30));
        assertThat(stopped.payloadUntil()).isEqualTo(C.plus(hours(74)));
        assertThat(stopped.stop(C.plus(hours(5)), deadline)).isSameAs(stopped);
        var accepted = stopped.accept(C.plus(hours(3)), C.plusSeconds(30), C.plus(hours(3))).orElseThrow();
        assertThat(accepted.payloadUntil()).isEqualTo(C.plus(hours(27)));
        assertThat(accepted.resultUntil()).isEqualTo(C.plus(hours(171)));
        assertThat(accepted.accept(C.plus(hours(5)), deadline, C.plus(hours(5))).orElseThrow()).isSameAs(accepted);
        assertThat(accepted.originalDeadline()).isEqualTo(deadline);
        assertThat(accepted.policy()).isSameAs(policy);
        assertThat(initial.payloadUntil()).isEqualTo(C.plus(hours(96)));
    }
    @Test void successCannotCutAnAuthorizedEvidenceWindowAndPayloadDoesNotMechanicallyWaitForIntentDeadline() {
        var values = copy(AlertRetentionPolicy.defaults(ON_PREM));
        values.put(ACCEPTED_PAYLOAD, Duration.ofMinutes(15)); values.put(ACCEPTED_RESULT, hours(6));
        var policy = AlertRetentionPolicy.configure(ON_PREM, values); Instant deadline = C.plus(hours(24));
        var retention = DeliveryRetention.begin(policy, DeliveryRetention.Kind.OP_EXTERNAL, C, deadline)
                .accept(C, deadline, C).orElseThrow();
        assertThat(retention.payloadUntil()).isEqualTo(C.plusSeconds(900));
        assertThat(retention.resultUntil()).isEqualTo(deadline);
    }
    @Test void lateAcceptanceDoesNotReopenExpiredDetailsOrRecreatePrunedPayload() {
        var retention = DeliveryRetention.begin(AlertRetentionPolicy.defaults(ON_PREM), DeliveryRetention.Kind.OP_EXTERNAL,
                C, C.plus(hours(24))).stop(C, C);
        Instant originalPayloadCutoff = retention.payloadUntil();
        var late = retention.accept(C.plus(hours(96)), C, C.plus(hours(96))).orElseThrow();
        assertThat(late.payloadUntil()).isEqualTo(originalPayloadCutoff);
        assertThat(late.resultUntil()).isBefore(retention.resultUntil());
        assertThat(late.accept(late.resultUntil(), C, late.resultUntil())).isEmpty();
        assertThat(late.hasResultWindow(late.resultUntil())).isFalse();
    }
    @Test void sdkSnapshotAndConfigurationTestHaveTheirOwnFixedCutoffsWithoutExtendingSendAge() {
        var cloud = DeliveryRetention.begin(AlertRetentionPolicy.defaults(CLOUD_MANAGED), DeliveryRetention.Kind.CLOUD_SDK,
                C, C.plusSeconds(60)).accept(C.plusSeconds(1), C.plusSeconds(60), C.plusSeconds(1)).orElseThrow();
        assertThat(cloud.payloadUntil()).isEqualTo(C.plusSeconds(3601));
        assertThat(cloud.resultUntil()).isEqualTo(C.plusSeconds(86401));
        var test = DeliveryRetention.begin(AlertRetentionPolicy.defaults(ON_PREM), DeliveryRetention.Kind.CHANNEL_TEST,
                C, C.plusSeconds(60)).accept(C, C.plusSeconds(60), C).orElseThrow();
        assertThat(test.payloadUntil()).isEqualTo(C.plus(hours(1)));
        assertThat(test.resultUntil()).isEqualTo(C.plus(hours(24)));
        assertThat(test.originalDeadline()).isEqualTo(C.plusSeconds(60));
        assertThatThrownBy(() -> DeliveryRetention.begin(AlertRetentionPolicy.defaults(CLOUD_MANAGED), DeliveryRetention.Kind.CHANNEL_TEST,
                C, C.plusSeconds(60))).isInstanceOf(TapstateException.class);
    }
    @Test void invalidTimesAndOverflowAreCodedInsteadOfSilentlyClamped() {
        var policy = AlertRetentionPolicy.defaults(ON_PREM);
        assertThatThrownBy(() -> DeliveryRetention.begin(policy, DeliveryRetention.Kind.OP_EXTERNAL, C, C.plus(hours(25))))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> DeliveryRetention.begin(policy, DeliveryRetention.Kind.OP_EXTERNAL, Instant.MAX.minusSeconds(10), Instant.MAX))
                .isInstanceOf(TapstateException.class);
        var retention = DeliveryRetention.begin(policy, DeliveryRetention.Kind.OP_EXTERNAL, C, C.plusSeconds(60));
        assertThatThrownBy(() -> retention.accept(C, C.plusSeconds(61), C)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> retention.stop(C.plusSeconds(61), C)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> retention.accept(C.plusSeconds(1), C, C)).isInstanceOf(TapstateException.class);
    }
    @Test void personalReadVisibilityNeverDeletesSharedUnreadContentOrRefreshesFirstRead() {
        for (var deployment : new AlertChannelPolicy.Deployment[]{ON_PREM, CLOUD_MANAGED}) {
            var policy = AlertRetentionPolicy.defaults(deployment); Instant published = C.plusSeconds(1);
            var retention = DeliveryRetention.begin(policy, DeliveryRetention.Kind.IN_APP, C, C.plusSeconds(60))
                    .accept(published, C.plusSeconds(60), published).orElseThrow();
            var notification = new NotificationVisibility(new AlarmKey.Scope("org", "cluster"), "notification-1", retention);
            var a = notification.markRead("principal-a-1", C.plusSeconds(2), null).orElseThrow();
            Instant afterPersonal = a.visibleUntil();
            assertThat(notification.visibleFor("principal-a-1", afterPersonal, a)).isFalse();
            assertThat(notification.unreadFor("principal-a-1", afterPersonal, a)).isFalse();
            assertThat(notification.markRead("principal-a-1", afterPersonal, a).orElseThrow()).isSameAs(a);
            assertThat(notification.unreadFor("principal-b-1", afterPersonal, null)).isTrue();
            assertThat(a.markerUntil()).isEqualTo(notification.sharedVisibleUntil());
            assertThat(notification.markRead("principal-a-1", notification.sharedVisibleUntil(), a)).isEmpty();
            assertThat(notification.visibleFor("principal-b-1", notification.sharedVisibleUntil(), null)).isFalse();
        }
    }
    @Test void terminalUnpublishedNotificationCannotPublishAgainButOriginalEvidenceCanStillArrive() {
        var stopped = DeliveryRetention.begin(AlertRetentionPolicy.defaults(ON_PREM), DeliveryRetention.Kind.IN_APP,
                C, C.plus(hours(24))).stop(C.plus(hours(1)), C);
        assertThatThrownBy(() -> stopped.accept(C.plus(hours(2)), C, C.plus(hours(2))))
                .isInstanceOf(TapstateException.class);
        Instant originalPublication = C.plusSeconds(30);
        var late = stopped.accept(originalPublication, C, C.plus(hours(2))).orElseThrow();
        assertThat(late.acceptedAt()).isEqualTo(originalPublication);
        assertThat(late.payloadUntil()).isEqualTo(stopped.payloadUntil());
        assertThat(late.payloadUntil()).isBefore(originalPublication.plus(hours(720)));
    }
    @Test void notificationVisibilityIsIndependentOfDiagnosticRetentionAndRejectsForeignReceipts() {
        var policy = AlertRetentionPolicy.defaults(ON_PREM);
        var retention = DeliveryRetention.begin(policy, DeliveryRetention.Kind.IN_APP, C, C.plusSeconds(60)).accept(C, C, C).orElseThrow();
        var scope = new AlarmKey.Scope("org", "cluster");
        var notification = new NotificationVisibility(scope, "n1", retention);
        assertThat(notification.unreadFor("principal-a-2", retention.resultUntil(), null)).isTrue();
        var receipt = notification.markRead("principal-a-1", C, null).orElseThrow();
        assertThatThrownBy(() -> notification.markRead("principal-a-2", C, receipt)).isInstanceOf(TapstateException.class);
        var next = new NotificationVisibility(scope, "n2", retention);
        assertThatThrownBy(() -> next.markRead("principal-a-1", C, receipt)).isInstanceOf(TapstateException.class);
        var foreign = new NotificationVisibility(new AlarmKey.Scope("other", "cluster"), "n1", retention);
        assertThatThrownBy(() -> foreign.markRead("principal-a-1", C, receipt)).isInstanceOf(TapstateException.class);
        assertThat(next.markRead("principal-a-1", C, null).orElseThrow().readAt()).isEqualTo(C);
    }
}
