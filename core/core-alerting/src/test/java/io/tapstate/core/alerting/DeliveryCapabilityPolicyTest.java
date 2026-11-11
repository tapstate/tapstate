package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.alerting.AlertChannelPolicy.*;
import static io.tapstate.core.alerting.DeliveryCapabilityPolicy.*;
import static org.assertj.core.api.Assertions.*;

class DeliveryCapabilityPolicyTest {
    private static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("tenant-a", "cluster-a");
    private static final Instant START = Instant.parse("2026-10-21T00:00:00Z");
    private static final Instant DEADLINE = START.plusSeconds(60);
    private static final Capability WEBHOOK = new Capability("webhook", 1, Family.OP_EXTERNAL);
    private static final Capability SDK = new Capability("cloud-sdk", 1, Family.CLOUD_SDK);

    @Test void unknownEnvelopeCannotTrustExpiryOrAnAllegedTerminalState() {
        var policy = installed();
        for (Integer version : new Integer[] { null, 0, 3 }) {
            var header = new Header(version, SCOPE, "delivery-1", "webhook", 1, START);
            assertThat(policy.assess(SCOPE, header, true, Deployment.ON_PREM, DEADLINE).disposition())
                    .isEqualTo(Disposition.ISOLATED);
        }
        assertThat(policy.assess(SCOPE, new Header(null, null, null, null, null, null), false,
                Deployment.ON_PREM, START).reason()).isEqualTo(Reason.MISSING_RECORD_VERSION);
    }

    @Test void missingIdentityChannelVersionOrDeadlineNeverGetsGuessed() {
        var invalid = List.of(
                new Header(1, null, "delivery-1", "webhook", 1, DEADLINE),
                new Header(1, SCOPE, null, "webhook", 1, DEADLINE),
                new Header(1, SCOPE, "bad/id", "webhook", 1, DEADLINE),
                new Header(1, SCOPE, "x".repeat(257), "webhook", 1, DEADLINE),
                new Header(1, SCOPE, "delivery-1", null, 1, DEADLINE),
                new Header(1, SCOPE, "delivery-1", "webhook", null, DEADLINE),
                new Header(1, SCOPE, "delivery-1", "webhook", 0, DEADLINE),
                new Header(1, SCOPE, "delivery-1", "Bad/Channel", 1, DEADLINE),
                new Header(1, SCOPE, "delivery-1", "x".repeat(65), 1, DEADLINE),
                new Header(1, SCOPE, "delivery-1", "webhook", 1, null));
        for (var header : invalid) {
            assertThat(installed().assess(SCOPE, header, false, Deployment.ON_PREM, DEADLINE).disposition())
                    .isEqualTo(Disposition.ISOLATED);
        }
    }

    @Test void scopeMismatchCannotBecomeAnExpiredRecordInTheCurrentScope() {
        var other = new AlarmKey.Scope("tenant-b", "cluster-a");
        assertThat(installed().assess(other, header(), false, Deployment.ON_PREM, DEADLINE))
                .isEqualTo(new Decision(Disposition.ISOLATED, Reason.SCOPE_MISMATCH));
    }

    @Test void descriptionDoesNotInventAnAdapterAndRestorationUsesTheUnchangedHeader() {
        var header = header();
        var missing = new DeliveryCapabilityPolicy(List.of(WEBHOOK), List.of());
        assertThat(missing.assess(SCOPE, header, false, Deployment.ON_PREM, START))
                .isEqualTo(new Decision(Disposition.PAUSED, Reason.ADAPTER_UNAVAILABLE));
        assertThat(installed().assess(SCOPE, header, false, Deployment.ON_PREM, START.plusSeconds(40)))
                .isEqualTo(new Decision(Disposition.COMPATIBLE, Reason.SUPPORTED));
        assertThat(header).isEqualTo(header());
    }

    @Test void originalDeadlineWinsOverMissingOrRestoredCapabilitiesAtItsInclusiveBoundary() {
        var missing = new DeliveryCapabilityPolicy(List.of(WEBHOOK), List.of());
        for (var policy : List.of(missing, installed(), new DeliveryCapabilityPolicy(List.of(), List.of()))) {
            for (var now : List.of(DEADLINE, DEADLINE.plusSeconds(60))) {
                assertThat(policy.assess(SCOPE, header(), false, Deployment.ON_PREM, now))
                        .isEqualTo(new Decision(Disposition.EXPIRED, Reason.DEADLINE_REACHED));
            }
        }
        assertThat(installed().assess(SCOPE, header(), false, Deployment.ON_PREM, DEADLINE.minusNanos(1)).disposition())
                .isEqualTo(Disposition.COMPATIBLE);
    }

    @Test void cancelledOrOtherStoppedWorkNeverResumesWhenCapabilityReturns() {
        for (var now : List.of(START, DEADLINE.plusSeconds(60))) {
            assertThat(installed().assess(SCOPE, header(), true, Deployment.ON_PREM, now))
                    .isEqualTo(new Decision(Disposition.STOPPED, Reason.ALREADY_STOPPED));
        }
    }

    @Test void unknownChannelAndUnsupportedVersionRemainDistinctAndDoNotBlockCompatibleWork() {
        var policy = installed();
        assertThat(policy.assess(SCOPE, new Header(1, SCOPE, "delivery-2", "future-channel", 1, DEADLINE), false,
                Deployment.ON_PREM, START)).isEqualTo(new Decision(Disposition.PAUSED, Reason.UNKNOWN_CHANNEL));
        assertThat(policy.assess(SCOPE, new Header(1, SCOPE, "delivery-3", "webhook", 2, DEADLINE), false,
                Deployment.ON_PREM, START)).isEqualTo(new Decision(Disposition.PAUSED, Reason.UNSUPPORTED_CHANNEL_VERSION));
        assertThat(policy.assess(SCOPE, header(), false, Deployment.ON_PREM, START).disposition())
                .isEqualTo(Disposition.COMPATIBLE);
    }

    @Test void trustedDeploymentPolicyStillAppliesBeforeAnAdapterCanResume() {
        var policy = new DeliveryCapabilityPolicy(List.of(WEBHOOK, SDK), List.of(WEBHOOK, SDK));
        assertThat(policy.assess(SCOPE, header(), false, Deployment.CLOUD_MANAGED, START))
                .isEqualTo(new Decision(Disposition.STOPPED, Reason.CHANNEL_NOT_PERMITTED));
        assertThat(policy.assess(SCOPE, header(), false, Deployment.UNRESOLVED, START))
                .isEqualTo(new Decision(Disposition.PAUSED, Reason.DEPLOYMENT_UNRESOLVED));
        for (var deployment : List.of(Deployment.ON_PREM, Deployment.CLOUD_MANAGED)) {
            assertThat(policy.assess(SCOPE, new Header(1, SCOPE, "delivery-2", "in-app", 1, DEADLINE), false,
                    deployment, START)).isEqualTo(new Decision(Disposition.PAUSED, Reason.UNKNOWN_CHANNEL));
        }
        var sdk = new Header(1, SCOPE, "delivery-3", "cloud-sdk", 1, DEADLINE);
        assertThat(policy.assess(SCOPE, sdk, false, Deployment.ON_PREM, START).disposition()).isEqualTo(Disposition.STOPPED);
        assertThat(policy.assess(SCOPE, sdk, false, Deployment.CLOUD_MANAGED, START).disposition()).isEqualTo(Disposition.COMPATIBLE);
    }

    @Test void boundedStaticRegistrationRejectsDuplicatesAndInconsistentAvailableCapabilities() {
        assertThatThrownBy(() -> new DeliveryCapabilityPolicy(List.of(WEBHOOK, WEBHOOK), List.of()))
                .isInstanceOf(TapstateException.class);
        for (var capability : List.of(SDK, new Capability("webhook", 2, Family.OP_EXTERNAL),
                new Capability("webhook", 1, Family.CLOUD_SDK))) {
            assertThatThrownBy(() -> new DeliveryCapabilityPolicy(List.of(WEBHOOK), List.of(capability)))
                    .isInstanceOf(TapstateException.class);
        }
        assertThatThrownBy(() -> new DeliveryCapabilityPolicy(List.of(WEBHOOK), List.of(WEBHOOK, WEBHOOK)))
                .isInstanceOf(TapstateException.class);
        var channels = IntStream.range(0, MAX_CHANNELS).mapToObj(i -> new Capability("channel-" + i, 1, Family.OP_EXTERNAL)).toList();
        assertThatCode(() -> new DeliveryCapabilityPolicy(channels, channels)).doesNotThrowAnyException();
        var tooMany = new ArrayList<>(channels);
        tooMany.add(new Capability("extra-channel", 1, Family.OP_EXTERNAL));
        assertThatThrownBy(() -> new DeliveryCapabilityPolicy(tooMany, List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new DeliveryCapabilityPolicy(channels, tooMany)).isInstanceOf(TapstateException.class);
    }

    @Test void hostRegistrationIsSnapshottedRatherThanRetainingMutableCollections() {
        var descriptions = new ArrayList<>(List.of(WEBHOOK));
        var available = new ArrayList<>(List.of(WEBHOOK));
        var policy = new DeliveryCapabilityPolicy(descriptions, available);
        descriptions.clear(); available.clear();
        assertThat(policy.assess(SCOPE, header(), false, Deployment.ON_PREM, START).disposition())
                .isEqualTo(Disposition.COMPATIBLE);
    }

    private static DeliveryCapabilityPolicy installed() { return new DeliveryCapabilityPolicy(List.of(WEBHOOK), List.of(WEBHOOK)); }
    private static Header header() { return new Header(1, SCOPE, "delivery-1", "webhook", 1, DEADLINE); }
}
