package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.spi.alerting.NotificationChannel.*;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ChannelContractTest {
    static final Instant T = Instant.parse("2026-10-21T00:00:00Z");
    static Request request(Instant deadline) {
        var round = new DeliveryKey.NotificationRound(new AlarmKey.Scope("org", "cluster"), "episode", "1", "system", "binding-1", DeliveryKey.Kind.FAULT, 1);
        return Request.business(new DeliveryKey(round, "cloud-sdk", "system", "cluster"), "delivery", "attempt", "binding-1", deadline, "application/json", new byte[]{123, 125});
    }
    @Test void frozenDestinationAndBindingUseTheSameUnicodeCodepointLimit() {
        var scope = new AlarmKey.Scope("org", "cluster");
        String supplementary = new String(Character.toChars(0x1f680)).repeat(256);
        for (int field = 0; field < 3; field++) {
            String[] ids = {"receiver", "target", "binding"}; ids[field] = supplementary;
            Destination destination = new Destination(scope, "webhook", ids[0], ids[1], ids[2]);
            assertThatCode(() -> new TargetBinding(scope, destination.receiverIncarnation(), destination.targetId(), destination.bindingId()))
                    .doesNotThrowAnyException();
        }
    }
    @Test void frozenDestinationAndBindingRejectMalformedOrOversizedUnicode() {
        var scope = new AlarmKey.Scope("org", "cluster");
        for (String invalid : new String[]{"\ud800", "\udc00", new String(Character.toChars(0x1f680)).repeat(257)}) {
            for (int field = 0; field < 3; field++) {
                String[] ids = {"receiver", "target", "binding"}; ids[field] = invalid;
                assertThatThrownBy(() -> new Destination(scope, "webhook", ids[0], ids[1], ids[2]))
                        .isInstanceOf(io.tapstate.core.common.TapstateException.class);
                assertThatThrownBy(() -> new TargetBinding(scope, ids[0], ids[1], ids[2]))
                        .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            }
        }
    }
    @Test void localNormalReturnIsAcceptedAndNoPublisherInvocationIsNotSuccess() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (var channel = new SdkPlaceholderChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED, new TargetBinding(new AlarmKey.Scope("org", "cluster"), "system", "cluster", "binding-1"), ignored -> calls.incrementAndGet(), Clock.fixed(T, ZoneOffset.UTC))) {
            Result result = channel.send(request(T.plusSeconds(1)));
            assertThat(calls).hasValue(1);
            assertThat(result.outcome()).isEqualTo(Outcome.ACCEPTED);
            assertThat(result.implementation()).isEqualTo("sdk-placeholder");
            assertThat(result.diagnosticCode()).isNull();
            assertThat(channel.send(request(T)).outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
            assertThat(calls).hasValue(1);
        }
    }
    @Test void programmerFailuresStayVisibleAndPayloadIsImmutable() throws Exception {
        Request request = request(T.plusSeconds(1));
        byte[] bytes = request.payload(); bytes[0] = 0;
        assertThat(request.payload()).containsExactly((byte)123, (byte)125);
        try (var channel = new SdkPlaceholderChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED, new TargetBinding(new AlarmKey.Scope("org", "cluster"), "system", "cluster", "binding-1"), ignored -> { throw new IllegalStateException("invariant"); }, Clock.fixed(T, ZoneOffset.UTC))) {
            assertThatThrownBy(() -> channel.send(request)).isInstanceOf(IllegalStateException.class).hasMessage("invariant");
        }
    }
    @Test void aBlockingLocalPublisherHasAFixedDeadlineAndDoesNotCreateMoreWorkers() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var channel = new SdkPlaceholderChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED, new TargetBinding(new AlarmKey.Scope("org", "cluster"), "system", "cluster", "binding-1"), ignored -> {
            entered.countDown();
            boolean done = false;
            while (!done) { try { done = release.await(1, TimeUnit.SECONDS); } catch (InterruptedException expected) { } }
        }, Clock.systemUTC())) {
            Instant deadline = Instant.now().plusMillis(100);
            Result result = channel.send(request(deadline));
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(result.outcome()).isEqualTo(Outcome.UNKNOWN);
            assertThat(result.action()).isEqualTo(Action.STOP);
            assertThat(channel.activeCalls()).isEqualTo(1);
            assertThat(channel.send(request(Instant.now().plusSeconds(1))).action()).isEqualTo(Action.PAUSE);
            release.countDown();
        } finally { release.countDown(); }
    }
    @Test void knownLocalCompletionIsNotLostIfFuturePublicationReachesTheDeadline() throws Exception {
        var release = new CountDownLatch(1); var returned = new CountDownLatch(1); var calls = new AtomicInteger();
        var worker = new java.util.concurrent.ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS,
                new java.util.concurrent.SynchronousQueue<>(), task -> { Thread thread = new Thread(task); thread.setDaemon(true); return thread; }) {
            @Override protected <T> java.util.concurrent.RunnableFuture<T> newTaskFor(java.util.concurrent.Callable<T> callable) {
                return new java.util.concurrent.FutureTask<>(callable) {
                    @Override protected void set(T value) {
                        if (Boolean.TRUE.equals(value)) {
                            returned.countDown();
                            while (release.getCount() > 0) { try { release.await(5, TimeUnit.SECONDS); } catch (InterruptedException controlledCancellation) { } }
                        }
                        super.set(value);
                    }
                };
            }
        };
        try (var channel = new SdkPlaceholderChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED, new TargetBinding(new AlarmKey.Scope("org", "cluster"), "system", "cluster", "binding-1"),
                ignored -> calls.incrementAndGet(), Clock.systemUTC(), worker)) {
            Result result = channel.send(request(Instant.now().plusSeconds(1)));
            assertThat(returned.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(calls).hasValue(1);
            assertThat(result.outcome()).isEqualTo(Outcome.ACCEPTED);
        } finally { release.countDown(); }
    }
    @Test void cloudPlaceholderDoesNotOfferConfigurationTestSending() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (var channel = new SdkPlaceholderChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED, new TargetBinding(new AlarmKey.Scope("org", "cluster"), "system", "cluster", "binding-1"),
                ignored -> calls.incrementAndGet(), Clock.systemUTC())) {
            Request test = Request.channelTest(new Destination(new AlarmKey.Scope("org", "cluster"), "cloud-sdk", "system", "cluster", "binding-1"),
                    "test", "attempt", Instant.now().plusSeconds(1), "application/json", new byte[]{123, 125});
            assertThatThrownBy(() -> channel.send(test)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(calls).hasValue(0);
        }
    }
    @Test void adaptersRejectUntrustedOrWrongDeploymentBeforeConstructingAnOutputPath() throws Exception {
        TargetBinding target = new TargetBinding(new AlarmKey.Scope("org", "cluster"), "receiver", "target", "binding-1");
        for (var deployment : new AlertChannelPolicy.Deployment[]{AlertChannelPolicy.Deployment.CLOUD_MANAGED, AlertChannelPolicy.Deployment.UNRESOLVED}) {
            assertThatThrownBy(() -> new WebhookChannel(deployment, target, java.net.URI.create("https://example.invalid/"),
                    java.util.Set.of(new WebhookChannel.Origin("example.invalid", 443)), javax.net.ssl.SSLContext.getDefault(), null, null, Clock.systemUTC()))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThatThrownBy(() -> new SmtpChannel(deployment, target, "example.invalid", new java.net.InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 465),
                    SmtpChannel.Encryption.IMPLICIT_TLS, javax.net.ssl.SSLContext.getDefault(), SmtpChannel.Authentication.NONE, null, null,
                    "from@example.test", "to@example.test", Clock.systemUTC())).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        }
        assertThatThrownBy(() -> new SdkPlaceholderChannel(AlertChannelPolicy.Deployment.ON_PREM, target, ignored -> { }, Clock.systemUTC()))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
    @Test void protocolClassificationSeparatesIntermediateSuccessFromFinalAcceptance() {
        assertThat(ChannelFailureClassifier.smtp(Phase.ENVELOPE, 250)).isEmpty();
        assertThat(ChannelFailureClassifier.smtp(Phase.BODY, 354)).isEmpty();
        assertThat(ChannelFailureClassifier.smtp(Phase.FINAL_RESPONSE, 250).orElseThrow().outcome()).isEqualTo(Outcome.ACCEPTED);
        assertThat(ChannelFailureClassifier.smtp(Phase.FINAL_RESPONSE, 451).orElseThrow().action()).isEqualTo(Action.RETRY);
        assertThat(ChannelFailureClassifier.smtp(Phase.AUTHENTICATION, 535).orElseThrow().action()).isEqualTo(Action.STOP);
        for (int status : new int[]{200, 202}) { assertThat(ChannelFailureClassifier.http(status, null).outcome()).isEqualTo(Outcome.ACCEPTED); }
        for (int status : new int[]{408, 429, 500, 503}) { assertThat(ChannelFailureClassifier.http(status, T).action()).isEqualTo(Action.RETRY); }
        for (int status : new int[]{302, 400, 401, 403, 409, 501, 505}) { assertThat(ChannelFailureClassifier.http(status, null).action()).isEqualTo(Action.STOP); }
        assertThat(ChannelFailureClassifier.http(503, null).outcome()).isEqualTo(Outcome.UNKNOWN);
        assertThat(ChannelFailureClassifier.http(409, null).outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
    }
}
