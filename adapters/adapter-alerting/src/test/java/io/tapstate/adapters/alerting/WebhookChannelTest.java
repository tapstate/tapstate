package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class WebhookChannelTest {
    @TempDir static Path temporary;
    static TlsTestMaterial tls;
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org", "cluster");
    static final TargetBinding BINDING = new TargetBinding(SCOPE, "receiver", "target", "binding-1");
    @BeforeAll static void tls() throws Exception { tls = TlsTestMaterial.load(temporary); }
    static Request request(Instant deadline) {
        var round = new DeliveryKey.NotificationRound(SCOPE, "episode", "1", "route", "route-1", DeliveryKey.Kind.FAULT, 1);
        return Request.business(new DeliveryKey(round, "webhook", "receiver", "target"), "delivery", "attempt", "binding-1", deadline, "application/json", "{\"fixed\":true}".getBytes(StandardCharsets.UTF_8));
    }
    static WebhookChannel channel(Fixture fixture, SSLContext client, String host) {
        return new WebhookChannel(AlertChannelPolicy.Deployment.ON_PREM, BINDING, URI.create("https://" + host + ":" + fixture.port() + "/notify"),
                Set.of(new WebhookChannel.Origin(host, fixture.port())), client, "fixture-token", new byte[32], Clock.systemUTC());
    }
    @Test void completeAcceptanceSurvivesHugeChunkedBodyCancellationAndReleasesItsResources() throws Exception {
        var peerClosed = new CountDownLatch(1);
        var body = new AtomicReference<String>();
        var headers = new AtomicReference<String>();
        try (Fixture fixture = new Fixture(socket -> {
            var received = readRequest(socket.getInputStream()); headers.set(received.headers()); body.set(received.body());
            var out = socket.getOutputStream();
            out.write("HTTP/1.1 202 Accepted\r\nTransfer-Encoding: chunked\r\n\r\n".getBytes(StandardCharsets.US_ASCII)); out.flush();
            try { for (int i = 0; i < 100000; i++) { out.write("1\r\nx\r\n".getBytes(StandardCharsets.US_ASCII)); out.flush(); } }
            catch (IOException closed) { peerClosed.countDown(); }
        }); WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
            Request request = request(Instant.now().plusSeconds(5));
            Result result = channel.send(request);
            assertThat(result.outcome()).isEqualTo(Outcome.ACCEPTED);
            assertThat(result.protocolStatus()).isEqualTo(202);
            assertThat(peerClosed.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(channel.activeAttempts()).isZero();
            assertThat(channel.queuedClientTasks()).isZero();
            assertThat(body.get()).isEqualTo(new String(request.payload(), StandardCharsets.UTF_8));
            assertThat(headers.get().toLowerCase()).contains("authorization: bearer fixture-token", "x-tapstate-delivery-id: delivery", "x-tapstate-signature: v1=");
            assertThat(fixture.calls).hasValue(1);
        }
    }
    @Test void oversizedResponseHeadersCannotBecomeAccepted() throws Exception {
        try (Fixture fixture = new Fixture(socket -> {
            readRequest(socket.getInputStream());
            socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nX-Large: " + "x".repeat(17000) + "\r\nContent-Length: 0\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
            socket.getOutputStream().flush();
        }); WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
            assertThat(channel.send(request(Instant.now().plusSeconds(5))).outcome()).isEqualTo(Outcome.UNKNOWN);
            assertThat(channel.activeAttempts()).isZero();
            assertThat(fixture.calls).hasValue(1);
        }
    }
    @Test void deadlineStopsAStalledResponseWithoutAnImplicitSecondRequest() throws Exception {
        var peerClosed = new CountDownLatch(1);
        try (Fixture fixture = new Fixture(socket -> {
            readRequest(socket.getInputStream());
            try { if (socket.getInputStream().read() < 0) { peerClosed.countDown(); } } catch (IOException closed) { peerClosed.countDown(); }
        }); WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
            Result result = channel.send(request(Instant.now().plusSeconds(2)));
            assertThat(result.outcome()).isEqualTo(Outcome.UNKNOWN);
            assertThat(peerClosed.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.calls).hasValue(1);
            assertThat(channel.activeAttempts()).isZero();
        }
    }
    @Test void redirectsAreNotFollowedAndRateLimitsUseReceiptTime() throws Exception {
        Instant before = Instant.now();
        try (Fixture fixture = new Fixture(socket -> {
            readRequest(socket.getInputStream());
            socket.getOutputStream().write("HTTP/1.1 429 Too Many Requests\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII)); socket.getOutputStream().flush();
        }); WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
            Result result = channel.send(request(Instant.now().plusSeconds(5)));
            assertThat(result.action()).isEqualTo(Action.RETRY);
            assertThat(result.retryAfter()).isBetween(before.plusSeconds(30), Instant.now().plusSeconds(30));
            assertThat(fixture.calls).hasValue(1);
        }
        try (Fixture fixture = new Fixture(socket -> {
            readRequest(socket.getInputStream());
            socket.getOutputStream().write("HTTP/1.1 302 Found\r\nLocation: https://unapproved.invalid/\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII)); socket.getOutputStream().flush();
        }); WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
            Result redirected = channel.send(request(Instant.now().plusSeconds(5)));
            assertThat(redirected.action()).isEqualTo(Action.STOP);
            assertThat(redirected.protocolStatus()).isEqualTo(302);
            assertThat(fixture.calls).hasValue(1);
        }
    }
    @Test void tlsHostnameTrustAndFrozenBindingsAreCheckedBeforeAnHttpRequest() throws Exception {
        AtomicInteger httpCalls = new AtomicInteger();
        try (Fixture fixture = new Fixture(socket -> { readRequest(socket.getInputStream()); httpCalls.incrementAndGet(); })) {
            try (WebhookChannel wrongName = channel(fixture, tls.client, "127.0.0.1")) {
                Result result = wrongName.send(request(Instant.now().plusSeconds(5)));
                assertThat(result.action()).isEqualTo(Action.STOP);
                assertThat(result.phase()).isEqualTo(Phase.TLS);
                assertThat(result.outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
            }
            try (WebhookChannel untrusted = channel(fixture, SSLContext.getDefault(), "localhost")) {
                assertThat(untrusted.send(request(Instant.now().plusSeconds(5))).outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
            }
            try (WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
                Request original = request(Instant.now().plusSeconds(5));
                Request replaced = new Request(new Destination(original.destination().scope(), "webhook", "receiver", "target", "binding-2"), original.purpose(), original.deliveryId(), original.attemptId(), original.deadline(), original.contentType(), original.payload());
                assertThatThrownBy(() -> channel.send(replaced)).isInstanceOf(TapstateException.class);
            }
            assertThat(httpCalls).hasValue(0);
        }
    }
    @Test void twoSlotsStayBoundedAndFinishingOneCallDoesNotCancelTheOther() throws Exception {
        var both = new CountDownLatch(2); var firstRelease = new CountDownLatch(1); var secondRelease = new CountDownLatch(1);
        var callers = java.util.concurrent.Executors.newFixedThreadPool(2);
        try (Fixture fixture = new Fixture(socket -> {
            Received received = readRequest(socket.getInputStream()); both.countDown();
            (received.body().contains("first") ? firstRelease : secondRelease).await(5, TimeUnit.SECONDS);
            socket.getOutputStream().write("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII)); socket.getOutputStream().flush();
        }); WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
            java.util.function.Function<String, Request> bound = id -> {
                var round = new DeliveryKey.NotificationRound(SCOPE, id, "1", "route", "route-1", DeliveryKey.Kind.FAULT, 1);
                return Request.business(new DeliveryKey(round, "webhook", "receiver", "target"), id, "attempt-" + id, "binding-1",
                        Instant.now().plusSeconds(8), "application/json", ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8));
            };
            var first = callers.submit(() -> channel.send(bound.apply("first")));
            var second = callers.submit(() -> channel.send(bound.apply("second")));
            assertThat(both.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(channel.send(bound.apply("third")).phase()).isEqualTo(Phase.CAPACITY);
            assertThat(fixture.calls).hasValue(2);
            firstRelease.countDown();
            assertThat(first.get(2, TimeUnit.SECONDS).outcome()).isEqualTo(Outcome.ACCEPTED);
            assertThat(second.isDone()).isFalse();
            assertThat(channel.activeAttempts()).isEqualTo(1);
            secondRelease.countDown();
            assertThat(second.get(2, TimeUnit.SECONDS).outcome()).isEqualTo(Outcome.ACCEPTED);
            assertThat(channel.activeAttempts()).isZero();
        } finally { firstRelease.countDown(); secondRelease.countDown(); callers.shutdownNow(); }
    }
    @Test void explicitConfigurationTestsReuseTheSameTlsAdapterWithoutAnEpisode() throws Exception {
        var captured = new AtomicReference<String>();
        try (Fixture fixture = new Fixture(socket -> {
            captured.set(readRequest(socket.getInputStream()).body());
            socket.getOutputStream().write("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII)); socket.getOutputStream().flush();
        }); WebhookChannel channel = channel(fixture, tls.client, "localhost")) {
            Instant at = Instant.now();
            var facts = new NotificationRenderer.Facts(NotificationRenderer.Kind.CHANNEL_TEST, "test-delivery", 0, at, List.of(), 0, null, List.of(), "test-1", "Webhook");
            byte[] json = new NotificationRenderer().render(facts, MessageSettings.empty()).snapshot().webhook();
            Request request = Request.channelTest(new Destination(SCOPE, "webhook", "receiver", "target", "binding-1"),
                    "test-delivery", "attempt", at.plusSeconds(5), "application/json", json);
            assertThat(channel.send(request).outcome()).isEqualTo(Outcome.ACCEPTED);
            assertThat(captured.get()).isEqualTo(new String(json, StandardCharsets.UTF_8))
                    .contains("\"purpose\":\"channel-test\"", "\"isTest\":true").doesNotContain("episode", "alarmTypeId");
            assertThat(fixture.calls).hasValue(1);
        }
    }
    @Test void retryAfterParsingUsesTheOriginalTimestampWithoutRenewal() {
        Instant time = Instant.parse("2026-10-21T00:00:00Z");
        assertThat(WebhookChannel.retryAfter("30", time)).isEqualTo(time.plusSeconds(30));
        assertThat(WebhookChannel.retryAfter("Wed, 21 Oct 2026 00:00:10 GMT", time)).isEqualTo(time.plusSeconds(10));
        assertThat(WebhookChannel.retryAfter("invalid", time)).isNull();
        assertThat(WebhookChannel.retryAfter("-1", time)).isNull();
    }
    record Received(String headers, String body) { }
    static Received readRequest(InputStream in) throws IOException {
        StringBuilder headers = new StringBuilder(); int length = 0;
        for (String line = line(in); !line.isEmpty(); line = line(in)) {
            headers.append(line).append('\n');
            if (line.toLowerCase().startsWith("content-length:")) { length = Integer.parseInt(line.substring(15).trim()); }
        }
        if (length > 65536) { throw new IOException("fixture request too large"); }
        return new Received(headers.toString(), new String(in.readNBytes(length), StandardCharsets.UTF_8));
    }
    static String line(InputStream in) throws IOException {
        var bytes = new ByteArrayOutputStream();
        for (int i = 0; i < 20000; i++) {
            int next = in.read(); if (next < 0) { throw new IOException("fixture peer closed"); }
            if (next == '\n') { return bytes.toString(StandardCharsets.US_ASCII).replace("\r", ""); }
            bytes.write(next);
        }
        throw new IOException("fixture line too long");
    }
    @FunctionalInterface interface Handler { void handle(Socket socket) throws Exception; }
    static final class Fixture implements AutoCloseable {
        private final SSLServerSocket server;
        private final java.util.concurrent.ExecutorService workers = Executors.newFixedThreadPool(4);
        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final Thread acceptor;
        final AtomicInteger calls = new AtomicInteger();
        Fixture(Handler handler) throws Exception {
            server = (SSLServerSocket) tls.server.getServerSocketFactory().createServerSocket(0, 8, java.net.InetAddress.getLoopbackAddress());
            acceptor = Thread.ofPlatform().daemon(true).start(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket socket = server.accept(); sockets.add(socket); socket.setSoTimeout(3000); calls.incrementAndGet();
                        workers.submit(() -> { try (socket) { handler.handle(socket); } catch (Exception expectedFixtureFailure) { } finally { sockets.remove(socket); } });
                    } catch (IOException closed) { break; }
                }
            });
        }
        int port() { return server.getLocalPort(); }
        @Override public void close() throws Exception {
            server.close(); for (Socket socket : sockets) { socket.close(); }
            workers.shutdownNow(); workers.awaitTermination(1, TimeUnit.SECONDS); acceptor.join(1000);
        }
    }
}
