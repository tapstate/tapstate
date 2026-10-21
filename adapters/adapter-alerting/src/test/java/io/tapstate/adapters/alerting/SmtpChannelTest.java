package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.spi.alerting.NotificationChannel.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class SmtpChannelTest {
    @TempDir static Path temporary;
    static TlsTestMaterial tls;
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org", "cluster");
    static final TargetBinding BINDING = new TargetBinding(SCOPE, "receiver", "mailbox", "binding-1");
    static final String FROM = "alerts@example.test";
    static final String TO = "Case.Sensitive@example.test";
    @BeforeAll static void tls() throws Exception { tls = TlsTestMaterial.load(temporary); }
    static Request request(Instant deadline, boolean large) {
        var round = new DeliveryKey.NotificationRound(SCOPE, "episode", "1", "route", "route-1", DeliveryKey.Kind.FAULT, 1);
        var snapshot = large ? new NotificationRenderer.Snapshot(1, "en", "[Alert] load", "x".repeat(90000), "x".repeat(90000), new byte[0])
                : new NotificationRenderer().render(NotificationTemplateRenderTest.facts(NotificationRenderer.Kind.FAULT), MessageSettings.empty()).snapshot();
        byte[] mime = EmailMime.freeze("delivery", Instant.parse("2026-10-21T00:00:00Z"), FROM, TO, snapshot).bytes();
        assertThat(mime).isNotNull();
        return Request.business(new DeliveryKey(round, "email", "receiver", "mailbox"), "delivery", "attempt", "binding-1", deadline, "message/rfc822", mime);
    }
    static SmtpChannel channel(Fixture fixture, SmtpChannel.Encryption mode, SmtpChannel.Authentication auth, SSLContext ssl, String host) {
        return new SmtpChannel(AlertChannelPolicy.Deployment.ON_PREM, BINDING, host, new InetSocketAddress(InetAddress.getLoopbackAddress(), fixture.port()), mode, ssl,
                auth, auth == SmtpChannel.Authentication.NONE ? null : "fixture-user", auth == SmtpChannel.Authentication.NONE ? null : "fixture-password".toCharArray(), FROM, TO, Clock.systemUTC());
    }
    @Test void implicitTlsAndRequiredStarttlsSubmitTheExactFrozenMimeToOneAddress() throws Exception {
        for (boolean implicit : new boolean[]{true, false}) {
            try (Fixture fixture = new Fixture(implicit, Behavior.NORMAL);
                    SmtpChannel channel = channel(fixture, implicit ? SmtpChannel.Encryption.IMPLICIT_TLS : SmtpChannel.Encryption.STARTTLS_REQUIRED,
                            implicit ? SmtpChannel.Authentication.PLAIN : SmtpChannel.Authentication.LOGIN, tls.client, "localhost")) {
                Request request = request(Instant.now().plusSeconds(5), false);
                Result result = channel.send(request);
                assertThat(result.outcome()).isEqualTo(Outcome.ACCEPTED);
                assertThat(result.phase()).isEqualTo(Phase.FINAL_RESPONSE);
                assertThat(result.protocolStatus()).isEqualTo(250);
                assertThat(fixture.data.get()).isEqualTo(new String(request.payload(), StandardCharsets.US_ASCII));
                assertThat(fixture.commands.stream().filter(c -> c.startsWith("RCPT ")).toList()).containsExactly("RCPT TO:<" + TO + ">");
                assertThat(fixture.authBeforeTls).hasValue(0);
                assertThat(fixture.calls).hasValue(1);
                idle(channel);
            }
        }
    }
    @Test void explicitConfigurationTestsReuseTheSameSmtpTlsPathWithoutAnEpisode() throws Exception {
        try (Fixture fixture = new Fixture(true, Behavior.NORMAL);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.PLAIN, tls.client, "localhost")) {
            Instant at = Instant.now();
            var facts = new NotificationRenderer.Facts(NotificationRenderer.Kind.CHANNEL_TEST, "test-delivery", 0, at, List.of(), 0, null, List.of(), "test-1", "Email");
            var rendered = new NotificationRenderer().render(facts, MessageSettings.empty()).snapshot();
            byte[] data = EmailMime.freeze("test-delivery", at, FROM, TO, rendered).bytes();
            Request request = Request.channelTest(new Destination(SCOPE, "email", "receiver", "mailbox", "binding-1"),
                    "test-delivery", "attempt", at.plusSeconds(5), "message/rfc822", data);
            assertThat(channel.send(request).outcome()).isEqualTo(Outcome.ACCEPTED);
            assertThat(fixture.data.get()).isEqualTo(new String(data, StandardCharsets.US_ASCII));
            String part = fixture.data.get().split("Content-Transfer-Encoding: base64\r\n\r\n", -1)[1].split("\r\n--", -1)[0];
            String visible = new String(java.util.Base64.getMimeDecoder().decode(part), StandardCharsets.UTF_8);
            assertThat(visible).contains("[TEST]", "test-1", at.toString()).doesNotContain("Episode:", "pipeline.failed");
            assertThat(fixture.authBeforeTls).hasValue(0); assertThat(fixture.calls).hasValue(1);
        }
    }
    @Test void absenceOfStarttlsNeverSendsAuthenticationOrMailInCleartext() throws Exception {
        try (Fixture fixture = new Fixture(false, Behavior.NO_STARTTLS);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.STARTTLS_REQUIRED, SmtpChannel.Authentication.PLAIN, tls.client, "localhost")) {
            Result result = channel.send(request(Instant.now().plusSeconds(5), false));
            assertThat(result.outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
            assertThat(result.action()).isEqualTo(Action.STOP);
            assertThat(result.phase()).isEqualTo(Phase.TLS);
            assertThat(fixture.commands).containsExactly("EHLO tapstate.invalid");
            assertThat(fixture.data.get()).isNull();
            assertThat(fixture.authBeforeTls).hasValue(0); idle(channel);
        }
    }
    @Test void invalidTrustHostnameAndEncryptionModeFailBeforeCredentials() throws Exception {
        try (Fixture fixture = new Fixture(true, Behavior.NORMAL);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.PLAIN, SSLContext.getDefault(), "localhost")) {
            assertThat(channel.send(request(Instant.now().plusSeconds(5), false)).outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
            assertThat(fixture.commands).isEmpty();
        }
        try (Fixture fixture = new Fixture(true, Behavior.NORMAL);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.PLAIN, tls.client, "wrong.example.test")) {
            Result result = channel.send(request(Instant.now().plusSeconds(5), false));
            assertThat(result.action()).isEqualTo(Action.STOP);
            assertThat(result.phase()).isEqualTo(Phase.TLS);
            assertThat(fixture.commands).isEmpty();
        }
        try (Fixture fixture = new Fixture(false, Behavior.NORMAL);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.PLAIN, tls.client, "localhost")) {
            Result result = channel.send(request(Instant.now().plusSeconds(5), false));
            assertThat(result.action()).isEqualTo(Action.STOP);
            assertThat(fixture.authBeforeTls).hasValue(0);
            assertThat(fixture.data.get()).isNull();
        }
    }
    @Test void successfulIntermediateRepliesDoNotHideALostFinalDataReceipt() throws Exception {
        try (Fixture fixture = new Fixture(true, Behavior.DISCONNECT_AFTER_DATA);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.NONE, tls.client, "localhost")) {
            Result result = channel.send(request(Instant.now().plusSeconds(5), false));
            assertThat(fixture.data.get()).isNotNull();
            assertThat(result.outcome()).isEqualTo(Outcome.UNKNOWN);
            assertThat(result.action()).isEqualTo(Action.RETRY);
            assertThat(fixture.calls).hasValue(1); idle(channel);
        }
    }
    @Test void bareLfIsNotACompleteValidFinalSmtpReply() throws Exception {
        try (Fixture fixture = new Fixture(true, Behavior.FINAL_BARE_LF);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.NONE, tls.client, "localhost")) {
            assertThat(channel.send(request(Instant.now().plusSeconds(5), false)).outcome()).isEqualTo(Outcome.UNKNOWN);
        }
    }
    @Test void aCodeOnlyFinalReplyWithValidCrlfIsAccepted() throws Exception {
        try (Fixture fixture = new Fixture(true, Behavior.FINAL_CODE_ONLY);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.NONE, tls.client, "localhost")) {
            assertThat(channel.send(request(Instant.now().plusSeconds(5), false)).outcome()).isEqualTo(Outcome.ACCEPTED);
        }
    }
    @Test void tlsCorruptionAfterCompleteDataKeepsTheResultUnknown() throws Exception {
        try (Fixture fixture = new Fixture(true, Behavior.TLS_CORRUPTION);
                SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.NONE, tls.client, "localhost")) {
            Result result = channel.send(request(Instant.now().plusSeconds(5), false));
            assertThat(fixture.data.get()).isNotNull();
            assertThat(result.outcome()).isEqualTo(Outcome.UNKNOWN);
            assertThat(result.action()).isEqualTo(Action.RETRY);
        }
    }
    @Test void knownFinalAcceptanceSurvivesTimeoutDuringResourceCleanup() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var first = new java.util.concurrent.atomic.AtomicBoolean(true);
        javax.net.SocketFactory sockets = new javax.net.SocketFactory() {
            @Override public Socket createSocket() {
                return new Socket() {
                    @Override public void close() throws IOException {
                        super.close();
                        if (first.compareAndSet(true, false)) {
                            entered.countDown();
                            while (release.getCount() > 0) { try { release.await(5, TimeUnit.SECONDS); } catch (InterruptedException controlledCancellation) { } }
                        }
                    }
                };
            }
            @Override public Socket createSocket(String h, int p) { throw new UnsupportedOperationException(); }
            @Override public Socket createSocket(String h, int p, InetAddress l, int q) { throw new UnsupportedOperationException(); }
            @Override public Socket createSocket(InetAddress h, int p) { throw new UnsupportedOperationException(); }
            @Override public Socket createSocket(InetAddress h, int p, InetAddress l, int q) { throw new UnsupportedOperationException(); }
        };
        try (Fixture fixture = new Fixture(true, Behavior.NORMAL);
                SmtpChannel channel = new SmtpChannel(AlertChannelPolicy.Deployment.ON_PREM, BINDING, "localhost", new InetSocketAddress(InetAddress.getLoopbackAddress(), fixture.port()),
                        SmtpChannel.Encryption.IMPLICIT_TLS, tls.client, SmtpChannel.Authentication.NONE, null, null, FROM, TO, Clock.systemUTC(), sockets)) {
            Result result = channel.send(request(Instant.now().plusSeconds(2), false));
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.data.get()).isNotNull();
            assertThat(result.outcome()).isEqualTo(Outcome.ACCEPTED);
        } finally { release.countDown(); }
    }
    @Test void multilineAndWholeAttemptReplyBudgetsAreEnforcedBeforeSendingMail() throws Exception {
        for (Behavior behavior : List.of(Behavior.OVERSIZE_REPLY, Behavior.TOTAL_REPLY_BUDGET)) {
            try (Fixture fixture = new Fixture(true, behavior);
                    SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.PLAIN, tls.client, "localhost")) {
                Result result = channel.send(request(Instant.now().plusSeconds(5), false));
                assertThat(result.outcome()).isEqualTo(Outcome.NOT_ACCEPTED);
                assertThat(result.action()).isEqualTo(Action.STOP);
                assertThat(fixture.data.get()).isNull(); idle(channel);
            }
        }
    }
    @Test void blockedDataWritesAndMissingFinalRepliesStopAtTheOriginalDeadline() throws Exception {
        for (Behavior behavior : List.of(Behavior.BLOCK_DATA, Behavior.STALL_FINAL)) {
            try (Fixture fixture = new Fixture(true, behavior);
                    SmtpChannel channel = channel(fixture, SmtpChannel.Encryption.IMPLICIT_TLS, SmtpChannel.Authentication.NONE, tls.client, "localhost")) {
                Request request = request(Instant.now().plusSeconds(2), behavior == Behavior.BLOCK_DATA);
                long started = System.nanoTime();
                Result result = channel.send(request);
                assertThat(fixture.dataStarted.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(result.outcome()).isEqualTo(Outcome.UNKNOWN);
                assertThat(result.phase()).isEqualTo(Phase.DEADLINE);
                assertThat(System.nanoTime() - started).isLessThan(TimeUnit.SECONDS.toNanos(4));
                assertThat(fixture.calls).hasValue(1); idle(channel);
            }
        }
    }
    static void idle(SmtpChannel channel) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while ((channel.activeCalls() != 0 || channel.pendingTimers() != 0) && System.nanoTime() < until) { java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1)); }
        assertThat(channel.activeCalls()).isZero(); assertThat(channel.pendingTimers()).isZero();
    }
    enum Behavior { NORMAL, NO_STARTTLS, DISCONNECT_AFTER_DATA, FINAL_BARE_LF, FINAL_CODE_ONLY, TLS_CORRUPTION, OVERSIZE_REPLY, TOTAL_REPLY_BUDGET, BLOCK_DATA, STALL_FINAL }
    static final class Fixture implements AutoCloseable {
        final ServerSocket server;
        final Thread thread;
        final Behavior behavior;
        final AtomicReference<Socket> transport = new AtomicReference<>();
        final AtomicReference<Socket> active = new AtomicReference<>();
        final AtomicReference<String> data = new AtomicReference<>();
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger authBeforeTls = new AtomicInteger();
        final List<String> commands = new CopyOnWriteArrayList<>();
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch dataStarted = new CountDownLatch(1);
        Fixture(boolean implicit, Behavior behavior) throws Exception {
            this.behavior = behavior;
            server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress());
            thread = Thread.ofPlatform().daemon(true).start(() -> {
                try {
                    Socket socket = server.accept(); transport.set(socket); active.set(socket); calls.incrementAndGet(); socket.setSoTimeout(5000); socket.setReceiveBufferSize(1024);
                    boolean encrypted = implicit;
                    if (implicit) {
                        SSLSocket wrapped = (SSLSocket) tls.server.getSocketFactory().createSocket(socket, "localhost", socket.getPort(), true);
                        wrapped.setUseClientMode(false); active.set(wrapped); socket = wrapped; wrapped.setSoTimeout(5000); wrapped.startHandshake();
                    }
                    InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream();
                    reply(out, "220 ", "fixture");
                    while (!socket.isClosed()) {
                        String command = WebhookChannelTest.line(in); commands.add(command);
                        if (command.startsWith("EHLO ")) {
                            if (behavior == Behavior.OVERSIZE_REPLY) { write(out, "250-" + "x".repeat(17000) + "\r\n250 done\r\n"); }
                            else {
                                write(out, "250-fixture\r\n");
                                if (!encrypted && behavior != Behavior.NO_STARTTLS) { write(out, "250-STARTTLS\r\n"); }
                                write(out, "250-AUTH PLAIN LOGIN\r\n"); reply(out, "250 ", "ready");
                            }
                        } else if (command.equals("STARTTLS")) {
                            reply(out, "220 ", "ready");
                            SSLSocket upgrade = (SSLSocket) tls.server.getSocketFactory().createSocket(socket, "localhost", socket.getPort(), true);
                            upgrade.setUseClientMode(false); active.set(upgrade); upgrade.setSoTimeout(5000); upgrade.startHandshake();
                            socket = upgrade; in = socket.getInputStream(); out = socket.getOutputStream(); encrypted = true;
                        } else if (command.startsWith("AUTH PLAIN")) {
                            if (!encrypted) { authBeforeTls.incrementAndGet(); }
                            if (behavior == Behavior.TOTAL_REPLY_BUDGET) { reply(out, "334 ", "continue"); WebhookChannelTest.line(in); }
                            reply(out, "235 ", "authenticated");
                        } else if (command.equals("AUTH LOGIN")) {
                            if (!encrypted) { authBeforeTls.incrementAndGet(); }
                            write(out, "334 VXNlcm5hbWU6\r\n"); WebhookChannelTest.line(in);
                            write(out, "334 UGFzc3dvcmQ6\r\n"); WebhookChannelTest.line(in); reply(out, "235 ", "authenticated");
                        } else if (command.startsWith("MAIL FROM:") || command.startsWith("RCPT TO:")) { reply(out, "250 ", "accepted-envelope"); }
                        else if (command.equals("DATA")) {
                            reply(out, "354 ", "continue-data"); dataStarted.countDown();
                            if (behavior == Behavior.BLOCK_DATA) { release.await(5, TimeUnit.SECONDS); return; }
                            StringBuilder message = new StringBuilder();
                            for (String line = WebhookChannelTest.line(in); !line.equals("."); line = WebhookChannelTest.line(in)) {
                                if (line.startsWith("..")) { line = line.substring(1); }
                                message.append(line).append("\r\n"); if (message.length() > 512 * 1024) { throw new IOException("fixture body budget"); }
                            }
                            data.set(message.toString());
                            if (behavior == Behavior.DISCONNECT_AFTER_DATA) { return; }
                            if (behavior == Behavior.TLS_CORRUPTION) {
                                transport.get().getOutputStream().write(new byte[]{23, 3, 3, 0, 1, 0}); transport.get().getOutputStream().flush(); return;
                            }
                            if (behavior == Behavior.STALL_FINAL) { in.read(); return; }
                            write(out, behavior == Behavior.FINAL_BARE_LF ? "250 accepted\n" : behavior == Behavior.FINAL_CODE_ONLY ? "250\r\n" : "250 accepted\r\n"); return;
                        } else { write(out, "500 unsupported\r\n"); return; }
                    }
                } catch (Exception expectedNegativeFixture) { }
                finally { Socket raw = transport.get(); if (raw != null) { try { raw.close(); } catch (IOException ignored) { } }
                    Socket socket = active.get(); if (socket != null) { try { socket.close(); } catch (IOException ignored) { } } }
            });
        }
        int port() { return server.getLocalPort(); }
        void reply(OutputStream out, String code, String text) throws IOException {
            write(out, code + (behavior == Behavior.TOTAL_REPLY_BUDGET ? "x".repeat(15000) : text) + "\r\n");
        }
        static void write(OutputStream out, String value) throws IOException { out.write(value.getBytes(StandardCharsets.US_ASCII)); out.flush(); }
        @Override public void close() throws Exception {
            release.countDown(); server.close(); Socket raw = transport.get(); if (raw != null) { raw.close(); } Socket socket = active.get(); if (socket != null) { socket.close(); }
            thread.join(1000);
        }
    }
}
