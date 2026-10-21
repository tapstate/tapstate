package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.AlertChannelPolicy;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;

/** One SMTP submission to one frozen address, over mandatory TLS. No MX lookup or internal retry. */
public final class SmtpChannel implements NotificationChannel {
    private static final AlertChannelPolicy.Capability CAPABILITY = new AlertChannelPolicy.Capability("email", 1, AlertChannelPolicy.Family.OP_EXTERNAL);
    public enum Encryption { IMPLICIT_TLS, STARTTLS_REQUIRED }
    public enum Authentication { NONE, PLAIN, LOGIN }
    private final TargetBinding binding;
    private final String host;
    private final InetSocketAddress address;
    private final Encryption encryption;
    private final Authentication authentication;
    private final SSLContext ssl;
    private final String username;
    private final char[] password;
    private final String from;
    private final String recipient;
    private final Clock clock;
    private final javax.net.SocketFactory socketFactory;
    private final ThreadPoolExecutor workers;
    private final ScheduledThreadPoolExecutor deadlines;
    private final Set<Call> calls = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    /** Address resolution and outbound authorization must finish before the host grants this attempt. */
    public SmtpChannel(AlertChannelPolicy.Deployment deployment, TargetBinding binding, String host, InetSocketAddress approvedAddress, Encryption encryption,
            SSLContext ssl, Authentication authentication, String username, char[] password,
            String from, String recipient, Clock clock) {
        this(deployment, binding, host, approvedAddress, encryption, ssl, authentication, username, password, from, recipient, clock, javax.net.SocketFactory.getDefault());
    }
    SmtpChannel(AlertChannelPolicy.Deployment deployment, TargetBinding binding, String host, InetSocketAddress approvedAddress, Encryption encryption,
            SSLContext ssl, Authentication authentication, String username, char[] password,
            String from, String recipient, Clock clock, javax.net.SocketFactory socketFactory) {
        AlertChannelPolicy.requireDeliverable(deployment, CAPABILITY);
        this.socketFactory = Objects.requireNonNull(socketFactory, "socketFactory");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.encryption = Objects.requireNonNull(encryption, "encryption");
        this.authentication = Objects.requireNonNull(authentication, "authentication");
        this.ssl = Objects.requireNonNull(ssl, "ssl"); this.clock = Objects.requireNonNull(clock, "clock");
        if (host == null || !host.matches("[A-Za-z0-9._:-]{1,253}") || approvedAddress == null
                || approvedAddress.isUnresolved() || approvedAddress.getPort() <= 0 || !EmailMime.mailbox(from) || !EmailMime.mailbox(recipient)) { throw invalid("smtpEndpoint"); }
        if (authentication == Authentication.NONE ? username != null || password != null
                : username == null || username.isBlank() || username.length() > 256 || username.codePoints().anyMatch(Character::isISOControl)
                    || password == null || password.length == 0 || password.length > 4096) { throw invalid("smtpAuthentication"); }
        if (password != null) { for (char c : password) { if (c == 0 || c == '\r' || c == '\n') { throw invalid("smtpAuthentication"); } } }
        this.host = host; this.address = approvedAddress; this.username = username;
        this.password = password == null ? null : password.clone(); this.from = from; this.recipient = recipient;
        workers = new ThreadPoolExecutor(0, 2, 1, TimeUnit.SECONDS, new SynchronousQueue<>(), task -> {
            Thread thread = new Thread(task, "tapstate-alert-smtp"); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
        deadlines = new ScheduledThreadPoolExecutor(1, task -> { Thread thread = new Thread(task, "tapstate-alert-smtp-deadline"); thread.setDaemon(true); return thread; });
        deadlines.setRemoveOnCancelPolicy(true);
    }
    @Override public AlertChannelPolicy.Capability capability() { return CAPABILITY; }
    @Override public Result send(Request request) throws InterruptedException {
        if (!binding.matches(request) || !request.destination().channelType().equals(typeId()) || !request.contentType().equals("message/rfc822")) { throw invalid("frozenRequest"); }
        byte[] payload = request.payload();
        String header = new String(payload, 0, Math.min(payload.length, 8192), StandardCharsets.US_ASCII);
        if (!header.contains("\r\nFrom: <" + from + ">\r\n") || !header.contains("\r\nTo: <" + recipient + ">\r\n")) { throw invalid("frozenMimeTarget"); }
        if (closed) { return result(Outcome.NOT_ACCEPTED, Action.PAUSE, Phase.CAPACITY, 0); }
        if (remainingNanos(request) <= 0) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.DEADLINE, 0); }
        Call call = new Call(request);
        Future<Result> future;
        try { future = workers.submit(() -> protocol(call, payload)); }
        catch (RejectedExecutionException ex) { return result(Outcome.NOT_ACCEPTED, Action.PAUSE, Phase.CAPACITY, 0); }
        try {
            long nanos = remainingNanos(request);
            if (nanos <= 0) { call.abort(); future.cancel(true); return knownOr(call, result(Outcome.UNKNOWN, Action.RETRY, Phase.DEADLINE, 0)); }
            return future.get(nanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException ex) {
            call.abort(); future.cancel(true); call.finished.await(1, TimeUnit.SECONDS);
            return knownOr(call, result(call.submitted ? Outcome.UNKNOWN : Outcome.NOT_ACCEPTED, Action.RETRY, Phase.DEADLINE, 0));
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException bug) { throw bug; }
            if (ex.getCause() instanceof Error bug) { throw bug; }
            throw new IllegalStateException("unexpected SMTP worker failure", ex.getCause());
        } catch (java.util.concurrent.CancellationException ex) { return knownOr(call, result(Outcome.UNKNOWN, Action.STOP, Phase.DEADLINE, 0)); }
        catch (InterruptedException ex) { call.abort(); future.cancel(true); throw ex; }
    }
    private Result protocol(Call call, byte[] payload) {
        calls.add(call);
        try {
            Result result = exchange(call, payload);
            call.evidence.set(result);
            return result;
        } finally {
            call.abort(); if (call.watchdog != null) { call.watchdog.cancel(false); }
            calls.remove(call); call.finished.countDown();
        }
    }
    private Result knownOr(Call call, Result fallback) {
        Result known = call.evidence.get();
        return known == null ? fallback : known;
    }
    private Result exchange(Call call, byte[] payload) {
        try {
            call.check();
            Socket socket = socketFactory.createSocket(); socket.setSendBufferSize(16 * 1024); socket.setReceiveBufferSize(16 * 1024);
            call.rawSocket.set(socket); call.socket.set(socket);
            call.watchdog = deadlines.schedule(call::abort, remainingNanos(call.request), TimeUnit.NANOSECONDS);
            call.check(); socket.connect(address, call.millis());
            call.input = socket.getInputStream(); call.output = socket.getOutputStream();
            if (encryption == Encryption.IMPLICIT_TLS) { call.tls(); }
            call.phase = Phase.CONNECT; call.expect(220);
            call.phase = Phase.ENVELOPE; call.command("EHLO tapstate.invalid"); Reply capabilities = call.expect(250);
            if (encryption == Encryption.STARTTLS_REQUIRED) {
                call.phase = Phase.TLS;
                if (!capabilities.supports("STARTTLS")) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.TLS, 0); }
                call.command("STARTTLS"); call.expect(220); call.tls();
                call.phase = Phase.ENVELOPE; call.command("EHLO tapstate.invalid"); capabilities = call.expect(250);
            }
            if (authentication != Authentication.NONE) {
                call.phase = Phase.AUTHENTICATION;
                if (!capabilities.supports("AUTH " + authentication.name())) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.AUTHENTICATION, 0); }
                if (authentication == Authentication.PLAIN) {
                    String value = Base64.getEncoder().encodeToString(("\0" + username + "\0" + new String(password)).getBytes(StandardCharsets.UTF_8));
                    call.command("AUTH PLAIN " + value); Reply reply = call.expect(235, 334);
                    if (reply.code == 334) { call.command(value); call.expect(235); }
                } else {
                    call.command("AUTH LOGIN"); call.expect(334);
                    call.command(Base64.getEncoder().encodeToString(username.getBytes(StandardCharsets.UTF_8))); call.expect(334);
                    call.command(Base64.getEncoder().encodeToString(new String(password).getBytes(StandardCharsets.UTF_8))); call.expect(235);
                }
            }
            call.phase = Phase.ENVELOPE; call.command("MAIL FROM:<" + from + ">"); call.expect(250);
            call.command("RCPT TO:<" + recipient + ">"); call.expect(250, 251, 252);
            call.phase = Phase.BODY; call.command("DATA"); call.expect(354);
            byte[] data = dotStuff(payload);
            for (int offset = 0; offset < data.length; offset += 4096) {
                call.check(); call.submitted = true;
                call.output.write(data, offset, Math.min(4096, data.length - offset));
            }
            call.output.flush(); call.phase = Phase.FINAL_RESPONSE;
            Reply finalReply = call.read();
            return ChannelFailureClassifier.smtp(Phase.FINAL_RESPONSE, finalReply.code)
                    .orElseGet(() -> result(Outcome.UNKNOWN, Action.STOP, Phase.FINAL_RESPONSE, finalReply.code));
        } catch (Stop terminal) { return terminal.result; }
        catch (IOException ex) {
            if (call.aborted.get() || remainingNanos(call.request) <= 0) {
                return result(call.submitted ? Outcome.UNKNOWN : Outcome.NOT_ACCEPTED, Action.RETRY, Phase.DEADLINE, 0);
            }
            if (!call.submitted && ex instanceof javax.net.ssl.SSLException) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.TLS, 0); }
            return result(call.submitted ? Outcome.UNKNOWN : Outcome.NOT_ACCEPTED,
                    ex instanceof ReplyBudgetException ? Action.STOP : Action.RETRY, call.phase, 0);
        }
    }
    public int activeCalls() { return workers.getActiveCount(); }
    public int pendingTimers() { return deadlines.getQueue().size(); }
    @Override public void close() {
        closed = true; calls.forEach(Call::abort); workers.shutdownNow(); deadlines.shutdownNow();
        if (password != null) { Arrays.fill(password, (char) 0); }
    }
    private long remainingNanos(Request request) {
        Duration value = Duration.between(clock.instant(), request.deadline());
        if (value.isNegative() || value.isZero()) { return 0; }
        if (value.compareTo(Duration.ofDays(1)) > 0) { throw invalid("attemptDeadline"); }
        return value.toNanos();
    }
    private final class Call {
        final Request request;
        final AtomicReference<Result> evidence = new AtomicReference<>();
        volatile ScheduledFuture<?> watchdog;
        final AtomicReference<Socket> rawSocket = new AtomicReference<>();
        final AtomicReference<Socket> socket = new AtomicReference<>();
        final AtomicBoolean aborted = new AtomicBoolean();
        final CountDownLatch finished = new CountDownLatch(1);
        volatile boolean submitted;
        Phase phase = Phase.CONNECT;
        InputStream input;
        OutputStream output;
        int replyBytes;
        Call(Request request) { this.request = request; }
        void check() throws SocketTimeoutException {
            if (aborted.get() || remainingNanos(request) <= 0) { throw new SocketTimeoutException("SMTP attempt deadline"); }
        }
        int millis() throws SocketTimeoutException { check(); return (int) Math.min(Integer.MAX_VALUE, TimeUnit.NANOSECONDS.toMillis(remainingNanos(request)) + 1); }
        void tls() throws IOException {
            check(); phase = Phase.TLS;
            SSLSocket encrypted = (SSLSocket) ssl.getSocketFactory().createSocket(socket.get(), host, address.getPort(), true);
            socket.set(encrypted); check();
            var parameters = encrypted.getSSLParameters(); parameters.setEndpointIdentificationAlgorithm("HTTPS");
            parameters.setProtocols(Arrays.stream(encrypted.getSupportedProtocols()).filter(p -> p.equals("TLSv1.3") || p.equals("TLSv1.2")).toArray(String[]::new));
            encrypted.setSSLParameters(parameters); encrypted.setSoTimeout(millis()); encrypted.startHandshake();
            input = encrypted.getInputStream(); output = encrypted.getOutputStream();
        }
        void command(String command) throws IOException { check(); output.write((command + "\r\n").getBytes(StandardCharsets.UTF_8)); output.flush(); }
        Reply expect(int... allowed) throws IOException, Stop {
            Reply reply = read();
            for (int code : allowed) { if (code == reply.code) { return reply; } }
            throw new Stop(ChannelFailureClassifier.smtp(phase, reply.code)
                    .orElseGet(() -> result(Outcome.NOT_ACCEPTED, Action.STOP, phase, reply.code)));
        }
        Reply read() throws IOException {
            int currentBytes = 0; Integer code = null; List<String> lines = new ArrayList<>();
            while (true) {
                StringBuilder line = new StringBuilder(); boolean carriage = false;
                while (true) {
                    check(); socket.get().setSoTimeout(millis());
                    if (currentBytes >= 16 * 1024 || replyBytes >= 64 * 1024) { throw new ReplyBudgetException(); }
                    int next = input.read(); if (next < 0) { throw new IOException("SMTP peer closed"); }
                    currentBytes++; replyBytes++;
                    if (carriage) { if (next != '\n') { throw new ReplyBudgetException(); } break; }
                    if (next == '\r') { carriage = true; continue; }
                    if (next < 32 && next != 9 || next > 126) { throw new ReplyBudgetException(); }
                    line.append((char) next);
                }
                String value = line.toString();
                if (value.length() < 3 || !value.substring(0, 3).matches("[2-5][0-9]{2}") || value.length() > 3 && value.charAt(3) != ' ' && value.charAt(3) != '-') { throw new ReplyBudgetException(); }
                int parsed = Integer.parseInt(value.substring(0, 3));
                if (code != null && parsed != code) { throw new ReplyBudgetException(); }
                code = parsed; lines.add(value.length() > 3 ? value.substring(4) : "");
                if (value.length() == 3 || value.charAt(3) == ' ') { return new Reply(code, lines); }
            }
        }
        void abort() {
            aborted.set(true); Socket raw = rawSocket.get();
            if (raw != null) { try { raw.close(); } catch (IOException ignoredClose) { } }
            Socket current = socket.get();
            if (current != null) { try { current.close(); } catch (IOException ignoredClose) { } }
        }
    }
    private record Reply(int code, List<String> lines) {
        boolean supports(String capability) {
            if (capability.startsWith("AUTH ")) {
                String mechanism = capability.substring(5);
                return lines.stream().map(s -> s.toUpperCase(Locale.ROOT)).anyMatch(s -> s.startsWith("AUTH ") && Arrays.asList(s.substring(5).split(" +")).contains(mechanism));
            }
            return lines.stream().anyMatch(line -> line.equalsIgnoreCase(capability));
        }
    }
    private static byte[] dotStuff(byte[] payload) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(payload.length + 3); boolean lineStart = true;
        for (byte b : payload) { if (lineStart && b == '.') { bytes.write('.'); } bytes.write(b); lineStart = b == '\n'; }
        if (payload.length < 2 || payload[payload.length - 2] != 13 || payload[payload.length - 1] != 10) { bytes.write(13); bytes.write(10); }
        bytes.write('.'); bytes.write(13); bytes.write(10); return bytes.toByteArray();
    }
    private static Result result(Outcome outcome, Action action, Phase phase, int status) { return new Result(outcome, action, null, phase, status, "smtp"); }
    private static TapstateException invalid(String field) { return new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", field, "reason", "unsupported-or-untrusted-channel-configuration"), null); }
    private static final class ReplyBudgetException extends IOException { }
    private static final class Stop extends Exception { final Result result; Stop(Result result) { this.result = result; } }
}
