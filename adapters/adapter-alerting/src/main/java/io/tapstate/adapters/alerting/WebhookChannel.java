package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlertError;
import io.tapstate.core.alerting.AlertChannelPolicy;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

/** A bounded single HTTPS POST using a frozen target. No body is retained or drained after valid headers. */
public final class WebhookChannel implements NotificationChannel {
    private static final AlertChannelPolicy.Capability CAPABILITY = new AlertChannelPolicy.Capability("webhook", 1, AlertChannelPolicy.Family.OP_EXTERNAL);
    public record Origin(String host, int port) {
        public Origin {
            if (host == null || host.isBlank() || host.length() > 253 || port < 1 || port > 65535) { throw invalid("origin"); }
            host = host.toLowerCase(Locale.ROOT);
        }
    }
    private final TargetBinding binding;
    private final URI endpoint;
    private final SSLContext ssl;
    private final Clock clock;
    private final String bearer;
    private final byte[] signingKey;
    private final Semaphore slots = new Semaphore(2);
    private final Set<HttpClient> clients = ConcurrentHashMap.newKeySet();
    private final ThreadPoolExecutor executor;
    private volatile boolean closed;

    /** Credentials are resolved by the host outside its transaction; equivalent rotation keeps bindingId. */
    public WebhookChannel(AlertChannelPolicy.Deployment deployment, TargetBinding binding, URI endpoint, Set<Origin> allowedOrigins, SSLContext ssl,
            String bearer, byte[] signingKey, Clock clock) {
        AlertChannelPolicy.requireDeliverable(deployment, CAPABILITY);
        requireJvmGuards();
        this.binding = Objects.requireNonNull(binding, "binding");
        this.ssl = Objects.requireNonNull(ssl, "ssl"); this.clock = Objects.requireNonNull(clock, "clock");
        if (endpoint == null || !"https".equalsIgnoreCase(endpoint.getScheme()) || endpoint.getHost() == null
                || endpoint.getUserInfo() != null || endpoint.getFragment() != null || endpoint.toASCIIString().length() > 2048
                || !allowedOrigins.contains(new Origin(endpoint.getHost(), endpoint.getPort() < 0 ? 443 : endpoint.getPort()))) { throw invalid("endpoint"); }
        if (bearer != null && (bearer.isBlank() || bearer.length() > 4096 || bearer.codePoints().anyMatch(Character::isISOControl))) { throw invalid("authorization"); }
        if (signingKey != null && (signingKey.length < 16 || signingKey.length > 4096)) { throw invalid("signingKey"); }
        this.endpoint = endpoint; this.bearer = bearer; this.signingKey = signingKey == null ? null : signingKey.clone();
        executor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(64), task -> {
            Thread thread = new Thread(task, "tapstate-alert-webhook"); thread.setDaemon(true); return thread;
        }, new ThreadPoolExecutor.AbortPolicy());
    }
    @Override public AlertChannelPolicy.Capability capability() { return CAPABILITY; }
    @Override public Result send(Request request) throws InterruptedException {
        if (!binding.matches(request) || !request.destination().channelType().equals(typeId())
                || !request.contentType().equals("application/json") || request.payload().length > 64 * 1024) { throw invalid("frozenRequest"); }
        reap();
        if (closed || !slots.tryAcquire()) { return local(Outcome.NOT_ACCEPTED, Action.PAUSE, Phase.CAPACITY); }
        HttpClient client = null;
        CompletableFuture<?> future = null;
        AtomicReference<Result> headers = new AtomicReference<>();
        try {
            Duration remaining = remaining(request.deadline());
            if (remaining.isZero()) { return local(Outcome.NOT_ACCEPTED, Action.STOP, Phase.DEADLINE); }
            client = HttpClient.newBuilder().executor(executor).sslContext(ssl).followRedirects(HttpClient.Redirect.NEVER)
                    .version(HttpClient.Version.HTTP_1_1).connectTimeout(remaining.compareTo(Duration.ofSeconds(5)) < 0 ? remaining : Duration.ofSeconds(5))
                    .proxy(new ProxySelector() {
                        @Override public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
                        @Override public void connectFailed(URI uri, SocketAddress address, IOException failure) { }
                    }).build();
            clients.add(client);
            byte[] payload = request.payload();
            HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint).timeout(remaining)
                    .header("Content-Type", "application/json").header("X-Tapstate-Delivery-Id", request.deliveryId())
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload));
            if (bearer != null) { builder.header("Authorization", "Bearer " + bearer); }
            if (signingKey != null) {
                String timestamp = Long.toString(clock.instant().getEpochSecond());
                builder.header("X-Tapstate-Timestamp", timestamp).header("X-Tapstate-Signature", signature(timestamp, request.deliveryId(), payload));
            }
            remaining = remaining(request.deadline());
            if (remaining.isZero()) { return local(Outcome.NOT_ACCEPTED, Action.STOP, Phase.DEADLINE); }
            builder.timeout(remaining);
            future = client.sendAsync(builder.build(), response -> {
                Instant receivedAt = clock.instant();
                headers.set(ChannelFailureClassifier.http(response.statusCode(), retryAfter(response.headers().firstValue("Retry-After").orElse(null), receivedAt)));
                return new CancelBody();
            });
            Duration wait = remaining(request.deadline());
            if (wait.isZero()) { future.cancel(true); return headers.get() != null ? headers.get() : local(Outcome.UNKNOWN, Action.RETRY, Phase.DEADLINE); }
            future.get(wait.toNanos(), TimeUnit.NANOSECONDS);
            return Objects.requireNonNull(headers.get(), "completed response must have valid headers");
        } catch (TimeoutException ex) {
            if (future != null) { future.cancel(true); }
            return headers.get() != null ? headers.get() : local(Outcome.UNKNOWN, Action.RETRY, Phase.DEADLINE);
        } catch (ExecutionException ex) {
            if (headers.get() != null) { return headers.get(); }
            Throwable cause = ex.getCause();
            while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) { cause = cause.getCause(); }
            if (cause instanceof SSLHandshakeException) { return local(Outcome.NOT_ACCEPTED, Action.STOP, Phase.TLS); }
            if (cause instanceof ConnectException || cause instanceof HttpConnectTimeoutException || cause instanceof UnknownHostException) {
                return local(Outcome.NOT_ACCEPTED, Action.RETRY, Phase.CONNECT);
            }
            if (cause instanceof java.util.concurrent.RejectedExecutionException) { return local(Outcome.UNKNOWN, Action.PAUSE, Phase.CAPACITY); }
            if (cause instanceof IOException) { return local(Outcome.UNKNOWN, Action.RETRY, Phase.FINAL_RESPONSE); }
            if (cause instanceof RuntimeException bug) { throw bug; }
            if (cause instanceof Error bug) { throw bug; }
            throw new IllegalStateException("unexpected HTTP client failure", cause);
        } catch (java.util.concurrent.CancellationException ex) {
            return headers.get() != null ? headers.get() : local(Outcome.UNKNOWN, Action.STOP, Phase.DEADLINE);
        } catch (java.util.concurrent.RejectedExecutionException ex) {
            return headers.get() != null ? headers.get() : local(Outcome.UNKNOWN, Action.PAUSE, Phase.CAPACITY);
        } finally {
            if (future != null && !future.isDone()) { future.cancel(true); }
            if (client == null) { slots.release(); }
            else {
                client.shutdownNow();
                try { client.awaitTermination(Duration.ofSeconds(1)); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                releaseIfTerminated(client);
            }
        }
    }
    public int activeAttempts() { reap(); return clients.size(); }
    public int queuedClientTasks() { return executor.getQueue().size(); }
    @Override public void close() {
        closed = true; clients.forEach(HttpClient::shutdownNow); executor.shutdownNow();
        if (signingKey != null) { Arrays.fill(signingKey, (byte) 0); }
    }
    private void reap() { clients.forEach(this::releaseIfTerminated); }
    private void releaseIfTerminated(HttpClient client) { if (client.isTerminated() && clients.remove(client)) { slots.release(); } }
    private Duration remaining(Instant deadline) {
        Duration value = Duration.between(clock.instant(), deadline);
        if (value.isNegative() || value.isZero()) { return Duration.ZERO; }
        if (value.compareTo(Duration.ofDays(1)) > 0) { throw invalid("attemptDeadline"); }
        return value;
    }
    private String signature(String time, String id, byte[] payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256"); mac.init(new SecretKeySpec(signingKey, "HmacSHA256"));
            mac.update((time + "\n" + id + "\n").getBytes(StandardCharsets.UTF_8));
            return "v1=" + HexFormat.of().formatHex(mac.doFinal(payload));
        } catch (GeneralSecurityException ex) { throw new IllegalStateException("required signing algorithm unavailable", ex); }
    }
    public static Instant retryAfter(String value, Instant receivedAt) {
        if (value == null) { return null; }
        try {
            if (value.matches("[0-9]{1,10}")) { return receivedAt.plusSeconds(Long.parseLong(value)); }
            return ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
        } catch (DateTimeParseException | ArithmeticException ex) { return null; }
    }
    private static void requireJvmGuards() {
        Map<String, String> startup = new java.util.HashMap<>();
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (argument.startsWith("-D")) {
                int equals = argument.indexOf('=');
                startup.put(equals < 0 ? argument.substring(2) : argument.substring(2, equals),
                        equals < 0 ? "" : argument.substring(equals + 1));
            }
        }
        int launchLimit;
        int currentLimit;
        try {
            launchLimit = Integer.parseInt(startup.getOrDefault("jdk.http.maxHeaderSize", ""));
            currentLimit = Integer.parseInt(System.getProperty("jdk.http.maxHeaderSize", ""));
        } catch (NumberFormatException ex) { throw invalid("httpJvmGuards"); }
        if (launchLimit <= 0 || launchLimit > 16384 || currentLimit != launchLimit
                || !"true".equalsIgnoreCase(startup.get("jdk.httpclient.disableRetryConnect"))
                || !Boolean.getBoolean("jdk.httpclient.disableRetryConnect")
                || !"false".equalsIgnoreCase(startup.get("jdk.httpclient.enableAllMethodRetry"))
                || !"false".equalsIgnoreCase(System.getProperty("jdk.httpclient.enableAllMethodRetry"))
                || startup.containsKey("jdk.internal.httpclient.disableHostnameVerification")
                || System.getProperties().containsKey("jdk.internal.httpclient.disableHostnameVerification")) { throw invalid("httpJvmGuards"); }
    }
    private static Result local(Outcome outcome, Action action, Phase phase) { return new Result(outcome, action, null, phase, 0, "webhook"); }
    private static TapstateException invalid(String field) {
        return new TapstateException(AlertError.INVALID_CONFIGURATION, Map.of("field", field, "reason", "unsupported-or-untrusted-channel-configuration"), null);
    }
    private static final class CancelBody implements HttpResponse.BodySubscriber<Void> {
        @Override public CompletionStage<Void> getBody() { return CompletableFuture.completedFuture(null); }
        @Override public void onSubscribe(Flow.Subscription subscription) { subscription.cancel(); }
        @Override public void onNext(List<ByteBuffer> ignored) { }
        @Override public void onError(Throwable ignored) { }
        @Override public void onComplete() { }
    }
}
