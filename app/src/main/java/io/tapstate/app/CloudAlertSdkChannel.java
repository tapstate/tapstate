package io.tapstate.app;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapstate.cloud.sdk.ApiClient;
import io.tapstate.cloud.sdk.ApiException;
import io.tapstate.cloud.sdk.api.C2ClusterEventsApi;
import io.tapstate.cloud.sdk.model.ClusterEventRequest;
import io.tapstate.core.alerting.AlertChannelPolicy;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.spi.alerting.NotificationChannel;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.io.CloseMode;

/** Single-attempt SDK adapter. Durable admission, deduplication and DNS refresh belong to the host. */
final class CloudAlertSdkChannel implements NotificationChannel {
    private static final AlertChannelPolicy.Capability CAPABILITY = new AlertChannelPolicy.Capability("cloud-sdk", 2, AlertChannelPolicy.Family.CLOUD_SDK);
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Destination binding;
    private final URI origin;
    private final String token;
    private final String expectedOrg;
    private final InetAddress[] addresses;
    private final SSLContext ssl;
    private final Clock clock;
    private final Duration totalTimeout;
    private final Semaphore slots = new Semaphore(2);
    private final Set<CloseableHttpClient> clients = ConcurrentHashMap.newKeySet();
    private final ScheduledThreadPoolExecutor deadlines = new ScheduledThreadPoolExecutor(1, task -> {
        Thread t = new Thread(task, "tapstate-cloud-alert-deadline"); t.setDaemon(true); return t;
    });
    private final AtomicBoolean closed = new AtomicBoolean();

    CloudAlertSdkChannel(AlertChannelPolicy.Deployment deployment, Destination binding, URI origin, String token,
            String expectedOrg, InetAddress[] trustedAddresses, SSLContext ssl, Clock clock) {
        this(deployment, binding, origin, token, expectedOrg, trustedAddresses, ssl, clock, Duration.ofSeconds(30), false);
    }
    CloudAlertSdkChannel(AlertChannelPolicy.Deployment deployment, Destination binding, URI origin, String token,
            String expectedOrg, InetAddress[] trustedAddresses, SSLContext ssl, Clock clock, Duration timeout, boolean localFixture) {
        AlertChannelPolicy.requireDeliverable(deployment, CAPABILITY);
        this.binding = Objects.requireNonNull(binding); this.origin = Objects.requireNonNull(origin);
        this.ssl = Objects.requireNonNull(ssl); this.clock = Objects.requireNonNull(clock);
        if (!binding.channelType().equals("cloud-sdk") || !binding.scope().clusterId().matches("[a-fA-F0-9]{24}")
                || origin.getHost() == null || origin.getRawUserInfo() != null || origin.getRawQuery() != null || origin.getRawFragment() != null
                || !(origin.getPath().isEmpty() || origin.getPath().equals("/"))
                || !("https".equalsIgnoreCase(origin.getScheme()) || (localFixture && "http".equalsIgnoreCase(origin.getScheme()) && origin.getHost().equals("127.0.0.1")))
                || token == null || token.isBlank() || token.length() > 4096 || token.codePoints().anyMatch(Character::isISOControl)
                || trustedAddresses == null || trustedAddresses.length == 0 || trustedAddresses.length > 8
                || timeout == null || timeout.isZero() || timeout.isNegative() || timeout.compareTo(Duration.ofSeconds(30)) > 0
                || (expectedOrg != null && (!expectedOrg.matches("[a-fA-F0-9]{24}")))) { throw CloudAlertEventWire.invalid("sdkBinding"); }
        this.token = token; this.expectedOrg = expectedOrg; this.addresses = trustedAddresses.clone();
        for (InetAddress address : addresses) { Objects.requireNonNull(address); }
        this.totalTimeout = timeout;
        deadlines.setRemoveOnCancelPolicy(true);
    }
    @Override public AlertChannelPolicy.Capability capability() { return CAPABILITY; }
    @Override public Result send(Request request) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) { throw new InterruptedException(); }
        if (!binding.equals(request.destination()) || request.purpose() != Purpose.BUSINESS || !request.contentType().equals("application/json")
                || request.payload().length > CloudAlertEventWire.MAX_BYTES) { throw CloudAlertEventWire.invalid("frozenRequest"); }
        final ClusterEventRequest event;
        try {
            var tree = JSON.readTree(request.payload());
            if (!tree.isObject() || tree.size() != 6 || !tree.path("type").asText().startsWith("cluster.alert.")
                    || !tree.path("dedupeKey").asText().matches("[a-f0-9]{64}") || !tree.path("subject").isTextual()
                    || !tree.path("occurredAt").isTextual() || !tree.path("payload").isObject()
                    || tree.path("payload").path("wireVersion").asInt(-1) != CloudAlertEventWire.VERSION) { throw CloudAlertEventWire.invalid("eventPayload"); }
            event = JSON.treeToValue(tree, ClusterEventRequest.class);
            if (event.getSeverity() == null || event.getOccurredAt() == null) { throw CloudAlertEventWire.invalid("eventPayload"); }
        } catch (IOException bad) { throw CloudAlertEventWire.invalid("eventPayload"); }
        Duration remaining = Duration.between(clock.instant(), request.deadline());
        if (remaining.isNegative() || remaining.isZero()) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.DEADLINE, 0, null); }
        if (closed.get() || !slots.tryAcquire()) { return result(Outcome.NOT_ACCEPTED, Action.PAUSE, Phase.CAPACITY, 0, null); }
        CloseableHttpClient http = null;
        java.util.concurrent.ScheduledFuture<?> timer = null;
        java.util.concurrent.ScheduledFuture<?> interrupt = null;
        AtomicBoolean expired = new AtomicBoolean();
        try {
            if (closed.get()) { return result(Outcome.NOT_ACCEPTED, Action.PAUSE, Phase.CAPACITY, 0, null); }
            DnsResolver dns = new DnsResolver() {
                @Override public InetAddress[] resolve(String host) throws java.net.UnknownHostException {
                    if (!host.equalsIgnoreCase(origin.getHost())) { throw new java.net.UnknownHostException("unbound host"); }
                    return addresses.clone();
                }
                @Override public String resolveCanonicalHostname(String host) { return origin.getHost(); }
            };
            var manager = PoolingHttpClientConnectionManagerBuilder.create().setDnsResolver(dns)
                    .setConnectionFactory(org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory.builder()
                            .http1Config(org.apache.hc.core5.http.config.Http1Config.custom().setMaxHeaderCount(64).setMaxLineLength(8192).build()).build())
                    .setTlsSocketStrategy(ClientTlsStrategyBuilder.create().setSslContext(ssl).buildClassic())
                    .setMaxConnTotal(1).setMaxConnPerRoute(1).build();
            http = HttpClients.custom().setConnectionManager(manager).disableAutomaticRetries().disableRedirectHandling()
                    .disableCookieManagement().disableAuthCaching().build();
            clients.add(http);
            CloseableHttpClient active = http;
            Thread caller = Thread.currentThread();
            remaining = Duration.between(clock.instant(), request.deadline());
            if (remaining.isNegative() || remaining.isZero() || closed.get()) { return result(Outcome.NOT_ACCEPTED, Action.STOP, Phase.DEADLINE, 0, null); }
            Duration budget = remaining.compareTo(totalTimeout) < 0 ? remaining : totalTimeout;
            timer = deadlines.schedule(() -> { expired.set(true); active.close(CloseMode.IMMEDIATE); }, budget.toNanos(), TimeUnit.NANOSECONDS);
            interrupt = deadlines.scheduleAtFixedRate(() -> { if (caller.isInterrupted()) { active.close(CloseMode.IMMEDIATE); } }, 0, 10, TimeUnit.MILLISECONDS);
            long started = System.nanoTime();
            long millis = Math.max(1, budget.toMillis());
            ApiClient client = new BoundedApiClient(http).setBasePath(origin.toString().replaceAll("/$", ""))
                    .setConnectTimeout((int)Math.min(5000, millis)).setReadTimeout((int)Math.min(10000, millis));
            var response = new C2ClusterEventsApi(client).ingestClusterEvent(binding.scope().clusterId(), event, "Bearer " + token);
            if (Thread.currentThread().isInterrupted()) { throw new InterruptedException(); }
            var data = response == null ? null : response.getData();
            if (response == null || !"ok".equals(response.getCode()) || data == null
                    || !identifier(data.getId()) || !identifier(data.getOrgId()) || (expectedOrg != null && !expectedOrg.equals(data.getOrgId()))
                    || !"cluster".equals(data.getSource()) || !event.getType().equals(data.getType()) || !event.getDedupeKey().equals(data.getDedupeKey())
                    || !Integer.valueOf(1).equals(data.getSchemaVersion()) || data.getPayload() == null
                    || !binding.scope().clusterId().equals(data.getPayload().get("clusterId"))) { return result(Outcome.UNKNOWN, Action.STOP, Phase.FINAL_RESPONSE, 200, null); }
            String opId = response.getOpId();
            if (opId != null && (opId.isBlank() || opId.length() > 256 || opId.codePoints().anyMatch(Character::isISOControl) || !java.nio.charset.StandardCharsets.UTF_8.newEncoder().canEncode(opId))) {
                return result(Outcome.UNKNOWN, Action.STOP, Phase.FINAL_RESPONSE, 200, null);
            }
            return result(Outcome.ACCEPTED, Action.STOP, Phase.FINAL_RESPONSE, 200, new Receipt(data.getId(), opId));
        } catch (ApiException failure) {
            if (Thread.currentThread().isInterrupted()) { throw new InterruptedException(); }
            int status = Math.max(0, Math.min(599, failure.getCode()));
            boolean rejected = status >= 400 && status < 500 && status != 408;
            return result(rejected ? Outcome.NOT_ACCEPTED : Outcome.UNKNOWN, Action.STOP,
                    expired.get() ? Phase.DEADLINE : status == 401 || status == 403 ? Phase.AUTHENTICATION : Phase.FINAL_RESPONSE, status, null);
        } catch (IllegalStateException shutdown) {
            if (!expired.get() && !closed.get()) { throw shutdown; }
            return result(Outcome.UNKNOWN, Action.STOP, Phase.DEADLINE, 0, null);
        } catch (java.util.concurrent.RejectedExecutionException shutdown) {
            return result(Outcome.NOT_ACCEPTED, Action.PAUSE, Phase.CAPACITY, 0, null);
        } finally {
            if (timer != null) { timer.cancel(false); }
            if (interrupt != null) { interrupt.cancel(false); }
            if (http != null) { http.close(CloseMode.IMMEDIATE); clients.remove(http); }
            slots.release();
        }
    }
    private static boolean identifier(String value) { return value != null && value.matches("[a-fA-F0-9]{24}"); }
    private static Result result(Outcome outcome, Action action, Phase phase, int status, Receipt receipt) {
        return new Result(outcome, action, null, phase, status, "sdk-event-center", receipt);
    }
    int activeCalls() { return clients.size(); }
    int pendingDeadlineTasks() { return deadlines.getQueue().size(); }
    @Override public void close() {
        closed.set(true); for (var client : clients) { client.close(CloseMode.IMMEDIATE); } deadlines.shutdownNow();
    }
    private static final class BoundedApiClient extends ApiClient {
        private final CloseableHttpClient transport;
        BoundedApiClient(CloseableHttpClient client) { super(client); transport = client; }
        @Override protected <T> T processResponse(CloseableHttpResponse response, TypeReference<T> type) throws ApiException, IOException, org.apache.hc.core5.http.ParseException {
            try {
                int status = response.getCode();
                if (status < 200 || status >= 300) { throw new ApiException(status, "Cloud event rejected"); }
                if (response.getEntity() == null) { return null; }
                byte[] body = response.getEntity().getContent().readNBytes(CloudAlertEventWire.MAX_BYTES + 1);
                if (body.length > CloudAlertEventWire.MAX_BYTES) { throw new ApiException(0, "Cloud event response exceeds budget"); }
                transport.close(CloseMode.IMMEDIATE);
                response.close(CloseMode.IMMEDIATE);
                response.setEntity(new ByteArrayEntity(body, ContentType.APPLICATION_JSON));
                return super.processResponse(response, type);
            } finally {
                // Adapted responses can discard the close mode. Abort the owned transport before any response close.
                transport.close(CloseMode.IMMEDIATE);
                response.close(CloseMode.IMMEDIATE);
            }
        }
    }
}
