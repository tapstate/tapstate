package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.*;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.*;
import org.apache.hc.client5.http.impl.io.*;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;

/** One pinned HTTPS POST. Final headers decide acceptance; the body is never retained or drained. */
public final class WebhookChannel implements NotificationChannel {
    private static final AlertChannelPolicy.Capability CAPABILITY = new AlertChannelPolicy.Capability("webhook",1,AlertChannelPolicy.Family.OP_EXTERNAL);
    public record Origin(String host,int port) {
        public Origin { if(host==null||host.isBlank()||host.length()>253||port<1||port>65535)throw invalid("origin");host=host.toLowerCase(Locale.ROOT); }
    }
    private final TargetBinding binding; private final URI endpoint; private final SSLContext ssl;
    private final Clock clock; private final String bearer; private final byte[] signingKey; private final InetAddress[] addresses;
    private final Semaphore slots=new Semaphore(2);
    private final Set<CloseableHttpClient> clients=ConcurrentHashMap.newKeySet();
    private final ScheduledThreadPoolExecutor timers=new ScheduledThreadPoolExecutor(1,r->{var t=new Thread(r,"tapstate-alert-webhook-deadline");t.setDaemon(true);return t;});
    private volatile boolean closed;
    public WebhookChannel(AlertChannelPolicy.Deployment deployment,TargetBinding binding,URI endpoint,Set<Origin> allowed,
            SSLContext ssl,String bearer,byte[] signingKey,Clock clock) {
        this(deployment,binding,endpoint,allowed,ssl,bearer,signingKey,clock,null);
    }
    /** The host supplies its freshly approved DNS answer; connections never resolve a second address set. */
    public WebhookChannel(AlertChannelPolicy.Deployment deployment,TargetBinding binding,URI endpoint,Set<Origin> allowed,
            SSLContext ssl,String bearer,byte[] signingKey,Clock clock,InetAddress[] approved) {
        AlertChannelPolicy.requireDeliverable(deployment,CAPABILITY);
        this.binding=Objects.requireNonNull(binding);this.ssl=Objects.requireNonNull(ssl);this.clock=Objects.requireNonNull(clock);
        if(endpoint==null||!"https".equalsIgnoreCase(endpoint.getScheme())||endpoint.getHost()==null||endpoint.getUserInfo()!=null
                ||endpoint.getFragment()!=null||endpoint.toASCIIString().length()>2048
                ||!allowed.contains(new Origin(endpoint.getHost(),endpoint.getPort()<0?443:endpoint.getPort())))throw invalid("endpoint");
        if(bearer!=null&&(bearer.isBlank()||bearer.length()>4096||bearer.codePoints().anyMatch(Character::isISOControl)))throw invalid("authorization");
        if(signingKey!=null&&(signingKey.length<16||signingKey.length>4096))throw invalid("signingKey");
        if(approved!=null&&(approved.length==0||approved.length>8||Arrays.stream(approved).anyMatch(Objects::isNull)))throw invalid("addresses");
        this.endpoint=endpoint;this.bearer=bearer;this.signingKey=signingKey==null?null:signingKey.clone();this.addresses=approved==null?null:approved.clone();
        timers.setRemoveOnCancelPolicy(true);
    }
    @Override public AlertChannelPolicy.Capability capability(){return CAPABILITY;}
    @Override public Result send(Request request)throws InterruptedException {
        if(Thread.currentThread().isInterrupted())throw new InterruptedException();
        if(!binding.matches(request)||!request.destination().channelType().equals(typeId())||!request.contentType().equals("application/json")||request.payload().length>65536)throw invalid("frozenRequest");
        if(closed||!slots.tryAcquire())return local(Outcome.NOT_ACCEPTED,Action.PAUSE,Phase.CAPACITY);
        CloseableHttpClient client=null;ScheduledFuture<?> deadline=null,interrupt=null;Result known=null;
        var expired=new AtomicBoolean();var submitted=new AtomicBoolean();
        try {
            Duration remaining=Duration.between(clock.instant(),request.deadline());
            if(remaining.isNegative()||remaining.isZero())return local(Outcome.NOT_ACCEPTED,Action.STOP,Phase.DEADLINE);
            if(remaining.compareTo(Duration.ofSeconds(30))>0)remaining=Duration.ofSeconds(30);
            var manager=PoolingHttpClientConnectionManagerBuilder.create()
                    // At most fifteen 1000-byte header lines plus the status line: at most 16KiB before classification.
                    .setConnectionFactory(ManagedHttpClientConnectionFactory.builder().http1Config(Http1Config.custom().setMaxHeaderCount(15).setMaxLineLength(1000).build()).build())
                    .setTlsSocketStrategy(ClientTlsStrategyBuilder.create().setSslContext(ssl).setTlsVersions("TLSv1.3","TLSv1.2").buildClassic())
                    .setMaxConnTotal(1).setMaxConnPerRoute(1);
            if(addresses!=null)manager.setDnsResolver(new DnsResolver(){
                public InetAddress[] resolve(String host)throws UnknownHostException{if(!host.equalsIgnoreCase(endpoint.getHost()))throw new UnknownHostException("unbound host");return addresses.clone();}
                public String resolveCanonicalHostname(String host){return endpoint.getHost();}
            });
            client=HttpClients.custom().setConnectionManager(manager.build()).disableAutomaticRetries().disableRedirectHandling().disableCookieManagement().disableAuthCaching().build();
            clients.add(client);var active=client;var caller=Thread.currentThread();
            if(closed){client.close(CloseMode.IMMEDIATE);return local(Outcome.NOT_ACCEPTED,Action.PAUSE,Phase.CAPACITY);}
            deadline=timers.schedule(()->{expired.set(true);active.close(CloseMode.IMMEDIATE);},remaining.toNanos(),TimeUnit.NANOSECONDS);
            interrupt=timers.scheduleAtFixedRate(()->{if(caller.isInterrupted())active.close(CloseMode.IMMEDIATE);},0,10,TimeUnit.MILLISECONDS);
            var post=new HttpPost(endpoint);long millis=Math.max(1,remaining.toMillis());
            post.setConfig(RequestConfig.custom().setConnectTimeout(Timeout.ofMilliseconds(Math.min(5000,millis))).setResponseTimeout(Timeout.ofMilliseconds(millis)).setConnectionRequestTimeout(Timeout.ofMilliseconds(millis)).build());
            post.setHeader("Content-Type","application/json");post.setHeader("X-Tapstate-Delivery-Id",request.deliveryId());
            if(bearer!=null)post.setHeader("Authorization","Bearer "+bearer);
            var payload=request.payload();if(signingKey!=null){String stamp=Long.toString(clock.instant().getEpochSecond());post.setHeader("X-Tapstate-Timestamp",stamp);post.setHeader("X-Tapstate-Signature",signature(stamp,request.deliveryId(),payload));}
            post.setEntity(new ByteArrayEntity(payload,ContentType.APPLICATION_JSON));
            submitted.set(true);
            var response=client.execute(post);
            try {
                int bytes=32;for(var h:response.getHeaders())bytes+=h.getName().length()+h.getValue().length()+4;
                if(bytes>16384)return local(Outcome.UNKNOWN,Action.RETRY,Phase.FINAL_RESPONSE);
                var header=response.getFirstHeader("Retry-After");
                known=ChannelFailureClassifier.http(response.getCode(),retryAfter(header==null?null:header.getValue(),clock.instant()));
                return known;
            } finally { response.close(CloseMode.IMMEDIATE); }
        } catch(SSLHandshakeException failure){return known!=null?known:local(Outcome.NOT_ACCEPTED,Action.STOP,Phase.TLS);}
        catch(ConnectException|UnknownHostException failure){return known!=null?known:local(Outcome.NOT_ACCEPTED,Action.RETRY,Phase.CONNECT);}
        catch(IOException failure){return known!=null?known:local(submitted.get()?Outcome.UNKNOWN:Outcome.NOT_ACCEPTED,Action.RETRY,expired.get()?Phase.DEADLINE:Phase.FINAL_RESPONSE);}
        catch(RejectedExecutionException failure){return known!=null?known:local(Outcome.NOT_ACCEPTED,Action.PAUSE,Phase.CAPACITY);}
        catch(IllegalStateException failure){if(!closed&&!expired.get())throw failure;return known!=null?known:local(Outcome.UNKNOWN,Action.RETRY,Phase.DEADLINE);}
        finally {
            if(deadline!=null)deadline.cancel(false);if(interrupt!=null)interrupt.cancel(false);
            if(client!=null){client.close(CloseMode.IMMEDIATE);clients.remove(client);}slots.release();
        }
    }
    public int activeAttempts(){return clients.size();}
    public int queuedClientTasks(){return timers.getQueue().size();}
    @Override public void close(){closed=true;clients.forEach(c->c.close(CloseMode.IMMEDIATE));timers.shutdownNow();if(signingKey!=null)Arrays.fill(signingKey,(byte)0);}
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
    private static Result local(Outcome outcome,Action action,Phase phase){return new Result(outcome,action,null,phase,0,"webhook");}
    private static TapstateException invalid(String field){return new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field",field,"reason","unsupported-or-untrusted-channel-configuration"),null);}
}
