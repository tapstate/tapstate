package io.tapstate.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class CloudAlertSdkChannelTest {
    static final String CLUSTER = "012345678901234567890123";
    static final String ORG = "112345678901234567890123";
    static final String EVENT = "212345678901234567890123";
    static final ObjectMapper JSON = new ObjectMapper();
    final AtomicInteger calls = new AtomicInteger();
    final AtomicReference<Throwable> serverFailure = new AtomicReference<>();
    final AtomicReference<JsonNode> body = new AtomicReference<>();
    final AtomicReference<String> auth = new AtomicReference<>();
    HttpServer server;
    URI origin;
    java.util.concurrent.ExecutorService executor;
    Handler handler;
    interface Handler { void run(HttpExchange exchange, JsonNode request) throws Exception; }
    @BeforeEach void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = java.util.concurrent.Executors.newCachedThreadPool(); server.setExecutor(executor);
        origin = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
        handler = (exchange, request) -> reply(exchange, 200, receipt(request));
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            try {
                assertThat(exchange.getRequestMethod()).isEqualTo("POST");
                assertThat(exchange.getRequestURI().toString()).isEqualTo("/v1/api/clusters/" + CLUSTER + "/events");
                auth.set(exchange.getRequestHeaders().getFirst("Authorization"));
                assertThat(exchange.getRequestHeaders().getFirst("Cookie")).isNull();
                JsonNode request = JSON.readTree(exchange.getRequestBody()); body.set(request); handler.run(exchange, request);
            } catch (java.io.IOException disconnected) { /* Rejection fixtures intentionally abort the socket. */ }
            catch (Throwable failure) { serverFailure.set(failure); }
            finally { exchange.close(); }
        });
        server.start();
    }
    @AfterEach void stop() throws Exception {
        server.stop(0); executor.shutdownNow(); assertThat(executor.awaitTermination(2, TimeUnit.SECONDS)).isTrue();
        if (serverFailure.get() != null) { throw new AssertionError(serverFailure.get()); }
    }
    NotificationChannel.Destination binding() {
        return new NotificationChannel.Destination(new AlarmKey.Scope("sdk-verified-cluster", CLUSTER), "cloud-sdk", "receiver-1", "target-1", "binding-1");
    }
    DeliveryKey key(NotificationChannel.Destination d, DeliveryKey.Kind kind, long round) {
        AlarmKey alarm = new AlarmKey(d.scope(), new AlarmKey.Resource("pipeline", "p1", "p1-inc"), "pipeline.failed", "rule-1");
        return new DeliveryKey(DeliveryKey.NotificationRound.forEpisode(alarm, 7, "route-1", "route-inc", kind, round), "cloud-sdk", d.receiverIncarnation(), d.targetId());
    }
    byte[] payload(NotificationChannel.Destination d, DeliveryKey.Kind kind, long round, AlarmSeverity severity) {
        return CloudAlertEventWire.encode(origin, d, key(d, kind, round), new AlarmKey(d.scope(), new AlarmKey.Resource("pipeline", "p1", "p1-inc"), "pipeline.failed", "rule-1"),
                Instant.parse("2026-10-26T10:00:00Z"), severity, "Pipeline p1", false);
    }
    NotificationChannel.Request request() {
        return new NotificationChannel.Request(binding(), NotificationChannel.Purpose.BUSINESS, "delivery-1", "attempt-1", Instant.now().plusSeconds(3), "application/json", payload(binding(), DeliveryKey.Kind.FAULT, 1, AlarmSeverity.MAJOR));
    }
    CloudAlertSdkChannel channel(Duration timeout) throws Exception {
        return new CloudAlertSdkChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED, binding(), origin, "fixture-token", null,
                new InetAddress[]{InetAddress.getByAddress(new byte[]{127,0,0,1})}, SSLContext.getDefault(), Clock.systemUTC(), timeout, true);
    }
    String receipt(JsonNode request) {
        return "{\"code\":\"ok\",\"opId\":\"request-1\",\"data\":{\"id\":\""+EVENT+"\",\"orgId\":\""+ORG+"\",\"source\":\"cluster\",\"type\":\""+request.path("type").asText()
            +"\",\"dedupeKey\":\""+request.path("dedupeKey").asText()+"\",\"schemaVersion\":1,\"payload\":{\"clusterId\":\""+CLUSTER+"\"}}}";
    }
    static void reply(HttpExchange exchange, int status, String response) throws Exception {
        byte[] bytes = response.getBytes(StandardCharsets.UTF_8); exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length); exchange.getResponseBody().write(bytes);
    }
    @Test void fixedWireEnvelopeMatchesGoldenAndReceiptIsAdditive() throws Exception {
        var d=binding();
        byte[] encoded=CloudAlertEventWire.encode(URI.create("https://control.example"),d,key(d,DeliveryKey.Kind.FAULT,1),
                new AlarmKey(d.scope(),new AlarmKey.Resource("pipeline","p1","p1-inc"),"pipeline.failed","rule-1"),
                Instant.parse("2026-10-26T10:00:00Z"),AlarmSeverity.MAJOR,"Pipeline p1",false);
        try(var in=getClass().getResourceAsStream("/cloud-alert-event-v1.golden.json")) {
            assertThat(in).isNotNull();assertThat(new String(encoded,StandardCharsets.UTF_8)+"\n").isEqualTo(new String(in.readAllBytes(),StandardCharsets.UTF_8));
        }
        var legacy=new NotificationChannel.Result(DeliveryEvidence.Outcome.ACCEPTED,NotificationChannel.Action.STOP,null,NotificationChannel.Phase.LOCAL,0,"sdk-placeholder");
        assertThat(legacy.receipt()).isNull();
        assertThatThrownBy(()->new NotificationChannel.Result(DeliveryEvidence.Outcome.UNKNOWN,NotificationChannel.Action.STOP,null,NotificationChannel.Phase.DEADLINE,0,"sdk-event-center",new NotificationChannel.Receipt(EVENT,null))).isInstanceOf(IllegalArgumentException.class);
    }
    @Test void actualSdkSendsBearerAndRequiresRemoteReceiptDespiteLocalOrganizationSentinel() throws Exception {
        try (var channel = channel(Duration.ofSeconds(2))) {
            var result = channel.send(request());
            assertThat(result.outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
            assertThat(result.receipt()).isEqualTo(new NotificationChannel.Receipt(EVENT, "request-1"));
            assertThat(result.implementation()).isEqualTo("sdk-event-center");
            assertThat(channel.capability().version()).isEqualTo(2);
            assertThat(channel.activeCalls()).isZero();
        }
        assertThat(auth.get()).isEqualTo("Bearer fixture-token"); assertThat(calls).hasValue(1);
        assertThat(body.get().path("severity").asText()).isEqualTo("CRITICAL");
        assertThat(body.get().path("occurredAt").asText()).contains("2026-10-26");
        assertThat(body.get().path("payload").path("originalSeverity").asText()).isEqualTo("MAJOR");
    }
    @Test void stableKeySeparatesRoundRecoveryBindingAndClusterWithoutMutableSubject() throws Exception {
        var d = binding();
        var one = JSON.readTree(payload(d, DeliveryKey.Kind.FAULT, 1, AlarmSeverity.MAJOR));
        assertThat(JSON.readTree(payload(d, DeliveryKey.Kind.FAULT, 1, AlarmSeverity.WARNING)).path("dedupeKey")).isEqualTo(one.path("dedupeKey"));
        assertThat(JSON.readTree(payload(d, DeliveryKey.Kind.FAULT, 2, AlarmSeverity.MAJOR)).path("dedupeKey")).isNotEqualTo(one.path("dedupeKey"));
        var recovered = JSON.readTree(payload(d, DeliveryKey.Kind.RECOVERY, 1, AlarmSeverity.CRITICAL));
        assertThat(recovered.path("dedupeKey")).isNotEqualTo(one.path("dedupeKey")); assertThat(recovered.path("severity").asText()).isEqualTo("INFO");
        var other = new NotificationChannel.Destination(new AlarmKey.Scope(d.scope().authorityId(), "312345678901234567890123"), "cloud-sdk", d.receiverIncarnation(), d.targetId(), d.bindingId());
        assertThat(JSON.readTree(payload(other, DeliveryKey.Kind.FAULT, 1, AlarmSeverity.MAJOR)).path("dedupeKey")).isNotEqualTo(one.path("dedupeKey"));
        var rebound = new NotificationChannel.Destination(d.scope(), "cloud-sdk", d.receiverIncarnation(), d.targetId(), "binding-2");
        assertThat(JSON.readTree(payload(rebound, DeliveryKey.Kind.FAULT, 1, AlarmSeverity.MAJOR)).path("dedupeKey")).isNotEqualTo(one.path("dedupeKey"));
        for (AlarmSeverity severity : List.of(AlarmSeverity.MINOR, AlarmSeverity.WARNING, AlarmSeverity.INDETERMINATE)) {
            assertThat(JSON.readTree(payload(d, DeliveryKey.Kind.FAULT, 1, severity)).path("severity").asText()).isEqualTo("WARNING");
        }
    }
    @Test void successfulHttpWithBadOrMismatchedEnvelopeRemainsUnknown() throws Exception {
        try (var channel = channel(Duration.ofSeconds(2))) {
            for (String bad : List.of("{}", "{\"code\":\"not-ok\"}", "{\"code\":\"ok\",\"data\":null}", "not-json")) {
                handler = (exchange, request) -> reply(exchange, 200, bad);
                assertThat(channel.send(request()).outcome()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
            }
            for (String field : List.of("id", "orgId", "source", "type", "dedupeKey", "schemaVersion", "clusterId")) {
                handler = (exchange, request) -> {
                    var root = (com.fasterxml.jackson.databind.node.ObjectNode)JSON.readTree(receipt(request));
                    var data = (com.fasterxml.jackson.databind.node.ObjectNode)root.get("data");
                    if (field.equals("clusterId")) { ((com.fasterxml.jackson.databind.node.ObjectNode)data.get("payload")).put(field, "wrong"); }
                    else if (field.equals("schemaVersion")) { data.put(field, 2); } else { data.put(field, "wrong"); }
                    reply(exchange, 200, root.toString());
                };
                var result = channel.send(request()); assertThat(result.outcome()).as(field).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN); assertThat(result.receipt()).isNull();
            }
        }
    }
    @Test void rejectionServerErrorAndRedirectNeverTriggerHiddenRetry() throws Exception {
        try (var channel = channel(Duration.ofSeconds(2))) {
            for (int status : List.of(401,403,429,500,302)) {
                handler = (exchange, request) -> { exchange.getResponseHeaders().set("Location", origin+"/redirect"); reply(exchange, status, "private fixture response"); };
                int before = calls.get(); var result = channel.send(request());
                assertThat(calls.get()).isEqualTo(before+1); assertThat(result.action()).isEqualTo(NotificationChannel.Action.STOP);
                assertThat(result.outcome()).isEqualTo(status >= 400 && status < 500 ? DeliveryEvidence.Outcome.NOT_ACCEPTED : DeliveryEvidence.Outcome.UNKNOWN);
                assertThat(result.toString()).doesNotContain("private fixture", "fixture-token");
            }
        }
    }
    @Test void responseBytesAreBounded() throws Exception {
        handler = (exchange, request) -> reply(exchange, 200, "x".repeat(CloudAlertEventWire.MAX_BYTES+1));
        try (var channel = channel(Duration.ofSeconds(2))) { assertThat(channel.send(request()).outcome()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN); assertThat(channel.activeCalls()).isZero(); }
        assertThat(calls).hasValue(1);
    }
    @Test void slowTrickleIsAbortedAtTotalDeadlineAndActualConnectionCloses() throws Exception {
        var disconnected = new CountDownLatch(1);
        handler = (exchange, request) -> {
            try {
                exchange.sendResponseHeaders(200, 0);
                for (int i=0; i<150; i++) { exchange.getResponseBody().write(' '); exchange.getResponseBody().flush(); Thread.sleep(20); }
            } catch (java.io.IOException gone) { disconnected.countDown(); }
        };
        try (var channel = channel(Duration.ofMillis(180))) {
            long started=System.nanoTime(); var result=channel.send(request());
            assertThat(Duration.ofNanos(System.nanoTime()-started)).isLessThan(Duration.ofSeconds(2));
            assertThat(result.outcome()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN); assertThat(result.phase()).isEqualTo(NotificationChannel.Phase.DEADLINE);
            assertThat(channel.activeCalls()).isZero(); assertThat(disconnected.await(2,TimeUnit.SECONDS)).isTrue();
        }
        assertThat(calls).hasValue(1);
    }
    @Test void interruptionClosesActualRequestAndPreservesInterruption() throws Exception {
        CountDownLatch entered=new CountDownLatch(1), disconnected=new CountDownLatch(1);
        handler=(exchange,request)-> { exchange.sendResponseHeaders(200,0); entered.countDown(); try { while(true){exchange.getResponseBody().write(' ');exchange.getResponseBody().flush();Thread.sleep(20);} }catch(java.io.IOException gone){disconnected.countDown();} };
        AtomicReference<Throwable> outcome=new AtomicReference<>();
        try(var channel=channel(Duration.ofSeconds(2))){
            Thread sender=new Thread(()->{try{channel.send(request());}catch(Throwable t){outcome.set(t);}});sender.start();
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();sender.interrupt();sender.join(1500);
            assertThat(sender.isAlive()).isFalse(); assertThat(outcome.get()).isInstanceOf(InterruptedException.class);
            assertThat(disconnected.await(2,TimeUnit.SECONDS)).isTrue();assertThat(channel.activeCalls()).isZero();
        }
    }
    @Test void oversizedResponseHeadersAreRejectedBeforeBodyParsing() throws Exception {
        try(var channel=channel(Duration.ofSeconds(2))) {
            handler=(exchange,request)->{exchange.getResponseHeaders().set("X-Oversize","x".repeat(9000));reply(exchange,200,receipt(request));};
            assertThat(channel.send(request()).outcome()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
            handler=(exchange,request)->{for(int i=0;i<80;i++){exchange.getResponseHeaders().set("X-Field-"+i,"v");}reply(exchange,200,receipt(request));};
            assertThat(channel.send(request()).outcome()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
        }
    }
    @Test void rejectedAndOversizedChunkedBodiesAreAbortedRatherThanDrainedToDeadline() throws Exception {
        for(int status:List.of(200,401)) {
            CountDownLatch disconnected=new CountDownLatch(1);
            handler=(exchange,request)-> {
                exchange.sendResponseHeaders(status,0);
                try {
                    if(status==200){exchange.getResponseBody().write(new byte[CloudAlertEventWire.MAX_BYTES+1]);exchange.getResponseBody().flush();}
                    while(true){exchange.getResponseBody().write(' ');exchange.getResponseBody().flush();Thread.sleep(20);}
                }catch(java.io.IOException gone){disconnected.countDown();}
            };
            try(var channel=channel(Duration.ofSeconds(3))) {
                var result=channel.send(request());
                assertThat(result.phase()).as("status %s must abort before the timer",status).isEqualTo(status==401?NotificationChannel.Phase.AUTHENTICATION:NotificationChannel.Phase.FINAL_RESPONSE);
                assertThat(disconnected.await(1,TimeUnit.SECONDS)).isTrue();assertThat(channel.activeCalls()).isZero();
            }
        }
    }
    @Test void completedCallsRemoveTheirDeadlineTasksWithoutClosingChannel() throws Exception {
        try(var channel=channel(Duration.ofSeconds(3))) {
            for(int i=0;i<10;i++){assertThat(channel.send(request()).outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);}
            assertThat(channel.pendingDeadlineTasks()).isZero();
        }
    }
    @Test void unterminatedHeaderLineIsRejectedWithoutWaitingForTotalDeadline() throws Exception {
        try(var raw=new java.net.ServerSocket(0,1,InetAddress.getByAddress(new byte[]{127,0,0,1}))) {
            var failure=new AtomicReference<Throwable>();
            Thread responder=new Thread(()-> {
                try(var socket=raw.accept()) {
                    socket.setSoTimeout(4000);var in=socket.getInputStream();var header=new StringBuilder();
                    while(!header.toString().endsWith("\r\n\r\n")){int b=in.read();if(b<0){return;}header.append((char)b);}
                    var length=java.util.regex.Pattern.compile("(?i)Content-Length: (\\d+)").matcher(header);assertThat(length.find()).isTrue();
                    in.readNBytes(Integer.parseInt(length.group(1)));
                    socket.getOutputStream().write(("HTTP/1.1 200 OK\r\nX-Unfinished: "+"x".repeat(32768)).getBytes(StandardCharsets.US_ASCII));socket.getOutputStream().flush();
                    assertThat(in.read()).isEqualTo(-1);
                } catch(java.net.SocketException aborted) { /* Immediate close may reset a partially received header. */ }
                catch(Throwable t) {failure.set(t);}
            });responder.start();
            origin=URI.create("http://127.0.0.1:"+raw.getLocalPort());
            try(var channel=channel(Duration.ofSeconds(3))) {
                var result=channel.send(request());assertThat(result.outcome()).isEqualTo(DeliveryEvidence.Outcome.UNKNOWN);
                assertThat(result.phase()).isEqualTo(NotificationChannel.Phase.FINAL_RESPONSE);assertThat(channel.activeCalls()).isZero();
            }
            responder.join(4500);assertThat(responder.isAlive()).isFalse();assertThat(failure.get()).isNull();
        }
    }
    @Test void productionHttpsChecksHostnameWithTrustedResolvedAddress(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
        java.nio.file.Path store=directory.resolve("sdk-fixture.p12");
        var process=new ProcessBuilder(java.nio.file.Path.of(System.getProperty("java.home"),"bin","keytool").toString(),"-genkeypair","-alias","fixture","-keyalg","RSA","-keysize","2048","-validity","2","-dname","CN=localhost","-ext","SAN=dns:localhost","-storetype","PKCS12","-keystore",store.toString(),"-storepass","fixture-only","-keypass","fixture-only","-noprompt").redirectErrorStream(true).redirectOutput(directory.resolve("keytool.log").toFile()).start();
        if(!process.waitFor(10,TimeUnit.SECONDS)){process.destroyForcibly();throw new AssertionError("fixture keytool timeout");}assertThat(process.exitValue()).isZero();
        var keys=java.security.KeyStore.getInstance("PKCS12");try(var in=java.nio.file.Files.newInputStream(store)){keys.load(in,"fixture-only".toCharArray());}
        var km=javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());km.init(keys,"fixture-only".toCharArray());
        var trust=java.security.KeyStore.getInstance("PKCS12");trust.load(null,null);trust.setCertificateEntry("fixture",keys.getCertificate("fixture"));
        var tm=javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());tm.init(trust);
        var serverTls=SSLContext.getInstance("TLS");serverTls.init(km.getKeyManagers(),tm.getTrustManagers(),null);
        var clientTls=SSLContext.getInstance("TLS");clientTls.init(null,tm.getTrustManagers(),null);
        var https=com.sun.net.httpserver.HttpsServer.create(new InetSocketAddress("127.0.0.1",0),0);https.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(serverTls));
        var received=new AtomicInteger();https.createContext("/",exchange->{try{received.incrementAndGet();reply(exchange,200,receipt(JSON.readTree(exchange.getRequestBody())));}catch(Exception e){throw new RuntimeException(e);}finally{exchange.close();}});https.start();
        try {
            var addresses=new InetAddress[]{InetAddress.getByAddress(new byte[]{127,0,0,1})};origin=URI.create("https://localhost:"+https.getAddress().getPort());
            try(var channel=new CloudAlertSdkChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED,binding(),origin,"fixture-token",ORG,addresses,clientTls,Clock.systemUTC())) {
                assertThat(channel.send(request()).outcome()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
            }
            origin=URI.create("https://wrong.example:"+https.getAddress().getPort());
            try(var channel=new CloudAlertSdkChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED,binding(),origin,"fixture-token",ORG,addresses,clientTls,Clock.systemUTC())) {
                assertThat(channel.send(request()).outcome()).isNotEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
            }
            assertThat(received).hasValue(1);
        }finally{https.stop(0);}
    }
    @Test void deploymentExpiredAndTestPurposeGatesMakeNoNetworkRequest() throws Exception {
        assertThatThrownBy(()->new CloudAlertSdkChannel(AlertChannelPolicy.Deployment.ON_PREM,binding(),origin,"fixture-token",null,new InetAddress[]{InetAddress.getLoopbackAddress()},SSLContext.getDefault(),Clock.systemUTC())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(()->new CloudAlertSdkChannel(AlertChannelPolicy.Deployment.CLOUD_MANAGED,binding(),origin,"fixture-token",null,new InetAddress[]{InetAddress.getLoopbackAddress()},SSLContext.getDefault(),Clock.systemUTC())).isInstanceOf(TapstateException.class);
        try(var channel=channel(Duration.ofSeconds(1))){
            var r=request();var expired=new NotificationChannel.Request(r.destination(),r.purpose(),r.deliveryId(),r.attemptId(),Instant.EPOCH,r.contentType(),r.payload());
            assertThat(channel.send(expired).outcome()).isEqualTo(DeliveryEvidence.Outcome.NOT_ACCEPTED);
            assertThatThrownBy(()->channel.send(NotificationChannel.Request.channelTest(binding(),r.deliveryId(),r.attemptId(),r.deadline(),r.contentType(),r.payload()))).isInstanceOf(TapstateException.class);
        }
        assertThat(calls).hasValue(0);
    }
}
