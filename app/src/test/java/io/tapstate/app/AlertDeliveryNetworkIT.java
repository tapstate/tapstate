package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import io.tapstate.testsupport.RequiresDocker;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyStore;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.net.ssl.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

@RequiresDocker
class AlertDeliveryNetworkIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @TempDir static Path temp;static SSLContext serverTls;static byte[] pem;
    @BeforeAll static void tls()throws Exception{
        Path key=temp.resolve("local-test-ca.p12");var process=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","keytool").toString(),"-genkeypair","-alias","fixture","-keyalg","RSA","-keysize","2048","-validity","2","-dname","CN=localhost","-ext","SAN=dns:localhost","-ext","bc=ca:true","-storetype","PKCS12","-keystore",key.toString(),"-storepass","fixture-only","-keypass","fixture-only","-noprompt").redirectErrorStream(true).redirectOutput(temp.resolve("keytool.log").toFile()).start();
        if(!process.waitFor(10,TimeUnit.SECONDS)){process.destroyForcibly();throw new AssertionError("fixture certificate timeout");}assertThat(process.exitValue()).isZero();
        var keys=KeyStore.getInstance("PKCS12");try(var input=Files.newInputStream(key)){keys.load(input,"fixture-only".toCharArray());}
        var km=KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());km.init(keys,"fixture-only".toCharArray());serverTls=SSLContext.getInstance("TLS");serverTls.init(km.getKeyManagers(),null,null);
        pem=("-----BEGIN CERTIFICATE-----\n"+Base64.getMimeEncoder(64,new byte[]{10}).encodeToString(keys.getCertificate("fixture").getEncoded())+"\n-----END CERTIFICATE-----\n").getBytes(StandardCharsets.US_ASCII);
    }
    static AlertChannels channels(AlertEvaluationHostIT.Fixture f,AtomicInteger resolutions){return new AlertChannels(AlertChannelPolicy.Deployment.ON_PREM,CloudRuntimeSettings.resolve(new CloudProperties()),f.clock(),ACTIVE,null,host->{resolutions.incrementAndGet();return new InetAddress[]{InetAddress.getLoopbackAddress()};});}
    static void receiver(AlertEvaluationHostIT.Fixture f,AlertReceiver.Transport transport,URI endpoint){
        var before=(AlertReceiver)f.store().definition(AlertConfiguration.Kind.RECEIVER,"receiver",access()).orElseThrow();
        var reference=AlertReceiver.CredentialReference.create(before.header().identity(),AlertReceiver.authenticationIdentity(transport));
        var after=before.edit(before.name(),true,transport,reference,T,"operator");
        try(var material=new AlertStore.PrivateMaterial(null,pem,endpoint)){assertThat(f.store().saveReceiver(before,after,material,false,access())).isEqualTo(AlertStore.Outcome.STAGED);}
    }
    @Test void productionWorkerSubmitsFrozenMimeThroughPrivateCaImplicitTlsAndRequiredStartTls()throws Exception{
        for(boolean implicit:List.of(true,false))try(var smtp=new SmtpPeer(implicit)){
            AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);
                var email=new AlertReceiver.Email("localhost",smtp.port(),implicit?AlertReceiver.Encryption.IMPLICIT_TLS:AlertReceiver.Encryption.STARTTLS_REQUIRED,AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("a@example.test"),AlertReceiver.Trust.PRIVATE_CA);
                receiver(f,email,null);var planner=planner(f,"planner");f.evaluator().withPlanner(planner);bad(f);planner.tick();var identity=intents(f).getFirst().value().frozen().identity();var frozen=f.store().intent(identity,null).orElseThrow().value().state().payload().bytes();
                var store=sendingStore(f);var resolutions=new AtomicInteger();
                try(var worker=new AlertDeliveryRuntime(store,f.clock(),"sender",ACTIVE,channels(f,resolutions),a->access())){
                    worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                    assertThat(store.intent(identity,null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
                    assertThat(smtp.bodies).hasSize(1);assertThat(smtp.bodies.getFirst()).isEqualTo(new String(frozen,StandardCharsets.US_ASCII));
                    assertThat(resolutions).hasValue(1);assertThat(worker.occupied()).isZero();
                    worker.tick();AlertDeliveryRuntimeIT.settled(worker);assertThat(smtp.bodies).hasSize(1);
                }
                await().atMost(Duration.ofSeconds(2)).until(()->smtp.active.get()==0);
            });
        }
    }
    @Test void productionWebhookUsesPinnedDnsEncryptedEndpointAndClosesHugeResponseAfterAcceptance()throws Exception{
        var executor=Executors.newFixedThreadPool(2);var server=com.sun.net.httpserver.HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(),0),2);
        server.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(serverTls));server.setExecutor(executor);
        var calls=new AtomicInteger();var closed=new CountDownLatch(1);var received=new AtomicReference<byte[]>();
        server.createContext("/secret-endpoint",exchange->{try{calls.incrementAndGet();received.set(exchange.getRequestBody().readAllBytes());exchange.sendResponseHeaders(202,0);try(var out=exchange.getResponseBody()){for(int i=0;i<100000;i++){out.write(new byte[1024]);out.flush();}}}catch(java.io.IOException stopped){closed.countDown();}finally{exchange.close();}});server.start();
        try{AlertEvaluationHostIT.withFixture(f->{var route=route(f,Duration.ZERO);var endpoint=URI.create("https://localhost:"+server.getAddress().getPort()+"/secret-endpoint");
            var transport=AlertReceiver.Webhook.fromEndpoint(endpoint,AlertReceiver.Authentication.NONE,null,AlertReceiver.Trust.PRIVATE_CA);receiver(f,transport,endpoint);
            var original=(AlertReceiver)f.store().definition(AlertConfiguration.Kind.RECEIVER,"receiver",access()).orElseThrow();
            var edited=route.edit(route.name(),route.match(),true,List.of(new AlertConfiguration.RouteChannel("webhook",List.of(original.header().identity()),MessageSettings.empty())),route.groupBy(),route.grouping(),route.inhibition(),T,"operator");
            f.store().saveDefinition(route,edited,access());var planner=planner(f,"planner");f.evaluator().withPlanner(planner);bad(f);planner.tick();var identity=intents(f).getFirst().value().frozen().identity();var originalBytes=f.store().intent(identity,null).orElseThrow().value().state().payload().bytes();var store=sendingStore(f);
            try(var worker=new AlertDeliveryRuntime(store,f.clock(),"sender",ACTIVE,channels(f,new AtomicInteger()),a->access())){worker.tick();AlertDeliveryRuntimeIT.settled(worker);
                assertThat(store.intent(identity,null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(received.get()).isEqualTo(originalBytes);assertThat(calls).hasValue(1);assertThat(worker.occupied()).isZero();
            }
            try{assertThat(closed.await(2,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}
        });}finally{server.stop(0);executor.shutdownNow();}
    }
    @Test void unsavedCandidateUsesTheSameTlsWorkerAndReplayNeverSavesOrSendsTwice()throws Exception{
        try(var smtp=new SmtpPeer(true)){AlertEvaluationHostIT.withFixture(f->{var store=sendingStore(f);var transport=new AlertReceiver.Email("localhost",smtp.port(),AlertReceiver.Encryption.IMPLICIT_TLS,AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("a@example.test"),AlertReceiver.Trust.PRIVATE_CA);
            var commands=new AlertDeliveryCommands(store,f.storeHost().scope(),AlertChannelPolicy.Deployment.ON_PREM,f.clock());var command=new AlertTestCommand.ClientId(Instant.ofEpochSecond(T.getEpochSecond()),"1".repeat(32));
            AlertStore.Stored<AlertIntent.Record> admitted;
            try(var material=new AlertStore.PrivateMaterial(null,pem,null)){admitted=commands.test(command,"candidate",transport,"a@example.test",null,material,Set.of(),null,access());}
            var id=admitted.value().frozen().identity();assertThat(store.definitions(AlertConfiguration.Kind.RECEIVER,null,10,access()).rows()).isEmpty();
            try(var worker=new AlertDeliveryRuntime(store,f.clock(),"sender",ACTIVE,channels(f,new AtomicInteger()),a->access())){worker.tick();AlertDeliveryRuntimeIT.settled(worker);assertThat(store.intent(id,null).orElseThrow().value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(smtp.bodies).hasSize(1);}
            f.now().set(T.plusSeconds(1));try(var material=new AlertStore.PrivateMaterial(null,pem,null)){assertThat(commands.test(command,"candidate",transport,"a@example.test",null,material,Set.of(),null,access()).value().frozen()).isEqualTo(admitted.value().frozen());}
            assertThat(store.active(null,10,f.reads()).total()).isZero();assertThat(store.definitions(AlertConfiguration.Kind.RECEIVER,null,10,access()).rows()).isEmpty();assertThat(smtp.bodies.getFirst()).contains("MIME-Version");
        });}
    }
    @Test void managedWorkerUsesRealGeneratedSdkBearerAndDurableEventReceiptOnce()throws Exception {
        String cluster="012345678901234567890123",org="112345678901234567890123",event="212345678901234567890123";
        var executor=Executors.newFixedThreadPool(2);var server=com.sun.net.httpserver.HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(),0),2);
        server.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(serverTls));server.setExecutor(executor);var calls=new AtomicInteger();var bearer=new AtomicReference<String>();var json=new com.fasterxml.jackson.databind.ObjectMapper();
        server.createContext("/v1/api/clusters/"+cluster+"/events",exchange->{try{calls.incrementAndGet();bearer.set(exchange.getRequestHeaders().getFirst("Authorization"));var request=json.readTree(exchange.getRequestBody());
            byte[] response=json.writeValueAsBytes(Map.of("code","ok","data",Map.of("id",event,"orgId",org,"source","cluster","type",request.path("type").asText(),"dedupeKey",request.path("dedupeKey").asText(),"schemaVersion",1,"payload",Map.of("clusterId",cluster))));
            exchange.getResponseHeaders().add("Content-Type","application/json");exchange.sendResponseHeaders(200,response.length);exchange.getResponseBody().write(response);
        }finally{exchange.close();}});server.start();
        try{AlertEvaluationHostIT.withFixture(false,true,new AlarmKey.Scope(org,cluster),f->{
            var properties=new CloudProperties();properties.setBaseUrl("HTTPS://localhost:"+server.getAddress().getPort());properties.setToken("isolated-machine-token");properties.setAtlasUri("mongodb://config.invalid/config");properties.setClusterId(cluster);var cloud=CloudRuntimeSettings.resolve(properties);
            var planner=new AlertNotificationPlanner(f.store(),f.clock(),"planner",cloud.baseUrl(),ACTIVE);f.evaluator().withPlanner(planner);bad(f);planner.tick();
            var identity=f.store().dueIntents(T.plusSeconds(1),null,10).rows().getFirst().value().frozen().identity();
            var store=f.configuration().alerts(f.storeHost().withChannels(Set.of(new AlertChannelPolicy.Capability("cloud-sdk",2,AlertChannelPolicy.Family.CLOUD_SDK))),BuiltinAlarmTypes.catalog(),f.clock());var resolutions=new AtomicInteger();
            try(var worker=new AlertDeliveryRuntime(store,f.clock(),"managed-sender",ACTIVE,new AlertChannels(AlertChannelPolicy.Deployment.CLOUD_MANAGED,cloud,f.clock(),ACTIVE,org,host->{resolutions.incrementAndGet();return new InetAddress[]{InetAddress.getLoopbackAddress()};},AlertChannels.trust(pem)),a->{throw new AssertionError("managed test authority");})){
                worker.tick();AlertDeliveryRuntimeIT.settled(worker);var result=store.intent(identity,null).orElseThrow().value();
                assertThat(result.state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);assertThat(result.state().acceptedReceipt().source()).isEqualTo("sdk-event-center");assertThat(result.state().acceptedReceipt().remoteId()).isEqualTo(event);
                assertThat(result.state().attemptsConsumed()).isOne();assertThat(result.state().manualCycles()).isZero();assertThat(bearer.get()).isEqualTo("Bearer isolated-machine-token");assertThat(resolutions).hasValue(1);
                worker.tick();AlertDeliveryRuntimeIT.settled(worker);assertThat(calls).hasValue(1);assertThat(worker.occupied()).isZero();
            }
        });}finally{server.stop(0);executor.shutdownNow();}
    }
    @Test void shuttingDownTheHostClosesAnActualInFlightSmtpSocketAndNeverResendsItsPermit()throws Exception {
        try(var smtp=new SmtpPeer(true,true)){AlertEvaluationHostIT.withFixture(f->{route(f,Duration.ZERO);
            var email=new AlertReceiver.Email("localhost",smtp.port(),AlertReceiver.Encryption.IMPLICIT_TLS,AlertReceiver.Authentication.NONE,null,"alerts@example.test",List.of("a@example.test"),AlertReceiver.Trust.PRIVATE_CA);
            receiver(f,email,null);var planner=planner(f,"planner");f.evaluator().withPlanner(planner);bad(f);planner.tick();var identity=intents(f).getFirst().value().frozen().identity();var store=sendingStore(f);
            long baseline=channelThreads();
            var worker=new AlertDeliveryRuntime(store,f.clock(),"sender",ACTIVE,channels(f,new AtomicInteger()),a->access());
            try{worker.tick();await().atMost(Duration.ofSeconds(5)).until(()->smtp.bodies.size()==1);worker.close();
                await().atMost(Duration.ofSeconds(2)).until(()->smtp.active.get()==0 && worker.occupied()==0 && channelThreads()<=baseline);
                var original=store.intent(identity,null).orElseThrow().value();assertThat(original.state().attemptsConsumed()).isOne();
                f.now().set(T.plusSeconds(1));try(var replacement=new AlertDeliveryRuntime(store,f.clock(),"replacement",ACTIVE,channels(f,new AtomicInteger()),a->access())){replacement.tick();AlertDeliveryRuntimeIT.settled(replacement);assertThat(smtp.bodies).hasSize(1);}
            }finally{worker.close();}
        });}
    }
    static long channelThreads(){return Thread.getAllStackTraces().keySet().stream().filter(Thread::isAlive).filter(t->t.getName().startsWith("alert-delivery")||t.getName().startsWith("alert-dns")||t.getName().startsWith("tapstate-alert-smtp")).count();}
    static final class SmtpPeer implements AutoCloseable {
        final java.net.ServerSocket server;final ExecutorService executor=Executors.newFixedThreadPool(2);final List<String> bodies=new CopyOnWriteArrayList<>();final AtomicInteger active=new AtomicInteger();final boolean implicit,stallFinal;
        SmtpPeer(boolean implicit)throws Exception{this(implicit,false);}
        SmtpPeer(boolean implicit,boolean stallFinal)throws Exception{this.implicit=implicit;this.stallFinal=stallFinal;server=implicit?serverTls.getServerSocketFactory().createServerSocket(0,2,InetAddress.getLoopbackAddress()):new java.net.ServerSocket(0,2,InetAddress.getLoopbackAddress());executor.submit(()->{while(!server.isClosed())try{var socket=server.accept();executor.submit(()->serve(socket));}catch(java.io.IOException closed){break;}});}
        int port(){return server.getLocalPort();}
        void serve(java.net.Socket initial){active.incrementAndGet();java.net.Socket socket=initial;try{socket.setSoTimeout(3000);if(socket instanceof SSLSocket tls){tls.setUseClientMode(false);tls.startHandshake();}var input=socket.getInputStream();var out=socket.getOutputStream();write(out,"220 ready\r\n");
            while(true){String line=line(input);if(line==null)break;
                if(line.startsWith("EHLO"))write(out,implicit||socket instanceof SSLSocket?"250 ready\r\n":"250-localhost\r\n250 STARTTLS\r\n");
                else if(line.equals("STARTTLS")){write(out,"220 begin\r\n");var tls=(SSLSocket)serverTls.getSocketFactory().createSocket(socket,"localhost",port(),true);tls.setUseClientMode(false);tls.startHandshake();socket=tls;input=socket.getInputStream();out=socket.getOutputStream();}
                else if(line.startsWith("MAIL FROM")||line.startsWith("RCPT TO"))write(out,"250 ok\r\n");
                else if(line.equals("DATA")){write(out,"354 continue\r\n");var body=new StringBuilder();while(true){var part=line(input);if(part==null||part.equals("."))break;body.append(part.startsWith("..")?part.substring(1):part).append("\r\n");}bodies.add(body.toString());if(stallFinal){while(input.read()>=0){}break;}write(out,"250 accepted\r\n");}
                else if(line.equals("QUIT")){write(out,"221 bye\r\n");break;}
                else throw new AssertionError("unexpected SMTP command");
            }
        }catch(Exception failure){throw new AssertionError(failure);}finally{try{socket.close();}catch(Exception ignored){}active.decrementAndGet();}}
        static void write(java.io.OutputStream out,String value)throws Exception{out.write(value.getBytes(StandardCharsets.US_ASCII));out.flush();}
        static String line(java.io.InputStream input)throws Exception{var bytes=new java.io.ByteArrayOutputStream();int b;while((b=input.read())>=0){if(b=='\n')return bytes.toString(StandardCharsets.US_ASCII).replace("\r","");bytes.write(b);if(bytes.size()>8192)throw new AssertionError("fixture line budget");}return null;}
        public void close()throws Exception{server.close();executor.shutdownNow();assertThat(executor.awaitTermination(2,TimeUnit.SECONDS)).isTrue();}
    }
}
