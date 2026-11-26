package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.MongoRenewedTopology;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** A necessary CACHE write fails while CONFIG stays writable, including the original role's restart recovery. */
@RequiresDocker
class NativeCacheAlertIT {
    @Container static final MongoDBContainer MONGO=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet","docker-rs","--setParameter","enableTestCommands=1");
    @TempDir Path work;
    @Test void anActualRequiredCacheCommandFailureCanBeAcceptedAfterTheDataMemberFailsClosed() throws Exception {
        verifyFailureAndRecovery(false);
    }
    @Test void aQueuedControlRejectionCannotInterruptTheIndependentCacheAlert() throws Exception {
        verifyFailureAndRecovery(true);
    }
    private void verifyFailureAndRecovery(boolean queuedControl) throws Exception {
        var storage=NativeAppStorage.createOnPremWithAdmin(MONGO,"native_cache_alert","cache-admin","isolated-cache-password");
        var settings=new MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5));
        try(var observer=MongoClients.create(storage.configurationUri())) {
            var config=observer.getDatabase(storage.configurationDatabase());var admin=observer.getDatabase("admin");
            String[] args={"--server.address=127.0.0.1","--server.port=0","--logging.level.root=WARN","--tapstate.store.mongo.uri="+storage.configurationUri(),
                    "--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2","--tapstate.cluster.node-session-ttl=6s","--tapstate.cluster.node-session-renew-interval=1s",
                    "--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),"--tapstate.control.auth.jwt-secret=cache-alert-fixture-signing"};
            var topology = new MongoRenewedTopology(storage.clusterId());
            AlarmKey originalKey;
            try (var app = start(args, topology, MongoRenewedTopology.ORIGINAL)) {
                var child = NativeAppStorage.business(app);
                var member = child.getBean(com.hazelcast.core.HazelcastInstance.class);
                try (var peer = start(args, topology, MongoRenewedTopology.PEER)) {
                    NativeAppStorage.awaitBusiness(app);
                    assertThat(member.getCluster().getMembers()).hasSize(2);
                }
                // The committed topology keeps Mongo renewal active after the peer leaves.
                // Only the member under test can publish the healthy or failing sample.
                await().atMost(Duration.ofSeconds(15)).until(() -> member.getCluster().getMembers().size() == 1);
                var client=HttpClient.newHttpClient();String base="http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort();
                var login=post(client,base,"/auth/login",null,Map.of("username","cache-admin","password","isolated-cache-password"));
                String token=String.valueOf(login.get("token"));
                post(client,base,"/api/alerts/configuration/save",token,Map.of("kind","RULE","id","cache-required-write","etag","0","form",Map.of("name","Required CACHE access","typeId","cluster.storage-unavailable",
                        "resourceKinds",List.of("cluster-storage"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true)));
                var lease = child.getBean(NodeSessionLease.class);
                var fence = lease.currentFence().orElseThrow();
                var claims = child.getBean(WorkloadClaimStore.class);
                var initial = claims.read(fence.key()).orElseThrow().claim();
                await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                        assertThat(claims.read(fence.key()).orElseThrow().claim().leaseUntil()).isAfter(initial.leaseUntil()));
                // A sample bound to the saved rule confirms that evaluation has loaded it.
                await().atMost(Duration.ofSeconds(20)).untilAsserted(()->{
                    var healthy=config.getCollection("alert_instances").find(new Document("_id.resourceKind", "cluster-storage").append("_id.resourceId", "cache")
                            .append("binding.ruleIdentity.id", "cache-required-write")).first();
                    assertThat(healthy).isNotNull();assertThat(healthy).containsEntry("status","CLEAR");
                });
                var runtime = child.getBean(PipelineAlertRuntime.class);
                var controlRelease = new java.util.concurrent.CountDownLatch(queuedControl ? 1 : 0);
                var controlExecuted = new java.util.concurrent.atomic.AtomicBoolean();
                try {
                    try (var queued = queuedControl ? holdQueuedControl(child, controlRelease, controlExecuted) : null) {
                        var entered = new java.util.concurrent.CountDownLatch(2);
                        var release = new java.util.concurrent.CountDownLatch(1);
                        var exited = new java.util.concurrent.CountDownLatch(2);
                        try {
                            Runnable block = () -> {
                                entered.countDown();
                                try {
                                    if (!release.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                                        throw new AssertionError("bounded fixture gate expired");
                                    }
                                } catch (InterruptedException stopped) {
                                    Thread.currentThread().interrupt();
                                } finally {
                                    exited.countDown();
                                }
                            };
                            org.springframework.test.util.ReflectionTestUtils.invokeMethod(runtime, "submit", null, 0, block, (Runnable) () -> {});
                            org.springframework.test.util.ReflectionTestUtils.invokeMethod(runtime, "submit", null, 0, block, (Runnable) () -> {});
                            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                            long initialFaults = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                                    .append("mode", "alwaysOn")
                                    .append("data", new Document("namespace", storage.cacheDatabase() + ".workload_claims")
                                            .append("appName", "tapstate/" + fence.owner().bootId())
                                            .append("failCommands", List.of("findAndModify")).append("errorCode", 8)))
                                    .get("count")).longValue();
                            admin.runCommand(new Document("waitForFailPoint", "failCommand")
                                    .append("timesEntered", initialFaults + 1).append("maxTimeMS", 5000));
                            await().atMost(Duration.ofSeconds(12)).until(() -> !lease.active());
                            if (queued != null) {
                                controlRelease.countDown();
                                assertThatThrownBy(() -> queued.released().toCompletableFuture()
                                        .get(5, java.util.concurrent.TimeUnit.SECONDS))
                                        .isInstanceOf(java.util.concurrent.ExecutionException.class)
                                        .cause().isInstanceOfSatisfying(TapstateException.class,
                                                failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                                assertThat(controlExecuted).isFalse();
                                assertThat(exited.getCount()).isEqualTo(2);
                            }
                            release.countDown();
                            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                                var raw = config.getCollection("alert_instances")
                                        .find(new Document("_id.resourceKind", "cluster-storage").append("_id.resourceId", "cache")
                                                .append("binding.ruleIdentity.id", "cache-required-write"))
                                        .first();
                                assertThat(raw).isNotNull();
                                assertThat(raw).containsEntry("status", "FIRING");
                            });
                        } finally {
                            release.countDown();
                            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                        }
                        try (var domain = MongoConfigurationDomain.open(settings, storage.clusterId())) {
                            var setup = domain.bindings().load().orElseThrow().setup();
                            var scope = new AlarmKey.Scope(setup.installationId(), setup.clusterId());
                            var store = domain.alerts(new AlertStore.Host(scope, setup, AlertChannelPolicy.Deployment.ON_PREM, () -> {}),
                                    BuiltinAlarmTypes.catalog(), Clock.systemUTC());
                            var head = store.active(null, 10, new AlertStore.ReadAccess(true, Set.of(), () -> {}))
                                    .rows().stream().map(value -> value.value())
                                    .filter(value -> value.snapshot().rule().key().resource().id().equals("cache"))
                                    .findFirst().orElseThrow();
                            originalKey = head.snapshot().rule().key();
                            assertThat(head.operationalEvidence().failedCapabilities())
                                    .isEqualTo(1 << OperationalEvidence.Capability.CACHE_WRITE.ordinal());
                        }
                    }
                } finally {
                    controlRelease.countDown();
                }
            }
            await().atMost(Duration.ofSeconds(10)).until(()->{
                var row=observer.getDatabase(storage.cacheDatabase()).getCollection("workload_claims").find(new Document("resourceType", "NODE_SESSION")
                        .append("resourceId", MongoRenewedTopology.ORIGINAL)).first();
                return row==null || row.getDate("leaseUntil").before(Date.from(Instant.now()));
            });
            try(var restarted = start(args, topology, MongoRenewedTopology.ORIGINAL);
                    var domain=MongoConfigurationDomain.open(settings,storage.clusterId())) {
                NativeAppStorage.awaitBusiness(restarted);var setup=domain.bindings().load().orElseThrow().setup();
                var store=domain.alerts(new AlertStore.Host(originalKey.scope(),setup,AlertChannelPolicy.Deployment.ON_PREM,()->{}),BuiltinAlarmTypes.catalog(),Clock.systemUTC());
                await().atMost(Duration.ofSeconds(40)).untilAsserted(()->{
                    var head=store.state(originalKey,new AlertStore.ReadAccess(true,Set.of(),()->{})).orElseThrow().value();
                    assertThat(head.snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
                    assertThat(head.episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.RECOVERED);
                });
            }
        }finally{
            try(var cleanup=MongoClients.create(storage.configurationUri())){cleanup.getDatabase(storage.configurationDatabase()).drop();cleanup.getDatabase(storage.cacheDatabase()).drop();cleanup.getDatabase(storage.masterDatabase()).drop();}
        }
    }
    private ConfigurableApplicationContext start(String[] base, MongoRenewedTopology topology, String node) {
        var arguments = new ArrayList<>(List.of(base));
        arguments.remove("--tapstate.hz.member-port=0");
        arguments.addAll(topology.arguments(node));
        arguments.add("--tapstate.connectors.plugins-dir=" + work.resolve(node));
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .run(arguments.toArray(String[]::new));
    }
    private static ValidationMemberResources.Control holdQueuedControl(
            org.springframework.context.ConfigurableApplicationContext child,
            java.util.concurrent.CountDownLatch release,
            java.util.concurrent.atomic.AtomicBoolean executed) throws Exception {
        var controls = (java.util.concurrent.ThreadPoolExecutor) org.springframework.test.util.ReflectionTestUtils.getField(
                child.getBean(ValidationMemberResources.class), "controls");
        var entered = new java.util.concurrent.CountDownLatch(4);
        for (int index = 0; index < 4; index++) {
            controls.execute(() -> {
                entered.countDown();
                try {
                    if (!release.await(20, java.util.concurrent.TimeUnit.SECONDS)) {
                        throw new AssertionError("bounded control gate expired");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        var queued = child.getBean(ValidationMemberResources.Member.class).admitControl();
        try {
            queued.execute(() -> executed.set(true));
            assertThat(queued.released().toCompletableFuture()).isNotDone();
            assertThat(controls.getQueue()).isNotEmpty();
            return queued;
        } catch (RuntimeException | Error failure) {
            queued.close();
            throw failure;
        }
    }
    private static Map<String,Object> post(HttpClient client,String base,String path,String token,Map<String,?> body)throws Exception {
        var builder=HttpRequest.newBuilder(URI.create(base+path)).header("Content-Type","application/json");if(token!=null)builder.header("Authorization","Bearer "+token);
        var reply=client.send(builder.POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());
        assertThat(reply.statusCode()).as(reply.body()).isEqualTo(200);return AlertHttpModel.object(JsonReader.parse(reply.body()));
    }
}
