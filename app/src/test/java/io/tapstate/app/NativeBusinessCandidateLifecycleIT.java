package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.config.ScheduledTaskHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** Actual native candidate owners and resources; the failure is injected after their real startup. */
@RequiresDocker
class NativeBusinessCandidateLifecycleIT {
    @Container private static final MongoDBContainer MONGO=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet","docker-rs","--setParameter","enableTestCommands=1");
    @TempDir Path work;

    @ParameterizedTest @ValueSource(booleans={false,true})
    void failedCandidatesStopTheirOwnResourcesAndUnknownCleanupQuarantinesFurtherAssembly(boolean unknownCleanup) {
        String suffix=UUID.randomUUID().toString().replace("-","");
        String configName="candidate_config_"+suffix,cacheName="candidate_cache_"+suffix,masterName="candidate_master_"+suffix;
        var scope=new StorageSetupScope("candidate-"+suffix,"installation","setup",1);
        var cacheConnection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,MONGO.getReplicaSetUrl(cacheName),cacheName,null);
        var settings=new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
        var factory=new Candidates(unknownCleanup);
        try (var observer=MongoClients.create(MONGO.getReplicaSetUrl())) {
            try {
                try (var configuration=MongoConfigurationBootstrap.prepare(settings,scope.clusterId(),scope.installationId(),scope.operationId(),1)) {
                    var source=new SourceResource("views",null,"mongodb",Map.of("isUri",true,"uri",MONGO.getReplicaSetUrl(masterName)),null,null,null,null);
                    var committed=configuration.bindings().commitWithFirstCache(scope,StorageBinding.Mode.OP,cacheConnection,source);
                    try (var cache=MongoCacheDomain.connect(cacheConnection,Duration.ofSeconds(5),"candidate-fixture")) {
                        cache.prepareFirstAuth(committed.firstCreation().orElseThrow()); cache.prepareInitialBusiness(committed.preparation(),() -> {});
                    }
                    configuration.bindings().complete(scope);
                    configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),new User("admin",new BCryptPasswordHasher().hash("candidate-password"),"admin"));
                }
                var original=fingerprints(observer.getDatabase(configName));
                var admin=observer.getDatabase("admin");
                long originalKeyWrites=armKeyWrites(admin,configName);
                try (var application=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                        .initializers(context -> {
                            var definition=new RootBeanDefinition(StorageBusinessFactory.class,() -> (StorageBusinessFactory)bound ->
                                    factory.create(context.getBean(StorageRoot.class),bound,context)); definition.setPrimary(true);
                            ((BeanDefinitionRegistry)context.getBeanFactory()).registerBeanDefinition("faultingNativeCandidates",definition);
                        }).run("--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR",
                                "--tapstate.store.mongo.uri="+settings.uri(),"--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2",
                                "--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),"--tapstate.connectors.plugins-dir="+work.resolve("plugins"),
                                "--tapstate.control.auth.jwt-secret=candidate-test-signing-key")) {
                    var root=application.getBean(StorageRoot.class); var retained=root.runtime().current().orElseThrow();
                    assertThat(factory.calls.get()).isEqualTo(1); factory.failed.getFirst().assertStopped();
                    for (int round=1;round<3;round++) {
                        root.loadExisting(bound -> factory.create(root,bound,application));
                        assertThat(factory.calls.get()).isEqualTo(unknownCleanup ? 1 : round+1);
                        factory.failed.forEach(Candidate::assertStopped);
                        assertThat(root.runtime().current()).contains(retained);
                        retained.cache().verifyBusinessReady(retained.preparation().businessScope());
                        assertThat(fingerprints(observer.getDatabase(configName))).isEqualTo(original);
                    }
                    assertThat(root.runtime().business()).isEmpty();
                    assertThat(root.runtime().cleanupRequired()).isEqualTo(unknownCleanup);
                    if (!unknownCleanup) {
                        root.loadExisting(bound -> factory.create(root,bound,application));
                        assertThat(factory.calls.get()).isEqualTo(4);
                        await().atMost(Duration.ofSeconds(15)).until(root.runtime()::businessStarted);
                        var successor=(NativeBusinessContext)root.runtime().business().orElseThrow();
                        var child=(ConfigurableApplicationContext)ReflectionTestUtils.getField(successor,"context");
                        var member=child.getBean(HazelcastInstance.class); var node=child.getBean(NodeSessionLease.class);
                        var exact=node.currentFence().orElseThrow();
                        var claims=retained.cache().workloadClaims(retained.preparation().businessScope());
                        var before=claims.read(exact.key()).orElseThrow(); assertThat(before.leased()).isTrue();
                        assertThat(WorkloadClaimFence.from(before.claim())).isEqualTo(exact);
                        factory.failed.forEach(candidate -> candidate.nativeOwner.close());
                        factory.failed.forEach(Candidate::assertStopped);
                        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                            var live=claims.read(exact.key()).orElseThrow();
                            assertThat(live.leased()).isTrue(); assertThat(WorkloadClaimFence.from(live.claim())).isEqualTo(exact);
                            assertThat(live.claim().leaseUntil()).isAfter(before.claim().leaseUntil());
                        });
                        assertThat(child.isActive()).isTrue(); assertThat(member.getLifecycleService().isRunning()).isTrue();
                        assertThat(node.currentFence()).contains(exact); assertThat(root.runtime().current()).contains(retained);
                        retained.cache().verifyBusinessReady(retained.preparation().businessScope());
                        root.loadExisting(bound -> { throw new AssertionError("a published successor cannot assemble again"); });
                        assertThat(factory.calls.get()).isEqualTo(4);
                    }
                    int port=((WebServerApplicationContext)application).getWebServer().getPort();
                    var http=org.springframework.web.client.RestClient.create("http://127.0.0.1:"+port);
                    var login=http.post().uri("/auth/login").body(Map.of("username","admin","password","candidate-password"))
                            .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
                    assertThat(login).isNotNull();
                    @SuppressWarnings("unchecked") var status=http.post().uri("/api/storage/status").header("Authorization","Bearer "+login.token())
                            .body(Map.of()).retrieve().body(Map.class);
                    assertThat(status).containsEntry("businessPhase",unknownCleanup ? "CLEANUP_REQUIRED" : "READY");
                    assertThat(factory.calls.get()).isEqualTo(unknownCleanup ? 1 : 4);
                    assertThat(fingerprints(observer.getDatabase(configName))).isEqualTo(original);
                } finally {
                    try { factory.failed.forEach(candidate -> candidate.nativeOwner.close()); }
                    finally {
                        long observed=keyWritesOff(admin);
                        assertThat(observed).as("pre-join, failed startup, ordinary renewal and all cleanup leave the existing key untouched").isEqualTo(originalKeyWrites);
                    }
                }
                long positive=armKeyWrites(admin,configName);
                try {
                    var key=SystemCollections.CLUSTER_KEYS.on(observer.getDatabase(configName)).find().first();
                    assertThatThrownBy(() -> SystemCollections.CLUSTER_KEYS.on(observer.getDatabase(configName))
                            .updateOne(new Document(),new Document("$set",new Document("keyId",key.getString("keyId")))))
                            .isInstanceOfSatisfying(com.mongodb.MongoException.class,failure -> assertThat(failure.getCode()).isEqualTo(13));
                } finally { assertThat(keyWritesOff(admin)).isEqualTo(positive+1); }
                assertThat(fingerprints(observer.getDatabase(configName))).isEqualTo(original);
            } finally { observer.getDatabase(configName).drop(); observer.getDatabase(cacheName).drop(); observer.getDatabase(masterName).drop(); }
        }
    }

    private static long armKeyWrites(MongoDatabase admin,String database) {
        return ((Number)admin.runCommand(new Document("configureFailPoint","failCommand").append("mode",new Document("times",1))
                .append("data",new Document("namespace",database+".cluster_keys").append("failCommands",List.of("update","findAndModify","insert","delete"))
                        .append("errorCode",13))).get("count")).longValue();
    }
    private static long keyWritesOff(MongoDatabase admin) {
        return ((Number)admin.runCommand(new Document("configureFailPoint","failCommand").append("mode","off")).get("count")).longValue();
    }

    private static Map<String,String> fingerprints(MongoDatabase database) {
        var result=new TreeMap<String,String>();
        for (var collection:List.of(SystemCollections.CLUSTER_KEYS,SystemCollections.SOURCES,SystemCollections.STORAGE_BINDING,
                SystemCollections.STORAGE_SETUP,SystemCollections.OP_BOOTSTRAP)) {
            var rows=collection.on(database).find().sort(new Document("_id",1)).into(new ArrayList<>());
            result.put(collection.collectionName(),CanonicalHash.ofText(rows.stream().map(Document::toJson).toList().toString()));
        }
        return result;
    }
    private static final class Candidates {
        final boolean unknown; final AtomicInteger calls=new AtomicInteger(); final List<Candidate> failed=new ArrayList<>();
        Candidates(boolean unknown) { this.unknown=unknown; }
        StorageRuntime.Business create(StorageRoot root,StorageRuntime.Bound bound,ConfigurableApplicationContext parent) {
            var owner=new NativeBusinessContext(root,bound,parent);
            if (calls.incrementAndGet()>3) return owner;
            var candidate=new Candidate(owner,unknown); failed.add(candidate); return candidate;
        }
    }
    private static final class Candidate implements StorageRuntime.Business {
        final NativeBusinessContext nativeOwner; final boolean unknown;
        ConfigurableApplicationContext child; HazelcastInstance member;
        final List<ExecutorService> executors=new ArrayList<>(); final List<ScheduledFuture<?>> tasks=new ArrayList<>();
        Candidate(NativeBusinessContext owner,boolean unknown) { nativeOwner=owner; this.unknown=unknown; }
        @Override public void start() {
            nativeOwner.start(); child=(ConfigurableApplicationContext)ReflectionTestUtils.getField(nativeOwner,"context");
            member=child.getBean(HazelcastInstance.class);
            executors.add((ExecutorService)ReflectionTestUtils.getField(child.getBean(NodeSessionLease.class),"renewer"));
            executors.add((ExecutorService)ReflectionTestUtils.getField(child.getBean(ExecutionAuthorization.class),"refresher"));
            var cleanup=ReflectionTestUtils.getField(nativeOwner,"payloadCleanup");
            if (cleanup!=null && ReflectionTestUtils.getField(cleanup,"worker") instanceof ExecutorService executor) executors.add(executor);
            for (var holder:child.getBeansOfType(ScheduledTaskHolder.class).values()) for (var task:holder.getScheduledTasks())
                if (ReflectionTestUtils.getField(task,"future") instanceof ScheduledFuture<?> future) tasks.add(future);
            assertThat(member.getLifecycleService().isRunning()).isTrue(); assertThat(tasks).isNotEmpty();
            assertThat(executors).allSatisfy(executor -> assertThat(executor.isShutdown()).isFalse());
            throw StorageRuntime.unavailable();
        }
        @Override public void close() { nativeOwner.close(); if (unknown) throw StorageRuntime.unavailable(); }
        void assertStopped() {
            assertThat(child).isNotNull(); assertThat(child.isActive()).isFalse();
            assertThat(member.getLifecycleService().isRunning()).isFalse();
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(executors).allSatisfy(executor -> assertThat(executor.isTerminated()).isTrue());
                assertThat(tasks).allSatisfy(task -> assertThat(task.isCancelled() || task.isDone()).isTrue());
            });
        }
    }
}
