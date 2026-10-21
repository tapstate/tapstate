package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Actual CONFIG transactions after minimum authentication recovery; these checks do not run business jobs. */
@RequiresDocker
class MongoCacheRecoveryStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final WorkloadClaimFence NODE = new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node"),new WorkloadOwner("node","boot"),1,0,0);
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "install", "setup", 1);
    private static final SourceResource SOURCE = new SourceResource("source", null, "mysql", Map.of("host", "source.invalid"),
            SourceMode.CDC, List.of(TableRef.literal("orders")), null, null);
    private static PipelineResource pipeline(String id) {
        return new PipelineResource(id, null, List.of(SourceRef.spec("source", false)), null, null,
                new ServeBlock.Inline("serve", FromRef.literal("orders"), List.of(new SyncElement("sync", "views", null, null, null)), null, null), null, null);
    }
    @Test void oneTransactionRebindsAllPipelinesStoppedWithoutCreatingRuntimeHistory() {
        fixture(f -> {
            var changes = f.domain.cacheRecoveries().inspectCluster(f.current());
            assertThat(changes).hasSize(2);
            assertThat(changes).allSatisfy(change -> {
                assertThat(change.previousScope().cacheIncarnation()).isEqualTo(f.previous.cacheIncarnation());
                assertThat(change.targetScope().stateGeneration()).isEqualTo(2);
            });
            var running = f.confirmed(); long before = f.domain.configurationRevision();
            var summaries = f.domain.cacheRecoveries().stageCluster(running);
            assertThat(f.domain.configurationRevision()).isEqualTo(before + 1);
            for (var summary : summaries) {
                var intent = f.domain.intents().load(summary.targetScope().pipelineId()).orElseThrow();
                assertThat(intent.scope()).isEqualTo(summary.targetScope());
                assertThat(intent.desired().targetState()).isEqualTo(PipelineState.STOPPED);
                assertThat(intent.desired().purgeState()).isFalse();
                assertThat(f.domain.intents().loadForActuation(summary.targetScope().pipelineId())).isEmpty();
                assertThat(f.domain.pipelineStates().read(summary.targetScope().pipelineId()).orElseThrow().stateGeneration()).isEqualTo(2);
            }
            assertThat(f.cache.listCollectionNames().into(new ArrayList<>())).containsExactlyInAnyOrder("cache_identity", "schema_migrations", "op_sessions");
            assertThat(f.config.getCollection("users").countDocuments()).isOne();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster")) {
                assertThat(reopened.cacheRecoveries().stageCluster(running)).isEqualTo(summaries);
                assertThat(reopened.configurationRevision()).isEqualTo(before + 1);
            }
        });
    }
    @Test void independentClientsPublishOneGenerationAndOneIntentRevision() {
        fixture(f -> {
            var running = f.confirmed(); long version = f.domain.configurationRevision();
            try (var second = MongoConfigurationDomain.open(f.settings, "cluster"); var workers = Executors.newFixedThreadPool(2)) {
                var start = new CountDownLatch(1);
                var a = workers.submit(() -> { start.await(); return f.domain.cacheRecoveries().stageCluster(running); });
                var b = workers.submit(() -> { start.await(); return second.cacheRecoveries().stageCluster(running); });
                start.countDown(); assertThat(a.get(20, TimeUnit.SECONDS)).isEqualTo(b.get(20, TimeUnit.SECONDS));
                assertThat(f.domain.configurationRevision()).isEqualTo(version + 1);
                assertThat(f.domain.intents().load("a").orElseThrow().intentRevision()).isEqualTo(2);
            }
        });
    }
    @Test void unconfirmedOrChangedDefinitionsCannotPartiallyPublishMappings() {
        fixture(f -> {
            var planned = f.domain.recoveryCommands().plan(f.current(), f.manifest(), null, null);
            var before = f.config.getCollection("operator_generations").find().into(new ArrayList<>());
            assertThatThrownBy(() -> f.domain.cacheRecoveries().stageCluster(planned)).isInstanceOf(TapstateException.class);
            var running = f.domain.recoveryCommands().confirm(planned);
            var changed = new SourceResource("source", new Metadata(Map.of(), "changed"), SOURCE.connector(), SOURCE.config(), SOURCE.mode(), SOURCE.tables(), null, null);
            f.domain.artifacts().replace("source", CanonicalHash.of(SOURCE), changed);
            assertThatThrownBy(() -> f.domain.cacheRecoveries().stageCluster(running)).isInstanceOf(TapstateException.class);
            assertThat(f.config.getCollection("operator_generations").find().into(new ArrayList<>())).isEqualTo(before);
        });
    }
    @Test void corruptPredecessorOrAnotherAuthenticationGenerationCannotAuthorizeTheOldPlan() {
        fixture(f -> {
            var running = f.confirmed(); var summaries = f.domain.cacheRecoveries().stageCluster(running);
            var original = f.config.getCollection("operator_generations").find(new Document("_id", "a")).first();
            f.config.getCollection("operator_generations").updateOne(new Document("_id", "a"),
                    new Document("$set", new Document("cacheRecovery.targetScope.pipelineId", "b")));
            assertThatThrownBy(() -> f.domain.cacheRecoveries().read("a")).isInstanceOf(TapstateException.class);
            f.config.getCollection("operator_generations").replaceOne(new Document("_id", "a"), original);
            f.cache.drop();
            try (var cache = MongoCacheDomain.connect(f.binding.cache(), Duration.ofSeconds(5), "recovery-next")) {
                f.domain.prepareAuthenticationRecovery(cache, ACTIVE);
            }
            assertThatThrownBy(() -> f.domain.cacheRecoveries().stageCluster(running)).isInstanceOf(TapstateException.class);
            assertThat(f.domain.cacheRecoveries().read("a")).contains(summaries.getFirst());
        });
    }
    @Test void aNeverPreparedPipelineDoesNotInventAPredecessor() {
        fixture(f -> {
            f.domain.artifacts().saveAll(List.of(pipeline("never-started")));
            var change = f.domain.cacheRecoveries().inspectCluster(f.current()).stream().filter(value -> value.targetScope().pipelineId().equals("never-started")).findFirst().orElseThrow();
            assertThat(change.previousScope()).isNull();
            assertThat(change.targetScope().stateGeneration()).isEqualTo(2);
        });
    }
    @Test void aChangedPhysicalGenerationRequiresANewInfluenceConfirmation() {
        fixture(f -> {
            var running = f.confirmed();
            f.config.getCollection("operator_generations").updateOne(new Document("_id", "b"), new Document("$set", new Document("stateGeneration", 2L)));
            f.config.getCollection("pipeline_intents").updateOne(new Document("_id", "b"), new Document("$set", new Document("stateGeneration", 2L)));
            var before = f.config.getCollection("operator_generations").find().into(new ArrayList<>());
            assertThatThrownBy(() -> f.domain.cacheRecoveries().stageCluster(running)).isInstanceOf(TapstateException.class);
            assertThat(f.config.getCollection("operator_generations").find().into(new ArrayList<>())).isEqualTo(before);
        });
    }
    @Test void onlyOneExplicitFullInvocationChecksThePredecessorAndReceivesANewGrant() {
        fixture(f -> {
            var permission=f.full("a");var scope=permission.recovery().targetScope();var inputs=inputs(scope);
            assertThatThrownBy(() -> f.domain.preparations().issueFirstCreate(scope,NODE,inputs)).isInstanceOf(TapstateException.class);
            var checks=new java.util.concurrent.atomic.AtomicInteger();
            try (var second=MongoConfigurationDomain.open(f.settings,"cluster");var workers=Executors.newFixedThreadPool(2)) {
                var plan=f.domain.recoveryCommands().current(f.current()).orElseThrow();
                var other=second.cacheRecoveries().authorizeFull(plan,"a");var start=new CountDownLatch(1);
                var one=workers.submit(() -> {start.await();return f.domain.preparations().issueRecoveredFirstCreate(scope,NODE,inputs,permission,checks::incrementAndGet);});
                var two=workers.submit(() -> {start.await();return second.preparations().issueRecoveredFirstCreate(scope,NODE,inputs,other,checks::incrementAndGet);});
                start.countDown();var results=List.of(one.get(20,TimeUnit.SECONDS),two.get(20,TimeUnit.SECONDS));
                assertThat(results.stream().filter(FirstPipelineCreationResult.Issued.class::isInstance)).hasSize(1);
                assertThat(results.stream().filter(FirstPipelineCreationResult.AlreadyIssued.class::isInstance)).hasSize(1);
                assertThat(checks).hasValue(1);
            }
        });
    }
    @Test void aFailedPredecessorCheckNeverReissuesTheLostInvocation() {
        fixture(f -> {
            var permission=f.full("a");var scope=permission.recovery().targetScope();
            assertThatThrownBy(() -> f.domain.preparations().issueRecoveredFirstCreate(scope,NODE,inputs(scope),permission,
                    () -> {throw new IllegalStateException("injected predecessor check failure");})).hasMessage("injected predecessor check failure");
            assertThat(f.domain.preparations().read("a").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.ISSUED);
            assertThat(f.domain.preparations().issueRecoveredFirstCreate(scope,NODE,inputs(scope),permission,
                    () -> {throw new AssertionError("must not release the predecessor twice");})).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
        });
    }
    @Test void retainedPrivateNotesNeedTheExactFullPermissionAndKeepTheirOriginalIdentity() {
        fixture(f -> {
            var notes=f.domain.requiredNotes();
            var old=notes.initialize(new RequiredNoteOwner(1,"pdk.state.a.capture.source",RequiredNoteOwner.Kind.PIPELINE,"a",1));
            var origin=new RequiredNoteOrigin("mysql","a".repeat(64),new byte[]{1,2,3});notes.prepareOrigin(old,origin);
            assertThat(notes.compareAndSet(old,"slot",0,new byte[]{4,5},false)).isTrue();
            var permission=f.full("a");
            assertThatThrownBy(() -> notes.open(old.owner())).isInstanceOf(TapstateException.class);
            assertThat(notes.privateContexts(permission)).filteredOn(context -> context.scope().equals(old)).singleElement().satisfies(context -> {
                assertThat(context.scope()).isEqualTo(old);assertThat(context.origin()).contains(origin);assertThat(context.completed()).isFalse();
            });
            var retirement=notes.retirement(old,permission);
            assertThat(retirement.notes().read(old,"slot").bytes()).containsExactly(4,5);
            var wrong=new RequiredNoteScope(new RequiredNoteOwner(1,"pdk.state.b.capture.source",RequiredNoteOwner.Kind.PIPELINE,"b",1),1);
            assertThatThrownBy(() -> notes.retirement(wrong,permission)).isInstanceOf(TapstateException.class);
            var next=retirement.completeAfterVerifiedRelease();assertThat(next.generation()).isEqualTo(old.generation()+1);
            assertThat(retirement.completeAfterVerifiedRelease()).isEqualTo(next);
            assertThat(notes.privateContexts(permission)).filteredOn(context -> context.scope().equals(old)).singleElement().satisfies(context -> assertThat(context.completed()).isTrue());
            var running=f.domain.recoveryCommands().current(f.current()).orElseThrow();f.domain.recoveryCommands().complete(running);
            assertThatThrownBy(retirement::origin).isInstanceOf(TapstateException.class);
            assertThatThrownBy(retirement::completeAfterVerifiedRelease).isInstanceOf(TapstateException.class);
        });
    }
    @Test void ordinaryTargetOwnerCannotReadOrClearPredecessorNotesBeforeFullPreparation() {
        fixture(f -> {
            var notes=f.domain.requiredNotes();var owner=RequiredNoteOwner.forNode(1,new PipelineNode("a","source"),PipelineNode.Purpose.CAPTURE,1);
            var old=notes.initialize(owner);notes.prepareOrigin(old,new RequiredNoteOrigin("mysql","a".repeat(64),new byte[]{1}));
            notes.compareAndSet(old,"slot",0,new byte[]{7},false);
            var permission=f.full("a");var target=permission.recovery().targetScope();
            var replacement=RequiredNoteOwner.forNode(1,new PipelineNode("a","source"),PipelineNode.Purpose.CAPTURE,target.stateGeneration());
            assertThatThrownBy(() -> notes.open(replacement)).isInstanceOf(TapstateException.class);
            var guessed=new RequiredNoteScope(replacement,old.generation());
            assertThatThrownBy(() -> notes.read(guessed,"slot")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> notes.clear(guessed,false)).isInstanceOf(TapstateException.class);
            assertThat(notes.retirement(old,permission).notes().read(old,"slot").bytes()).containsExactly(7);
        });
    }
    @Test void neverStartedEmptyNoteReservationsAreAcceptedWithoutInventingAnOldScope() {
        fixture(f -> {
            f.domain.artifacts().saveAll(List.of(pipeline("never-started")));
            var permission=f.full("never-started");
            assertThat(permission.recovery().previousScope()).isNull();
            assertThat(f.domain.requiredNotes().privateContexts(permission)).isEmpty();
        });
    }
    @Test void repeatedLossDoesNotPromoteARecoveryGeneratedIntentIntoAnExecutionPredecessor() {
        fixture(f -> {
            f.domain.artifacts().saveAll(List.of(pipeline("never-started")));
            f.domain.cacheRecoveries().stageCluster(f.confirmed());
            assertThat(f.domain.cacheRecoveries().read("never-started").orElseThrow().previousScope()).isNull();
            f.cache.drop();try(var cache=MongoCacheDomain.connect(f.binding.cache(),Duration.ofSeconds(5),"second-loss")) {
                f.domain.prepareAuthenticationRecovery(cache,ACTIVE);
            }
            var proposed=f.domain.cacheRecoveries().inspectCluster(f.current()).stream().filter(change -> change.targetScope().pipelineId().equals("never-started")).findFirst().orElseThrow();
            assertThat(proposed.previousScope()).isNull();assertThat(proposed.targetScope().stateGeneration()).isEqualTo(3);
        });
    }
    @Test void stoppedRecoveryAllowsOnlyNativeUnpreparedModelCopiesWithoutMintingPipelineHistory() {
        fixture(f -> {
            f.domain.cacheRecoveries().stageCluster(f.confirmed());
            var running=f.domain.recoveryCommands().current(f.current()).orElseThrow();
            try(var cache=MongoCacheDomain.connect(f.binding.cache(),Duration.ofSeconds(5),"authoring-models")) {
                f.domain.prepareStoppedRecovery(cache,running,ACTIVE);f.domain.recoveryCommands().complete(running);
                var scope=f.domain.cacheRecoveries().read("a").orElseThrow().targetScope();
                var business=f.domain.bindings().cachePreparation().orElseThrow().businessScope();
                var node=WorkloadClaimFence.from(cache.workloadClaims(business).acquire(NODE.key(),NODE.owner(),1,Duration.ofSeconds(30)).claim());
                var derived=cache.unpreparedPipelineDerivedSchemas(f.domain.authoringModels(scope),node,ACTIVE);
                assertThat(derived.latest("a","step")).isEmpty();
                derived.record("a","step",Map.of("id","integer"),"statement","input","test");
                assertThat(derived.latest("a","step")).isPresent();
                assertThatThrownBy(() -> derived.pin("a","step",0)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> cache.unpreparedPipelineDerivedSchemas(scope,node,ACTIVE).latest("a","step")).isInstanceOf(TapstateException.class);
                assertThat(f.cache.getCollection("pipeline_state").countDocuments()).isZero();
                assertThat(f.cache.getCollection("workload_claims").countDocuments()).isOne();
            }
        });
    }
    @Test void pendingRecoveryAllowsAuthoringEditsWithoutOpeningOldPrivateValues() {
        fixture(f -> {
            var notes=f.domain.requiredNotes();var owner=RequiredNoteOwner.forNode(1,new PipelineNode("a","source"),PipelineNode.Purpose.CAPTURE,1);
            var old=notes.initialize(owner);notes.prepareOrigin(old,new RequiredNoteOrigin("mysql","a".repeat(64),new byte[]{1}));
            notes.compareAndSet(old,"slot",0,new byte[]{7},false);
            f.domain.cacheRecoveries().stageCluster(f.confirmed());var running=f.domain.recoveryCommands().current(f.current()).orElseThrow();
            f.domain.recoveryCommands().complete(running);
            var previous=pipeline("a");var changed=new PipelineResource("a",new Metadata(Map.of(),"updated before full"),previous.sources(),previous.transforms(),previous.view(),previous.serve(),previous.settings(),previous.experimental());
            f.domain.artifacts().replace("a",CanonicalHash.of(previous),changed);
            assertThat(f.domain.artifacts().get("a")).contains(changed);
            var target=f.domain.cacheRecoveries().read("a").orElseThrow().targetScope();
            var newOwner=RequiredNoteOwner.forNode(1,new PipelineNode("a","source"),PipelineNode.Purpose.CAPTURE,target.stateGeneration());
            assertThatThrownBy(() -> notes.open(newOwner)).isInstanceOf(TapstateException.class);
        });
    }
    private static PipelinePreparationInputs inputs(PipelineIntentScope scope) {
        return PipelinePreparationInputs.of(scope,pipeline(scope.pipelineId()),Map.of("source",SOURCE),Map.of("source",Set.of("orders")));
    }
    private record Fixture(MongoConfigurationDomain domain, MongoConnectionSettings settings, MongoDatabase config,
            MongoDatabase cache, StorageBinding binding, CacheAuthScope previous) {
        CacheAuthScope current() { return domain.bindings().cachePreparation().orElseThrow().authScope(); }
        StorageRecoveryCommands.Manifest manifest() {
            var hashes = new TreeMap<String,String>(); var pipelines = new TreeSet<String>();
            for (var resource : domain.artifacts().list()) {
                hashes.put(resource.id(), CanonicalHash.of(resource));
                if (resource instanceof PipelineResource) pipelines.add(resource.id());
            }
            return new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of()),
                    pipelines, hashes, CacheRecoveryStore.clusterDependencyHash(current(), domain.cacheRecoveries().inspectCluster(current())), domain.configurationRevision());
        }
        StorageRecoveryCommands.Plan confirmed() {
            var commands = domain.recoveryCommands(); return commands.confirm(commands.plan(current(), manifest(), null, null));
        }
        CacheRecoveryStore.FullPreparation full(String pipeline) {
            var commands=domain.recoveryCommands();domain.cacheRecoveries().stageCluster(confirmed());
            var cluster=commands.complete(commands.current(current()).orElseThrow());
            var all=manifest();var full=new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.PIPELINE,Set.of(pipeline)),
                    Set.of(pipeline),all.definitionHashes(),CanonicalHash.ofText("explicit-full-"+pipeline),domain.configurationRevision());
            var running=commands.start(commands.confirm(commands.plan(current(),full,null,cluster.operationId())));
            return domain.cacheRecoveries().authorizeFull(running,pipeline);
        }
    }
    private static void fixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var config = admin.getDatabase("recovery_config_" + suffix); var cache = admin.getDatabase("recovery_cache_" + suffix);
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(config.getName()), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, "cluster", "install", "setup", 1)) {
                var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(cache.getName()), cache.getName(), null);
                var target = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                var committed = domain.bindings().commitWithFirstCache(SETUP, StorageBinding.Mode.OP, endpoint, target);
                try (var initial = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "recovery-before")) {
                    initial.prepareFirstAuth(committed.firstCreation().orElseThrow()); initial.prepareInitialBusiness(committed.preparation(), ACTIVE);
                }
                domain.bindings().complete(SETUP);
                domain.firstAdmin().create(domain.firstAdmin().load().orElseThrow().scope(), new User("admin", "fixture-hash", "admin"));
                domain.artifacts().saveAll(List.of(SOURCE, pipeline("a"), pipeline("b")));
                var previous = committed.preparation().authScope();
                for (String id : List.of("a", "b")) {
                    var scope = new PipelineIntentScope("cluster", 1, previous.cacheIncarnation(), id, 1);
                    assertThat(domain.intents().compareAndSet(scope, 0, new DesiredState(id, id.equals("a") ? PipelineState.RUNNING : PipelineState.STOPPED,
                            CanonicalHash.of(pipeline(id)), false, null, false, null))).isTrue();
                }
                cache.drop();
                try (var auth = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "recovery-auth")) { domain.prepareAuthenticationRecovery(auth, ACTIVE); }
                scenario.run(new Fixture(domain, settings, config, cache, committed.binding(), previous));
            } finally { config.drop(); cache.drop(); }
        } catch (Exception failure) { throw new AssertionError(failure); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
