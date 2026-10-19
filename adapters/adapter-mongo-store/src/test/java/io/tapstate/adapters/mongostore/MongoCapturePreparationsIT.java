package io.tapstate.adapters.mongostore;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.event.*;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Actual CONFIG grants and CACHE creation on independent servers, from the real initial domain and Pipeline paths. */
@RequiresDocker
class MongoCapturePreparationsIT {
    @Container private static final MongoDBContainer CONFIG=new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE=new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE=() -> {};
    private static final WorkloadOwner OWNER=new WorkloadOwner("node-a","boot-a");
    private static final Duration TTL=Duration.ofMinutes(2);
    private static final Instant AT=Instant.parse("2026-10-19T16:00:00Z");

    @ParameterizedTest @EnumSource(CapturePreparation.Mode.class)
    void firstCreationBindsModeAndOriginalScopeWhileSnapshotAndDirectCreateNoSrsRoot(CapturePreparation.Mode mode) {
        withFixture(mode,f -> {
            var grant=f.issue(f.request);var original=grant.preparation();assertThat(original.mode()).isEqualTo(mode);
            assertThat(f.configuration.capturePreparations().read(f.scope,original.captureId())).contains(original);
            var state=f.preparations().createFirst(grant);assertThat(state.preparation()).isEqualTo(original);assertThat(state.claim().claimGeneration()).isEqualTo(1);
            assertThat(state.chain().isPresent()).isEqualTo(mode==CapturePreparation.Mode.SHARED_SRS);
            assertThat(f.claims().read(new WorkloadClaimKey(f.scope.clusterId(),WorkloadClaimType.CAPTURE,original.captureId())).orElseThrow().leased()).isFalse();
            var acquired=f.claims().acquire(state.claim().key(),OWNER,1,TTL);assertThat(acquired.acquired()).isTrue();assertThat(acquired.claim().claimGeneration()).isEqualTo(2);
            var renewed=f.claims().renew(acquired.claim(),TTL).orElseThrow();assertThat(renewed.claimGeneration()).isEqualTo(2);
            assertThat(f.preparations().load(original).claim()).isEqualTo(WorkloadClaimFence.from(renewed));
            assertThat(SystemCollections.SRS_CHAINS.on(f.cache).countDocuments()).isEqualTo(mode==CapturePreparation.Mode.SHARED_SRS?1:0);
            assertThat(SystemCollections.SRS_MODELS.on(f.cache).countDocuments()).isZero();assertThat(SystemCollections.SRS_EVENTS.on(f.cache).countDocuments()).isZero();
            assertCode(() -> f.preparations().createFirst(grant),IoError.PIPELINE_INTENT_FENCED);
        });
    }
    @ParameterizedTest @EnumSource(CapturePreparation.Mode.class)
    void knownIssuedGrantCannotBeReissuedAfterCompleteClaimHistoryLoss(CapturePreparation.Mode mode) {
        withFixture(mode,f -> {
            var grant=f.issue(f.request);var original=grant.preparation();var state=f.preparations().createFirst(grant);
            var claim=f.claims().acquire(state.claim().key(),OWNER,1,TTL).claim();
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cache).deleteOne(new Document("_id",MongoWorkloadClaimStore.id(claim.key())));
            var before=f.snapshot();assertThat(f.configuration.capturePreparations().issueFirstCreate(f.request)).isEqualTo(new CaptureCreationResult.AlreadyIssued(original));
            assertCode(() -> f.claims().acquire(claim.key(),OWNER,1,TTL),IoError.CAPTURE_STATE_REBUILD_REQUIRED);
            assertCode(() -> f.preparations().load(original),IoError.CAPTURE_STATE_REBUILD_REQUIRED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void twoIndependentConfigClientsIssueOneGrantForTheSameSharedCapture() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            try(var second=MongoConfigurationDomain.open(f.settings,"cluster-a");var pool=Executors.newFixedThreadPool(2)) {
                var start=new CountDownLatch(1);var one=pool.submit(() -> {start.await();return f.configuration.capturePreparations().issueFirstCreate(f.request);});
                var two=pool.submit(() -> {start.await();return second.capturePreparations().issueFirstCreate(f.request);});start.countDown();
                var results=List.of(one.get(20,TimeUnit.SECONDS),two.get(20,TimeUnit.SECONDS));
                assertThat(results).filteredOn(CaptureCreationResult.Issued.class::isInstance).hasSize(1);assertThat(results).filteredOn(CaptureCreationResult.AlreadyIssued.class::isInstance).hasSize(1);
                assertThat(f.controls().countDocuments()).isEqualTo(1);
                var issued=(CaptureCreationResult.Issued)results.stream().filter(CaptureCreationResult.Issued.class::isInstance).findFirst().orElseThrow();
                assertThat(f.preparations().createFirst(issued.creation()).chain()).isPresent();
            }
        });
    }
    @Test void anotherPipelineUsesTheSameSharedGrantButDirectContractsRemainPrivate() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var first=f.issue(f.request);var b=f.pipeline("independent",CapturePreparation.Mode.SHARED_SRS,Set.of("customers"));
            assertThat(f.configuration.capturePreparations().issueFirstCreate(b)).isEqualTo(new CaptureCreationResult.AlreadyIssued(first.preparation()));
            f.preparations().createFirst(first);assertThat(f.preparations().load(first.preparation()).chain()).isPresent();
            assertThat(f.controls().countDocuments()).isEqualTo(1);
        });
        withFixture(CapturePreparation.Mode.DIRECT,f -> {
            var first=f.issue(f.request);var b=f.issue(f.pipeline("independent",CapturePreparation.Mode.DIRECT,Set.of("orders")));
            assertThat(first.preparation().captureId()).isNotEqualTo(b.preparation().captureId());
            f.preparations().createFirst(first);f.preparations().createFirst(b);assertThat(f.controls().countDocuments()).isEqualTo(2);
        });
    }
    @Test void configCommitWithoutDeliveredGrantLeavesStrictUnknownHistory() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var guard=f.configuration.capturePreparations().guardedBy(() -> {if(f.controls().countDocuments()>0)throw fenced();});
            assertCode(() -> guard.issueFirstCreate(f.request),IoError.WORKLOAD_CLAIM_FENCED);
            var raw=f.controls().find().first();assertThat(raw).isNotNull();var original=MongoCapturePreparationFacts.decode(raw);
            assertThat(f.configuration.capturePreparations().issueFirstCreate(f.request)).isEqualTo(new CaptureCreationResult.AlreadyIssued(original));
            assertCode(() -> f.preparations().load(original),IoError.CAPTURE_STATE_REBUILD_REQUIRED);
            assertThat(f.captureRows()).isEmpty();
        });
    }
    @Test void losingOriginalGuardConsumesTheInvocationAndDoesNotCreateCacheHistory() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var active=new AtomicBoolean(true);var issuer=f.configuration.capturePreparations().guardedBy(() -> {if(!active.get())throw fenced();});
            var grant=((CaptureCreationResult.Issued)issuer.issueFirstCreate(f.request)).creation();active.set(false);
            assertCode(() -> f.preparations().createFirst(grant),IoError.WORKLOAD_CLAIM_FENCED);active.set(true);
            assertCode(() -> f.preparations().createFirst(grant),IoError.PIPELINE_INTENT_FENCED);assertThat(f.captureRows()).isEmpty();
        });
    }
    @Test void wrongCacheTargetConsumesTheGrantBeforeAnyCacheIo() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var grant=f.issue(f.request);String name="wrong_capture_"+UUID.randomUUID().toString().replace("-","");
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,CACHE.getReplicaSetUrl(name),name,null);
            try(var other=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"wrong-target")) {
                assertCode(() -> other.capturePreparations(f.scope,ACTIVE).createFirst(grant),IoError.PIPELINE_INTENT_FENCED);
                assertCode(() -> f.preparations().createFirst(grant),IoError.PIPELINE_INTENT_FENCED);
                assertThat(f.cacheAdmin.getDatabase(name).listCollectionNames().into(new ArrayList<>())).isEmpty();
            }finally{f.cacheAdmin.getDatabase(name).drop();}
            assertThat(f.captureRows()).isEmpty();
        });
    }
    @Test void aFabricatedDtoNeverCallsAnyGrantAccessor() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var called=new AtomicInteger();FirstCaptureCreation fake=new FirstCaptureCreation(){
                public CapturePreparation preparation(){called.incrementAndGet();return null;}public CapturePreparationRequest request(){called.incrementAndGet();return null;}public String retention(){called.incrementAndGet();return null;}
                public void consume(){called.incrementAndGet();}public void requireActive(){called.incrementAndGet();}
            };
            assertCode(() -> f.preparations().createFirst(fake),IoError.PIPELINE_INTENT_FENCED);assertThat(called).hasValue(0);assertThat(f.captureRows()).isEmpty();
        });
    }
    @Test void aConfirmedCacheInsertFollowedByGuardLossRollsBackClaimChainAndOriginalProofs() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var grant=f.issue(f.request);var before=f.snapshot();
            try(var witness=new MongoCommandWitness(CACHE.getReplicaSetUrl(),f.cache.getName(),"insert","workload_claims")) {
                var store=new MongoCaptureCachePreparations(witness.client,witness.database,f.scope,MongoStorageBindingStore.targetFingerprint(f.connection),witness::refuseAfterCommand,MongoRateHistoryStore.DEFAULT_RETENTION);
                assertCode(() -> store.createFirst(grant),IoError.CACHE_BUSINESS_FENCED);witness.requireCommand();
            }
            assertThat(f.snapshot()).isEqualTo(before);assertCode(() -> f.preparations().load(grant.preparation()),IoError.CAPTURE_STATE_REBUILD_REQUIRED);
        });
    }
    @Test void losingSrsRootCannotUseTheRetainedClaimToRecreateTheRoot() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var grant=f.issue(f.request);var original=grant.preparation();var first=f.preparations().createFirst(grant);var claim=f.claims().acquire(first.claim().key(),OWNER,1,TTL).claim();
            SystemCollections.SRS_CHAINS.on(f.cache).deleteMany(new Document());var before=f.snapshot();
            assertCode(() -> f.preparations().load(original),IoError.CAPTURE_STATE_REBUILD_REQUIRED);
            assertCode(() -> f.domain.srs(f.scope,ACTIVE).chains().prepare(original.chainId(),null,WorkloadClaimFence.from(claim)),IoError.SRS_CHECKPOINT_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"formatVersion","mode","preparationId","unknown"})
    void malformedConfigFactsAreNotDefaultedOrReissued(String field) {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var grant=f.issue(f.request);var prep=grant.preparation();Object value=field.equals("formatVersion")?1L:"damaged";
            f.controls().updateOne(new Document(),new Document("$set",new Document(field,value)));var before=f.controls().find().first();
            assertCode(() -> f.configuration.capturePreparations().read(f.scope,prep.captureId()),IoError.DOCUMENT_UNREADABLE);
            assertCode(() -> f.configuration.capturePreparations().issueFirstCreate(f.request),IoError.DOCUMENT_UNREADABLE);assertThat(f.controls().find().first()).isEqualTo(before);
        });
    }
    @Test void ordinaryAccessDoesNotRecreateTheControlCollectionOrAdoptAChangedSource() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            f.controls().drop();assertCode(() -> f.configuration.capturePreparations().issueFirstCreate(f.request),IoError.DOCUMENT_UNREADABLE);
            assertThat(f.config.listCollectionNames().into(new ArrayList<>())).doesNotContain("capture_preparations");
        });
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var source=(SourceResource)f.configuration.artifacts().get("input").orElseThrow();
            f.configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),Map.of("host","new-server"),source.mode(),source.tables(),source.srs(),source.experimental()));
            assertThatThrownBy(() -> f.configuration.capturePreparations().issueFirstCreate(f.request)).isInstanceOf(TapstateException.class);assertThat(f.controls().countDocuments()).isZero();
        });
    }
    @ParameterizedTest @EnumSource(value=CapturePreparation.Mode.class,names={"DIRECT","SNAPSHOT_ONLY"})
    void ordinarySrsPrepareCannotCreateARootForNonSharedCapture(CapturePreparation.Mode mode) {
        withFixture(mode,f -> {
            var first=f.preparations().createFirst(f.issue(f.request));
            var live=WorkloadClaimFence.from(f.claims().acquire(first.claim().key(),OWNER,1,TTL).claim());
            var before=f.snapshot();
            assertCode(() -> f.domain.srs(f.scope,ACTIVE).chains().prepare("unrelated-chain",null,live),IoError.SRS_CHECKPOINT_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @Test void ordinarySrsPrepareCannotCreateAnotherRootWithTheSameSharedClaim() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var first=f.preparations().createFirst(f.issue(f.request));
            var live=WorkloadClaimFence.from(f.claims().acquire(first.claim().key(),OWNER,1,TTL).claim());
            assertCode(() -> f.domain.srs(f.scope,ACTIVE).chains().prepare("unrelated-chain",null,live),IoError.SRS_CHECKPOINT_UNPROVEN);
        });
    }
    @ParameterizedTest @ValueSource(strings={"mode","chain","sourceIdentityHash"})
    void issuedConfigIdentityMustMatchTheFreshlyVerifiedCapture(String field) {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            f.issue(f.request);
            Object value=switch(field){case "mode" -> "DIRECT";case "chain" -> OperatorStateText.encode("different-chain");default -> "mc-"+"a".repeat(64);};
            f.controls().updateOne(new Document(),new Document("$set",new Document(field,value)));
            assertCode(() -> f.configuration.capturePreparations().issueFirstCreate(f.request),IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN);
            assertThat(f.controls().countDocuments()).isEqualTo(1);
        });
    }
    @Test void twoPhysicalSourcesCannotShareTheSameExplicitCaptureKey() {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var source=(SourceResource)f.configuration.artifacts().get("input").orElseThrow();
            f.configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),source.config(),source.mode(),source.tables(),new Srs("asserted-key",null,null,null,true),null));
            var left=f.pipeline("left",CapturePreparation.Mode.SHARED_SRS,Set.of("orders"));var original=f.issue(left).preparation();
            f.changeSource();var right=f.pipeline("right",CapturePreparation.Mode.SHARED_SRS,Set.of("orders"));
            assertCode(() -> f.configuration.capturePreparations().issueFirstCreate(right),IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN);
            assertThat(f.configuration.capturePreparations().read(f.scope,original.captureId())).contains(original);
            assertThat(f.controls().countDocuments()).isEqualTo(1);
        });
    }
    @ParameterizedTest @EnumSource(value=CapturePreparation.Mode.class,names={"SHARED_SRS","SNAPSHOT_ONLY"})
    void firstCacheCreationMustUseTheOriginalPipelineSelection(CapturePreparation.Mode mode) {
        withFixture(mode,f -> {
            var input=f.inputs(Set.of("customers"));
            var alternate=new CapturePreparationRequest(f.request.pipeline(),f.request.originalPipeline(),f.node,"input",input);
            var grant=f.issue(alternate);var before=f.snapshot();
            assertCode(() -> f.preparations().createFirst(grant),IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
            assertCode(() -> f.preparations().createFirst(grant),IoError.PIPELINE_INTENT_FENCED);
        });
    }
    @ParameterizedTest @EnumSource(value=CapturePreparation.Mode.class,names={"SHARED_SRS","SNAPSHOT_ONLY"})
    void firstCacheCreationMustUseTheOriginalPipelineSourceHash(CapturePreparation.Mode mode) {
        withFixture(mode,f -> {
            f.changeSource();
            var alternate=new CapturePreparationRequest(f.request.pipeline(),f.request.originalPipeline(),f.node,"input",f.inputs(Set.of("orders")));
            var grant=f.issue(alternate);var before=f.snapshot();
            assertCode(() -> f.preparations().createFirst(grant),IoError.PIPELINE_CAPTURE_IDENTITY_UNPROVEN);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }
    @ParameterizedTest @ValueSource(strings={"source","mapping"})
    void aConfigChangeAfterSnapshotValidationCannotCommitAStaleFirstGrant(String change) {
        withFixture(CapturePreparation.Mode.SHARED_SRS,f -> {
            var applied=new AtomicBoolean();var failure=new AtomicReference<Throwable>();var pending=ConcurrentHashMap.<Integer>newKeySet();
            var witness=new CommandListener(){
                public void commandStarted(CommandStartedEvent event){
                    if(event.getDatabaseName().equals(f.config.getName()) && event.getCommandName().equals("find")
                            && event.getCommand().getString("find").getValue().equals("sources"))pending.add(event.getRequestId());
                }
                public void commandSucceeded(CommandSucceededEvent event){
                    if(!pending.remove(event.getRequestId()) || event.getResponse().getDocument("cursor").getArray("firstBatch").isEmpty() || !applied.compareAndSet(false,true))return;
                    try {
                        if(change.equals("source"))f.changeSource();
                        else {
                            var intent=f.configuration.intents().load("flow").orElseThrow();
                            f.configuration.rebuilds().begin(new PipelineRebuildRequest("confirmed-rebuild",intent,f.request.originalPipeline(),0,f.request.inputs()));
                        }
                    }catch(Throwable thrown){failure.set(thrown);}
                }
            };
            try(var client=MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(CONFIG.getReplicaSetUrl())).addCommandListener(witness).build())) {
                var database=client.getDatabase(f.config.getName());
                var material=SystemCollections.CLUSTER_KEYS.on(f.config).find().first().getString("material");
                var cipher=SourceConfigCipherProvider.fixed(new SourceConfigCipher(Base64.getUrlDecoder().decode(material)));
                var bindings=new MongoStorageBindingStore(client,database,cipher);
                var store=new MongoCaptureStatePreparations(client,database,"cluster-a",bindings,new MongoConfigurationArtifactStore(client,database,cipher),ACTIVE);
                assertCode(() -> store.issueFirstCreate(f.request),IoError.PIPELINE_INTENT_FENCED);
                assertThat(failure.get()).isNull();assertThat(applied).isTrue();assertThat(f.controls().countDocuments()).isZero();
            }
        });
    }
    private static void assertCode(Runnable action,TapstateErrorCode expected){assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(expected));}
    private static TapstateException fenced(){return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);}
    private static void withFixture(CapturePreparation.Mode mode,Scenario scenario) {
        String suffix=UUID.randomUUID().toString().replace("-","");String configName="capture_cfg_"+suffix,cacheName="capture_cache_"+suffix;
        try(var configAdmin=MongoClients.create(CONFIG.getReplicaSetUrl());var cacheAdmin=MongoClients.create(CACHE.getReplicaSetUrl())) {
            var config=configAdmin.getDatabase(configName);var cache=cacheAdmin.getDatabase(cacheName);
            var settings=new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName),null,Duration.ofSeconds(5));
            var connection=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,CACHE.getReplicaSetUrl(cacheName),cacheName,null);
            try(var configuration=MongoConfigurationBootstrap.prepare(settings,"cluster-a","install-a","setup-a",1);var domain=MongoCacheDomain.connect(connection,Duration.ofSeconds(5),"capture-preparations-test")) {
                var master=new SourceResource("views",null,"mongodb",Map.of("uri","mongodb://master.invalid/master"),null,null,null,null);
                var binding=configuration.bindings().commitWithFirstCache(new StorageSetupScope("cluster-a","install-a","setup-a",1),StorageBinding.Mode.OP,connection,master);
                domain.prepareFirstAuth(binding.firstCreation().orElseThrow());var scope=domain.prepareInitialBusiness(binding.preparation(),ACTIVE);
                var node=WorkloadClaimFence.from(domain.workloadClaims(scope).acquire(new WorkloadClaimKey("cluster-a",WorkloadClaimType.NODE_SESSION,OWNER.nodeId()),OWNER,0,TTL).claim());
                var source=new SourceResource("input",null,"mysql",Map.of("host","server-a","password","secret-sentinel"),SourceMode.CDC,List.of(TableRef.literal("orders"),TableRef.literal("customers")),null,null);
                configuration.artifacts().create(source);
                var fixture=new Fixture(configuration,config,cacheAdmin,cache,domain,settings,connection,scope,node,null);
                var request=fixture.pipeline("flow",mode,Set.of("orders"));scenario.run(new Fixture(configuration,config,cacheAdmin,cache,domain,settings,connection,scope,node,request));
            }finally{config.drop();cache.drop();}
        }catch(RuntimeException|Error failure){throw failure;}catch(Exception failure){throw new AssertionError(failure);}
    }
    private record Fixture(MongoConfigurationDomain configuration,MongoDatabase config,MongoClient cacheAdmin,MongoDatabase cache,MongoCacheDomain domain,MongoConnectionSettings settings,
            StorageBinding.Connection connection,CacheBusinessScope scope,WorkloadClaimFence node,CapturePreparationRequest request) {
        CapturePreparationRequest pipeline(String id,CapturePreparation.Mode mode,Set<String> tables) {
            String readMode=mode==CapturePreparation.Mode.SNAPSHOT_ONLY?"snapshot_only":"snapshot_and_cdc";
            var pipeline=(PipelineResource)new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: "+id+"\nsource: [{id: input, srs: "+(mode!=CapturePreparation.Mode.DIRECT)+"}]\nsettings: {read_mode: "+readMode+"}\n");
            configuration.artifacts().create(pipeline);var original=new PipelineIntentScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation(),id,1);
            configuration.intents().compareAndSet(original,0,new DesiredState(id,PipelineState.RUNNING,CanonicalHash.of(pipeline),false));
            var source=(SourceResource)configuration.artifacts().get("input").orElseThrow();var inputs=PipelinePreparationInputs.of(original,pipeline,Map.of("input",source),Map.of("input",tables));
            var creation=((FirstPipelineCreationResult.Issued)configuration.preparations().issueFirstCreate(original,node,inputs)).creation();
            var preparer=domain.pipelinePreparations(scope,ACTIVE);var reserved=preparer.reserveFirst(creation,TTL);configuration.preparations().complete(preparer.seed(reserved,configuration.preparations(),AT));
            var owner=claims().acquirePipeline(original,OWNER,1,TTL).claim();var running=claims().advanceExecution(owner,1,Set.of(OWNER.nodeId())).orElseThrow();
            return new CapturePreparationRequest(original,WorkloadClaimFence.from(running),node,"input",inputs);
        }
        PipelinePreparationInputs inputs(Set<String> tables) {
            var pipeline=(PipelineResource)configuration.artifacts().get("flow").orElseThrow();
            var source=(SourceResource)configuration.artifacts().get("input").orElseThrow();
            return PipelinePreparationInputs.of(request.pipeline(),pipeline,Map.of("input",source),Map.of("input",tables));
        }
        void changeSource() {
            var source=(SourceResource)configuration.artifacts().get("input").orElseThrow();
            configuration.artifacts().save(new SourceResource(source.id(),source.metadata(),source.connector(),Map.of("host","different-server"),source.mode(),source.tables(),source.srs(),source.experimental()));
        }
        FirstCaptureCreation issue(CapturePreparationRequest original){return((CaptureCreationResult.Issued)configuration.capturePreparations().issueFirstCreate(original)).creation();}
        CaptureCachePreparations preparations(){return domain.capturePreparations(scope,ACTIVE);}WorkloadClaimStore claims(){return domain.workloadClaims(scope);}
        MongoCollection<Document> controls(){return SystemCollections.CAPTURE_PREPARATIONS.on(config);}
        List<Document> captureRows(){return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(new Document("resourceType","CAPTURE")).into(new ArrayList<>());}
        Map<String,List<Document>> snapshot(){var result=new TreeMap<String,List<Document>>();for(var table:List.of(SystemCollections.CACHE_IDENTITY,SystemCollections.CACHE_WORKLOAD_CLAIMS,SystemCollections.CACHE_PIPELINE_STATE,SystemCollections.SRS_CHAINS))result.put(table.collectionName(),table.on(cache).find().sort(new Document("_id",1)).into(new ArrayList<>()));return result;}
    }
    @FunctionalInterface private interface Scenario{void run(Fixture fixture)throws Exception;}
}
