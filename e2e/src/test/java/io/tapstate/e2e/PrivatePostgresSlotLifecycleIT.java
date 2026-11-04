package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.PipelineStateMapping;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import io.tapstate.testsupport.DockerGate;

/** Private PostgreSQL slot cleanup follows the original clearing intent and preserves other readers. */
class PrivatePostgresSlotLifecycleIT {
    private static final Duration BOUND = Duration.ofSeconds(180);
    private static final String SOURCE = "pg_slot_source";
    private static final String TABLE = "orders";
    private static final String A = "direct_slot_a";
    private static final String B = "direct_slot_b";
    private static final String S = "shared_slot_baseline";
    private final Path evidence = Path.of(System.getProperty("slot.probe.evidence", "target/private-pg-slot-lifecycle"));
    private final Map<String,Object> facts = new LinkedHashMap<>();
    private final List<Map<String,Object>> snapshots = new ArrayList<>();

    @BeforeAll static void requireRealInputs() { DockerGate.require(); RealConnectorGate.require("postgres", "mongodb"); }

    @Test void aPrivateClearingStopReleasesOnlyItsSlotAndPauseKeepsTheOtherSlot() throws Exception {
        facts.put("scenario", "private-postgres-slot-lifecycle");
        facts.put("fidelity", "REAL_PROCESS");
        facts.put("startedAt", Instant.now().toString());
        facts.put("snapshots", snapshots);
        facts.put("stage", "prepare");
        String databasePrefix = "private_slot_" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0,12);
        Map<String,Object> source = SharedPostgres.settings(databasePrefix);
        facts.put("postgresDatabase", source.get("database"));
        try (Connection c = SharedPostgres.connect(source); Statement sql = c.createStatement()) {
            sql.execute("CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))");
            sql.execute("ALTER TABLE orders REPLICA IDENTITY FULL");
            sql.execute("INSERT INTO orders VALUES (1,'seed-1'),(2,'seed-2')");
        }
        Map<String,String> targets = new LinkedHashMap<>();
        for (String p : List.of(A,B,S)) targets.put(p, SharedMongo.replicaSetUrl(databasePrefix+"_"+p));
        try (PreparedNativeStore storage = PreparedNativeStore.create();
             var launch = RealProcessServer.prepareNative(storage.configurationUri(), "127.0.0.1", port -> List.of(
                     "--tapstate.cluster.id="+storage.clusterId(), "--tapstate.hz.member-port=0",
                     "--tapstate.hz.jet.cooperative-thread-count=2"));
             RealProcessServer server = launch.launch(); MongoEndpoints mongo = new MongoEndpoints()) {
            facts.put("serverOutput", launch.output().toString());
            facts.put("configurationDatabase", storage.configurationDatabase());
            server.awaitHealthy();
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String,String> resources = new LinkedHashMap<>();
            resources.put(SOURCE+".tap.yml", sourceYaml(source));
            for (String p : List.of(A,B,S)) {
                resources.put(p+"_target.tap.yml", "version: tapstate/v1\nkind: source\nid: "+p+"_target\nconnector: mongodb\nconfig: { uri: \""+targets.get(p)+"\" }\n");
                resources.put(p+".tap.yml", pipeline(p,p.equals(S)));
            }
            control.apply(resources);
            Map<String,Object> discovery = new LinkedHashMap<>();
            for (String key : List.of("host","port","database")) discovery.put(key,source.get(key));
            discovery.put("schema","public");discovery.put("user",source.get("username"));discovery.put("password",source.get("password"));
            control.discoverSchema(SOURCE,"postgres",discovery);
            assertThat(PostgresSlots.names(source)).isEmpty();
            Map<String,String> slots = new LinkedHashMap<>();
            for (String p : List.of(A,B,S)) {
                var before = PostgresSlots.names(source);
                facts.put("stage", "start-"+p);save();
                control.lifecycle(p,LifecycleVerb.START);state(control,p,PipelineState.RUNNING);
                row(mongo,targets.get(p),1);
                Await.until("one original slot for "+p,BOUND,
                        () -> PostgresSlots.names(source).size()==before.size()+1,
                        () -> PostgresSlots.of(source).toString());
                var added = PostgresSlots.names(source).stream().filter(n -> !before.contains(n)).toList();
                assertThat(added).hasSize(1); slots.put(p,added.getFirst());
                snapshot("started-"+p,source,control);save();
            }
            facts.put("slotByPipeline",slots);
            assertThat(slots.values()).doesNotHaveDuplicates();
            insert(source,101);for(String p:List.of(A,B,S))row(mongo,targets.get(p),101);
            facts.put("allThreeInitialCdc",true);
            control.lifecycle(B,LifecycleVerb.PAUSE);state(control,B,PipelineState.PAUSED);
            assertThat(PostgresSlots.names(source)).contains(slots.get(B));
            snapshot("direct-b-paused",source,control);facts.put("pauseRetainedSameSlot",true);save();
            insert(source,102);
            control.lifecycle(B,LifecycleVerb.RESUME);state(control,B,PipelineState.RUNNING);
            row(mongo,targets.get(B),102);
            assertThat(PostgresSlots.names(source)).contains(slots.get(B));
            facts.put("pauseResumeCdc",true);
            long originalA = generation(storage,A);
            control.stop(A,true);clearCompleted(control,storage,A,originalA);
            Await.until("the cleared direct reader to physically release its active connection",BOUND,
                    () -> PostgresSlots.of(source).stream().noneMatch(s -> s.name().equals(slots.get(A)) && s.active()),
                    () -> PostgresSlots.of(source).toString());
            snapshot("direct-a-clear-complete",source,control);
            facts.put("directClearSuccessorGeneration",generation(storage,A));
            assertThat(PostgresSlots.names(source)).contains(slots.get(B),slots.get(S));
            insert(source,103);row(mongo,targets.get(B),103);row(mongo,targets.get(S),103);
            facts.put("otherDirectAndSharedCdcAfterDirectClear",true);save();
            long originalS = generation(storage,S);
            control.stop(S,true);clearCompleted(control,storage,S,originalS);
            assertThat(PostgresSlots.names(source)).contains(slots.get(S));
            facts.put("srsClearingStopRetainedOriginalSlot",true);
            snapshot("srs-clear-complete-retained",source,control);
            insert(source,104);row(mongo,targets.get(B),104);
            facts.put("otherDirectCdcAfterSrsClear",true);
            facts.put("stage","expected-own-slot-removal");save();
            long start = System.nanoTime();
            try {
                Await.until("non-SRS clearing stop must remove only its original PostgreSQL slot",BOUND,
                        () -> !PostgresSlots.names(source).contains(slots.get(A)),
                        () -> "original="+slots.get(A)+", actual="+PostgresSlots.of(source));
                facts.put("directStopRemovedSlot",true);
            } catch (AssertionError expected) {
                facts.put("directStopRemovedSlot",false);
                facts.put("failure",expected.toString());
                throw expected;
            } finally {
                facts.put("slotRemovalObservationSeconds",(System.nanoTime()-start)/1_000_000_000.0);
                snapshot("final",source,control);save();
            }
            facts.put("stage","new-private-full");save();
            control.startAnswering(A,"target-not-empty","keep");state(control,A,PipelineState.RUNNING);
            Await.until("one fresh slot for the new private full",BOUND,
                    () -> PostgresSlots.names(source).stream().filter(n -> !n.equals(slots.get(B)) && !n.equals(slots.get(S))).count()==1,
                    () -> PostgresSlots.of(source).toString());
            String fresh=PostgresSlots.names(source).stream().filter(n -> !n.equals(slots.get(B)) && !n.equals(slots.get(S))).findFirst().orElseThrow();
            assertThat(fresh).isNotEqualTo(slots.get(A));
            insert(source,105);row(mongo,targets.get(A),105);row(mongo,targets.get(B),105);
            facts.put("newFullCreatedDifferentPrivateSlotAndDeliveredCdc",true);
            control.stop(A,false);state(control,A,PipelineState.STOPPED);
            Await.until("the retained private reader to end",BOUND,
                    () -> PostgresSlots.of(source).stream().noneMatch(slot -> slot.name().equals(fresh) && slot.active()),
                    () -> PostgresSlots.of(source).toString());
            assertThat(PostgresSlots.names(source)).contains(fresh);insert(source,106);row(mongo,targets.get(B),106);
            control.startAnswering(A,"target-not-empty","keep");state(control,A,PipelineState.RUNNING);row(mongo,targets.get(A),106);
            assertThat(PostgresSlots.names(source)).contains(fresh);
            facts.put("keepStopRetainedSlotAndResumedStoppedWindowCdc",true);save();
            long beforeCarried=generation(storage,A);
            try(var cache=MongoClients.create(storage.cacheUri());var held=cache.startSession();
                    var config=MongoConfigurationDomain.openExisting(new MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5)))) {
                held.startTransaction();
                try {
                    var checkpoints=SystemCollections.CACHE_PIPELINE_STATE.on(cache.getDatabase(storage.cacheDatabase()));
                    assertThat(checkpoints.updateOne(held,new Document("_id",A),new Document("$inc",new Document("epoch",1L))).getMatchedCount()).isEqualTo(1);
                    control.stop(A,true);
                    assertThat(config.intents().load(A).orElseThrow().desired().purgeState()).isTrue();
                    control.startAnswering(A,"target-not-empty","keep");
                    var carried=config.intents().load(A).orElseThrow().desired();
                    assertThat(carried.targetState()).isEqualTo(PipelineState.RUNNING);
                    assertThat(carried.purgeState()).isTrue();assertThat(carried.reassemble()).isTrue();
                    facts.put("clearingStopCarriedByStartBeforeCheckpointBarrierReleased",true);
                } finally {if(held.hasActiveTransaction())held.abortTransaction();}
            }
            replacementCompleted(control,storage,A,beforeCarried,PipelineState.RUNNING);
            Await.until("the carried clearing stop to remove its old private slot",BOUND,
                    () -> !PostgresSlots.names(source).contains(fresh),() -> PostgresSlots.of(source).toString());
            insert(source,107);row(mongo,targets.get(A),107);row(mongo,targets.get(B),107);
            assertThat(PostgresSlots.names(source)).contains(slots.get(B),slots.get(S));
            facts.put("carriedStopRemovedOldPrivateSlotAndRestartedCdc",true);snapshot("carried-stop-complete",source,control);save();
        } catch (Throwable failure) {
            facts.put("terminalFailure",failure.toString());throw failure;
        } finally {
            facts.put("finishedAt",Instant.now().toString());save();
            Object output=facts.get("serverOutput");
            if(output!=null && Files.exists(Path.of(output.toString())))Files.copy(Path.of(output.toString()),evidence.resolve("server.out"),java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void snapshot(String stage,Map<String,Object> source,ControlPlane control) {
        Map<String,Object> value=new LinkedHashMap<>();value.put("stage",stage);value.put("at",Instant.now().toString());
        value.put("slots",PostgresSlots.of(source).stream().map(s -> Map.of("name",s.name(),"active",s.active(),"confirmedFlushLsn",String.valueOf(s.confirmedFlushLsn()))).toList());
        Map<String,Object> states=new LinkedHashMap<>();for(String p:List.of(A,B,S))states.put(p,control.state(p).map(Enum::name).orElse("UNKNOWN"));
        value.put("states",states);snapshots.add(value);
    }
    private void save() throws java.io.IOException {Files.createDirectories(evidence);Files.writeString(evidence.resolve("observations.json"),JsonWriter.write(facts));}
    private static void state(ControlPlane c,String p,PipelineState expected) {Await.until(p+" "+expected,BOUND,() -> c.state(p).filter(expected::equals).isPresent(),() -> c.state(p)+", "+c.logs(p));}
    private static void row(MongoEndpoints mongo,String uri,int id) {Await.until("actual PG CDC row "+id+" at "+uri.substring(uri.lastIndexOf('/')+1),BOUND,() -> mongo.documents(EndpointAddress.uri(uri),TABLE).stream().anyMatch(d -> String.valueOf(id).equals(String.valueOf(d.get("id")))),() -> mongo.documents(EndpointAddress.uri(uri),TABLE).toString());}
    private static void insert(Map<String,Object> settings,int id) throws Exception {try(Connection c=SharedPostgres.connect(settings);Statement s=c.createStatement()){s.execute("SELECT pg_current_xact_id()");s.execute("INSERT INTO orders VALUES ("+id+",'cdc-"+id+"')");}}
    private static long generation(PreparedNativeStore s,String p) {try(var c=MongoConfigurationDomain.openExisting(new MongoConnectionSettings(s.configurationUri(),null,Duration.ofSeconds(5)))){return c.pipelineStates().read(p).orElseThrow().stateGeneration();}}
    private static void clearCompleted(ControlPlane control,PreparedNativeStore storage,String p,long generation) {
        replacementCompleted(control,storage,p,generation,PipelineState.STOPPED);
    }
    private static void replacementCompleted(ControlPlane control,PreparedNativeStore storage,String p,long generation,PipelineState expected) {
        try(var c=MongoConfigurationDomain.openExisting(new MongoConnectionSettings(storage.configurationUri(),null,Duration.ofSeconds(5)));var cache=MongoClients.create(storage.cacheUri())) {
            Await.until(p+" exact native clear completion",BOUND,() -> {
                var before=c.pipelineStates().read(p).orElseThrow();var intent=c.intents().load(p).orElseThrow();
                var checkpoint=SystemCollections.CACHE_PIPELINE_STATE.on(cache.getDatabase(storage.cacheDatabase())).find(new Document("_id",p)).first();
                var after=c.pipelineStates().read(p).orElseThrow();
                return before.equals(after)&&after.stateGeneration()==generation+1&&after.phase()==PipelineStateMapping.Phase.ACTIVE
                        &&intent.scope().stateGeneration()==generation+1&&!intent.desired().purgeState()
                        &&intent.desired().targetState()==expected&&checkpoint!=null
                        &&checkpoint.get("scope",Document.class).getLong("stateGeneration")==generation+1
                        &&control.state(p).filter(expected::equals).isPresent();
            },() -> c.pipelineStates().read(p)+", "+control.logs(p));
        }
    }
    private static String pipeline(String p,boolean shared) {return """
            version: tapstate/v1
            kind: pipeline
            id: %s
            source: [ { id: %s, srs: %s } ]
            settings: { read_mode: snapshot_and_cdc }
            transforms:
              - { id: %s_step, from: [ orders ], type: filter, expr: "op != 'x'" }
            serve:
              from: %s_step
              sync:
                - source: %s_target
            """.formatted(p,SOURCE,shared,p,p,p);}
    private static String sourceYaml(Map<String,Object> s) {return """
            version: tapstate/v1
            kind: source
            id: %s
            connector: postgres
            config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s, autoClearSlot: true }
            mode: cdc
            tables: [ orders ]
            """.formatted(SOURCE,s.get("host"),s.get("port"),s.get("database"),s.get("username"),s.get("password"));}
}
