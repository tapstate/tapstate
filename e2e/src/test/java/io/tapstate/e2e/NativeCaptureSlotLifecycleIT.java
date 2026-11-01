package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Real stopped-period CDC and the original non-deleting connector policy. */
class NativeCaptureSlotLifecycleIT {
    private static final Duration BOUND=Duration.ofSeconds(180);
    @BeforeAll static void prerequisites(){DockerGate.require();RealConnectorGate.require("postgres","mongodb");}

    @Test void aFreshRunAfterFinalClearCanUseTheExistingSharedLossRecovery() throws Exception {
        var source=SharedPostgres.settings("clear_then_shared_recovery");
        try(var connection=SharedPostgres.connect(source);var statement=connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("ALTER TABLE orders REPLICA IDENTITY FULL");statement.execute("INSERT INTO orders VALUES (1,'original')");
        }
        String target=SharedMongo.replicaSetUrl("clear_then_shared_recovery_target"),pipeline="clear_then_recover";
        try(var storage=PreparedNativeStore.create();var server=Tiers.IN_PROCESS.launch(storage);var mongo=new MongoEndpoints();
                var configClient=MongoClients.create(storage.configurationUri());var cacheClient=MongoClients.create(storage.cacheUri())) {
            var control=new ControlPlane(server.baseUrl());storage.loginAndAwaitBusiness(control);
            control.registerConnector("postgres",ConnectorJars.bytesFor("postgres"));control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("input.tap.yml","""
                    version: tapstate/v1
                    kind: source
                    id: input
                    connector: postgres
                    config: {host: %s, port: %s, database: %s, schema: public, user: %s, password: %s, autoClearSlot: true}
                    mode: cdc
                    tables: [orders]
                    """.formatted(source.get("host"),source.get("port"),source.get("database"),source.get("username"),source.get("password")),
                    "output.tap.yml","""
                    version: tapstate/v1
                    kind: source
                    id: output
                    connector: mongodb
                    config: {uri: "%s"}
                    """.formatted(target),pipeline+".tap.yml",Workspaces.pipelineYaml(pipeline,"input","output","orders")));
            var discovery=new LinkedHashMap<String,Object>(source);discovery.put("user",source.get("username"));discovery.put("schema","public");
            control.discoverSchema("input","postgres",discovery);start(control,pipeline);delivered(mongo,target,1);
            String first=PostgresSlots.only(source).name();control.stop(pipeline,true);state(control,pipeline,PipelineState.STOPPED);
            Await.until("original slot removed before the successor runs",BOUND,() -> PostgresSlots.names(source).isEmpty(),() -> PostgresSlots.of(source).toString());
            control.startAnswering(pipeline,"target-not-empty","keep");state(control,pipeline,PipelineState.RUNNING);
            Await.until("successor capture active",BOUND,() -> PostgresSlots.active(source),() -> PostgresSlots.of(source).toString());
            insert(source,501,"before shared loss");delivered(mongo,target,501);
            String successorSlot=PostgresSlots.only(source).name();assertThat(successorSlot).isNotEqualTo(first);
            control.stop(pipeline,false);state(control,pipeline,PipelineState.STOPPED);
            var config=configClient.getDatabase(storage.configurationDatabase());var cache=cacheClient.getDatabase(storage.cacheDatabase());
            var prior=config.getCollection("capture_preparations").find(new Document("currentClear.phase","READY")).first();
            assertThat(prior).isNotNull().containsEntry("formatVersion",3);
            var clear=prior.get("currentClear",Document.class);String physical=clear.getString("nextChain");
            String canonical=config.getCollection("pdk_note_scopes").find(new Document("ownerKind","CHAIN")).first().getString("ownerId");
            assertThat(cache.getCollection("srs_models").deleteMany(new Document("_id.chain",physical)).getDeletedCount()).isPositive();
            var preview=storage(control,"plan",Map.of("scope","SHARED_SRS","pipelineIds",java.util.List.of(),"canonicalChain",canonical,"physicalChain",physical));
            assertThat(preview.get("affectedPipelineIds")).isEqualTo(java.util.List.of(pipeline));assertThat(preview.get("preflightCurrent")).isEqualTo(true);
            storage(control,"confirm",Map.of("operationId",preview.get("operationId"),"planHash",preview.get("planHash"),
                    "controlRevision",preview.get("controlRevision"),"confirmed",true));
            Await.until("the original shared-loss recovery completes after a clear successor",BOUND,() -> {
                var status=storage(control,"status",Map.of("operationId",preview.get("operationId")));
                if(status.get("executionErrorCode")!=null)throw new AssertionError("shared recovery failed: "+status);
                return "COMPLETE".equals(status.get("phase"));
            },() -> String.valueOf(storage(control,"status",Map.of("operationId",preview.get("operationId")))));
            var recovered=config.getCollection("capture_preparations").find(new Document("currentRecovery.operationId",preview.get("operationId"))).first();
            assertThat(recovered).containsEntry("formatVersion",4);var tagged=recovered.get("currentRecovery",Document.class);
            assertThat(tagged).containsEntry("formatVersion",2).containsEntry("phase","READY");
            assertThat(tagged.get("previous",Document.class)).containsEntry("formatVersion",3).containsEntry("physicalChain",physical);
            var gate=config.getCollection("capture_external_retirements").find().first();
            assertThat(gate.get("currentPreparation")).isEqualTo(tagged.get("replacement"));
            assertThat(PostgresSlots.names(source)).containsExactly(successorSlot);
            assertThat(control.state(pipeline)).contains(PipelineState.STOPPED);
            control.stop(pipeline,true);state(control,pipeline,PipelineState.STOPPED);
            control.startAnswering(pipeline,"target-not-empty","keep");state(control,pipeline,PipelineState.RUNNING);
            Await.until("recovered capture active",BOUND,() -> PostgresSlots.active(source),() -> PostgresSlots.of(source).toString());
            insert(source,502,"after confirmed shared recovery");delivered(mongo,target,502);
            assertThat(PostgresSlots.names(source)).containsExactly(successorSlot);
            control.stop(pipeline,false);state(control,pipeline,PipelineState.STOPPED);
            assertThat(PostgresSlots.names(source)).containsExactly(successorSlot);
        }
    }
    private static void insert(Map<String,Object> source,int id,String value)throws Exception {
        try(var connection=SharedPostgres.connect(source);var statement=connection.createStatement()) {
            statement.execute("SELECT pg_current_xact_id()");statement.execute("INSERT INTO orders VALUES ("+id+",'"+value+"')");
        }
    }
    private static Map<?,?> storage(ControlPlane control,String action,Map<String,?> request) {
        return (Map<?,?>)io.tapstate.core.common.JsonReader.parse(control.sourceRequest("POST","/api/storage/recovery/"+action,request,null,200).body());
    }

    @ParameterizedTest @ValueSource(booleans={true,false})
    void aStoppedRetainedPeerReceivesChangesWrittenAcrossAnotherClear(boolean automatic) throws Exception {
        String suffix=automatic?"automatic":"retained";
        var source=SharedPostgres.settings("slot_lifecycle_"+suffix);
        try(var connection=SharedPostgres.connect(source);var statement=connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("ALTER TABLE orders REPLICA IDENTITY FULL");
            statement.execute("INSERT INTO orders VALUES (1,'original')");
        }
        var target=SharedMongo.replicaSetUrl("slot_lifecycle_"+suffix);
        String p="clear_peer_"+suffix,q="retained_peer_"+suffix;
        try(var storage=PreparedNativeStore.create();var server=Tiers.IN_PROCESS.launch(storage);var mongo=new MongoEndpoints()) {
            var control=new ControlPlane(server.baseUrl());storage.loginAndAwaitBusiness(control);
            control.registerConnector("postgres",ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb",ConnectorJars.bytesFor("mongodb"));
            var resources=new LinkedHashMap<String,String>();
            resources.put("input.tap.yml","""
                    version: tapstate/v1
                    kind: source
                    id: input
                    connector: postgres
                    config: {host: %s, port: %s, database: %s, schema: public, user: %s, password: %s, autoClearSlot: %s}
                    mode: cdc
                    tables: [orders]
                    """.formatted(source.get("host"),source.get("port"),source.get("database"),source.get("username"),source.get("password"),automatic));
            resources.put("output.tap.yml","""
                    version: tapstate/v1
                    kind: source
                    id: output
                    connector: mongodb
                    config: {uri: "%s"}
                    """.formatted(target));
            resources.put(p+".tap.yml",Workspaces.pipelineYaml(p,"input","output","orders"));
            resources.put(q+".tap.yml",Workspaces.pipelineYaml(q,"input","output","orders"));
            control.apply(resources);
            var discovery=new LinkedHashMap<String,Object>(source);discovery.put("user",source.get("username"));discovery.put("schema","public");
            control.discoverSchema("input","postgres",discovery);
            start(control,p);delivered(mongo,target,1);
            String slot=PostgresSlots.only(source).name();
            control.startAnswering(q,"target-not-empty","keep");state(control,q,PipelineState.RUNNING);
            control.stop(q,false);state(control,q,PipelineState.STOPPED);
            control.stop(p,true);state(control,p,PipelineState.STOPPED);
            assertThat(PostgresSlots.names(source)).containsExactly(slot);
            try(var connection=SharedPostgres.connect(source);var statement=connection.createStatement()) {
                statement.execute("SELECT pg_current_xact_id()");
                statement.execute("INSERT INTO orders VALUES (401,'written while the peer is stopped')");
            }
            assertThat(control.state(q)).contains(PipelineState.STOPPED);
            start(control,q);delivered(mongo,target,401);
            assertThat(PostgresSlots.names(source)).containsExactly(slot);
            control.stop(q,true);state(control,q,PipelineState.STOPPED);
            if(automatic)Await.until("final explicit clear drops the original slot",BOUND,() -> PostgresSlots.names(source).isEmpty(),() -> PostgresSlots.of(source).toString());
            else assertThat(PostgresSlots.names(source)).containsExactly(slot);
            control.startAnswering(p,"target-not-empty","keep");state(control,p,PipelineState.RUNNING);
            Await.until("fresh capture active",BOUND,() -> PostgresSlots.active(source),() -> PostgresSlots.of(source).toString());
            try(var connection=SharedPostgres.connect(source);var statement=connection.createStatement()) {
                statement.execute("SELECT pg_current_xact_id()");
                statement.execute("INSERT INTO orders VALUES (402,'after the next full')");
            }
            delivered(mongo,target,402);
            if(automatic)assertThat(PostgresSlots.names(source)).hasSize(1).doesNotContain(slot);
            else assertThat(PostgresSlots.names(source)).containsExactly(slot);
            control.stop(p,true);state(control,p,PipelineState.STOPPED);
            if(automatic)Await.until("final fresh run releases its slot",BOUND,() -> PostgresSlots.names(source).isEmpty(),() -> PostgresSlots.of(source).toString());
            else assertThat(PostgresSlots.names(source)).containsExactly(slot);
        }
    }
    private static void start(ControlPlane control,String pipeline){control.lifecycle(pipeline,LifecycleVerb.START);state(control,pipeline,PipelineState.RUNNING);}
    private static void state(ControlPlane control,String pipeline,PipelineState expected) {
        Await.until(pipeline+" reaches "+expected,BOUND,() -> control.state(pipeline).filter(expected::equals).isPresent(),() -> control.state(pipeline)+", "+control.logs(pipeline));
    }
    private static void delivered(MongoEndpoints mongo,String target,int id) {
        Await.until("row "+id+" delivered",BOUND,() -> mongo.documents(EndpointAddress.uri(target),"orders").stream()
                .anyMatch(row -> String.valueOf(id).equals(String.valueOf(row.get("id")))),() -> mongo.documents(EndpointAddress.uri(target),"orders").toString());
    }
}
