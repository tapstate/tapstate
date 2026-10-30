package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.control.core.*;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static io.tapstate.app.AlertNotificationPlannerIT.*;
import static org.assertj.core.api.Assertions.*;

/** Configuration publication must not copy, replace or reinterpret any alert ledger. */
@RequiresDocker
class AlertConfigurationTransferIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    @Test void ordinaryDefinitionsTransferWhileAllTwelveAlertCollectionsStayWithTheirOriginalInstallation(){
        for(boolean cloud:List.of(false,true))AlertEvaluationHostIT.withFixture(false,cloud,new AlarmKey.Scope("installation-a","source-cluster"),source->{
            AlertEvaluationHostIT.withFixture(false,cloud,new AlarmKey.Scope("installation-a","destination-cluster"),destination->{
                for(var fixture:List.of(source,destination)){
                    fixture.configuration().bindings().complete(fixture.storeHost().setup());
                    if(!cloud)fixture.configuration().firstAdmin().create(fixture.storeHost().setup(),new User("transfer-admin","$2a$04$"+"A".repeat(53),"admin"));
                    if(!cloud)route(fixture,Duration.ZERO);
                    var planner=new AlertNotificationPlanner(fixture.store(),fixture.clock(),"transfer-planner",cloud?java.net.URI.create("https://cloud.example.test"):null,ACTIVE);
                    fixture.evaluator().withPlanner(planner);bad(fixture);planner.tick();
                }
                var resource=new DslParser().parse("version: tapstate/v1\nkind: transform\nid: portable_filter\ntype: filter\nexpr: \"true\"\n");
                source.configuration().artifacts().create(resource);
                try(var fromClient=MongoClients.create(source.settings().uri());var toClient=MongoClients.create(destination.settings().uri())){
                    var from=fromClient.getDatabase(new com.mongodb.ConnectionString(source.settings().uri()).getDatabase());
                    var to=toClient.getDatabase(new com.mongodb.ConnectionString(destination.settings().uri()).getDatabase());
                    // Sentinels cover even empty, future or unsupported rows that transfer must never inspect.
                    for(var database:List.of(from,to))for(String name:database.listCollectionNames())if(name.startsWith("alert_"))
                        database.getCollection(name).insertOne(new Document("_id","transfer-sentinel-"+database.getName()).append("formatVersion",999).append("evidence",database.getName()));
                    var originalFrom=alerts(from);var originalTo=alerts(to);assertThat(originalFrom).hasSize(12);assertThat(originalTo).hasSize(12);
                    var mode=cloud?StorageBinding.Mode.CLOUD:StorageBinding.Mode.OP;
                    var exportScope=new ConfigurationImportStore.Scope(source.storeHost().scope().clusterId(),source.business().bindingVersion(),mode,cloud?source.storeHost().scope().authorityId():null);
                    var importScope=new ConfigurationImportStore.Scope(destination.storeHost().scope().clusterId(),destination.business().bindingVersion(),mode,cloud?destination.storeHost().scope().authorityId():null);
                    var exported=source.configuration().exports().snapshot(exportScope,new ConfigurationExportStore.Selection(Set.of("portable_filter"),Set.of()));
                    assertThat(exported.payload().resources()).containsExactly(resource);
                    assertThat(exported.payload().users()).isEmpty();assertThat(exported.payload().layouts()).isEmpty();
                    var codec=new EncryptedConfigurationTransfer();String key=Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
                    var exporter=new ConfigurationExportService(source.configuration().exports(),codec,new AuditGate(source.configuration().audit(),source.clock()));
                    var encrypted=exporter.export("operator",exportScope,ACTIVE,new ConfigurationExportStore.Selection(Set.of("portable_filter"),Set.of()),key);
                    var audit=new AuditGate(destination.configuration().audit(),destination.clock());
                    var validation=new ApplyService(TapstateCatalog::load,destination.configuration().artifacts(),audit,new SchemaStore(){
                        public void save(DiscoveredSourceModel ignored){throw new AssertionError("transfer does not discover schemas");}
                        public Optional<DiscoveredSourceModel> get(String id){return Optional.empty();}
                    },PlanAdvisories.none(),SchemaDerivation.none());
                    var importer=new ConfigurationImportService(destination.configuration().imports(),codec,validation,audit,destination.configuration().artifacts()::get);
                    var mapping=exported.defaultSourceId()==null?Map.<String,String>of():Map.of(exported.defaultSourceId(),destination.configuration().artifacts().managedDefaultSourceId().orElseThrow());
                    var prepared=importer.preflight("operator",importScope,ACTIVE,new ConfigurationImportService.Submission(encrypted.transfer(),key,mapping));
                    assertThat(destination.configuration().artifacts().get("portable_filter")).isEmpty();
                    assertThat(alerts(from)).isEqualTo(originalFrom);assertThat(alerts(to)).isEqualTo(originalTo);
                    var confirmation=new ConfigurationImportService.Submission(prepared.preparedTransfer(),key,mapping);
                    var receipt=importer.commit("operator",importScope,ACTIVE,prepared.operation().ticket().id(),confirmation);
                    assertThat(receipt.configurationRevision()).isEqualTo(prepared.operation().ticket().destinationRevision()+1);
                    assertThat(destination.configuration().artifacts().get("portable_filter").map(io.tapstate.core.model.Resource::id)).contains("portable_filter");
                    assertThat(importer.commit("operator",importScope,ACTIVE,prepared.operation().ticket().id(),confirmation)).isEqualTo(receipt);
                    assertThat(alerts(from)).isEqualTo(originalFrom);assertThat(alerts(to)).isEqualTo(originalTo);
                }
            });
        });
    }
    private static Map<String,List<String>> alerts(MongoDatabase database){
        var rows=new TreeMap<String,List<String>>();
        for(String name:database.listCollectionNames())if(name.startsWith("alert_"))rows.put(name,database.getCollection(name).find()
                .map(Document::toJson).into(new ArrayList<>()).stream().sorted().toList());
        return rows;
    }
}
