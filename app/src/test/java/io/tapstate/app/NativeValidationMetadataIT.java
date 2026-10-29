package io.tapstate.app;

import io.tapstate.adapters.pdk.*;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Selected CONFIG to actual independent PDK metadata, without a validation owner, report or Job. */
@RequiresDocker
class NativeValidationMetadataIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    @Test @Timeout(45) void metadataDeadlineReturnsWhileTheOriginalNativeStopIsStillBlocked(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory)throws Exception{
        var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
        var gateKey="tapstate.metadata.fixture."+UUID.randomUUID();System.getProperties().put(gateKey,Map.of("entered",entered,"release",release));
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var caller=java.util.concurrent.Executors.newSingleThreadExecutor()){
            var resource=(SourceResource)f.configuration.artifacts().get("input").orElseThrow();
            f.configuration.artifacts().replace(resource.id(),CanonicalHash.of(resource),new SourceResource(resource.id(),resource.metadata(),"fixture",Map.of("gateKey",gateKey),
                    resource.mode(),resource.tables(),resource.srs(),resource.execution(),resource.experimental()));
            var artifact=io.tapstate.adapters.pdk.ValidationMetadataBlockingFixture.artifact(directory);
            var now=new java.util.concurrent.atomic.AtomicReference<>(Instant.parse("2026-10-29T00:00:00Z"));
            var clock=new Clock(){public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return now.get();}};
            var probe=new NativeValidationMetadata(f.configuration,f.resources,id->artifact,clock);
            var query=caller.submit(()->probe.inspect(new ValidationTaskStore.Selection("flow",List.of("out")),
                    List.of(new ValidationSelectedDeclarations.Choice("orders","out","input","orders")),ValidationMethod.UNIQUE_KEY,
                    Map.of("orders",new NativeValidationMetadata.Projection(List.of("id"),List.of("id"))),now.get().plusSeconds(30)));
            try{
                assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();now.updateAndGet(value->value.plusSeconds(40));
                assertThatThrownBy(()->query.get(2,TimeUnit.SECONDS)).isInstanceOf(java.util.concurrent.ExecutionException.class)
                        .hasCauseInstanceOf(io.tapstate.core.common.TapstateException.class);
                assertThat(f.resources.snapshot().metadataFrames()).isEqualTo(2);assertThat(f.resources.snapshot().processReserved().isEmpty()).isFalse();
                assertThat(f.resources.snapshot().data().runs()).isZero();assertThat(f.resources.snapshot().processors()).isZero();
            }finally{
                release.countDown();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(()->f.resources.snapshot().metadataFrames()==0&&f.resources.snapshot().processReserved().isEmpty());
            }
        }finally{release.countDown();System.getProperties().remove(gateKey);}
    }
    @Test @Timeout(90) void countInspectionUsesTheRealNativeProtocolWithoutCountsModelsOrRunAdmission()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var sourceDb="metadata_source_"+f.suffix;var targetDb="metadata_target_"+f.suffix;
            try{
                seed(f,sourceDb,targetDb);var clock=Clock.systemUTC();var artifact=NativeValidationPdkCountIT.artifact("mongodb");
                var probe=new NativeValidationMetadata(f.configuration,f.resources,id->artifact,clock);
                var original=f.tasks.read("flow","task").orElseThrow();long revision=f.configuration.configurationRevision();
                try(var result=probe.inspect(new ValidationTaskStore.Selection("flow",List.of("out")),choices(),ValidationMethod.COUNT,Map.of(),clock.instant().plusSeconds(30))){
                    assertThat(result.mappings()).hasSize(2).allSatisfy(mapping->{
                        assertThat(mapping.source().countOperation()).isEqualTo(PdkValidationCounts.Operation.DOCUMENT_COUNT);
                        assertThat(mapping.target().countOperation()).isEqualTo(PdkValidationCounts.Operation.DOCUMENT_COUNT);
                        assertThat(mapping.source().model()).isNull();assertThat(mapping.target().model()).isNull();
                    });
                    assertThat(result.declarations().original().configurationRevision()).isEqualTo(revision);
                    assertThat(f.resources.snapshot().data().runs()).isZero();assertThat(f.resources.snapshot().processors()).isZero();
                    assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();assertThat(f.resources.snapshot().metadataFrames()).isOne();
                    assertThat(result.released().toCompletableFuture()).isNotDone();
                    var compiled=ValidationTaskCompiler.compile(result,Map.of(),null);
                    assertThat(compiled.comparison().mappings()).allSatisfy(mapping->{assertThat(mapping.keys()).isNull();assertThat(mapping.source().modelVersion()).isNull();});
                    assertThat(ValidationTaskCompiler.compile(result,Map.of(),compiled.comparison()).fingerprint()).isEqualTo(compiled.fingerprint());
                    assertThat(f.tasks.read("flow","task").orElseThrow()).isEqualTo(original);assertThat(f.configuration.configurationRevision()).isEqualTo(revision);
                    assertThat(f.member.getJet().getJobs()).isEmpty();
                    for(var db:List.of(sourceDb,targetDb)){
                        var dataOps=new Document("$or",List.of(new Document("command.count",new Document("$exists",true)),new Document("command.find",new Document("$exists",true)),
                                new Document("command.aggregate",new Document("$exists",true))));
                        assertThat(f.raw.getDatabase(db).getCollection("system.profile").countDocuments(dataOps)).as("business counts/rows during metadata inspection").isZero();
                    }
                }
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(()->f.resources.snapshot().metadataFrames()==0);
            }finally{f.raw.getDatabase(sourceDb).drop();f.raw.getDatabase(targetDb).drop();}
        }
    }
    @Test @Timeout(90) void independentModelsAndCodecBindingsRemainUnderTheOriginalMetadataFrame()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var sourceDb="metadata_source_"+f.suffix;var targetDb="metadata_target_"+f.suffix;
            try{
                seed(f,sourceDb,targetDb);var clock=Clock.systemUTC();var artifact=NativeValidationPdkCountIT.artifact("mongodb");
                var probe=new NativeValidationMetadata(f.configuration,f.resources,id->artifact,clock);
                var projected=new NativeValidationMetadata.Projection(List.of("id"),List.of("id"));
                var result=probe.inspect(new ValidationTaskStore.Selection("flow",List.of("out")),choices(),ValidationMethod.UNIQUE_KEY,
                        Map.of("orders",projected,"items",projected),clock.instant().plusSeconds(30));
                try{
                    assertThat(result.mappings()).allSatisfy(mapping->{
                        assertThat(mapping.source().countOperation()).isNull();assertThat(mapping.target().countOperation()).isNull();
                        assertThat(mapping.source().model().model().fields()).extracting(SourceField::name).containsExactly("id");
                        assertThat(mapping.target().model().model().fields()).extracting(SourceField::name).containsExactly("id");
                        assertThat(mapping.source().model().binding().modelVersion()).isNotBlank();
                        assertThat(mapping.target().model().binding().codecBinding()).isNotEqualTo(mapping.source().model().binding().codecBinding());
                    });
                    assertThat(f.resources.snapshot().metadataFrames()).isOne();assertThat(f.resources.snapshot().data().runs()).isZero();
                    var confirmation=new ValidationTaskCompiler.Confirmation(UUID.randomUUID(),List.of(new ValidationTaskCompiler.Pair("id","id")));
                    var confirmations=Map.of("orders",confirmation,"items",confirmation);
                    var compiled=ValidationTaskCompiler.compile(result,confirmations,null);
                    assertThat(compiled.comparison().mappings()).allSatisfy(mapping->{
                        assertThat(mapping.keys().source().getFirst().domain()).isEqualTo(ValidationKey.Kind.EXACT_NUMBER);
                        assertThat(mapping.source().modelVersion()).isNotBlank();assertThat(mapping.target().codecBinding()).isNotBlank();
                    });
                    var task=new ValidationReportAdmission.Task("cluster","flow","compiled",UUID.randomUUID());
                    assertThat(f.tasks.create(new ValidationTaskStore.Create(task,compiled.comparison(),ValidationTaskStore.Settings.defaults(ValidationMethod.UNIQUE_KEY,0),UUID.randomUUID())))
                            .isEqualTo(ValidationTaskStore.Write.APPLIED);
                    assertThat(f.tasks.inputs(task,1).orElseThrow().task().comparison()).isEqualTo(compiled.comparison());
                    assertThat(ValidationTaskCompiler.compile(result,confirmations,compiled.comparison()).fingerprint()).isEqualTo(compiled.fingerprint());
                    var renewed=new ValidationTaskCompiler.Confirmation(UUID.randomUUID(),confirmation.fields());
                    var updated=ValidationTaskCompiler.compile(result,Map.of("orders",renewed,"items",confirmation),compiled.comparison());
                    assertThat(updated.comparison().mappingRevision()).isEqualTo(2);
                    assertThat(f.tasks.update(new ValidationTaskStore.Update(task,1,updated.comparison(),ValidationTaskStore.Settings.defaults(ValidationMethod.UNIQUE_KEY,0),UUID.randomUUID())))
                            .isEqualTo(ValidationTaskStore.Write.APPLIED);
                    var uninspected=new ValidationTaskCompiler.Confirmation(UUID.randomUUID(),List.of(new ValidationTaskCompiler.Pair("payload","payload")));
                    assertThatThrownBy(()->ValidationTaskCompiler.compile(result,Map.of("orders",uninspected,"items",confirmation),null))
                            .isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining("key-confirmation-preflight-required");
                }finally{result.close();}
                result.released().toCompletableFuture().get(2,TimeUnit.SECONDS);
                assertThat(f.resources.snapshot().metadataFrames()).isZero();assertThatThrownBy(result::mappings).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            }finally{f.raw.getDatabase(sourceDb).drop();f.raw.getDatabase(targetDb).drop();}
        }
    }
    @Test @Timeout(90) void fullColumnCompilationUsesCompleteModelsAndRejectsCrossSideIdentityCoercion()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var sourceDb="metadata_source_"+f.suffix;var targetDb="metadata_target_"+f.suffix;
            try{
                seed(f,sourceDb,targetDb);var clock=Clock.systemUTC();var artifact=NativeValidationPdkCountIT.artifact("mongodb");
                var evaluator=new NativeValidationMetadata(f.configuration,f.resources,id->artifact,clock);
                try(var result=evaluator.inspect(new ValidationTaskStore.Selection("flow",List.of("out")),choices(),ValidationMethod.FULL_COLUMN,Map.of(),clock.instant().plusSeconds(30))){
                    assertThat(result.mappings().getFirst().source().model().model().fields()).extracting(SourceField::name).contains("_id","id","payload","approximate");
                    var confirmed=new ValidationTaskCompiler.Confirmation(UUID.randomUUID(),List.of(new ValidationTaskCompiler.Pair("id","id")));
                    var confirmations=Map.of("orders",confirmed,"items",confirmed);var compiled=ValidationTaskCompiler.compile(result,confirmations,null);
                    assertThat(compiled.method()).isEqualTo(ValidationMethod.FULL_COLUMN);compiled.comparison().requireFor(ValidationMethod.FULL_COLUMN,ValidationMode.FULL);
                    var task=new ValidationReportAdmission.Task("cluster","flow","compiled-full",UUID.randomUUID());
                    assertThat(f.tasks.create(new ValidationTaskStore.Create(task,compiled.comparison(),ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,0),UUID.randomUUID())))
                            .isEqualTo(ValidationTaskStore.Write.APPLIED);
                    assertThat(f.tasks.inputs(task,1).orElseThrow().task().comparison()).isEqualTo(compiled.comparison());
                    for(var pair:List.of(new ValidationTaskCompiler.Pair("id","payload"),new ValidationTaskCompiler.Pair("id","approximate"))){
                        var incompatible=new ValidationTaskCompiler.Confirmation(UUID.randomUUID(),List.of(pair));
                        assertThatThrownBy(()->ValidationTaskCompiler.compile(result,Map.of("orders",incompatible,"items",confirmed),null))
                                .isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining("key-domain-mismatch");
                    }
                    var textId=new ValidationTaskCompiler.Confirmation(UUID.randomUUID(),List.of(new ValidationTaskCompiler.Pair("_id","payload")));
                    assertThatThrownBy(()->ValidationTaskCompiler.compile(result,Map.of("orders",textId,"items",confirmed),null))
                            .isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining("native-object-id-domain-mismatch");
                    var nativeId=new ValidationTaskCompiler.Confirmation(UUID.randomUUID(),List.of(new ValidationTaskCompiler.Pair("_id","_id")));
                    assertThat(ValidationTaskCompiler.compile(result,Map.of("orders",nativeId,"items",confirmed),null).comparison().mappings().getFirst().keys().source().getFirst().logicalTypes())
                            .containsExactly("object-id");
                    var mappings=compiled.comparison().mappings();var hashes=new HashMap<>(compiled.comparison().dependencyHashes());hashes.remove("flow");hashes.put("other","a".repeat(64));
                    var foreign=new ValidationTaskSpec("other",hashes,1,ValidationTaskCompiler.RULES_VERSION,mappings);
                    assertThatThrownBy(()->ValidationTaskCompiler.compile(result,confirmations,foreign)).isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining("previousPipeline");
                }
            }finally{f.raw.getDatabase(sourceDb).drop();f.raw.getDatabase(targetDb).drop();}
        }
    }
    @Test @Timeout(90) void configurationApiServiceReevaluatesRealPdkAndUsesTheOriginalConfigCas()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var sourceDb="api_source_"+f.suffix;var targetDb="api_target_"+f.suffix;
            try{
                seed(f,sourceDb,targetDb);var clock=Clock.systemUTC();var artifact=NativeValidationPdkCountIT.artifact("mongodb");
                var evaluations=new NativeValidationEvaluation(new NativeValidationMetadata(f.configuration,f.resources,id->artifact,clock));
                var audit=new ArrayList<AuditRecord>();var service=new io.tapstate.control.core.ValidationConfigurationService("cluster",f.tasks,evaluations,
                        new ValidationScheduleCalculator(),new io.tapstate.control.core.AuditGate(audit::add,clock),clock);
                var generation=UUID.randomUUID().toString();var command=UUID.randomUUID().toString();
                var input=new io.tapstate.control.core.ValidationConfigurationService.Input("flow","api-count",generation,"0","COUNT",List.of(
                        new io.tapstate.control.core.ValidationConfigurationService.Choice("orders","out","input","orders")),Map.of());
                String fingerprint;
                try(var result=service.evaluate(input)){
                    fingerprint=result.view().fingerprint();assertThat(fingerprint).isNotBlank();
                    assertThat(result.view().mappings()).singleElement().satisfies(mapping->{
                        assertThat(mapping.target().table()).isEqualTo("checked_orders");assertThat(mapping.source().countOperation()).isEqualTo("DOCUMENT_COUNT");
                        assertThat(mapping.source().fields()).isEmpty();
                    });assertThat(f.resources.snapshot().metadataFrames()).isOne();
                }
                var save=new io.tapstate.control.core.ValidationConfigurationService.Save(input,fingerprint,command,10,null,false);
                assertThat(service.save("alice",save).outcome()).isEqualTo("APPLIED");assertThat(service.save("alice",save).outcome()).isEqualTo("DUPLICATE");
                var firstStored=service.read("flow","api-count").orElseThrow();assertThat(firstStored.revision()).isEqualTo("1");
                var updateInput=new io.tapstate.control.core.ValidationConfigurationService.Input("flow","api-count",generation,"1","COUNT",input.mappings(),Map.of());
                var update=new io.tapstate.control.core.ValidationConfigurationService.Save(updateInput,fingerprint,UUID.randomUUID().toString(),8,null,false);
                assertThat(service.save("alice",update).outcome()).isEqualTo("APPLIED");
                assertThat(service.save("alice",update).outcome()).isEqualTo("DUPLICATE");
                var stored=service.read("flow","api-count").orElseThrow();assertThat(stored.revision()).isEqualTo("2");assertThat(stored.nextStartSequence()).isEqualTo("1");
                assertThat(stored.taskGeneration()).isEqualTo(generation);assertThat(stored.mappings().getFirst().keys()).isNull();
                var existing=(SourceResource)f.configuration.artifacts().get("input").orElseThrow();
                var config=new HashMap<>(existing.config());config.put("configurationMarker","changed");
                f.configuration.artifacts().replace("input",CanonicalHash.of(existing),new SourceResource(existing.id(),existing.metadata(),existing.connector(),config,existing.mode(),existing.tables(),existing.srs(),existing.execution(),existing.experimental()));
                assertThat(service.save("alice",save).outcome()).isEqualTo("CONFLICT");assertThat(service.read("flow","api-count").orElseThrow()).isEqualTo(stored);
                assertThat(audit).hasSize(5).allSatisfy(record->assertThat(record.operationId()).isEqualTo("validation.save"));
                var preview=service.preview(new io.tapstate.control.core.ValidationConfigurationService.Schedule("CRON",null,null,null,"*/5 * * * *","Asia/Shanghai"),"9007199254740993");
                assertThat(preview.inputRevision()).isEqualTo("9007199254740993");assertThat(preview.occurrences()).hasSize(5);
                assertThat(f.resources.snapshot().metadataFrames()).isZero();assertThat(f.resources.snapshot().data().runs()).isZero();assertThat(f.member.getJet().getJobs()).isEmpty();
            }finally{f.raw.getDatabase(sourceDb).drop();f.raw.getDatabase(targetDb).drop();}
        }
    }
    @Test @Timeout(90) void closingAnEvaluationReleasesItsProjectionEvenWhenTheClosedHandleIsRetained()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var sourceDb="lease_source_"+f.suffix;var targetDb="lease_target_"+f.suffix;
            try{
                seed(f,sourceDb,targetDb);var artifact=NativeValidationPdkCountIT.artifact("mongodb");var clock=Clock.systemUTC();
                var evaluator=new NativeValidationEvaluation(new NativeValidationMetadata(f.configuration,f.resources,id->artifact,clock));
                var held=new ArrayList<io.tapstate.runtime.probe.ValidationEvaluation>();
                var weak=closedView(evaluator,held,clock);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).pollInterval(Duration.ofMillis(100)).untilAsserted(()->{System.gc();assertThat(weak.get()).isNull();});
                assertThat(held).hasSize(1);assertThat(f.resources.snapshot().metadataFrames()).isZero();
                assertThatThrownBy(held.getFirst()::view).isInstanceOf(io.tapstate.core.common.TapstateException.class);
                java.lang.ref.Reference.reachabilityFence(held);
            }finally{f.raw.getDatabase(sourceDb).drop();f.raw.getDatabase(targetDb).drop();}
        }
    }
    private static java.lang.ref.WeakReference<io.tapstate.runtime.probe.ValidationEvaluation.View> closedView(NativeValidationEvaluation evaluator,
            List<io.tapstate.runtime.probe.ValidationEvaluation> retained,Clock clock){
        var lease=evaluator.evaluate(new io.tapstate.runtime.probe.ValidationEvaluation.Request(new ValidationTaskStore.Selection("flow",List.of("out")),
                List.of(new io.tapstate.runtime.probe.ValidationEvaluation.Choice("orders","out","input","orders")),ValidationMethod.COUNT,Map.of(),null,clock.instant().plusSeconds(30)));
        var weak=new java.lang.ref.WeakReference<>(lease.view());lease.close();retained.add(lease);return weak;
    }
    private static List<ValidationSelectedDeclarations.Choice> choices(){return List.of(new ValidationSelectedDeclarations.Choice("orders","out","input","orders"),
            new ValidationSelectedDeclarations.Choice("items","out","input","items"));}
    private static void seed(NativeValidationOwnerReservationsIT.Fixture f,String sourceDb,String targetDb){
        for(var name:List.of("orders","items")){
            f.raw.getDatabase(sourceDb).getCollection(name).insertOne(new Document("id",1L).append("payload","source").append("approximate",0.1d));
            f.raw.getDatabase(targetDb).getCollection("checked_"+name).insertOne(new Document("id",1L).append("payload","target").append("approximate",0.1d));
        }
        var source=new SourceResource("input",null,"mongodb",Map.of("uri",MONGO.getReplicaSetUrl(sourceDb),"database",sourceDb,"isUri",true),SourceMode.SNAPSHOT,
                List.of(TableRef.literal("orders"),TableRef.literal("items")),null,null);
        f.configuration.artifacts().replace("input",CanonicalHash.of(f.configuration.artifacts().get("input").orElseThrow()),source);
        f.configuration.artifacts().create(new SourceResource("destination",null,"mongodb",Map.of("uri",MONGO.getReplicaSetUrl(targetDb),"database",targetDb,"isUri",true),null,null,null,null));
        var pipeline=new PipelineResource("flow",null,List.of(SourceRef.spec("input",false)),null,null,
                new ServeBlock.Inline("serve",FromClause.list(FromRef.literal("input.orders"),FromRef.literal("input.items")),
                        List.of(new SyncElement("out","destination",null,new RenameSpec(null,null,"checked_",null),null)),null,null),null,null);
        f.configuration.artifacts().replace("flow",CanonicalHash.of(f.configuration.artifacts().get("flow").orElseThrow()),pipeline);
        f.raw.getDatabase(sourceDb).runCommand(new Document("profile",2));f.raw.getDatabase(targetDb).runCommand(new Document("profile",2));
    }
}
