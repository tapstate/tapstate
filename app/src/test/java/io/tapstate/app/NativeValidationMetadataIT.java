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
                }finally{result.close();}
                result.released().toCompletableFuture().get(2,TimeUnit.SECONDS);
                assertThat(f.resources.snapshot().metadataFrames()).isZero();assertThatThrownBy(result::mappings).isInstanceOf(io.tapstate.core.common.TapstateException.class);
            }finally{f.raw.getDatabase(sourceDb).drop();f.raw.getDatabase(targetDb).drop();}
        }
    }
    private static List<ValidationSelectedDeclarations.Choice> choices(){return List.of(new ValidationSelectedDeclarations.Choice("orders","out","input","orders"),
            new ValidationSelectedDeclarations.Choice("items","out","input","items"));}
    private static void seed(NativeValidationOwnerReservationsIT.Fixture f,String sourceDb,String targetDb){
        for(var name:List.of("orders","items")){
            f.raw.getDatabase(sourceDb).getCollection(name).insertOne(new Document("id",1L).append("payload","source"));
            f.raw.getDatabase(targetDb).getCollection("checked_"+name).insertOne(new Document("id",1L).append("payload","target"));
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
