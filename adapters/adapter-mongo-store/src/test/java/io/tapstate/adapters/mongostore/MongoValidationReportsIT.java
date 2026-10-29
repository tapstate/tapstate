package io.tapstate.adapters.mongostore;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Instant;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Actual CONFIG/CACHE transactions; no business connector or UI acceptance is implied. */
@RequiresDocker
class MongoValidationReportsIT {
    @Container static final MongoDBContainer CONFIG=MongoValidationTaskStoreIT.CONFIG;
    @Container static final MongoDBContainer CACHE=MongoValidationTaskStoreIT.CACHE;
    private record Run(MongoValidationTaskStoreIT.Reserved reservation,ValidationControlStore.Run run){}

    @Test void typedReportQueriesKeepExactEpochsValuesAndKnownTerminalDiagnostics(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create(rowComparison(f));
            var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),
                    ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),proposal.commandId());
            assertThat(f.tasks.create(create)).isEqualTo(ValidationTaskStore.Write.APPLIED);
            var original=open(f,create.task(),"original");var start=original.reservation().start();
            var writer=original.reservation().access().results();var reports=f.configuration.validationReports(()->{});
            var check=rowCheck(start.run(),7,1,Map.of("number",9007199254740993L,
                    "binary",new ValidationValues.Binary(128,new byte[]{0,1,-1})));
            assertThat(writer.publish(new ValidationResultStore.RootWrite(start.report(),check))).isEqualTo(ValidationResultStore.Publication.APPLIED);
            var reference=reference(start);var snapshot=reports.find(reference).orElseThrow();
            assertThat(snapshot.closed()).isFalse();assertThat(snapshot.terminal()).isNull();
            var page=reports.roots(reference,new ValidationReports.Paging(0,1));assertThat(page.total()).isOne();
            var root=reports.root(reference,page.items().getFirst().id()).orElseThrow();
            assertThat(root.observation().toString()).contains("9007199254740993","LOCAL_OBSERVATION");
            assertThat(reports.roots(reference,new ValidationReports.Paging(1,1)).items()).isEmpty();
            var stale=new ValidationReports.Reference(reference.pipelineId(),reference.taskId(),reference.mode(),reference.runId(),UUID.randomUUID(),reference.sequence());
            assertThat(reports.find(stale)).isEmpty();assertThat(reports.root(stale,page.items().getFirst().id())).isEmpty();
            var known=retire(original,ValidationRunState.FAILED);
            var failure=new ValidationReportMetadata.Failure(ValidationError.READ_FAILED.code(),Map.of("table","orders"),
                    "io.tapstate.core.common.TapstateException","read failed","bounded fixture diagnostic",false);
            var mirror=new ValidationReports.TerminalMirror(known,Instant.EPOCH.plusSeconds(3),
                    new ValidationReportMetadata.Window(Instant.EPOCH.plusSeconds(1),Instant.EPOCH.plusSeconds(2)),failure);
            assertThat(original.reservation().access().reports().mirror(mirror)).isEqualTo(ValidationReports.Mirror.APPLIED);
            assertThat(original.reservation().access().reports().mirror(mirror)).isEqualTo(ValidationReports.Mirror.DUPLICATE);
            snapshot=reports.find(reference).orElseThrow();assertThat(snapshot.terminal()).isEqualTo(ValidationRunState.FAILED);
            assertThat(snapshot.failure()).isEqualTo(failure);assertThat(snapshot.window()).isEqualTo(mirror.window());
            assertThat(reports.history("flow",create.task().taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).items())
                    .singleElement().satisfies(row->{assertThat(row.terminal()).isEqualTo(ValidationRunState.FAILED);assertThat(row.rootCount()).isOne();});
            assertThat(writer.publish(new ValidationResultStore.RootWrite(start.report(),check))).isEqualTo(ValidationResultStore.Publication.CLOSED_OR_EXPIRED);
            SystemCollections.VALIDATION_REPORTS.on(f.configDb).deleteOne(new Document("_id",MongoValidationAdmissionStore.reportId(start.run())));
            assertThat(reports.find(reference)).isEmpty();assertThat(reports.root(reference,page.items().getFirst().id())).isEmpty();
            assertThat(original.reservation().access().reports().mirror(mirror)).isEqualTo(ValidationReports.Mirror.CLOSED_OR_EXPIRED);
        }
    }

    @Test void countHistoryDoesNotTurnZeroProblemRootsIntoAnAgreement(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var create=f.create("flow");f.tasks.create(create);var original=open(f,create.task(),"counts");
            var start=original.reservation().start();var boundary=new ValidationCheckEvent.ReadBoundary("native","native-count-v1",Instant.EPOCH,Instant.EPOCH.plusSeconds(1));
            var check=new ValidationCountCheck(start.run(),"mapping","out",9007199254740993L,9007199254740992L,boundary,boundary);
            original.reservation().access().results().publishCount(new ValidationResultStore.CountWrite(start.report(),original.run().snapshot(),check));
            var reports=f.configuration.validationReports(()->{});
            var summary=reports.history("flow",create.task().taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).items().getFirst();
            assertThat(summary.rootCount()).isZero();assertThat(summary.countedTables()).isOne();assertThat(summary.mismatchedTables()).isOne();
            assertThat(reports.find(reference(start)).orElseThrow().counts()).singleElement()
                    .satisfies(row->assertThat(row.sourceCount()).isEqualTo(9007199254740993L));
        }
    }

    @Test void retentionCountsPendingTerminalsOnceAndCannotRecreateExpiredReports(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var settings=new ValidationTaskStore.Settings(
                    new ValidationTaskStore.Full(ValidationMethod.COUNT,1,null,false,1),proposal.settings().incremental());
            var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),settings,proposal.commandId());f.tasks.create(create);
            var runs=new ArrayList<Run>();
            for(int sequence=1;sequence<=3;sequence++){
                var original=open(f,create.task(),"run-"+sequence);runs.add(original);var start=original.reservation().start();
                var read=new ValidationCheckEvent.ReadBoundary("count","v1",Instant.EPOCH,Instant.EPOCH);
                original.reservation().access().results().publishCount(new ValidationResultStore.CountWrite(start.report(),original.run().snapshot(),
                        new ValidationCountCheck(start.run(),"mapping","out",1,1,read,read)));
                var known=retire(original,ValidationRunState.COMPLETED);
                if(sequence<3)original.reservation().access().reports().mirror(new ValidationReports.TerminalMirror(known,Instant.EPOCH.plusSeconds(sequence),null,null));
            }
            var last=runs.getLast();var known=last.reservation().access().control().read(last.run().identity().owner(),last.run()).orElseThrow();
            var reports=f.configuration.validationReports(()->{});
            var cleaned=reports.retain(new ValidationReports.Retention(create.task(),ValidationMode.FULL,1,List.of(known)));
            assertThat(cleaned.removed()).isEqualTo(2);assertThat(cleaned.blocked()).isFalse();
            assertThat(reports.history("flow",create.task().taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).items()).hasSize(1);
            assertThat(reports.find(reference(runs.getFirst().reservation().start()))).isEmpty();
            assertThat(runs.getFirst().reservation().access().admissions().open(new ValidationReportAdmission.Open(runs.getFirst().reservation().start(),runs.getFirst().run().snapshot())))
                    .isIn(ValidationReportAdmission.Birth.CLOSED,ValidationReportAdmission.Birth.STALE);
            assertThat(reports.find(reference(last.reservation().start()))).isPresent();
        }
    }

    @Test void readsRejectSameWidthCorruptionInsteadOfPresentingAnAgreement(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create(rowComparison(f));var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),
                    ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),proposal.commandId());f.tasks.create(create);
            var original=open(f,create.task(),"corrupted-root");var start=original.reservation().start();
            original.reservation().access().results().publish(new ValidationResultStore.RootWrite(start.report(),rowCheck(start.run(),7,1,Map.of())));
            var reports=f.configuration.validationReports(()->{});var reference=reference(start);var root=reports.roots(reference,new ValidationReports.Paging(0,10)).items().getFirst();
            var collection=SystemCollections.VALIDATION_DIFFERENCES.on(f.configDb);var stored=collection.find().first();
            var counts=stored.get("payload",Document.class).get("counts",Document.class);var kind=counts.keySet().iterator().next();
            var exact=(Long)counts.get(kind);collection.updateOne(new Document("_id",stored.get("_id")),new Document("$set",new Document("payload.counts."+kind,exact.doubleValue())));
            assertThatThrownBy(()->reports.root(reference,root.id())).isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining(IoError.DOCUMENT_UNREADABLE.code());
            assertThatThrownBy(()->reports.roots(reference,new ValidationReports.Paging(0,10))).isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining(IoError.DOCUMENT_UNREADABLE.code());
        }
    }
    @Test void countHistoryRejectsSameWidthCorruptionAndForeignMapping(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var create=f.create("flow");f.tasks.create(create);var original=open(f,create.task(),"corrupted-count");var start=original.reservation().start();
            var read=new ValidationCheckEvent.ReadBoundary("count","v1",Instant.EPOCH,Instant.EPOCH);
            original.reservation().access().results().publishCount(new ValidationResultStore.CountWrite(start.report(),original.run().snapshot(),new ValidationCountCheck(start.run(),"mapping","out",1,1,read,read)));
            var collection=SystemCollections.VALIDATION_REPORTS.on(f.configDb);var id=new Document("_id",MongoValidationAdmissionStore.reportId(start.run()));
            collection.updateOne(id,new Document("$set",new Document("countSummaries.0.sourceCount",1.0d)));
            var reports=f.configuration.validationReports(()->{});
            assertThatThrownBy(()->reports.history("flow",create.task().taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining(IoError.DOCUMENT_UNREADABLE.code());
            collection.updateOne(id,new Document("$set",new Document("countSummaries.0.sourceCount",1L).append("countSummaries.0.mappingId","foreign")));
            assertThatThrownBy(()->reports.find(reference(start))).isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining(IoError.DOCUMENT_UNREADABLE.code());
        }
    }
    @Test void retentionUsesOriginalTerminalTimeAcrossPendingAndDelayedMirrors(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,null,false,1),proposal.settings().incremental());
            var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),settings,proposal.commandId());f.tasks.create(create);
            var runs=new ArrayList<Run>();var known=new ArrayList<ValidationControlStore.Status>();
            for(int sequence=1;sequence<=3;sequence++){
                var original=open(f,create.task(),"ordered-"+sequence);runs.add(original);
                var read=new ValidationCheckEvent.ReadBoundary("count","v1",Instant.EPOCH,Instant.EPOCH);
                original.reservation().access().results().publishCount(new ValidationResultStore.CountWrite(original.run().report(),original.run().snapshot(),
                        new ValidationCountCheck(original.run().identity(),"mapping","out",1,1,read,read)));
                retire(original,ValidationRunState.COMPLETED);
                // Controlled original CACHE terminal clocks; CONFIG arrival deliberately has another order.
                var ended=Instant.EPOCH.plusSeconds(sequence==1?30:sequence==2?10:20);
                SystemCollections.VALIDATION_RUNS.on(f.cacheDb).updateOne(new Document("_id",MongoValidationControlStore.runId(original.run())),new Document("$set",new Document("endedAt",ended.toString())));
                var status=original.reservation().access().control().read(original.run().identity().owner(),original.run()).orElseThrow();known.add(status);
                if(sequence!=1)original.reservation().access().reports().mirror(new ValidationReports.TerminalMirror(status,Instant.EPOCH.plusSeconds(100-sequence),null,null));
            }
            var reports=f.configuration.validationReports(()->{});var cleaned=reports.retain(new ValidationReports.Retention(create.task(),ValidationMode.FULL,1,List.of(known.getFirst(),known.getLast())));
            assertThat(cleaned.removed()).isEqualTo(2);assertThat(reports.find(reference(runs.getFirst().reservation().start()))).isPresent();
            assertThat(reports.find(reference(runs.getLast().reservation().start()))).isEmpty();
        }
    }

    @Test void historyPagesUseExactTerminalInstantsRatherThanIsoLexicalOrder(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var create=f.create("flow");f.tasks.create(create);var ends=List.of(Instant.EPOCH,Instant.EPOCH.plusNanos(1000000),Instant.EPOCH.plusNanos(123000000),Instant.EPOCH.plusNanos(123001000));
            for(int index=0;index<ends.size();index++){
                var original=open(f,create.task(),"time-"+index);retire(original,ValidationRunState.STOPPED);
                SystemCollections.VALIDATION_RUNS.on(f.cacheDb).updateOne(new Document("_id",MongoValidationControlStore.runId(original.run())),new Document("$set",new Document("endedAt",ends.get(index).toString())));
                var known=original.reservation().access().control().read(original.run().identity().owner(),original.run()).orElseThrow();
                original.reservation().access().reports().mirror(new ValidationReports.TerminalMirror(known,Instant.EPOCH.plusSeconds(20-index),null,null));
            }
            var reports=f.configuration.validationReports(()->{});
            for(int page=0;page<ends.size();page++)assertThat(reports.history("flow",create.task().taskId(),ValidationMode.FULL,new ValidationReports.Paging(page,1)).items())
                    .singleElement().extracting(value->value.reference().runId()).isEqualTo("time-"+(3-page));
        }
    }

    @Test void storedSamplesCannotCrossTheSavedPhysicalSourceScope(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create(rowComparison(f));var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),proposal.commandId());f.tasks.create(create);
            var original=open(f,create.task(),"sample-scope");var start=original.reservation().start();
            original.reservation().access().results().publish(new ValidationResultStore.RootWrite(start.report(),rowCheck(start.run(),7,1,Map.of("amount",1L))));
            var reports=f.configuration.validationReports(()->{});var root=reports.roots(reference(start),new ValidationReports.Paging(0,1)).items().getFirst();
            var collection=SystemCollections.VALIDATION_DIFFERENCES.on(f.configDb);var row=collection.find().first();
            collection.updateOne(new Document("_id",row.get("_id")),new Document("$set",new Document("payload.samples.0.source.identity.sourceId","other")));
            assertThatThrownBy(()->reports.root(reference(start),root.id())).isInstanceOf(io.tapstate.core.common.TapstateException.class).hasMessageContaining(IoError.DOCUMENT_UNREADABLE.code());
        }
    }
    @Test void aLegalLargeExactCompoundKeyRemainsReadableAfterPublication(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var base=rowComparison(f);var mapping=base.mappings().getFirst();var components=new ArrayList<ValidationKeyBinding.Component>();
            for(int index=0;index<3;index++)components.add(new ValidationKeyBinding.Component(List.of("id-"+index),ValidationKey.Kind.EXACT_NUMBER,Set.of("number")));
            var comparison=new ValidationTaskSpec(base.pipelineId(),base.dependencyHashes(),base.mappingRevision(),base.comparisonRulesVersion(),List.of(
                    new ValidationTaskSpec.Mapping(mapping.mappingId(),mapping.outputKind(),mapping.outputId(),mapping.source(),mapping.target(),new ValidationTaskSpec.Keys("confirmed",components,components))));
            var proposal=f.create(comparison);var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),proposal.commandId());f.tasks.create(create);
            var original=open(f,create.task(),"large-exact-key");var start=original.reservation().start();
            var parts=new ArrayList<ValidationKey.Part>();for(int index=0;index<3;index++)parts.add(ValidationKey.Part.number(new java.math.BigDecimal("9".repeat(30000))));
            var check=rowCheck(start.run(),new ValidationKey(parts),1,null);assertThat(check.accountedBytes()).isLessThan(256*1024);
            original.reservation().access().results().publish(new ValidationResultStore.RootWrite(start.report(),check));
            var reports=f.configuration.validationReports(()->{});var roots=reports.roots(reference(start),new ValidationReports.Paging(0,1));
            assertThat(roots.items()).hasSize(1);assertThat(roots.items().getFirst().root().rootKey()).isEqualTo(check.observation().root().rootKey());
            assertThat(reports.root(reference(start),roots.items().getFirst().id()).orElseThrow().root().rootKey()).isEqualTo(check.observation().root().rootKey());
        }
    }

    @Test void observationAndTerminalReserveOnlyOneMetadataAllowanceAtTheReportLimit(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create(rowComparison(f));var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),proposal.commandId());f.tasks.create(create);
            var original=open(f,create.task(),"metadata-capacity");var start=original.reservation().start();var writer=original.reservation().access().results();var reports=original.reservation().access().reports();
            writer.publish(new ValidationResultStore.RootWrite(start.report(),rowCheck(start.run(),7,1,Map.of())));
            var collection=SystemCollections.VALIDATION_REPORTS.on(f.configDb);var id=new Document("_id",MongoValidationAdmissionStore.reportId(start.run()));var before=collection.find(id).first();
            // Model already accounted retained payload at the limit without allocating a 64 MiB data fixture.
            long occupied=MongoValidationResultStore.MAX_REPORT_BYTES-bsonBytes(before)-80L*1024;
            collection.updateOne(id,new Document("$set",new Document("resultBytes",occupied)));
            var failure=new ValidationReportMetadata.Failure(ValidationError.READ_FAILED.code(),Map.of("table","orders"),"Failure","first cause","x".repeat(6000),false);
            assertThat(reports.observe(new ValidationReports.Observation(start.report(),null,failure))).isEqualTo(ValidationReports.Mirror.APPLIED);
            assertThat(writer.publish(new ValidationResultStore.RootWrite(start.report(),rowCheck(start.run(),7,2,Map.of())))).isEqualTo(ValidationResultStore.Publication.APPLIED);
            assertThat(collection.find(id).first().getLong("resultBytes")).isEqualTo(occupied);
        }
    }
    @Test void boundedRawPagesRetainAllLargeValidRootFacts(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create(rowComparison(f));var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,0),proposal.commandId());
            assertThat(f.tasks.create(create)).isEqualTo(ValidationTaskStore.Write.APPLIED);var original=open(f,create.task(),"large-root-page");var start=original.reservation().start();
            var writer=original.reservation().access().results();var text="x".repeat(12*1024);
            for(int index=0;index<20;index++)assertThat(writer.publish(new ValidationResultStore.RootWrite(start.report(),rowCheck(start.run(),index,1,Map.of("amount",text)))))
                    .isEqualTo(ValidationResultStore.Publication.APPLIED);
            var reports=f.configuration.validationReports(()->{});var page=reports.roots(reference(start),new ValidationReports.Paging(0,20));
            assertThat(page.total()).isEqualTo(20);assertThat(page.items()).hasSize(20);
            for(var item:page.items())assertThat(reports.root(reference(start),item.id()).orElseThrow().observation().toString()).contains(text);
            assertThat(reports.history("flow",create.task().taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,20)).items()).hasSize(1);
        }
    }
    private static int bsonBytes(Document value){
        try(var output=new org.bson.io.BasicOutputBuffer();var writer=new org.bson.BsonBinaryWriter(output)){
            new org.bson.codecs.DocumentCodec().encode(writer,value,org.bson.codecs.EncoderContext.builder().build());return output.getSize();
        }
    }

    private static ValidationTaskSpec rowComparison(MongoValidationTaskStoreIT.Fixture f){
        var base=f.denseComparison("flow",1);var mapping=base.mappings().getFirst();
        var fields=List.of(new ValidationKeyBinding.Component(List.of("id"),ValidationKey.Kind.EXACT_NUMBER,Set.of("number")));
        return new ValidationTaskSpec(base.pipelineId(),base.dependencyHashes(),base.mappingRevision(),base.comparisonRulesVersion(),
                List.of(new ValidationTaskSpec.Mapping(mapping.mappingId(),mapping.outputKind(),mapping.outputId(),mapping.source(),mapping.target(),
                        new ValidationTaskSpec.Keys("confirmed-key",fields,fields))));
    }
    private static ValidationRootCheck rowCheck(ValidationRunIdentity run,long key,long revision,Map<String,Object> fields){
        return rowCheck(run,new ValidationKey(List.of(ValidationKey.Part.number(key))),revision,fields);
    }
    private static ValidationRootCheck rowCheck(ValidationRunIdentity run,ValidationKey key,long revision,Map<String,Object> fields){
        var root=new ValidationRoot("mapping","out",key,List.of());
        try(var result=ValidationRootCheck.builder(new ValidationObservation(run,root,revision),ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST,ValidationRootCheck.TargetPresence.PRESENT,new ValidationRootCheck.Limits(256*1024,10,10,10,1))){
            var read=new ValidationCheckEvent.ReadBoundary("read","v1",Instant.EPOCH,Instant.EPOCH);
            result.read(ValidationRootCheck.Side.SOURCE,read).read(ValidationRootCheck.Side.TARGET,read);
            var finding=new ValidationRootCheck.Finding(ValidationRootCheck.Kind.TARGET_VALUE,new ValidationRootCheck.Location(root,List.of("amount")));
            if(fields==null)result.finding(finding);
            else result.finding(finding,Optional.of(new ValidationRootCheck.SampledSide(new ValidationPhysicalIdentity("input","source",null,"orders",
                    key.parts().stream().map(part->List.of("id")).toList(),key),ValidationValues.ownRow(fields,new ValidationValues.Limits(65536,1024,16)),"read")),Optional.empty());
            for(var check:ValidationRootCheck.Check.values())result.checked(check);return result.finish();
        }
    }

    private static Run open(MongoValidationTaskStoreIT.Fixture f,ValidationReportAdmission.Task task,String id){
        var saved=f.read(task);var reserved=f.reserve(saved,id);var start=reserved.start();
        var spec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,saved.settings().full().method());
        assertThat(reserved.access().admissions().open(new ValidationReportAdmission.Open(start,spec))).isEqualTo(ValidationReportAdmission.Birth.CREATED);
        var control=reserved.access().control();var snapshot=control.snapshot(start.run().owner());
        var run=new ValidationControlStore.Run(start.run(),snapshot.countingGeneration()+(snapshot.closed()?1:0),start,spec);
        assertThat(control.join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
        assertThat(control.transition(new ValidationControlStore.Transition(run,ValidationRunState.STARTING,ValidationRunState.RUNNING))).isEqualTo(ValidationControlStore.Mutation.APPLIED);
        return new Run(reserved,run);
    }
    private static ValidationControlStore.Status retire(Run original,ValidationRunState terminal){
        var access=original.reservation().access();var run=original.run();access.admissions().close(run.admission());
        var previous=ValidationRunState.RUNNING;
        if(terminal==ValidationRunState.STOPPED){access.control().transition(new ValidationControlStore.Transition(run,previous,ValidationRunState.STOPPING));previous=ValidationRunState.STOPPING;}
        assertThat(access.control().transition(new ValidationControlStore.Transition(run,previous,terminal))).isEqualTo(ValidationControlStore.Mutation.APPLIED);
        assertThat(access.control().retire(new ValidationControlStore.Retirement(run,run.report(),terminal))).isEqualTo(ValidationControlStore.Mutation.APPLIED);
        return access.control().read(run.identity().owner(),run).orElseThrow();
    }
    private static ValidationReports.Reference reference(ValidationReportAdmission.Start start){
        return new ValidationReports.Reference(start.task().pipelineId(),start.task().taskId(),start.run().mode(),start.run().runId(),start.task().generation(),start.sequence());
    }
}
