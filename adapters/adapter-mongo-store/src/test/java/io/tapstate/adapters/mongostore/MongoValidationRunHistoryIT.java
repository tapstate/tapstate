package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
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

/** Original immutable CACHE facts remain readable after validation ownership ends; no new owner or Job. */
@RequiresDocker
class MongoValidationRunHistoryIT {
    @Container static final MongoDBContainer CONFIG=MongoValidationTaskStoreIT.CONFIG;
    @Container static final MongoDBContainer CACHE=MongoValidationTaskStoreIT.CACHE;
    private record Original(MongoValidationTaskStoreIT.Reserved reservation,ValidationControlStore.Run run){}
    @Test void originalTerminalsAreReadableAndMirrorableAfterTheValidationLeaseEnds(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var create=f.create("flow");f.tasks.create(create);var originals=new ArrayList<Original>();
            for(int index=0;index<3;index++)originals.add(ended(f,create.task(),"known-"+index));
            var last=originals.getLast();releaseValidation(last);
            var claims=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb);var beforeClaims=claims.find().into(new ArrayList<>());
            var beforeRows=SystemCollections.VALIDATION_RUNS.on(f.cacheDb).find().into(new ArrayList<>());
            assertThatThrownBy(()->last.reservation().access().control().read(last.run().identity().owner(),last.run())).isInstanceOf(TapstateException.class);
            var node=node(f);var history=last.reservation().access().history();
            var selected=history.terminals(node,new ValidationRunHistory.Selection(create.task(),ValidationMode.FULL,1,1));
            assertThat(selected.known()).isEqualTo(3);assertThat(selected.unknown()).isZero();assertThat(selected.unsettled()).isZero();
            assertThat(selected.recent()).singleElement().satisfies(status->{assertThat(status.run().identity()).isEqualTo(last.run().identity());assertThat(status.run().snapshot()).isNull();});
            assertThat(selected.oldest()).singleElement().satisfies(status->assertThat(status.run().identity()).isEqualTo(originals.getFirst().run().identity()));
            var known=history.terminal(node,selected.recent().getFirst().run()).orElseThrow();assertThat(known.run().snapshot()).isEqualTo(last.run().snapshot());
            assertThat(last.reservation().access().reports().mirror(new ValidationReports.TerminalMirror(known,Instant.now(),null,null))).isEqualTo(ValidationReports.Mirror.APPLIED);
            assertThat(claims.find().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(beforeClaims);
            assertThat(SystemCollections.VALIDATION_RUNS.on(f.cacheDb).find().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(beforeRows);
        }
    }
    @Test void missingTimesRemainUnknownAndStaleNodesOrDamagedOriginalsAreRejected(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var create=f.create("flow");f.tasks.create(create);var original=ended(f,create.task(),"unknown-time");releaseValidation(original);
            var node=node(f);var history=original.reservation().access().history();var rows=SystemCollections.VALIDATION_RUNS.on(f.cacheDb);
            var id=new Document("_id",MongoValidationControlStore.runId(original.run()));rows.updateOne(id,new Document("$unset",new Document("endedAt","")));
            var selected=history.terminals(node,new ValidationRunHistory.Selection(create.task(),ValidationMode.FULL,10,10));
            assertThat(selected.known()).isZero();assertThat(selected.unknown()).isOne();assertThat(selected.recent()).isEmpty();assertThat(selected.oldest()).isEmpty();
            assertThat(history.terminal(node,original.run()).orElseThrow().endedAt()).isNull();
            var stale=new WorkloadClaimFence(node.key(),new WorkloadOwner(node.owner().nodeId(),"wrong-boot"),node.claimGeneration(),node.executionGeneration(),node.topologyRevision());
            assertThatThrownBy(()->history.terminals(stale,new ValidationRunHistory.Selection(create.task(),ValidationMode.FULL,10,10))).isInstanceOf(TapstateException.class);
            rows.updateOne(id,new Document("$set",new Document("endedAt",1.5d)));
            assertThatThrownBy(()->history.terminals(node,new ValidationRunHistory.Selection(create.task(),ValidationMode.FULL,10,10))).isInstanceOf(TapstateException.class).hasMessageContaining(IoError.DOCUMENT_UNREADABLE.code());
            rows.drop();assertThatThrownBy(()->history.terminals(node,new ValidationRunHistory.Selection(create.task(),ValidationMode.FULL,10,10))).isInstanceOf(TapstateException.class);
            assertThat(f.cacheDb.listCollectionNames()).doesNotContain("validation_runs");
        }
    }
    @Test void nodeHistoryAndConfigurationRetentionUseTheSameNanosecondAndIdentityOrder(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,null,false,1),proposal.settings().incremental());
            var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),settings,proposal.commandId());f.tasks.create(create);
            var originals=List.of(ended(f,create.task(),"a"),ended(f,create.task(),"b"));var last=originals.getLast();releaseValidation(last);var node=node(f);
            var timestamp=Instant.EPOCH.plusNanos(123001000);var rows=SystemCollections.VALIDATION_RUNS.on(f.cacheDb);var history=last.reservation().access().history();
            for(var original:originals){
                rows.updateOne(new Document("_id",MongoValidationControlStore.runId(original.run())),new Document("$set",new Document("endedAt",timestamp.toString())));
                var known=history.terminal(node,original.run()).orElseThrow();original.reservation().access().reports().mirror(new ValidationReports.TerminalMirror(known,Instant.now(),null,null));
            }
            var selected=history.terminals(node,new ValidationRunHistory.Selection(create.task(),ValidationMode.FULL,1,1));
            assertThat(selected.recent()).singleElement().satisfies(status->assertThat(status.run().identity().runId()).isEqualTo("a"));
            assertThat(selected.oldest()).singleElement().satisfies(status->assertThat(status.run().identity().runId()).isEqualTo("b"));
            var reports=last.reservation().access().reports();assertThat(reports.retain(new ValidationReports.Retention(create.task(),ValidationMode.FULL,1)).removed()).isOne();
            assertThat(reports.history("flow",create.task().taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,1)).items()).singleElement()
                    .satisfies(report->assertThat(report.reference().runId()).isEqualTo("a"));
        }
    }

    @Test void expiredCacheControlsRequireTheCurrentOwnerAndIrreversibleConfigurationClosure(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,null,false,1),proposal.settings().incremental());
            var create=new ValidationTaskStore.Create(proposal.task(),proposal.comparison(),settings,proposal.commandId());f.tasks.create(create);
            var originals=List.of(ended(f,create.task(),"expire-a"),ended(f,create.task(),"expire-b"),ended(f,create.task(),"expire-c"));
            var last=originals.getLast();var access=last.reservation().access();var history=access.history();var node=node(f);var known=new ArrayList<ValidationControlStore.Status>();
            for(var original:originals){var status=history.terminal(node,original.run()).orElseThrow();known.add(status);
                if(original!=last)access.reports().mirror(new ValidationReports.TerminalMirror(status,Instant.now(),null,null));}
            var authority=last.run().identity().owner();var control=access.control();
            assertThat(control.expire(new ValidationControlStore.Expiration(authority,known.getLast()))).isEqualTo(ValidationControlStore.Mutation.STALE);
            assertThat(history.expired(node,known.getFirst().run())).isFalse();
            var cleaned=access.reports().retain(new ValidationReports.Retention(create.task(),ValidationMode.FULL,1,List.of(known.getLast())));assertThat(cleaned.removed()).isEqualTo(2);
            assertThatThrownBy(()->control.expire(new ValidationControlStore.Expiration(originals.getFirst().run().identity().owner(),known.getFirst()))).isInstanceOf(TapstateException.class);
            for(int index=0;index<2;index++)assertThat(control.expire(new ValidationControlStore.Expiration(authority,known.get(index)))).isEqualTo(ValidationControlStore.Mutation.APPLIED);
            assertThat(control.snapshot(authority).retainedRuns()).isOne();assertThat(control.snapshot(authority).countingGeneration()).isEqualTo(last.run().countingGeneration());
            assertThat(control.expire(new ValidationControlStore.Expiration(authority,known.getFirst()))).isEqualTo(ValidationControlStore.Mutation.ALREADY_APPLIED);
            assertThat(control.read(authority,originals.getFirst().run())).isEmpty();
            assertThat(history.expired(node,originals.getFirst().run())).isTrue();
            assertThat(history.expired(node,last.run())).isFalse();
            assertThatThrownBy(()->control.abandonClosedStart(originals.getFirst().run())).isInstanceOf(TapstateException.class).hasMessageContaining(IoError.WORKLOAD_CLAIM_FENCED.code());
            assertThat(originals.getFirst().reservation().access().admissions().open(new ValidationReportAdmission.Open(originals.getFirst().run().admission(),originals.getFirst().run().snapshot())))
                    .isIn(ValidationReportAdmission.Birth.CLOSED,ValidationReportAdmission.Birth.STALE);
            assertThat(control.snapshot(authority).retainedRuns()).isOne();
        }
    }
    @Test void configurationTaskBrowsingIsPagedAndDoesNotInitializeMissingDefinitions(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            assertThat(f.tasks.list("flow",0,1).items()).isEmpty();assertThat(f.tasks.pipelines(null,1)).isEmpty();
            var first=f.create("flow");var second=f.create("flow");f.tasks.create(first);f.tasks.create(second);
            var page=f.tasks.list("flow",0,1);assertThat(page.total()).isEqualTo(2);assertThat(page.items()).hasSize(1);
            var next=f.tasks.list("flow",1,1);assertThat(next.items()).hasSize(1);assertThat(next.items().getFirst().task()).isNotEqualTo(page.items().getFirst().task());
            assertThat(f.tasks.pipelines(null,1)).containsExactly("flow");assertThat(f.tasks.pipelines("flow",1)).isEmpty();
            assertThat(f.tasks.list("missing",0,1).items()).isEmpty();assertThat(f.tasks.list("flow",2,1).items()).isEmpty();
            assertThatThrownBy(()->f.tasks.list("flow",0,21)).isInstanceOf(TapstateException.class);
        }
    }

    private static WorkloadClaimFence node(MongoValidationTaskStoreIT.Fixture f){
        return WorkloadClaimFence.from(f.cache.workloadClaims(f.scope).read(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"node")).orElseThrow().claim());
    }
    private static void releaseValidation(Original original){
        var claims=original.reservation().access().claims();var known=claims.read(ValidationClaims.key("cluster","flow")).orElseThrow().claim();assertThat(claims.release(known)).isTrue();
    }
    private static Original ended(MongoValidationTaskStoreIT.Fixture f,ValidationReportAdmission.Task task,String id){
        var saved=f.read(task);var reserved=f.reserve(saved,id);var start=reserved.start();var spec=new ValidationRunSpec(saved.comparison(),ValidationMode.FULL,saved.settings().full().method());
        var control=reserved.access().control();var state=control.snapshot(start.run().owner());var run=new ValidationControlStore.Run(start.run(),state.countingGeneration()+(state.closed()?1:0),start,spec);
        assertThat(control.join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
        assertThat(reserved.access().admissions().open(new ValidationReportAdmission.Open(start,spec))).isEqualTo(ValidationReportAdmission.Birth.CREATED);
        control.transition(new ValidationControlStore.Transition(run,ValidationRunState.STARTING,ValidationRunState.RUNNING));
        var read=new ValidationCheckEvent.ReadBoundary("count","v1",Instant.EPOCH,Instant.EPOCH);
        reserved.access().results().publishCount(new ValidationResultStore.CountWrite(start.report(),spec,new ValidationCountCheck(start.run(),"mapping","out",1,1,read,read)));
        reserved.access().admissions().close(start);control.transition(new ValidationControlStore.Transition(run,ValidationRunState.RUNNING,ValidationRunState.COMPLETED));
        control.retire(new ValidationControlStore.Retirement(run,start.report(),ValidationRunState.COMPLETED));return new Original(reserved,run);
    }
}
