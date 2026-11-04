package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationReportServiceTest {
    private static final UUID GENERATION=UUID.randomUUID();
    private static final ValidationReportService.Reference ID=new ValidationReportService.Reference("flow","task",GENERATION.toString(),"1","run");
    private static class Reports implements ValidationReports {
        Snapshot row=snapshot();int writes,reads;boolean expireDuringRoots;
        @Override public Page<Summary> history(String pipeline,String task,ValidationMode mode,Paging page){reads++;return new Page<>(List.of(),0,page);}
        @Override public Optional<Snapshot> find(Reference reference){reads++;return Optional.ofNullable(row);}
        @Override public Page<RootSummary> roots(Reference reference,Paging page){reads++;if(expireDuringRoots)row=null;return new Page<>(List.of(),0,page);}
        @Override public Optional<RootDetail> root(Reference reference,String hash){reads++;return Optional.empty();}
        @Override public Mirror mirror(TerminalMirror command){writes++;throw new AssertionError("read cannot mirror");}
        @Override public Mirror observe(Observation command){writes++;throw new AssertionError("read cannot publish");}
        @Override public Cleanup retain(Retention command){writes++;throw new AssertionError("read cannot expire");}
    }
    @Test void reportKeepsExactCountsAndFrozenScopeWithoutPromotingAConfigTerminalToPhysicalDrain(){
        var pool=new ValidationMetadataBudget();var reports=new Reports();var service=new ValidationReportService(reports,pool);
        try(var response=service.report(ID)){
            assertThat(pool.inUse()).isOne();var report=response.value();assertThat(report.result()).isEqualTo("COMPLETE_WITH_OBSERVATIONS");
            assertThat(report.counts()).singleElement().satisfies(count->{assertThat(count.source()).isEqualTo("9007199254740993");assertThat(count.target()).isEqualTo("9007199254740992");assertThat(count.difference()).isEqualTo("1");});
            assertThat(report.physicalDrain()).isEqualTo("UNKNOWN");assertThat(report.premiseVerifiedBySystem()).isFalse();assertThat(report.mappings()).hasSize(1);
        }
        assertThat(pool.inUse()).isZero();assertThat(reports.writes).isZero();
    }
    @Test void missingExpiredOrIncompleteNeverAppearsAsACompletedIssueFreeReport(){
        var pool=new ValidationMetadataBudget();var reports=new Reports();var service=new ValidationReportService(reports,pool);
        var original=reports.row;reports.row=new ValidationReports.Snapshot(original.admission(),original.specification(),true,null,null,null,null,null,0,0,List.of());
        try(var response=service.report(ID)){assertThat(response.value().result()).isEqualTo("INCOMPLETE");}
        reports.row=null;try(var response=service.report(ID)){assertThat(response.value()).isNull();}
        try(var response=service.roots(ID,"0","10")){assertThat(response.value()).isNull();}
        reports.row=original;reports.expireDuringRoots=true;
        try(var response=service.roots(ID,"0","10")){assertThat(response.value()).isNull();}
        assertThat(pool.inUse()).isZero();
    }
    @Test void aBlockedOldReadKeepsItsFrameAgainstReplacementReadersUntilItActuallyReturns()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var pool=new ValidationMetadataBudget();
        var reports=new Reports(){@Override public Optional<Snapshot> find(Reference reference){entered.countDown();
            while(release.getCount()!=0)try{release.await();}catch(InterruptedException ignored){/* A store operation may ignore interruption. */}
            return super.find(reference);}};
        var old=new ValidationReportService(reports,pool);var next=new ValidationReportService(new Reports(),pool);
        try(var caller=Executors.newSingleThreadExecutor()){
            var running=caller.submit(()->old.report(ID));assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
            var held=new ArrayList<ValidationReportService.Response<?>>();
            try{
                for(int index=0;index<3;index++)held.add(next.report(ID));assertThat(pool.inUse()).isEqualTo(4);
                assertThatThrownBy(()->next.report(ID)).isInstanceOf(TapstateException.class).hasMessageContaining("metadataFrames");
                release.countDown();try(var response=running.get(2,TimeUnit.SECONDS)){assertThat(response.value()).isNotNull();assertThat(pool.inUse()).isEqualTo(4);}
                assertThat(pool.inUse()).isEqualTo(3);
            }finally{release.countDown();held.forEach(ValidationReportService.Response::close);}
        }finally{release.countDown();}
        assertThat(pool.inUse()).isZero();
    }
    @Test void malformedPagingOrReferenceDoesNotTouchConfigAndNeverConsumesCapacity(){
        var reports=new Reports();var pool=new ValidationMetadataBudget();var service=new ValidationReportService(reports,pool);
        assertThatThrownBy(()->service.history("flow","task","0","21")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(()->service.report(new ValidationReportService.Reference("flow","task",GENERATION.toString(),"01","run"))).isInstanceOf(TapstateException.class);
        assertThat(reports.reads).isZero();assertThat(pool.inUse()).isZero();
    }
    private static ValidationReports.Snapshot snapshot(){
        var task=new ValidationReportAdmission.Task("cluster","flow","task",GENERATION);var owner=new ValidationOwner("cluster","flow","node","boot",1,1,1);
        var table=new ValidationTaskSpec.Table("source","source",null,null,"orders",null,null);var target=new ValidationTaskSpec.Table("out","target",null,null,"checked_orders",null,null);
        var spec=new ValidationRunSpec(new ValidationTaskSpec("flow",Map.of("flow","a".repeat(64),"source","b".repeat(64),"target","c".repeat(64)),1,"direct-full-v1",
                List.of(new ValidationTaskSpec.Mapping("orders",ValidationTaskSpec.OutputKind.SYNC,"out",table,target,null))),ValidationMode.FULL,ValidationMethod.COUNT);
        var identity=new ValidationRunIdentity(owner,"task","run",ValidationMode.FULL,spec.fingerprint(),1,"direct-full-v1");var start=new ValidationReportAdmission.Start(task,1,1,UUID.randomUUID().toString(),identity);
        var boundary=new ValidationCheckEvent.ReadBoundary("native-count","v1",Instant.EPOCH,Instant.EPOCH);
        return new ValidationReports.Snapshot(start,spec,true,ValidationRunState.COMPLETED,Instant.EPOCH,Instant.EPOCH,new ValidationReportMetadata.Window(Instant.EPOCH,Instant.EPOCH),null,0,1,
                List.of(new ValidationCountCheck(identity,"orders","out",9007199254740993L,9007199254740992L,boundary,boundary)));
    }
}
