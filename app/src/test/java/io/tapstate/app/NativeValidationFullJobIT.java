package io.tapstate.app;

import io.tapstate.core.model.*;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Real native admission, original Jet execution, independent root checking and CONFIG publication. */
@RequiresDocker
class NativeValidationFullJobIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    static final ValidationValues.Limits ROW=new ValidationValues.Limits(16384,128,12);
    @Test void theOriginalPinnedJobChecksBothTailsAndPersistsOnlyCompleteProblemRoots()throws Exception {
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var registry=new ValidationProcessorRegistry(f.member,()->{})) {
            assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,1,f.comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),UUID.randomUUID())))
                    .isEqualTo(ValidationTaskStore.Write.APPLIED);
            var request=new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
            var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.FULL_COLUMN);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),ValidationMode.FULL,spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,2,1,request.command().toString(),identity);var reservation=f.owners.take(start);var access=reservation.access();
            var original=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);
            var plan=new Plan(identity,f.comparison.mappings().getFirst());
            runs.start(start,new NativeValidationFullJob(reservation,registry,f.member,f.process,ignored->plan));
            await(()->f.tasks.read("flow","task").orElseThrow().full().permission()!=null&&f.tasks.read("flow","task").orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
            var actual=access.control().read(identity.owner(),original).orElseThrow();
            assertThat(actual.state()).isEqualTo(ValidationRunState.COMPLETED);assertThat(actual.retired()).isTrue();
            await(()->f.resources.snapshot().metadataFrames()==0);assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();
            assertThat(plan.checked).hasValue(5);assertThat(plan.borrowed).hasValue(0);assertThat(plan.drained()).isTrue();
            var reports=f.raw.getDatabase(f.configName).getCollection("validation_reports");
            assertThat(reports.find().first()).containsEntry("closed",true).containsEntry("rootCount",3L);
            var differences=f.raw.getDatabase(f.configName).getCollection("validation_differences").find().into(new ArrayList<>());
            assertThat(differences).hasSize(3);
            assertThat(differences.toString()).contains("TARGET_VALUE","SOURCE_DUPLICATE","TARGET_EXTRA");
            assertThat(f.member.getJet().getJobs()).hasSize(1);var job=f.member.getJet().getJobs().getFirst();
            assertThat(job.getName()).startsWith("validation:"+identity.runId()+":");assertThat(job.getConfig().isAutoScaling()).isFalse();
            assertThat(job.getConfig().getInitialSnapshotName()).isNull();
        }
    }
    @Test void nativeCountUsesTheOriginalFullSlotJobAndConfigSummaryWithoutCreatingRoots()throws Exception {
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var registry=new ValidationProcessorRegistry(f.member,()->{})) {
            var request=f.request("FULL",1);var accepted=f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.COUNT);
            var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),ValidationMode.FULL,spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),identity);
            var reservation=f.owners.take(start);var access=reservation.access();long countingGeneration=reservation.countingGeneration();var closed=new AtomicBoolean();var calls=new AtomicInteger();
            var mapping=f.comparison.mappings().getFirst();
            runs.start(start,new NativeValidationFullJob(reservation,registry,f.member,f.process,original->new NativeValidationFullJob.Plan(){
                public List<ValidationFullScan.Mapping> mappings(){throw new AssertionError("COUNT must not open row readers");}
                public List<ValidationCountScan.Mapping> counts(){return List.of(new ValidationCountScan.Mapping(mapping.mappingId(),guard->{
                    guard.run();calls.incrementAndGet();return new ValidationCountScan.Scalar(mapping.source(),9007199254740993L,
                            new ValidationCheckEvent.ReadBoundary("source","native-count-v1",java.time.Instant.EPOCH,java.time.Instant.EPOCH));
                },guard->{guard.run();calls.incrementAndGet();return new ValidationCountScan.Scalar(mapping.target(),9007199254740992L,
                            new ValidationCheckEvent.ReadBoundary("target","native-count-v1",java.time.Instant.EPOCH,java.time.Instant.EPOCH));}));}
                public void close(){closed.set(true);}public boolean drained(){return closed.get();}
            }));
            await(()->f.tasks.read("flow","task").orElseThrow().full().permission()!=null&&f.tasks.read("flow","task").orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
            var original=new ValidationControlStore.Run(identity,countingGeneration,start,spec);
            assertThat(access.control().read(identity.owner(),original).orElseThrow().state()).isEqualTo(ValidationRunState.COMPLETED);
            await(()->f.resources.snapshot().processReserved().isEmpty()&&f.resources.snapshot().metadataFrames()==0);
            var report=f.raw.getDatabase(f.configName).getCollection("validation_reports").find().first();
            assertThat(report.getLong("rootCount")).isZero();
            assertThat(report.getList("countSummaries",org.bson.Document.class)).singleElement().satisfies(value->
                    assertThat(value).containsEntry("sourceCount",9007199254740993L).containsEntry("targetCount",9007199254740992L));
            assertThat(f.raw.getDatabase(f.configName).getCollection("validation_differences").countDocuments()).isZero();
            assertThat(calls).hasValue(2);assertThat(f.member.getJet().getJobs()).hasSize(1);
        }
    }
    @Test void refusedWorkerAdmissionCannotLeaveAPhantomReaderAfterTheJobFails()throws Exception {
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var registry=new ValidationProcessorRegistry(f.member,()->{})) {
            var start=prepare(f);var reservation=f.owners.take(start);var created=new AtomicInteger();var refuse=new AtomicBoolean(true);
            var held=new AtomicReference<ValidationLocalBudget.Reservation>();
            f.localGuard=()->{
                boolean launching=StackWalker.getInstance().walk(frames->frames.anyMatch(frame->frame.getClassName().endsWith("NativeValidationFullJob$Work")&&frame.getMethodName().equals("launch")));
                if(launching&&refuse.compareAndSet(true,false)){
                    long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    while(held.get()==null&&System.nanoTime()-until<0){
                        try{held.set(reservation.resources().budget().reserve(new ValidationLocalBudget.Amount(0,20,0,0)));}
                        catch(io.tapstate.core.common.TapstateException busy){if(busy.code()!=ValidationError.VALUE_LIMIT_EXCEEDED)throw busy;Thread.yield();}
                    }
                    assertThat(held.get()).isNotNull();
                }
            };
            try{
                runs.start(start,new NativeValidationFullJob(reservation,registry,f.member,f.process,original->{created.incrementAndGet();return new Plan(start.run(),f.comparison.mappings().getFirst());}));
                await(()->held.get()!=null&&!f.member.getJet().getJobs().isEmpty()&&f.member.getJet().getJobs().getFirst().getStatus().isTerminal());
                held.get().close();
                await(()->f.tasks.read("flow","task").orElseThrow().full().permission()!=null&&f.tasks.read("flow","task").orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
            }finally{if(held.get()!=null)held.get().close();}
            await(()->f.resources.snapshot().metadataFrames()==0);assertThat(created).hasValue(0);
            await(()->{try{runs.read(start.run());return false;}catch(io.tapstate.core.common.TapstateException ended){return ended.args().containsValue("runs.originalUnavailable");}});
        }
    }
    @Test void cancellationBeforePlanPublicationDoesNotConsumeItsFutureCleanupRetry()throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1);var returnPlan=new java.util.concurrent.CountDownLatch(1);
        var cleanupEntered=new java.util.concurrent.CountDownLatch(4);var releases=List.of(new java.util.concurrent.CountDownLatch(1),new java.util.concurrent.CountDownLatch(1),new java.util.concurrent.CountDownLatch(1),new java.util.concurrent.CountDownLatch(1));
        var closes=new AtomicInteger();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var registry=new ValidationProcessorRegistry(f.member,()->{})) {
            var start=prepare(f);var reservation=f.owners.take(start);
            runs.start(start,new NativeValidationFullJob(reservation,registry,f.member,f.process,original->{
                assertThat(Thread.currentThread().getName()).startsWith("validation-check");
                assertThat(original.resources().budget().used().bytes()).isGreaterThanOrEqualTo(8L*1024*1024);
                entered.countDown();waitFor(returnPlan);
                return new NativeValidationFullJob.Plan(){
                    public List<ValidationFullScan.Mapping> mappings(){throw new AssertionError("cancelled plan must not open readers");}
                    public void close(){if(closes.incrementAndGet()==1)throw new io.tapstate.core.common.TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","fixture.firstCleanupUnknown"),null);}
                    public boolean drained(){return closes.get()>=2;}
                };
            }));
            assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
            for(var release:releases)f.process.cleanup(()->{cleanupEntered.countDown();waitFor(release);});
            assertThat(cleanupEntered.await(5,TimeUnit.SECONDS)).isTrue();runs.stop(start.run());
            var originalCleanupPassed=new java.util.concurrent.CountDownLatch(1);f.process.cleanup(originalCleanupPassed::countDown);
            releases.getFirst().countDown();assertThat(originalCleanupPassed.await(5,TimeUnit.SECONDS)).isTrue();
            returnPlan.countDown();await(()->closes.get()>=2);await(()->f.resources.snapshot().metadataFrames()==0);
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();
        }finally{returnPlan.countDown();releases.forEach(java.util.concurrent.CountDownLatch::countDown);}
    }
    static void waitFor(java.util.concurrent.CountDownLatch latch){try{assertThat(latch.await(12,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}}
    static ValidationReportAdmission.Start prepare(NativeValidationOwnerReservationsIT.Fixture f){
        assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,1,f.comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),UUID.randomUUID())))
                .isEqualTo(ValidationTaskStore.Write.APPLIED);
        var request=new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,1,"FULL",UUID.randomUUID(),UUID.randomUUID().toString());
        var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
        var spec=new ValidationRunSpec(f.comparison,ValidationMode.FULL,ValidationMethod.FULL_COLUMN);
        var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),ValidationMode.FULL,spec.fingerprint(),1,"rules");
        return new ValidationReportAdmission.Start(f.task,2,1,request.command().toString(),identity);
    }
    static final class Plan implements NativeValidationFullJob.Plan {
        final ValidationRunIdentity run;final ValidationTaskSpec.Mapping mapping;final AtomicInteger checked=new AtomicInteger(),borrowed=new AtomicInteger();
        final List<Map<String,Object>> source=List.of(row(1,"a"),row(2,"b"),row(3,"c"),row(3,"duplicate"),row(5,"e"));
        final List<Map<String,Object>> target=List.of(row(1,"a"),row(2,"wrong"),row(4,"extra"),row(5,"e"));
        volatile boolean closed;
        Plan(ValidationRunIdentity run,ValidationTaskSpec.Mapping mapping){this.run=run;this.mapping=mapping;}
        public List<ValidationFullScan.Mapping> mappings(){return List.of(new ValidationFullScan.Mapping(mapping.mappingId(),guard->
                new ValidationOrderedMerge(groups(source,mapping.source(),guard),groups(target,mapping.target(),guard)),this::check));}
        ValidationOrderedGroups groups(List<Map<String,Object>> rows,ValidationTaskSpec.Table side,Runnable guard){
            return new ValidationOrderedGroups(side.sourceId(),cursor(rows,guard),row->new ValidationOrderedGroups.Key(key(row),physical(side,key(row))),ROW.bytes());
        }
        ValidationRootCheck check(ValidationObservation observation,Runnable guard){
            checked.incrementAndGet();var key=observation.root().rootKey();
            return ValidationRootReader.read(observation,ValidationMethod.FULL_COLUMN,
                    new ValidationRootReader.Query("source","fixture-fresh-v1",()->cursor(selected(source,key),guard)),new ValidationRootReader.Query("target","fixture-fresh-v1",()->cursor(selected(target,key),guard)),
                    new ValidationRelationRows.Limits(65536,ROW),new ValidationRootCheck.Limits(65536,32,32,8,2),Clock.systemUTC(),guard,
                    row->new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,row,Optional.of(physical(mapping.source(),key(row)))),List.of()),
                    row->new ValidationRootReader.Target(row,physical(mapping.target(),key(row))),
                    (pair,root)->ValidationFieldCheck.check(observation,List.of(),pair.source().working(),pair.target().working(),pair.sourceSample(),pair.targetSample(),Optional.empty(),String::equals,
                            new ValidationValueComparison.TraversalLimits(ROW,10000),root));
        }
        List<Map<String,Object>> selected(List<Map<String,Object>> rows,ValidationKey key){return rows.stream().filter(row->key.equals(key(ValidationValues.ownRow(row,ROW)))).toList();}
        ValidationRowCursor cursor(List<Map<String,Object>> rows,Runnable guard){return new ValidationRowCursor(){
            int index;boolean ended;
            public Optional<Row> next(){guard.run();if(index==rows.size())return Optional.empty();int ordinal=index++;borrowed.incrementAndGet();var value=ValidationValues.ownRow(rows.get(ordinal),ROW);
                return Optional.of(new Row(){boolean returned;public long ordinal(){return ordinal;}public ValidationValues.Row values(){return value;}public void close(){if(!returned){returned=true;borrowed.decrementAndGet();}}});}
            public void close(){ended=true;}
        };}
        public boolean drained(){return closed&&borrowed.get()==0;}public void close(){closed=true;}
        static Map<String,Object> row(long id,String value){return Map.of("id",id,"value",value);}
        static ValidationKey key(ValidationValues.Row row){return new ValidationKey(List.of(ValidationKey.Part.number(((Number)row.fields().get("id")).longValue())));}
        static ValidationPhysicalIdentity physical(ValidationTaskSpec.Table side,ValidationKey key){return new ValidationPhysicalIdentity(side.sourceId(),side.database(),side.schema(),side.table(),List.of(List.of("id")),key);}
    }
    static void await(java.util.function.BooleanSupplier fact)throws InterruptedException {long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);while(!fact.getAsBoolean()&&System.nanoTime()-until<0)Thread.sleep(10);assertThat(fact.getAsBoolean()).isTrue();}
}
