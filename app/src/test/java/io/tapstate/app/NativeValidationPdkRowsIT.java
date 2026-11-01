package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.ValidationTaskStore;
import io.tapstate.core.common.TapstateException;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.springframework.test.util.ReflectionTestUtils;
import static org.assertj.core.api.Assertions.*;

/** The actual owner/run budget cannot drain on worker exit while native cleanup is unresolved. */
@RequiresDocker
class NativeValidationPdkRowsIT {
    @Container static final MongoDBContainer MONGO = NativeValidationOwnerReservationsIT.MONGO;

    @Test void anOlderSavedTextKeyCannotBypassNativeOrderingAdmission() {
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var previous=f.comparison;var mapping=previous.mappings().getFirst();
            var field=new ValidationKeyBinding.Component(List.of("id"),ValidationKey.Kind.TEXT,java.util.Set.of("text"));
            var legacy=new ValidationTaskSpec(previous.pipelineId(),previous.dependencyHashes(),previous.mappingRevision()+1,previous.comparisonRulesVersion(),
                    List.of(new ValidationTaskSpec.Mapping(mapping.mappingId(),mapping.outputKind(),mapping.outputId(),mapping.source(),mapping.target(),
                            new ValidationTaskSpec.Keys("old-text-confirmation",List.of(field),List.of(field)))));
            assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,1,legacy,ValidationTaskStore.Settings.defaults(ValidationMethod.UNIQUE_KEY,1000),java.util.UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            var request=new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,1,"FULL",java.util.UUID.randomUUID(),java.util.UUID.randomUUID().toString());
            var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var original=f.owners.take(f.owners.original(request,accepted.owner()));
            assertThatThrownBy(()->new NativeValidationPdkRows(original,original.declarations,ValidationDirectMappings.declare(original.declarations),Map.of(),Clock.systemUTC()))
                    .isInstanceOfSatisfying(TapstateException.class,refused->{assertThat(refused.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED);
                        assertThat(refused.args()).containsEntry("rule","native-text-ordering-unproven");});
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints={0,1,2})
    void cancellationReceiptsKeepAsynchronousCleanupDiagnosesThroughRetryAndFinalMirror(int kind)throws Exception {verifyCancellationReceipt(kind);}
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(ints={3,4,5})
    void nestedAndConcurrentCancellationReceiptsKeepOnlyRealCleanupFailures(int kind)throws Exception {verifyCancellationReceipt(kind);}
    private void verifyCancellationReceipt(int kind)throws Exception {
        var entered=new java.util.concurrent.CountDownLatch(1);var resume=new java.util.concurrent.CountDownLatch(1);
        var workers=new CompletableFuture<Void>();var returned=new CompletableFuture<Void>();var cleaned=new CompletableFuture<Void>();
        var closes=new java.util.concurrent.atomic.AtomicInteger();
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers);
            var registry=new io.tapstate.runtime.engine.validation.ValidationProcessorRegistry(f.member,()->{})) {
            var start=NativeValidationFullJobIT.prepare(f);var original=f.owners.take(start);var access=original.access();
            var nativePlan=new java.util.concurrent.atomic.AtomicReference<NativeValidationPdkRows>();
            var diagnosis=kind==1?new AssertionError("Original asynchronous native cleanup failed"):
                    new TapstateException(ValidationError.READ_FAILED,Map.of("table","original-asynchronous-cleanup"),null);
            var interrupted=new TapstateException(ValidationError.READ_CANCELLED,Map.of("table","orders"),new InterruptedException());
            if(kind==5)interrupted.addSuppressed(diagnosis);
            var cancelled=new TapstateException(ValidationError.READ_CANCELLED,Map.of("table","orders"),
                    kind==2?new IllegalStateException("Unexpected original native cancellation cause"):kind>=4?interrupted:null);
            if(kind==0)cancelled.addSuppressed(diagnosis);
            if(kind==3)cancelled.addSuppressed(interrupted);
            Throwable receipt=kind==1?diagnosis:cancelled;
            if(kind==3){var wrapper=new java.util.concurrent.CompletionException(cancelled);wrapper.addSuppressed(interrupted);receipt=wrapper;}
            final Throwable originalReceipt=receipt;
            try {
                runs.start(start,new NativeValidationFullJob(original,registry,f.member,f.process,runs,reservation->{
                    var plan=new NativeValidationPdkRows(reservation,reservation.declarations,List.of(),Map.of(),Clock.systemUTC());nativePlan.set(plan);
                    long bytes=NativeValidationPdkRows.rowReadBytes(new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"io.tapdata.mongodb.MongodbConnector",null,null));
                    var read=ReflectionTestUtils.invokeMethod(plan,"admit",bytes);
                    ReflectionTestUtils.invokeMethod(read,"attach",(Runnable)closes::incrementAndGet,workers,returned,cleaned);
                    ReflectionTestUtils.invokeMethod(read,"opened");
                    return new NativeValidationFullJob.Plan(){
                        public List<io.tapstate.runtime.engine.validation.ValidationFullScan.Mapping> mappings(){
                            return List.of(new io.tapstate.runtime.engine.validation.ValidationFullScan.Mapping(f.comparison.mappings().getFirst().mappingId(),guard->{
                                entered.countDown();NativeValidationFullJobIT.waitFor(resume);guard.run();throw new AssertionError("Stopped original cannot open business readers");
                            },(observation,guard)->{throw new AssertionError("Stopped original cannot compare rows");}));
                        }
                        public Throwable failure(){return plan.failure();}public void close(){plan.close();}public boolean drained(){return plan.drained();}
                    };
                }));
                assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();runs.stop(start.run());NativeValidationFullJobIT.await(()->closes.get()>0);NativeValidationFullJobIT.await(()->original.cancelled.get());
                workers.completeExceptionally(originalReceipt);returned.complete(null);
                if(kind==3||kind==4)assertThat(nativePlan.get().failure()).as("Pure original cancellation chains carry no execution fault").isNull();
                else assertThat(nativePlan.get().failure()).as("The retained original worker receipt keeps its real diagnosis").isSameAs(kind==2?cancelled:diagnosis);
                assertThat(nativePlan.get().drained()).isFalse();assertThat(f.resources.snapshot().processReserved().isEmpty()).isFalse();
                nativePlan.get().close();assertThat(closes.get()).isGreaterThanOrEqualTo(2);
            }finally{resume.countDown();workers.completeExceptionally(cancelled);returned.complete(null);cleaned.complete(null);}
            var reference=new io.tapstate.spi.store.ValidationReports.Reference("flow","task",ValidationMode.FULL,start.run().runId(),f.task.generation(),1);
            NativeValidationFullJobIT.await(()->access.reports().find(reference).filter(report->report.closed()&&report.terminal()==ValidationRunState.STOPPED).isPresent());
            NativeValidationFullJobIT.await(()->f.resources.snapshot().metadataFrames()==0&&f.resources.snapshot().processReserved().isEmpty());
            assertThat(nativePlan.get().drained()).isTrue();var failure=access.reports().find(reference).orElseThrow().failure();
            if(kind==3||kind==4)assertThat(failure).isNull();
            else{
                assertThat(failure).isNotNull();
                if(kind==1)assertThat(failure.fatal()).isTrue();
                else if(kind==0||kind==5)assertThat(failure.params()).containsEntry("table","original-asynchronous-cleanup");
                else assertThat(failure.technical()).contains("Unexpected original native cancellation cause");
            }
        }finally{resume.countDown();returned.complete(null);cleaned.complete(null);}
    }

    @Test void exceptionalWorkerExitAndAnIdempotentCloseCannotReleaseTheNativeReadBudget() throws Exception {
        try (var f = new NativeValidationOwnerReservationsIT.Fixture()) {
            var request = f.request("FULL", 1);
            var accepted = f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var original = f.owners.take(f.owners.original(request, accepted.owner()));
            original.beginExecution();
            var allocation = original.resources();
            var plan = new NativeValidationPdkRows(original, original.declarations, List.of(), Map.of(), Clock.systemUTC());
            // Isolate the production retention state machine from plugin behavior covered by adapter tests.
            long nativeBytes=NativeValidationPdkRows.rowReadBytes(new io.tapstate.adapters.pdk.ConnectorRef(List.of(),"io.tapdata.mongodb.MongodbConnector",null,null));
            var read = ReflectionTestUtils.invokeMethod(plan, "admit",nativeBytes);
            var workers = new CompletableFuture<Void>();
            var rows = new CompletableFuture<Void>();
            var cleanup = new CompletableFuture<Void>();
            ReflectionTestUtils.invokeMethod(read, "attach", (Runnable) () -> {}, workers, rows, cleanup);
            ReflectionTestUtils.invokeMethod(read, "opened");
            var charged = allocation.budget().used();
            assertThat(charged.isEmpty()).isFalse();
            var originalFailure = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "orders"), null);
            workers.completeExceptionally(originalFailure);
            rows.complete(null);
            plan.close();
            allocation.close();
            allocation.processorsExited();
            assertThat(plan.drained()).isFalse();
            assertThat(plan.failure()).isSameAs(originalFailure);
            assertThat(allocation.budget().used()).isEqualTo(charged);
            assertThat(allocation.released().toCompletableFuture()).isNotDone();
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isFalse();
            // Successful native cleanup supplies the final receipt, without replacing the original read.
            cleanup.complete(null);
            allocation.released().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(plan.drained()).isTrue();
            assertThat(allocation.budget().used().isEmpty()).isTrue();
            assertThat(f.resources.snapshot().processReserved().isEmpty()).isTrue();
        }
    }
}
