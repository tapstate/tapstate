package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.*;
import io.tapstate.spi.store.ValidationResultStore;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Assembly of one original full scan; only member-local factory tokens travel through Jet. */
final class NativeValidationFullJob implements ValidationRunLifecycle.Activation {
    interface Plan {
        /** Construction is metadata-only; each mapping opens its independently admitted reads later. */
        default List<ValidationFullScan.Mapping> mappings(){return List.of();}
        default List<ValidationCountScan.Mapping> counts(){return List.of();}
        /** Actual native/callback/row drain, including cancellation and failed opening. */
        boolean drained();
        /** Late native/cleanup failures remain observable until every original handle has drained. */
        default Throwable failure(){return null;}
        /** Idempotently close original resources on admitted I/O, including concurrent cancellation. This call alone does not prove drain. */
        void close();
    }
    @FunctionalInterface interface Factory {
        Plan prepare(NativeValidationOwnerReservations.Reservation original);
    }
    private final NativeValidationOwnerReservations.Reservation original;
    private final ValidationProcessorRegistry registry;
    private final HazelcastInstance member;
    private final ValidationMemberResources process;
    private final NativeValidationRuns runs;
    private final Factory factory;
    NativeValidationFullJob(NativeValidationOwnerReservations.Reservation original,ValidationProcessorRegistry registry,
            HazelcastInstance member,ValidationMemberResources process,NativeValidationRuns runs,Factory factory) {
        this.original=Objects.requireNonNull(original);this.registry=Objects.requireNonNull(registry);this.member=Objects.requireNonNull(member);
        this.process=Objects.requireNonNull(process);this.factory=Objects.requireNonNull(factory);
        this.runs=Objects.requireNonNull(runs);
    }
    @Override public ValidationRunLifecycle.Readiness readiness(){original.requireReserved();return ValidationRunLifecycle.Readiness.READY;}
    @Override public ValidationRunLifecycle.Execution create() {
        original.requireReserved();var allocation=original.resources();var spec=original.spec();var identity=allocation.budget().identity();
        if(spec.mode()!=ValidationMode.FULL)throw invalid("method");
        var work=new Work(identity,spec,allocation);
        ValidationProcessorRegistry.Run processors=null;
        try {
            var vertex="full-scan";processors=registry.register(identity,Set.of(vertex),original::requireReserved);
            var local=member.getCluster().getLocalMember();
            var graph=new ValidationJobGraph(identity,local.getAddress(),local.getUuid(),new ValidationJobGraph.Limits(1,0,1),processors);
            graph.vertex(vertex,ProcessorSupplier.of(()->new AbstractProcessor(){
                @Override public boolean complete(){work.launch();work.throwFailure();return work.finished;}
                @Override public void close(){work.cancel();}
            }));
            return new ValidationJobExecution(identity,graph,processors,member.getJet(),work,original::requireReserved,()->{});
        }catch(RuntimeException|Error failed){
            if(processors!=null){processors.seal();if(processors.processorsExited())processors.forget();}
            work.cancel();throw failed;
        }
    }
    private final class Work implements ValidationJobExecution.Work {
        final ValidationRunIdentity identity;final ValidationRunSpec spec;final ValidationMemberResources.Run allocation;
        final ValidationFailureLatch failures=new ValidationFailureLatch();
        final AtomicBoolean attempted=new AtomicBoolean(),cancelled=new AtomicBoolean(),cleanupRequested=new AtomicBoolean();
        volatile Plan plan;volatile ValidationFullScan scan;volatile ValidationCountScan counts;volatile ValidationLocalBudget.Reservation planLease;
        volatile boolean workerOutstanding,finished,cleanupExited;
        Work(ValidationRunIdentity identity,ValidationRunSpec spec,ValidationMemberResources.Run allocation) {
            this.identity=identity;this.spec=spec;this.allocation=allocation;
        }
        void launch(){
            if(!attempted.compareAndSet(false,true))return;
            if(cancelled.get())return;
            workerOutstanding=true;
            try {allocation.executeCheck(()->{
                try{
                    require();planLease=allocation.budget().reserve(new ValidationLocalBudget.Amount(8L*1024*1024,0,0,0));
                    plan=Objects.requireNonNull(factory.prepare(original),"full scan plan");require();
                    if(spec.method()==ValidationMethod.COUNT){
                        counts=new ValidationCountScan(identity,spec,plan.counts(),this::require,check->{
                            var result=original.access().results().publishCount(new ValidationResultStore.CountWrite(
                                    new ValidationResultStore.Report(identity,original.request.startSequence()),spec,check));
                            if(result!=ValidationResultStore.Publication.APPLIED&&result!=ValidationResultStore.Publication.DUPLICATE)throw invalid("countPublicationClosed");
                        });
                        require();counts.run();
                    }else{
                    scan=new ValidationFullScan(identity,spec,plan.mappings(),this::require,this::publishRoot);
                    require();scan.run();
                    }
                }
                catch(RuntimeException|Error failed){record(failed);}
                finally{
                    try{closePlan();}catch(RuntimeException|Error failed){record(failed);}
                    finally{workerOutstanding=false;finished=true;releasePlan();}
                }
                if(failures.get() instanceof Error fatal)throw fatal;
            },()->workerOutstanding=false);}
            catch(RejectedExecutionException rejected){workerOutstanding=false;var failed=new TapstateException(ValidationError.READ_FAILED,Map.of("table","validation-full-scan"),rejected);record(failed);cancel();throw failed;}
            catch(RuntimeException|Error uncertain){record(uncertain);cancel();throw uncertain;}
        }
        void require(){
            throwFailure();if(cancelled.get())throw new TapstateException(ValidationError.EXECUTION_CANCELLED,Map.of("node",identity.owner().nodeId()),null);
            original.requireReserved();
        }
        void publishRoot(ValidationRootCheck check){
            if(check.observedAgreement())return;
            var write=new ValidationResultStore.RootWrite(new ValidationResultStore.Report(identity,original.request.startSequence()),check);
            while(true){
                require();var admitted=runs.publishRoot(write);
                if(admitted.isEmpty()){
                    java.util.concurrent.locks.LockSupport.parkNanos(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(1));
                    continue;
                }
                final ValidationResultStore.Publication result;
                try{result=admitted.orElseThrow().toCompletableFuture().join();}
                catch(java.util.concurrent.CompletionException failed){
                    var cause=failed.getCause();
                    if(cause instanceof Error fatal)throw fatal;
                    if(cause instanceof RuntimeException runtime)throw runtime;
                    throw failed;
                }
                require();
                if(result!=ValidationResultStore.Publication.APPLIED&&result!=ValidationResultStore.Publication.DUPLICATE)throw invalid("publicationClosed");
                return;
            }
        }
        @Override public boolean businessComplete(){
            var current=scan;var scalar=counts;var prepared=plan;
            boolean drained=prepared==null||cleanupExited&&prepared.drained();recordPlanFailure();
            return finished&&drained&&(current!=null&&current.complete()||scalar!=null&&scalar.complete())&&failures.get()==null;
        }
        @Override public boolean readersReleased(){
            boolean done=!workerOutstanding&&runs.publicationsDrained(identity)&&(plan==null||cleanupExited&&plan.drained());
            recordPlanFailure();if(done)releasePlan();return done;
        }
        @Override public Throwable failure(){recordPlanFailure();return failures.get();}
        @Override public void cancel(){
            cancelled.set(true);var current=scan;if(current!=null)current.cancel();var scalar=counts;if(scalar!=null)scalar.cancel();
            maintain();
        }
        @Override public void maintain(){
            var current=plan;if(!cancelled.get()||current!=null&&cleanupExited&&current.drained())return;
            if(cleanupRequested.compareAndSet(false,true))process.cleanup(this::closePlan).whenComplete((ignored,failed)->{
                if(failed!=null)record(failed);
                cleanupRequested.set(false);releasePlan();
            });
        }
        void closePlan(){
            var cleanup=new ValidationFailureLatch();
            try{var current=scan;if(current!=null)current.closeActive();}catch(RuntimeException|Error failed){cleanup.record(failed);}
            try{var current=plan;if(current!=null){current.close();cleanupExited=true;}}catch(RuntimeException|Error failed){cleanup.record(failed);}
            if(cleanup.get() instanceof Error fatal)throw fatal;if(cleanup.get() instanceof RuntimeException failed)throw failed;
        }
        void releasePlan(){
            var lease=planLease;var current=plan;
            if(lease!=null&&!workerOutstanding&&(current==null||cleanupExited&&current.drained()))lease.close();
        }
        void record(Throwable failed){failures.record(failed);ValidationFailureLatch.addContext(failures.get(),failed);}
        void recordPlanFailure(){var current=plan;if(current!=null){var failed=current.failure();if(failed!=null)record(failed);}}
        void throwFailure(){recordPlanFailure();if(failures.get() instanceof Error fatal)throw fatal;if(failures.get() instanceof RuntimeException failed)throw failed;}
    }
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","fullJob."+field),null);}
}
