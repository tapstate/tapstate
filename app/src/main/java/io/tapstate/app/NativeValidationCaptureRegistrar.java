package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.srs.CaptureObservation;
import io.tapstate.spi.store.ValidationReportAdmission;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Source-member control for original bindings. Never invoked on capture or normal ACK threads. */
final class NativeValidationCaptureRegistrar implements AutoCloseable {
    interface Authority {
        void run(ValidationReportAdmission.Start original,ExecutionFence production);
        void capture(ValidationCaptureRoute route);
    }
    @FunctionalInterface interface Routes {ValidationCaptureRoute read(ExecutionFence original,String alias,String table,ValidationMemberResources.Metadata paid);}
    private final Authority authority;private final Routes routes;
    private final ValidationCaptureObservations observations;
    private final ValidationMemberResources.Member resources;private final ValidationMemberResources process;
    private final ScheduledFuture<?> polling;
    private final Set<Lease> retained=ConcurrentHashMap.newKeySet();
    private boolean closed;private int admitting;
    NativeValidationCaptureRegistrar(Authority authority,Routes routes,ValidationCaptureObservations observations,
            ValidationMemberResources.Member resources,ValidationMemberResources process,ValidationOwnerTimers timers){
        this.authority=Objects.requireNonNull(authority);this.routes=Objects.requireNonNull(routes);this.observations=Objects.requireNonNull(observations);
        this.resources=Objects.requireNonNull(resources);this.process=Objects.requireNonNull(process);
        polling=timers.deadlines.scheduleWithFixedDelay(this::tick,100,250,TimeUnit.MILLISECONDS);
    }
    /** Factory only constructs admitted local keys/outbox; it starts no reader, worker or transport. */
    Lease register(ValidationReportAdmission.Start original,ExecutionFence production,String alias,String table,
            long keyScratchBytes,Function<ValidationCaptureRoute,ValidationCaptureHintTap.Binding> factory){
        if(original.run().mode()!=ValidationMode.INCREMENTAL||keyScratchBytes<1||keyScratchBytes>8L*1024*1024)throw invalid("capture.bindingBudget");
        synchronized(this){if(closed)throw invalid("capture.registrarClosed");admitting++;}
        ValidationMemberResources.Metadata paid=null;ValidationCaptureHintTap.Binding binding=null;Lease lease=null;
        var owned=new AtomicReference<Lease>();
        try{
            paid=resources.admitMetadata();authority.run(original,production);
            var route=routes.read(production,alias,table,paid);authority.capture(route);
            var candidates=observations.status().stream().filter(status->!status.registry().closed()).filter(status->matches(route,status.context())).toList();
            if(candidates.size()!=1)throw invalid("capture.originalOpeningUnavailable");var opening=candidates.getFirst().context();
            binding=Objects.requireNonNull(factory.apply(route));var sender=binding.outbox().sender();
            if(!binding.outbox().run().equals(original.run())||!sender.pipelineExecutionId().equals(production.operatorExecutionKey())
                    ||!sender.sourceId().equals(alias)||!sender.tableId().equals(table)
                    ||binding.outbox().limits().reservationBytes()+keyScratchBytes+32L*1024*1024>paid.reservedBytes())throw invalid("capture.bindingOriginal");
            authority.run(original,production);authority.capture(route);
            var envelope=paid;
            var registered=observations.register(route,opening,binding,value->{var admitted=new Lease(original,route,value,envelope);owned.set(admitted);retained.add(admitted);});
            lease=owned.get();paid=null;binding=null;
            if(!registered.accepting())throw invalid("capture.originalOpeningUnavailable");
            synchronized(this){if(closed){lease.close();throw invalid("capture.registrarClosed");}}
            authority.run(original,production);authority.capture(route);return lease;
        }catch(RuntimeException|Error failure){
            if(owned.get()!=null){lease=owned.get();paid=null;binding=null;}
            if(lease!=null)lease.close();
            if(binding!=null){binding.outbox().close();var pending=paid;if(pending!=null){paid=null;binding.outbox().drained().whenComplete((ignored,failed)->pending.close());}}
            throw failure;
        }finally{if(paid!=null)paid.close();synchronized(this){admitting--;}}
    }
    final class Lease implements AutoCloseable {
        private final ValidationReportAdmission.Start original;private final ValidationCaptureRoute route;
        private final ValidationCaptureHintRegistry.Registration registration;private final ValidationMemberResources.Metadata paid;
        private final AtomicBoolean inFlight=new AtomicBoolean();private volatile boolean closing;private volatile Throwable failure;
        private Lease(ValidationReportAdmission.Start original,ValidationCaptureRoute route,ValidationCaptureHintRegistry.Registration registration,ValidationMemberResources.Metadata paid){this.original=original;this.route=route;this.registration=registration;this.paid=paid;}
        ValidationCaptureRoute route(){return route;}
        boolean accepting(){return !closing&&registration.accepting();}
        Optional<Throwable> failure(){return Optional.ofNullable(failure==null?registration.failure().orElse(null):failure);}
        boolean drained(){return closing&&!inFlight.get()&&registration.drained();}
        @Override public void close(){closing=true;registration.close();}
        private void maintain(){
            if(!inFlight.compareAndSet(false,true))return;
            process.cleanup(()->{
                if(!closing){try{
                    authority.run(original,route.execution());authority.capture(route);
                    if(!route.equals(routes.read(route.execution(),route.alias(),route.table(),paid)))throw invalid("capture.routeChanged");
                    if(!registration.accepting()||registration.failure().isPresent())close();
                }catch(RuntimeException|Error refused){failure=refused;close();if(refused instanceof Error fatal)throw fatal;}}
            }).whenComplete((ignored,failed)->{if(failed!=null){if(failure==null)failure=failed;close();}inFlight.set(false);if(drained()&&retained.remove(this))paid.close();});
        }
    }
    private void tick(){
        synchronized(this){if(closed&&retained.isEmpty()&&admitting==0){polling.cancel(false);return;}}
        for(var lease:retained)lease.maintain();
    }
    @Override public void close(){synchronized(this){closed=true;}for(var lease:retained)lease.close();}
    private static boolean matches(ValidationCaptureRoute route,CaptureObservation.Context opening){try{route.requireOpening(opening);return true;}catch(TapstateException mismatch){return false;}}
    private static TapstateException invalid(String field){return new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field",field),null);}
}
