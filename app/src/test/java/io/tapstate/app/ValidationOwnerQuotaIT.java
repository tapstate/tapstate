package io.tapstate.app;

import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationProblemQuota;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class ValidationOwnerQuotaIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    @Test void fullAndIncrementalShareOneThousandRootsAndLatchAllActualCacheSlots() {
        try(var f=new NativeValidationOwnerReservationsIT.Fixture();var frame=f.resources.admitMetadata()) {
            assertThat(f.tasks.update(new ValidationTaskStore.Update(f.task,1,f.comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.FULL_COLUMN,1000),UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
            var full=run(f,ValidationMode.FULL);var incremental=run(f,ValidationMode.INCREMENTAL);
            var quota=new ValidationOwnerQuota(full.run().identity().owner(),full.run().countingGeneration(),8L*1024*1024,full.reservation().access().control(),full.reservation()::requireOwner);
            var fullStopped=new AtomicBoolean();var incrementalStopped=new AtomicBoolean();
            quota.joined(full.run(),full.reservation()::requireReserved,()->fullStopped.set(true));quota.joined(incremental.run(),incremental.reservation()::requireReserved,()->incrementalStopped.set(true));
            for(int index=0;index<700;index++)assertThat(quota.reserve(full.run(),root(index)).admission()).isEqualTo(ValidationProblemQuota.Admission.RESERVED);
            for(int index=700;index<1000;index++)assertThat(quota.reserve(incremental.run(),root(index)).admission()).isEqualTo(ValidationProblemQuota.Admission.RESERVED);
            assertThat(quota.reserve(full.run(),root(1)).admission()).isEqualTo(ValidationProblemQuota.Admission.EXISTING);assertThat(quota.roots()).isEqualTo(1000);
            var before=full.reservation().access().control().read(full.run().identity().owner(),full.run()).orElseThrow();assertThat(before.state()).isEqualTo(ValidationRunState.RUNNING);
            assertThat(quota.reserve(incremental.run(),root(1000)).admission()).isEqualTo(ValidationProblemQuota.Admission.LIMIT_REACHED);
            assertThat(fullStopped).isTrue();assertThat(incrementalStopped).isTrue();assertThat(quota.latchKnown()).isFalse();assertThat(quota.roots()).isEqualTo(1000);
            quota.reconcileLimit();quota.reconcileLimit();assertThat(quota.latchKnown()).isTrue();
            var control=full.reservation().access().control();var snapshot=control.snapshot(full.run().identity().owner());
            assertThat(snapshot.problemLimit()).isTrue();assertThat(snapshot.closed()).isTrue();assertThat(snapshot.active()).hasSize(2);
            assertThat(control.read(full.run().identity().owner(),full.run()).orElseThrow().state()).isEqualTo(ValidationRunState.STOPPING);
            assertThat(control.read(full.run().identity().owner(),incremental.run()).orElseThrow().state()).isEqualTo(ValidationRunState.STOPPING);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
        }
    }
    private record Active(ValidationControlStore.Run run,NativeValidationOwnerReservations.Reservation reservation) {}
    private static Active run(NativeValidationOwnerReservationsIT.Fixture f,ValidationMode mode) {
        var request=new ValidationOwnerRouter.Request("flow","task",f.task.generation(),2,1,mode.name(),UUID.randomUUID(),UUID.randomUUID().toString());
        var accepted=f.owners.reserve(request);assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
        var spec=new ValidationRunSpec(f.comparison,mode,ValidationMethod.FULL_COLUMN);
        var identity=new ValidationRunIdentity(accepted.owner(),"task",request.run(),mode,spec.fingerprint(),1,"rules");
        var start=new ValidationReportAdmission.Start(f.task,2,1,request.command().toString(),identity);var reservation=f.owners.take(start);
        var run=new ValidationControlStore.Run(identity,reservation.countingGeneration(),start,spec);var access=reservation.access();
        assertThat(access.admissions().reserve(start)).isEqualTo(ValidationReportAdmission.Reservation.RESERVED);assertThat(access.control().join(run)).isEqualTo(ValidationControlStore.Join.JOINED);
        access.admissions().open(new ValidationReportAdmission.Open(start,spec));access.control().transition(new ValidationControlStore.Transition(run,ValidationRunState.STARTING,ValidationRunState.RUNNING));
        return new Active(run,reservation);
    }
    private static ValidationRoot root(int index){return new ValidationRoot("mapping","out",new ValidationKey(List.of(ValidationKey.Part.number(index))),List.of());}
}
