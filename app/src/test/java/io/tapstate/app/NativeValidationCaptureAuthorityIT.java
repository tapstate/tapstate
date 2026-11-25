package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real CONFIG/CACHE gates; the separately tested production execution proof is supplied explicitly. */
@RequiresDocker
class NativeValidationCaptureAuthorityIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    @ParameterizedTest @ValueSource(strings={"permission","claim"})
    void actualBirthAndLiveValidationTenureAreRequiredInAdditionToPipelineProof(String lost){
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var generation=f.configuration.currentValidationGeneration("flow",f.scope,()->{});
            assertThatThrownBy(()->f.configuration.loadValidationAccess("flow",f.scope,f.cache,()->{})).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->f.configuration.loadValidationAccess("missing",f.scope,f.cache,()->{})).isInstanceOf(TapstateException.class);
            assertThat(f.configuration.currentValidationGeneration("flow",f.scope,()->{})).isEqualTo(generation);
            assertThat(f.configuration.preparations().read("flow").orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            var request=f.request("INCREMENTAL",1);var accepted=f.owners.reserve(request);
            assertThat(accepted.outcome()).isEqualTo(ValidationOwnerRouter.Outcome.ACCEPTED);
            var spec=new ValidationRunSpec(f.comparison,ValidationMode.INCREMENTAL,ValidationMethod.FULL_COLUMN);
            var run=new ValidationRunIdentity(accepted.owner(),"task",request.run(),spec.mode(),spec.fingerprint(),1,"rules");
            var start=new ValidationReportAdmission.Start(f.task,1,1,request.command().toString(),run);var reservation=f.owners.take(start);
            var original=new ValidationControlStore.Run(run,reservation.countingGeneration(),start,spec);var access=reservation.access();
            var production=new ExecutionFence(new PipelineIntentScope("cluster",1,f.scope.cacheIncarnation(),"flow",1),
                    new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("node","boot"),1,1,1));
            var productionProof=mock(ExecutionAuthorization.class);doAnswer(call->{assertThat(call.getArgument(0,ExecutionFence.class)).isEqualTo(production);return null;}).when(productionProof).require(any());
            var authority=new NativeValidationCaptureAuthority(f.configuration,f.cache,f.scope,mock(SrsStoreViews.class),productionProof,()->{});
            assertThatThrownBy(()->authority.run(start,production)).isInstanceOf(TapstateException.class).hasMessageContaining("originalBirth");
            access.admissions().reserve(start);access.control().join(original);
            assertThatThrownBy(()->authority.run(start,production)).isInstanceOf(TapstateException.class).hasMessageContaining("originalBirth");
            access.admissions().open(new ValidationReportAdmission.Open(start,spec));authority.run(start,production);
            assertThatThrownBy(()->f.configuration.loadValidationAccess("flow",new CacheBusinessScope("cluster",f.scope.bindingVersion(),"wrong-cache"),f.cache,()->{})).isInstanceOf(TapstateException.class);
            authority.run(start,production);
            if(lost.equals("permission"))access.admissions().close(start);
            else{var claims=access.claims();assertThat(claims.release(claims.read(ValidationClaims.key("cluster","flow")).orElseThrow().claim())).isTrue();}
            assertThatThrownBy(()->authority.run(start,production)).isInstanceOf(TapstateException.class);
            verify(productionProof,atLeastOnce()).require(production);
        }
    }
}
