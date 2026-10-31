package io.tapstate.app;

import com.hazelcast.jet.core.JobStatus;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.*;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeValidationStartupTest {
    @Test void ordinaryFullDoesNotDependOnProductionPreparationOrInitialReadiness() {
        var f=new Fixture();
        var gate=f.startup.inspect(f.saved,f.spec(ValidationMode.FULL));
        assertThat(gate.controlOnly()).isFalse();
        assertThat(gate.current().get()).isEqualTo(ValidationRunLifecycle.Readiness.READY);
        verifyNoInteractions(f.engine,f.initials,f.cache);
    }
    @Test void completionCannotStartAnIncrementalRunEvenWhenInitialCommitsAreReady() {
        var f=new Fixture();var gate=f.startup.inspect(f.saved,f.spec(ValidationMode.INCREMENTAL));
        f.status=JobStatus.COMPLETED;
        assertThatThrownBy(()->gate.current().get()).isInstanceOf(TapstateException.class).hasMessageContaining("originalPipelineEnded");
    }
    @Test void originalIntentAndJobArePinnedAcrossControlChecks() {
        var f=new Fixture();var gate=f.startup.inspect(f.saved,f.spec(ValidationMode.INCREMENTAL));
        when(f.engine.captureRun("flow")).thenReturn(Optional.empty());
        assertThatThrownBy(()->gate.current().get()).isInstanceOf(TapstateException.class).hasMessageContaining("originalPipelineEnded");
    }
    @Test void lookupUnknownRemainsUnknownInsteadOfInventingReady() {
        var f=new Fixture();var gate=f.startup.inspect(f.saved,f.spec(ValidationMode.INCREMENTAL));
        f.evidence=new ValidationInitialLoadState.Status(ValidationInitialLoadState.State.UNAVAILABLE,"initial.lookup-unknown");
        assertThat(gate.current().get()).isEqualTo(ValidationRunLifecycle.Readiness.UNKNOWN);
    }
    private static final class Fixture {
        final Engine engine=mock(Engine.class);final ValidationPipelineInitials initials=mock(ValidationPipelineInitials.class);
        final MongoConfigurationDomain configuration=mock(MongoConfigurationDomain.class,RETURNS_DEEP_STUBS);
        final MongoCacheDomain cache=mock(MongoCacheDomain.class);final ValidationSavedDeclarations saved=mock(ValidationSavedDeclarations.class);
        final PipelineIntentScope scope=new PipelineIntentScope("cluster",1,"cache","flow",1);
        final ExecutionFence fence=new ExecutionFence(scope,new WorkloadClaimFence(new WorkloadClaimKey("cluster",WorkloadClaimType.PIPELINE_ACTUATION,"flow"),new WorkloadOwner("node","boot"),1,1,1));
        final NativeValidationStartup startup=new NativeValidationStartup(engine,initials,configuration,cache,new CacheBusinessScope("cluster",1,"cache"),()->{});
        volatile JobStatus status=JobStatus.RUNNING;
        volatile ValidationInitialLoadState.Status evidence=new ValidationInitialLoadState.Status(ValidationInitialLoadState.State.READY,null);
        Fixture() {
            when(configuration.intents().guardedBy(any()).load("flow")).thenReturn(Optional.of(new PipelineIntentSnapshot(scope,1,new DesiredState("flow",PipelineState.RUNNING,"hash"))));
            when(configuration.preparations().read("flow")).thenReturn(Optional.of(new PipelinePreparation("flow",1,PipelinePreparation.Phase.UNASSIGNED,null,null,null)));
            var run=mock(Engine.Run.class);when(engine.captureRun("flow")).thenReturn(Optional.of(run));when(run.jobId()).thenReturn(41L);
            when(run.status()).thenAnswer(call->status);when(run.operatorManifest()).thenReturn(new OperatorRunManifest("flow",fence,List.of()));
            var inspection=mock(io.tapstate.core.dsl.ValidationDeclarations.Inspection.class);
            when(saved.mapping("mapping")).thenReturn(inspection);when(inspection.sync()).thenReturn(new SyncElement("out","target",null,null,null));
            when(initials.read(eq(fence),anySet())).thenAnswer(call->evidence);
        }
        ValidationRunSpec spec(ValidationMode mode) {
            var keys=List.of(new ValidationKeyBinding.Component(List.of("id"),ValidationKey.Kind.EXACT_NUMBER,Set.of("exact-number")));
            var comparison=new ValidationTaskSpec("flow",Map.of("flow","a".repeat(64),"input","b".repeat(64),"target","c".repeat(64)),1,"rules",
                    List.of(new ValidationTaskSpec.Mapping("mapping",ValidationTaskSpec.OutputKind.SYNC,"out",
                            new ValidationTaskSpec.Table("input","input","db",null,"orders","model","codec"),new ValidationTaskSpec.Table("out","target","db",null,"orders","model","codec"),
                            new ValidationTaskSpec.Keys("confirmed",keys,keys))));
            return new ValidationRunSpec(comparison,mode,mode==ValidationMode.FULL?ValidationMethod.COUNT:ValidationMethod.FULL_COLUMN);
        }
    }
}
