package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class CaptureExecutionAuthorizationTest {
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster",1,"cache");
    private static final PipelineIntentScope PIPELINE = new PipelineIntentScope("cluster",1,"cache","same-id",1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node","boot");
    private static final WorkloadClaim CAPTURE = claim(WorkloadClaimType.CAPTURE,OWNER,1,0,2);
    private static WorkloadClaim claim(WorkloadClaimType type,WorkloadOwner owner,long generation,long execution,long topology) {
        return new WorkloadClaim(new WorkloadClaimKey("cluster",type,"same-id"),owner,generation,execution,topology,Instant.EPOCH);
    }
    private final WorkloadClaimStore claims = mock(WorkloadClaimStore.class);
    private final AtomicLong nanos = new AtomicLong();
    private final AtomicBoolean active = new AtomicBoolean(true);
    private ExecutionAuthorization guard() { return ExecutionAuthorization.scoped(SCOPE,claims,Duration.ofHours(1),nanos::get,active::get); }
    private static void refused(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,e -> assertThat(e.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
    }

    @Test void captureAndPipelineWithTheSameResourceNameHaveIndependentProofs() {
        var pipeline = claim(WorkloadClaimType.PIPELINE_ACTUATION,OWNER,1,1,2);
        when(claims.read(CAPTURE.key())).thenReturn(Optional.of(new WorkloadClaimReading(CAPTURE,Duration.ofSeconds(5))));
        when(claims.readPipeline(PIPELINE)).thenReturn(Optional.of(new WorkloadClaimReading(pipeline,Duration.ofSeconds(5))));
        try (var guard = guard()) {
            assertThat(guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE))).isEqualTo(WorkloadClaimFence.from(CAPTURE));
            assertThat(guard.require(new ExecutionFence(PIPELINE,WorkloadClaimFence.from(pipeline)))).isEqualTo(WorkloadClaimFence.from(pipeline));
            assertThat(guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE))).isEqualTo(WorkloadClaimFence.from(CAPTURE));
            verify(claims,times(1)).read(CAPTURE.key());
            active.set(false); refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE)));
        }
    }
    @Test void aMatchingCaptureThatStoppedRenewingCannotOutliveItsLeaseAndAnUnreachableReadCannotExtendIt() {
        var count = new AtomicInteger();
        when(claims.read(CAPTURE.key())).thenAnswer(call -> {
            if (count.getAndIncrement() == 0) return Optional.of(new WorkloadClaimReading(CAPTURE,Duration.ofSeconds(5)));
            throw new TapstateException(IoError.CACHE_BUSINESS_FENCED,Map.of(),null);
        });
        try (var guard = guard()) {
            guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE)); nanos.set(Duration.ofSeconds(5).toNanos());
            refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE)));
            refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE))); assertThat(count).hasValue(2);
        }
    }
    @Test void aSlowCaptureReadCannotPublishAnAlreadySpentMonotonicProof() {
        when(claims.read(CAPTURE.key())).thenAnswer(call -> {
            nanos.set(Duration.ofSeconds(6).toNanos()); return Optional.of(new WorkloadClaimReading(CAPTURE,Duration.ofSeconds(5)));
        });
        try (var guard = guard()) { refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE))); }
    }
    @Test void movedOwnerGenerationTopologyOrMissingClaimCannotAuthorizeTheOriginalCapture() {
        for (var changed : List.of(claim(WorkloadClaimType.CAPTURE,new WorkloadOwner("other","boot"),1,0,2),
                claim(WorkloadClaimType.CAPTURE,OWNER,2,0,2),claim(WorkloadClaimType.CAPTURE,OWNER,1,0,3))) {
            when(claims.read(CAPTURE.key())).thenReturn(Optional.of(new WorkloadClaimReading(changed,Duration.ofSeconds(5))));
            try (var guard = guard()) { refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE))); }
        }
        when(claims.read(CAPTURE.key())).thenReturn(Optional.empty());
        try (var guard = guard()) { refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE))); }
    }
    @Test void aLegalTopologyChangeOrNewerExecutionRefreshesAPreheatedProofWithoutLettingTheOldFenceFollowIt() {
        for (var next : List.of(claim(WorkloadClaimType.CAPTURE,OWNER,1,0,0),claim(WorkloadClaimType.CAPTURE,OWNER,1,0,3),claim(WorkloadClaimType.CAPTURE,OWNER,1,1,2))) {
            var held = new AtomicReference<>(CAPTURE);
            when(claims.read(CAPTURE.key())).thenAnswer(call -> Optional.of(new WorkloadClaimReading(held.get(),Duration.ofSeconds(5))));
            try (var guard = guard()) {
                guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE)); held.set(next);
                assertThat(guard.requireCapture(SCOPE,WorkloadClaimFence.from(next))).isEqualTo(WorkloadClaimFence.from(next));
                refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE)));
            }
        }
    }

    @Test void foreignScopeAndPipelineAuthorityCannotOpenAnyCaptureRead() {
        try (var guard = guard()) {
            refused(() -> guard.requireCapture(new CacheBusinessScope("cluster",1,"later"),WorkloadClaimFence.from(CAPTURE)));
            refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(claim(WorkloadClaimType.PIPELINE_ACTUATION,OWNER,1,1,2))));
            verifyNoInteractions(claims);
        }
        try (var guard = ExecutionAuthorization.unfenced()) { refused(() -> guard.requireCapture(SCOPE,WorkloadClaimFence.from(CAPTURE))); }
    }
}
