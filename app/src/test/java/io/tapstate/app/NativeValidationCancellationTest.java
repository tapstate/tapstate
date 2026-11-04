package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NativeValidationCancellationTest {
    @Test void repeatedOriginalCancellationSignalsAreControlOnlyAfterAnExplicitStop(){
        var inner=cancel(new InterruptedException());var outer=cancel(null);outer.addSuppressed(inner);
        var wrapped=new CompletionException(outer);wrapped.addSuppressed(inner);
        var stopped=new ValidationFailureLatch();NativeValidationCancellation.record(stopped,wrapped,true);assertThat(stopped.get()).isNull();
        var spontaneous=new ValidationFailureLatch();NativeValidationCancellation.record(spontaneous,wrapped,false);assertThat(spontaneous.get()).isNotNull();
    }
    @Test void aGenuineCleanupFaultInsideAControlCauseBecomesTheDiagnosis(){
        var genuine=new TapstateException(ValidationError.READ_FAILED,Map.of("table","original-cleanup"),null);
        var interrupted=new InterruptedException();interrupted.addSuppressed(genuine);
        var result=new ValidationFailureLatch();NativeValidationCancellation.record(result,cancel(cancel(interrupted)),true);assertThat(result.get()).isSameAs(genuine);
    }
    @Test void fatalCleanupBehindACompanionCancellationStillDominates(){
        var ordinary=new IllegalStateException("ordinary cleanup");var fatal=new AssertionError("fatal cleanup");
        var inner=new CancellationException();inner.addSuppressed(ordinary);inner.addSuppressed(fatal);
        var outer=cancel(null);outer.addSuppressed(inner);var result=new ValidationFailureLatch();
        NativeValidationCancellation.record(result,outer,true);assertThat(result.get()).isSameAs(fatal);
    }
    @Test void anUnexpectedCancellationCauseIsNeverConvertedIntoNormalStop(){
        for(Throwable unexpected:java.util.List.of(new IllegalStateException("unexpected native cause"),new CompletionException((Throwable)null))){
            var original=cancel(unexpected);var result=new ValidationFailureLatch();
            NativeValidationCancellation.record(result,original,true);assertThat(result.get()).isSameAs(original);assertThat(result.get().getCause()).isSameAs(unexpected);
        }
    }
    @Test void cyclicNativeContextRemainsADiagnosisRatherThanRecursingOrClaimingCleanStop(){
        var first=cancel(null);var second=new CancellationException();first.addSuppressed(second);second.addSuppressed(first);var result=new ValidationFailureLatch();
        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(1),()->NativeValidationCancellation.record(result,first,true));
        assertThat(result.get()).isNotNull();
    }
    @Test void sharedCancellationContextsAreInspectedOnceRatherThanExpandingEveryPath(){
        var visits=new java.util.concurrent.atomic.AtomicInteger();var first=new CountingCancellation(visits);var second=new CountingCancellation(visits);int nodes=2;
        for(int level=0;level<12;level++){
            var a=new CountingCancellation(visits);var b=new CountingCancellation(visits);a.addSuppressed(first);a.addSuppressed(second);b.addSuppressed(first);b.addSuppressed(second);
            first=a;second=b;nodes+=2;
        }
        var root=new CountingCancellation(visits);root.addSuppressed(first);root.addSuppressed(second);nodes++;var result=new ValidationFailureLatch();
        NativeValidationCancellation.record(result,root,true);assertThat(result.get()).isNull();assertThat(visits.get()).as("Shared native contexts have a linear inspection bound").isLessThanOrEqualTo(nodes*8);
    }
    @Test void branchingCyclesAndOversizedContextsRemainBoundedDiagnoses(){
        var a=cancel(null);var b=cancel(null);var c=cancel(null);
        for(var value:java.util.List.of(a,b,c))for(var other:java.util.List.of(a,b,c))if(value!=other)value.addSuppressed(other);
        var cyclic=new ValidationFailureLatch();org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(1),()->NativeValidationCancellation.record(cyclic,a,true));
        assertThat(cyclic.get()).isNotNull();
        var wide=cancel(null);for(int index=0;index<300;index++)wide.addSuppressed(new CancellationException());var bounded=new ValidationFailureLatch();
        NativeValidationCancellation.record(bounded,wide,true);assertThat(bounded.get()).isNotNull();
    }
    private static final class CountingCancellation extends CancellationException {
        private static final long serialVersionUID=1L;
        private final java.util.concurrent.atomic.AtomicInteger visits;
        CountingCancellation(java.util.concurrent.atomic.AtomicInteger visits){this.visits=visits;}
        @Override public synchronized Throwable getCause(){visits.incrementAndGet();return null;}
    }
    private static TapstateException cancel(Throwable cause){return new TapstateException(ValidationError.READ_CANCELLED,Map.of("table","original"),cause);}
}
