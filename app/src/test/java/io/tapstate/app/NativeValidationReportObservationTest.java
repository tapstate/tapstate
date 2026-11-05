package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static io.tapstate.app.NativeValidationCommandsIT.ReportObservation;
import static org.assertj.core.api.Assertions.*;

class NativeValidationReportObservationTest {
    @Test void aMetadataRefusalAndAnUnsettledReportCanBeRetried()throws Exception{
        var calls=new AtomicInteger();var clock=new AtomicLong();
        int refusals=ReportObservation.await(()->{
            int attempt=calls.incrementAndGet();
            if(attempt==1)throw busy();
            return attempt==3;
        },clock::get,()->clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(20)));
        assertThat(refusals).isEqualTo(1);assertThat(calls).hasValue(3);
    }

    @Test void aSuccessfulObservationIsNotReadAgain()throws Exception{
        var calls=new AtomicInteger();
        assertThat(ReportObservation.await(()->{calls.incrementAndGet();return true;},()->0,()->{
            throw new AssertionError("A successful observation must return immediately");
        })).isZero();
        assertThat(calls).hasValue(1);
    }

    @Test void allOtherFailuresEscapeWithoutRetry(){
        for(var failure:List.of(
                new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.processEnvelope"),null),
                new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.cooperativeHeadroom"),null),
                new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of(),null),
                new TapstateException(ValidationError.INVALID_CONTRACT,Map.of("field","observation"),null),
                new IllegalStateException("Unexpected observation failure"))){
            var calls=new AtomicInteger();
            assertThatThrownBy(()->ReportObservation.await(()->{calls.incrementAndGet();throw failure;},()->0,()->{
                throw new AssertionError("An unknown failure must not be retried");
            })).isSameAs(failure);
            assertThat(calls).hasValue(1);
        }
        var fatal=new AssertionError("Fatal observation failure");
        assertThatThrownBy(()->ReportObservation.await(()->{throw fatal;},()->0,()->{})).isSameAs(fatal);
    }

    @Test void persistentMetadataContentionDoesNotResetTheDeadline(){
        var calls=new AtomicInteger();var clock=new AtomicLong();
        assertThatThrownBy(()->ReportObservation.await(()->{calls.incrementAndGet();throw busy();},clock::get,
                ()->clock.addAndGet(TimeUnit.SECONDS.toNanos(10))))
                .isInstanceOf(AssertionError.class).hasMessage("Report observation did not succeed within 30 seconds");
        assertThat(calls).hasValue(3);assertThat(clock).hasValue(TimeUnit.SECONDS.toNanos(30));
    }

    @Test void anUnsettledReportFailsAtTheSameDeadline(){
        var calls=new AtomicInteger();var clock=new AtomicLong();
        assertThatThrownBy(()->ReportObservation.await(()->{calls.incrementAndGet();return false;},clock::get,
                ()->clock.addAndGet(TimeUnit.SECONDS.toNanos(30))))
                .isInstanceOf(AssertionError.class).hasMessage("Report observation did not succeed within 30 seconds");
        assertThat(calls).hasValue(1);
    }

    @Test void interruptionEscapesWithoutAnotherObservation(){
        var interrupted=new InterruptedException("Observation interrupted");var calls=new AtomicInteger();
        assertThatThrownBy(()->ReportObservation.await(()->{calls.incrementAndGet();return false;},()->0,()->{throw interrupted;}))
                .isSameAs(interrupted);
        assertThat(calls).hasValue(1);
    }

    private static TapstateException busy(){
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,Map.of("limit","validation.metadataFrames"),null);
    }
}
