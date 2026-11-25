package io.tapstate.app;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AlertQuiescenceTest {
    @Test void timeoutRetainsAnEvaluationAndRetryDrainsItWithoutInterruptingTheTransaction() throws Exception {
        var fixture=new PipelineAlertRuntimeTest();
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var interrupted=new AtomicBoolean();
        when(fixture.fixture.store.evaluationRules(any(),anyInt())).thenAnswer(call->{
            entered.countDown();
            try {if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("evaluation was not released");}
            catch(InterruptedException stopped){interrupted.set(true);Thread.currentThread().interrupt();}
            return new io.tapstate.spi.store.AlertStore.DefinitionPage(java.util.List.of(),null);
        });
        var runtime=fixture.runtime();
        try {
            runtime.tick();assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
            runtime.close();
            assertThatThrownBy(()->runtime.awaitStopped(System.nanoTime()+Duration.ofMillis(50).toNanos()))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(runtime.health().queuedInputs()).isPositive();assertThat(interrupted).isFalse();
            release.countDown();runtime.awaitStopped(System.nanoTime()+Duration.ofSeconds(3).toNanos());
            assertThat(runtime.health().queuedInputs()).isZero();assertThat(interrupted).isFalse();
        }finally{release.countDown();runtime.close();}
    }

    @Test void lateServiceCreationIsIncludedInTheOwnersDrainAndCallerInterruptionDoesNotInterruptItsWorker() throws Exception {
        var fixture=new PipelineAlertRuntimeTest();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var planner=mock(AlertPlanningRuntime.class);
        var runtime=new PipelineAlertRuntime(()->{
            entered.countDown();
            try {if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("service creation was not released");}
            catch(InterruptedException failure){throw new AssertionError(failure);}
            return new PipelineAlertRuntime.Services(fixture.fixture.store,fixture.fixture.host,
                    io.tapstate.core.alerting.BuiltinAlarmTypes.catalog(),planner);
        },fixture.intentStore,(scope,claim)->Optional.of(()->{}),()->{},fixture.fixture.clock);
        try {
            runtime.tick();assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();runtime.close();
            Thread.currentThread().interrupt();
            try {assertThatThrownBy(()->runtime.awaitStopped(ShutdownAwait.deadline()))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);assertThat(Thread.currentThread().isInterrupted()).isTrue();}
            finally {Thread.interrupted();}
            release.countDown();runtime.awaitStopped(System.nanoTime()+Duration.ofSeconds(3).toNanos());
            verify(planner,never()).start();verify(planner).awaitStopped(anyLong());
        }finally{release.countDown();runtime.close();}
    }

    @Test void siblingPlannerCompletionIsRequiredAndNoCallerJoinsWhileHoldingItsMonitor() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var interrupted=new AtomicBoolean();
        var planner=mock(AlertNotificationPlanner.class);
        doAnswer(call->{entered.countDown();try{release.await(5,TimeUnit.SECONDS);}catch(InterruptedException failure){interrupted.set(true);}return null;})
                .when(planner).tick();
        var runtime=new AlertPlanningRuntime(planner);
        try(var executor=Executors.newFixedThreadPool(2)){
            runtime.start();assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();runtime.close();
            var first=executor.submit(()->runtime.awaitStopped(System.nanoTime()+Duration.ofSeconds(3).toNanos()));
            var second=executor.submit(()->runtime.awaitStopped(System.nanoTime()+Duration.ofSeconds(3).toNanos()));
            assertThat(first.isDone()).isFalse();release.countDown();
            first.get(3,TimeUnit.SECONDS);second.get(3,TimeUnit.SECONDS);assertThat(interrupted).isFalse();
        }finally{release.countDown();runtime.close();}
    }
}
