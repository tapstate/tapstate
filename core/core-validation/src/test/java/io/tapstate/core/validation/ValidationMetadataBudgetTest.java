package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationMetadataBudgetTest {
    @Test void fourMixedLeasesShareOneFixedPoolAndForeignOrReturnedOwnersNeverValidate(){
        var budget=new ValidationMetadataBudget();var owner=new Object();var leases=new ArrayList<ValidationMetadataBudget.Lease>();
        try{
            for(int index=0;index<4;index++)leases.add(budget.acquire(index==0?owner:new Object()));
            assertThat(budget.inUse()).isEqualTo(4);assertThatThrownBy(()->budget.acquire(new Object())).isInstanceOf(TapstateException.class);
            leases.getFirst().requireRetained(owner);assertThatThrownBy(()->leases.getFirst().requireRetained(new Object())).isInstanceOf(TapstateException.class);
            assertThat(budget.inUse()).isEqualTo(4);leases.getFirst().close();assertThatThrownBy(()->leases.getFirst().requireRetained(owner)).isInstanceOf(TapstateException.class);
            try(var next=budget.acquire(new Object())){assertThat(budget.inUse()).isEqualTo(4);}
        }finally{leases.forEach(ValidationMetadataBudget.Lease::close);}
        assertThat(budget.inUse()).isZero();
    }
    @Test void maintenanceCannotConsumeTheFinalTwoFramesOrEnlargeThePool(){
        var budget=new ValidationMetadataBudget();var owner=new Object();
        try(var first=budget.acquireMaintenance(owner);var second=budget.acquireMaintenance(owner)){
            assertThat(budget.inUse()).isEqualTo(2);
            assertThatThrownBy(()->budget.acquireMaintenance(owner)).isInstanceOf(TapstateException.class);
            try(var response=budget.acquire(owner);var probe=budget.acquire(owner)){
                assertThat(budget.inUse()).isEqualTo(4);
                assertThatThrownBy(()->budget.acquire(owner)).isInstanceOf(TapstateException.class);
            }
            assertThat(budget.inUse()).isEqualTo(2);
        }
        assertThat(budget.inUse()).isZero();
    }
    @Test void retainedDefinitionsAndSharedQuotaLeaveOneControlFrame(){
        var budget=new ValidationMetadataBudget();var owner=new Object();
        try(var first=budget.acquireRetained(owner);var second=budget.acquireRetained(owner);var quota=budget.acquireRetained(owner)){
            assertThat(budget.inUse()).isEqualTo(3);
            assertThatThrownBy(()->budget.acquireRetained(owner)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->budget.acquireWork(owner)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->budget.acquireMaintenance(owner)).isInstanceOf(TapstateException.class);
            try(var response=budget.acquire(owner)){assertThat(budget.inUse()).isEqualTo(4);}
            first.close();first.close();
            try(var replacement=budget.acquireRetained(owner)){assertThat(budget.inUse()).isEqualTo(3);}
        }
        assertThat(budget.inUse()).isZero();
    }
    @Test void maintenanceCanDispatchACompleteRootBundleAndStillLeaveControl(){
        var budget=new ValidationMetadataBudget();var owner=new Object();
        try(var maintenance=budget.acquireMaintenance(owner)){
            var pair=budget.acquireRetainedBundle(owner,2);
            try{
                assertThat(budget.inUse()).isEqualTo(3);
                assertThatThrownBy(()->budget.acquireWork(owner)).isInstanceOf(TapstateException.class);
                try(var response=budget.acquire(owner)){assertThat(budget.inUse()).isEqualTo(4);}
            }finally{pair.forEach(ValidationMetadataBudget.Lease::close);}
        }
        assertThat(budget.inUse()).isZero();
    }
    @Test void completeRetainedBundlesRefuseWithoutLeavingAPartialLease(){
        var budget=new ValidationMetadataBudget();var owner=new Object();
        var pair=budget.acquireRetainedBundle(owner,2);
        try{
            assertThatThrownBy(()->budget.acquireRetainedBundle(owner,2)).isInstanceOf(TapstateException.class);
            assertThat(budget.inUse()).isEqualTo(2);
            try(var third=budget.acquireRetained(owner)){
                assertThat(budget.inUse()).isEqualTo(3);
                assertThatThrownBy(()->budget.acquireRetainedBundle(owner,2)).isInstanceOf(TapstateException.class);
                assertThat(budget.inUse()).isEqualTo(3);
            }
        }finally{pair.forEach(ValidationMetadataBudget.Lease::close);}
        assertThat(budget.inUse()).isZero();
    }
    @Test void unresolvedWorkCannotConsumeTheControlSlotOrEnlargeThePool(){
        var budget=new ValidationMetadataBudget();var owner=new Object();
        try(var first=budget.acquireRetained(owner);var second=budget.acquireRetained(owner);var probe=budget.acquireWork(owner)){
            assertThat(budget.inUse()).isEqualTo(3);
            assertThatThrownBy(()->budget.acquireWork(owner)).isInstanceOf(TapstateException.class);
            try(var control=budget.acquire(owner)){assertThat(budget.inUse()).isEqualTo(4);}
        }
        assertThat(budget.inUse()).isZero();
    }
    @Test void simultaneousMaintenanceNeverCrowdsOutTwoForegroundCallers()throws Exception{
        var budget=new ValidationMetadataBudget();var barrier=new CyclicBarrier(5);
        try(var callers=Executors.newFixedThreadPool(5)){
            var tasks=new ArrayList<Future<?>>();
            for(int actor=0;actor<5;actor++){
                final boolean maintenance=actor<3;
                tasks.add(callers.submit(()->{
                    for(int attempt=0;attempt<100;attempt++){
                        barrier.await(2,TimeUnit.SECONDS);
                        ValidationMetadataBudget.Lease lease=null;
                        try{
                            try{lease=maintenance?budget.acquireMaintenance(callers):budget.acquire(callers);}
                            catch(TapstateException refused){if(!maintenance)throw refused;}
                            if(lease!=null)lease.requireRetained(callers);
                            assertThat(budget.inUse()).isLessThanOrEqualTo(4);
                            barrier.await(2,TimeUnit.SECONDS);
                        }finally{if(lease!=null)lease.close();}
                        barrier.await(2,TimeUnit.SECONDS);
                    }
                    return null;
                }));
            }
            for(var task:tasks)task.get(10,TimeUnit.SECONDS);
        }
        assertThat(budget.inUse()).isZero();
        var held=new ArrayList<ValidationMetadataBudget.Lease>();
        try{for(int i=0;i<4;i++)held.add(budget.acquire(new Object()));}
        finally{held.forEach(ValidationMetadataBudget.Lease::close);}
        assertThat(budget.inUse()).isZero();
    }
    @Test void onePendingBundleSurvivesDiscoveryAndPollingWithoutWaitingForItsOwnControl()throws Exception{
        var budget=new ValidationMetadataBudget();var owner=new Object();
        try(var start=budget.acquire(owner);var maintenance=budget.acquireMaintenance(owner);var progress=budget.acquire(owner)){
            var result=new FutureTask<>(()->budget.acquireRetainedBundleAfterControls(owner,2,TimeUnit.SECONDS.toNanos(3),()->{}));
            var waiter=Thread.ofPlatform().daemon().start(result);
            try{
                awaitMetadataWait(waiter);assertThat(budget.inUse()).isEqualTo(3);
                assertThatThrownBy(()->budget.acquireMaintenance(owner)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->budget.acquireWork(owner)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->budget.acquire(owner)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->budget.acquireRetainedBundleAfterControls(owner,2,TimeUnit.SECONDS.toNanos(3),()->{})).isInstanceOf(TapstateException.class);
                maintenance.close();var pair=result.get(2,TimeUnit.SECONDS);
                try{assertThat(budget.inUse()).isEqualTo(4);start.requireRetained(owner);progress.requireRetained(owner);}
                finally{pair.forEach(ValidationMetadataBudget.Lease::close);}
            }finally{waiter.interrupt();waiter.join(2000);}
        }
        assertFourControlsFit(budget);
    }
    @Test @org.junit.jupiter.api.Timeout(3) void genuineRetainedAndNativeWorkContentionNeverEntersTheTransientWait(){
        for(boolean retained:List.of(true,false)){
            var budget=new ValidationMetadataBudget();var owner=new Object();
            try(var work=retained?budget.acquireRetained(owner):budget.acquireWork(owner);var first=budget.acquire(owner);var second=budget.acquire(owner)){
                assertThatThrownBy(()->budget.acquireRetainedBundleAfterControls(owner,2,TimeUnit.SECONDS.toNanos(30),()->{})).isInstanceOf(TapstateException.class);
                assertThat(budget.inUse()).isEqualTo(3);
            }
            assertFourControlsFit(budget);
        }
    }
    @Test void timeoutInterruptionAndContextFailureRemoveOnlyThePendingIntention()throws Exception{
        for(String ending:List.of("timeout","interrupt","context")){
            var budget=new ValidationMetadataBudget();var owner=new Object();var active=new java.util.concurrent.atomic.AtomicBoolean(true);
            var gone=new IllegalStateException("original context retired");
            try(var first=budget.acquire(owner);var second=budget.acquire(owner);var third=budget.acquire(owner)){
                var result=new FutureTask<>(()->budget.acquireRetainedBundleAfterControls(owner,2,TimeUnit.MILLISECONDS.toNanos(500),()->{if(!active.get())throw gone;}));
                var waiter=Thread.ofPlatform().daemon().start(result);
                try{
                    awaitMetadataWait(waiter);
                    if(ending.equals("interrupt"))waiter.interrupt();
                    if(ending.equals("context")){active.set(false);budget.wakeWaiters();}
                    var failed=catchThrowable(()->result.get(2,TimeUnit.SECONDS));assertThat(failed).isInstanceOf(ExecutionException.class);
                    assertThat(failed.getCause()).isInstanceOf(ending.equals("timeout")?TapstateException.class:ending.equals("interrupt")?InterruptedException.class:IllegalStateException.class);
                    if(ending.equals("context"))assertThat(failed.getCause()).isSameAs(gone);
                    assertThat(budget.inUse()).isEqualTo(3);try(var fourth=budget.acquire(owner)){assertThat(budget.inUse()).isEqualTo(4);}
                }finally{waiter.interrupt();waiter.join(2000);}
            }
            assertFourControlsFit(budget);
        }
    }
    @Test void retirementBetweenTheExternalGuardAndMonitorEntryCannotLoseItsWakeup()throws Exception{
        var budget=new ValidationMetadataBudget();var owner=new Object();var active=new java.util.concurrent.atomic.AtomicBoolean(true);
        var first=new java.util.concurrent.atomic.AtomicBoolean(true);var checked=new CountDownLatch(1);var resume=new CountDownLatch(1);
        var gone=new IllegalStateException("context retired before waiting");
        try(var one=budget.acquire(owner);var two=budget.acquire(owner);var three=budget.acquire(owner)){
            var result=new FutureTask<>(()->budget.acquireRetainedBundleAfterControls(owner,2,TimeUnit.SECONDS.toNanos(10),()->{
                boolean live=active.get();if(first.compareAndSet(true,false)){checked.countDown();try{assertThat(resume.await(5,TimeUnit.SECONDS)).isTrue();}
                    catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}}
                if(!live)throw gone;
            }));
            var waiter=Thread.ofPlatform().daemon().start(result);
            try{
                assertThat(checked.await(2,TimeUnit.SECONDS)).isTrue();active.set(false);budget.wakeWaiters();resume.countDown();
                var failed=catchThrowable(()->result.get(3,TimeUnit.SECONDS));assertThat(failed).isInstanceOf(ExecutionException.class);assertThat(failed.getCause()).isSameAs(gone);
                assertThat(budget.inUse()).isEqualTo(3);try(var fourth=budget.acquire(owner)){assertThat(budget.inUse()).isEqualTo(4);}
            }finally{resume.countDown();waiter.interrupt();waiter.join(2000);}
        }
        assertFourControlsFit(budget);
    }
    private static void awaitMetadataWait(Thread waiter)throws Exception{
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);
        while(waiter.getState()!=Thread.State.TIMED_WAITING&&waiter.isAlive()&&System.nanoTime()-deadline<0)TimeUnit.MILLISECONDS.sleep(1);
        assertThat(waiter.getState()).isEqualTo(Thread.State.TIMED_WAITING);
    }
    private static void assertFourControlsFit(ValidationMetadataBudget budget){
        var leases=new ArrayList<ValidationMetadataBudget.Lease>();try{for(int index=0;index<4;index++)leases.add(budget.acquire(new Object()));assertThat(budget.inUse()).isEqualTo(4);}
        finally{leases.forEach(ValidationMetadataBudget.Lease::close);}assertThat(budget.inUse()).isZero();
    }
    @Test void concurrentDoubleCloseReturnsExactlyOneSlot()throws Exception{
        var budget=new ValidationMetadataBudget();var lease=budget.acquire(new Object());
        try(var callers=Executors.newFixedThreadPool(4)){
            var tasks=new ArrayList<Future<?>>();for(int index=0;index<32;index++)tasks.add(callers.submit(lease::close));
            for(var task:tasks)task.get(2,TimeUnit.SECONDS);
        }
        assertThat(budget.inUse()).isZero();var held=new ArrayList<ValidationMetadataBudget.Lease>();
        try{for(int index=0;index<4;index++)held.add(budget.acquire(new Object()));assertThatThrownBy(()->budget.acquire(new Object())).isInstanceOf(TapstateException.class);}
        finally{held.forEach(ValidationMetadataBudget.Lease::close);}
    }
}
