package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationLocalBudget;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;

class ValidationMemberResourcesTest {
    @Test void closeAfterPreparationRollsBackTheProcessChargeWhenMemberCommitRefuses()throws Exception{
        var entered=new CountDownLatch(1);var leave=new CountDownLatch(1);var done=new CountDownLatch(1);
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var control=new ValidationLocalBudget.Amount(65536,1,0,0);
            var run=member.admit(identity("growth-rollback"),control,0,()->{if(Thread.currentThread().getName().equals("rollback-growth")){entered.countDown();try{leave.await();}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}}});
            var held=run.budget().reserve(new ValidationLocalBudget.Amount(0,1,0,0));var failure=new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var growth=new Thread(()->{try{run.growTo(amount(64L*1024*1024,8),1);}catch(Throwable rejected){failure.set(rejected);}finally{done.countDown();}},"rollback-growth");growth.start();
            try{
                assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                synchronized(process){leave.countDown();long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);while(growth.getState()!=Thread.State.BLOCKED&&System.nanoTime()-until<0)Thread.onSpinWait();
                    assertThat(growth.getState()).isEqualTo(Thread.State.BLOCKED);run.close();}
                assertThat(done.await(2,TimeUnit.SECONDS)).isTrue();assertThat(failure.get()).isInstanceOf(TapstateException.class);
                assertThat(member.snapshot().processReserved()).isEqualTo(control);assertThat(run.budget().envelope()).isEqualTo(control);assertThat(member.snapshot().processors()).isZero();
            }finally{leave.countDown();growth.join(3000);held.close();run.processorsExited();run.close();}
            assertThat(member.snapshot().processReserved().isEmpty()).isTrue();
        }finally{leave.countDown();}
    }
    @Test void aBlockedGrowthGuardCannotHoldProcessorOrDataCleanup()throws Exception{
        var entered=new CountDownLatch(1);var leave=new CountDownLatch(1);
        try(var process=new ValidationMemberResources();var workers=Executors.newFixedThreadPool(2)){
            var member=process.bind("cluster","node","boot",4,()->{});
            var run=member.admit(identity("guard-cleanup"),new ValidationLocalBudget.Amount(65536,1,0,0),0,()->{
                if(Thread.currentThread().getName().equals("blocked-budget-growth")){entered.countDown();try{if(!leave.await(4,TimeUnit.SECONDS))throw new AssertionError("growth guard fixture timeout");}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}}
            });
            var growth=workers.submit(()->{Thread.currentThread().setName("blocked-budget-growth");run.growTo(amount(64L*1024*1024,8),1);});
            assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();var cleanup=workers.submit(()->{run.processorsExited();run.close();});
            try{assertThatCode(()->cleanup.get(500,TimeUnit.MILLISECONDS)).doesNotThrowAnyException();assertThat(member.snapshot().processReserved().isEmpty()).isTrue();}
            finally{leave.countDown();cleanup.get(3,TimeUnit.SECONDS);try{growth.get(3,TimeUnit.SECONDS);}catch(ExecutionException expectedClosed){assertThat(expectedClosed.getCause()).isInstanceOf(TapstateException.class);}}
        }finally{leave.countDown();}
    }
    @Test void aControlOnlyRunGrowsOnTheSameIdentityAndKeepsItsRealReadCharged()throws Exception{
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",8,()->{});
            var control=new ValidationLocalBudget.Amount(65536,1,0,0);var data=amount(64L*1024*1024,8);
            var run=member.admit(identity("waiting"),control,0,()->{});
            assertThat(member.snapshot().processors()).isZero();assertThat(member.snapshot().processReserved().connections()).isZero();
            run.growTo(data,2);run.growTo(data,2);
            assertThat(member.snapshot().processReserved()).isEqualTo(data);assertThat(run.budget().envelope()).isEqualTo(data);assertThat(member.snapshot().processors()).isEqualTo(2);
            var reader=run.budget().reserve(new ValidationLocalBudget.Amount(1024,0,1,1));run.close();run.processorsExited();
            assertThat(member.snapshot().processReserved()).isEqualTo(data);assertThat(member.snapshot().processors()).isZero();
            assertThatThrownBy(()->run.growTo(data,2)).isInstanceOf(TapstateException.class);
            reader.close();run.allReleased().toCompletableFuture().get(2,TimeUnit.SECONDS);assertThat(member.snapshot().processReserved().isEmpty()).isTrue();
        }
    }
    @Test void aFailedControlUpgradeDoesNotLoseEitherLayerOrItsOriginalOwner(){
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var live=new AtomicBoolean(true);
            var control=new ValidationLocalBudget.Amount(65536,1,0,0);var run=member.admit(identity("upgrade-refused"),control,0,()->{if(!live.get())throw new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED,Map.of("pipeline","flow"),null);});
            var before=member.snapshot();
            assertThatThrownBy(()->run.growTo(amount(64L*1024*1024,8),3)).isInstanceOf(TapstateException.class);
            assertThat(member.snapshot()).isEqualTo(before);live.set(false);
            assertThatThrownBy(()->run.growTo(amount(64L*1024*1024,8),1)).isInstanceOf(TapstateException.class);
            assertThat(member.snapshot()).isEqualTo(before);assertThat(run.budget().envelope()).isEqualTo(control);
            run.close();run.processorsExited();assertThat(member.snapshot().processReserved().isEmpty()).isTrue();
        }
    }
    @Test void aReleaseCallbackCannotHoldTheRunMonitorAndBlockItsOtherCleanup() throws Exception {
        var callback = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var process = new ValidationMemberResources(); var workers = Executors.newFixedThreadPool(2)) {
            var member = process.bind("cluster", "node", "boot", 4, () -> {});
            var run = member.admit(identity("release-callback"), amount(1024 * 1024, 1), 1, () -> {}); run.close();
            run.allReleased().thenRun(() -> {
                callback.countDown(); try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("release callback timeout"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            });
            var exiting = workers.submit(run::processorsExited);
            try {
                assertThat(callback.await(3, TimeUnit.SECONDS)).isTrue();
                var closing = workers.submit(run::close);
                assertThatCode(() -> closing.get(500, TimeUnit.MILLISECONDS)).doesNotThrowAnyException();
            } finally { release.countDown(); exiting.get(3, TimeUnit.SECONDS); }
        } finally { release.countDown(); }
    }
    @Test void anOwnerGuardErrorBeforeBindingReturnsTheUnusedEnvelopeAndProcessorSlot() {
        try (var process = new ValidationMemberResources()) {
            var member = process.bind("cluster", "node", "boot", 4, () -> {}); var fatal = new AssertionError("owner guard failed");
            assertThatThrownBy(() -> member.admit(identity("failed"), amount(64L * 1024 * 1024, 8), 1, () -> { throw fatal; })).isSameAs(fatal);
            assertThat(member.snapshot().processReserved().isEmpty()).isTrue(); assertThat(member.snapshot().processors()).isZero();
            assertThat(member.snapshot().data().runs()).isZero();
            var admitted = member.admit(identity("next"), amount(64L * 1024 * 1024, 8), 1, () -> {}); admitted.close(); admitted.processorsExited();
        }
    }
    @Test void localEnvelopePrecedesTheDurableOwnerAndBindsOnlyOnce() throws Exception {
        try (var process = new ValidationMemberResources()) {
            var member = process.bind("cluster", "node", "boot", 8, () -> {});
            var reserved = member.reserve(amount(64L * 1024 * 1024, 8), 1);
            assertThat(member.snapshot().data().runs()).isZero(); assertThat(member.snapshot().processReserved().bytes()).isEqualTo(64L * 1024 * 1024);
            var run = reserved.bind(identity("bound"), () -> {}); reserved.close();
            assertThat(member.snapshot().data().runs()).isOne();
            assertThatThrownBy(() -> reserved.bind(identity("other"), () -> {})).isInstanceOf(TapstateException.class);
            run.close(); run.processorsExited(); run.released().toCompletableFuture().get(2, TimeUnit.SECONDS);
            var cancelled = member.reserve(amount(64L * 1024 * 1024, 8), 1); cancelled.close();
            assertThatThrownBy(() -> cancelled.bind(identity("late"), () -> {})).isInstanceOf(TapstateException.class);
            assertThat(member.snapshot().processors()).isZero(); assertThat(member.snapshot().processReserved().isEmpty()).isTrue();
        }
    }
    @Test void replacingAContextNeverReturnsItsRetainedDataOrMetadataReservations() throws Exception {
        try (var process = new ValidationMemberResources()) {
            var original = process.bind("cluster", "node", "boot", 8, () -> {}); var metadata = original.admitMetadata();
            var old = original.admit(identity("old"), amount(64L * 1024 * 1024, 8), 1, () -> {});
            var nativeRead = old.budget().reserve(new ValidationLocalBudget.Amount(1024, 0, 1, 1));
            original.close(); old.close(); old.processorsExited();
            assertThat(old.released().toCompletableFuture()).isNotDone();
            var next = process.bind("cluster", "node", "boot", 8, () -> {});
            assertThat(next.snapshot().processReserved().bytes()).isEqualTo(64L * 1024 * 1024); assertThat(next.snapshot().metadataFrames()).isOne();
            var fresh = next.admit(identity("new"), amount(64L * 1024 * 1024, 8), 1, () -> {});
            assertThat(next.snapshot().processReserved().bytes()).isEqualTo(128L * 1024 * 1024);
            nativeRead.close(); old.released().toCompletableFuture().get(2, TimeUnit.SECONDS); metadata.close();
            assertThat(next.snapshot().processReserved().bytes()).isEqualTo(64L * 1024 * 1024); assertThat(next.snapshot().metadataFrames()).isZero();
            fresh.close(); fresh.released().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertThat(next.snapshot().processors()).isOne(); fresh.processorsExited();
            assertThat(next.snapshot().processors()).isZero(); assertThat(next.snapshot().processReserved().isEmpty()).isTrue();
        }
    }

    @Test void cancellingAQueuedFutureCannotFreeItsWrapperOrAnExecutingBody() throws Exception {
        var release = new CountDownLatch(1);
        try (var process = new ValidationMemberResources()) {
            var member = process.bind("cluster", "node", "boot", 8, () -> {});
            var run = member.admit(identity("workers"), amount(4L * 1024 * 1024, 32), 1, () -> {});
            var executor = run.queryExecutor(); var entered = new CountDownLatch(16); var futures = new ArrayList<Future<?>>();
            try {
                for (int index = 0; index < 16; index++) futures.add(executor.submit(() -> {
                    entered.countDown(); if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("worker gate timed out"); return null;
                }));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue(); var ran = new AtomicBoolean();
                var queued = executor.submit(() -> ran.set(true)); assertThat(queued.cancel(true)).isTrue();
                var cleanup = new AtomicBoolean(); process.cleanup(() -> cleanup.set(true)).toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(cleanup).isTrue(); run.close(); run.processorsExited();
                assertThat(run.released().toCompletableFuture()).isNotDone(); assertThat(run.budget().used().workers()).isEqualTo(17);
                assertThat(member.snapshot().processReserved().bytes()).isEqualTo(4L * 1024 * 1024);
                release.countDown(); for (var future : futures) future.get(5, TimeUnit.SECONDS);
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue(); run.released().toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertThat(ran).isFalse(); assertThat(member.snapshot().processReserved().isEmpty()).isTrue();
            } finally { release.countDown(); run.close(); run.processorsExited(); }
        } finally { release.countDown(); }
    }

    @Test void metadataAndProcessorLimitsDoNotResetWhenAMemberHandleCloses() {
        try (var process = new ValidationMemberResources()) {
            var old = process.bind("cluster", "node", "boot", 4, () -> {});
            var frames = new ArrayList<ValidationMemberResources.Metadata>(); for (int index = 0; index < 4; index++) frames.add(old.admitMetadata());
            old.close(); var current = process.bind("cluster", "node", "boot", 4, () -> {});
            assertThatThrownBy(current::admitMetadata).isInstanceOf(TapstateException.class).hasMessageContaining("metadataFrames");
            frames.removeFirst().close(); try (var reclaimed = current.admitMetadata()) { assertThat(reclaimed.reservedBytes()).isEqualTo(64L * 1024 * 1024); }
            frames.forEach(ValidationMemberResources.Metadata::close);
            var one = current.admit(identity("one"), amount(1024 * 1024, 1), 1, () -> {});
            var two = current.admit(identity("two"), amount(1024 * 1024, 1), 1, () -> {});
            assertThatThrownBy(() -> current.admit(identity("three"), amount(1024 * 1024, 1), 1, () -> {}))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("cooperativeHeadroom");
            one.close(); assertThatThrownBy(() -> current.admit(identity("three"), amount(1024 * 1024, 1), 1, () -> {})).isInstanceOf(TapstateException.class);
            one.processorsExited(); var three = current.admit(identity("three"), amount(1024 * 1024, 1), 1, () -> {});
            two.close(); two.processorsExited(); three.close(); three.processorsExited();
            assertThat(current.snapshot().processors()).isZero();
        }
    }

    @Test void changingNodeOrBootCannotCreateAnotherProcessBudget() throws Exception {
        try (var process = new ValidationMemberResources()) {
            var first = process.bind("cluster", "node", "boot", 8, () -> {});
            var old = first.admit(identity("old-budget"), amount(192L * 1024 * 1024, 8), 1, () -> {});
            var other = process.bind("cluster", "other", "other-boot", 8, () -> {});
            var identity = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "other", "other-boot", 1, 1, 1), "task", "new-budget", ValidationMode.FULL, "spec", 1, "rules");
            assertThatThrownBy(() -> other.admit(identity, amount(128L * 1024 * 1024, 8), 1, () -> {}))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("processEnvelope");
            old.close(); old.processorsExited(); old.released().toCompletableFuture().get(2, TimeUnit.SECONDS);
            var admitted = other.admit(identity, amount(128L * 1024 * 1024, 8), 1, () -> {}); admitted.close(); admitted.processorsExited();
        }
    }

    private static ValidationLocalBudget.Amount amount(long bytes, int workers) { return new ValidationLocalBudget.Amount(bytes, workers, 4, 4); }
    private static ValidationRunIdentity identity(String run) {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 1), "task", run, ValidationMode.FULL, "spec", 1, "rules");
    }
}
