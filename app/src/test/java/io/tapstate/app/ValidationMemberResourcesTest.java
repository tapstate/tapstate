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
    @Test void aMinimalWorkerReceiptPreservesFatalDiagnosisPriority()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            var workers=new CompletableFuture<Void>();var rows=new CompletableFuture<Void>();var attempts=new java.util.concurrent.atomic.AtomicInteger();
            probe.retain(()->{if(attempts.incrementAndGet()==1)throw new IllegalStateException("earlier metadata cleanup failure");
                entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("fatal worker fixture timeout");},workers.minimalCompletionStage(),rows);
            try{
                probe.close();assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();workers.completeExceptionally(new AssertionError("fatal metadata worker"));rows.complete(null);
                assertThat(member.snapshot().metadataFrames()).isOne();release.countDown();
                assertThatThrownBy(()->probe.released().toCompletableFuture().get(2,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                        .hasRootCauseInstanceOf(AssertionError.class).hasRootCauseMessage("fatal metadata worker");
            }finally{release.countDown();probe.close();}
        }finally{release.countDown();}
    }
    @Test void processShutdownKeepsOriginalProbeCleanupAliveUntilPhysicalDrain()throws Exception{
        var entered=new CountDownLatch(4);var release=new CountDownLatch(1);
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            var workers=new CompletableFuture<Void>();var rows=new CompletableFuture<Void>();var closed=new CountDownLatch(1);
            probe.retain(()->{closed.countDown();workers.complete(null);rows.complete(null);},workers,rows);
            Runnable blocked=()->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("shutdown cleanup saturation timeout");}catch(InterruptedException e){throw new AssertionError(e);}};
            for(int index=0;index<36;index++)process.cleanup(blocked);
            try{
                assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();process.close();
                assertThat(member.snapshot().metadataFrames()).isOne();assertThat(closed.getCount()).isOne();release.countDown();
                assertThat(closed.await(2,TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(()->probe.released().toCompletableFuture().get(2,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                        .hasRootCauseInstanceOf(RejectedExecutionException.class);
                assertThat(member.snapshot().processReserved().isEmpty()).isTrue();assertThat(member.snapshot().metadataFrames()).isZero();
                assertThatThrownBy(member::admitProbe).isInstanceOf(TapstateException.class);
            }finally{release.countDown();}
        }finally{release.countDown();}
    }
    @Test void cleanupFailureCannotPublishASuccessfulReleaseReceipt()throws Exception{
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            var workers=new CompletableFuture<Void>();var rows=new CompletableFuture<Void>();
            probe.retain(()->{workers.complete(null);rows.complete(null);throw new IllegalStateException("native close failed after receipts");},workers,rows);
            probe.close();
            assertThatThrownBy(()->probe.released().toCompletableFuture().get(2,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("native close failed after receipts");
            assertThat(member.snapshot().processReserved().isEmpty()).isTrue();assertThat(member.snapshot().metadataFrames()).isZero();
        }
    }
    @Test void aCleanupRetryNeedsItsOwnPhysicalExitReceipt()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            var workers=new CompletableFuture<Void>();var rows=new CompletableFuture<Void>();var attempts=new java.util.concurrent.atomic.AtomicInteger();
            probe.retain(()->{
                if(attempts.incrementAndGet()==1)throw new IllegalStateException("first metadata close failed");
                entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("cleanup retry timeout");
            },workers,rows);
            try{
                probe.close();assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();workers.complete(null);rows.complete(null);
                assertThat(member.snapshot().processReserved().isEmpty()).isFalse();assertThat(member.snapshot().metadataFrames()).isOne();
                release.countDown();assertThatThrownBy(()->probe.released().toCompletableFuture().get(2,TimeUnit.SECONDS))
                        .isInstanceOf(ExecutionException.class).hasRootCauseMessage("first metadata close failed");
                assertThat(member.snapshot().processReserved().isEmpty()).isTrue();assertThat(member.snapshot().metadataFrames()).isZero();
            }finally{release.countDown();probe.close();}
        }finally{release.countDown();}
    }
    @Test void failedMetadataWorkerReceiptStillProvesPhysicalExit()throws Exception{
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            var workers=new CompletableFuture<Void>();var rows=new CompletableFuture<Void>();var closed=new CountDownLatch(1);
            probe.retain(closed::countDown,workers,rows);probe.close();assertThat(closed.await(2,TimeUnit.SECONDS)).isTrue();
            workers.completeExceptionally(new IllegalStateException("metadata query failed after worker exit"));
            assertThat(member.snapshot().processReserved().isEmpty()).isFalse();rows.complete(null);
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).until(()->member.snapshot().processReserved().isEmpty()&&member.snapshot().metadataFrames()==0);
            assertThat(member.snapshot().metadataFrames()).isZero();assertThat(workers.isCompletedExceptionally()).isTrue();
            assertThatThrownBy(()->probe.released().toCompletableFuture().get(2,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("metadata query failed after worker exit");
        }
    }
    @Test void rejectedMetadataCleanupRetriesTheOriginalAfterCapacityReturns()throws Exception{
        var entered=new CountDownLatch(4);var release=new CountDownLatch(1);
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            var workers=new CompletableFuture<Void>();var rows=new CompletableFuture<Void>();var closed=new CountDownLatch(1);
            probe.retain(()->{closed.countDown();workers.complete(null);rows.complete(null);},workers,rows);
            Runnable blocked=()->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("cleanup saturation timeout");}catch(InterruptedException e){throw new AssertionError(e);}};
            for(int index=0;index<36;index++)process.cleanup(blocked);
            try{
                assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();probe.close();
                assertThat(member.snapshot().metadataFrames()).isOne();assertThat(closed.getCount()).isOne();
                release.countDown();assertThat(closed.await(2,TimeUnit.SECONDS)).isTrue();
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).until(()->member.snapshot().processReserved().isEmpty()&&member.snapshot().metadataFrames()==0);
                assertThat(member.snapshot().metadataFrames()).isZero();
                assertThatThrownBy(()->probe.released().toCompletableFuture().get(2,TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                        .hasRootCauseInstanceOf(RejectedExecutionException.class);
            }finally{release.countDown();probe.close();}
        }finally{release.countDown();}
    }
    @Test void metadataProbeNeedsNoRunAndKeepsWorkerReceiptsAndBlockedCleanupCharged()throws Exception{
        var work=new CountDownLatch(1);var workRelease=new CountDownLatch(1);var cleanup=new CountDownLatch(1);var cleanupRelease=new CountDownLatch(1);
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            var workers=new CompletableFuture<Void>();var rows=new CompletableFuture<Void>();
            probe.retain(()->{cleanup.countDown();if(!cleanupRelease.await(5,TimeUnit.SECONDS))throw new AssertionError("metadata close timeout");},workers,rows);
            var actual=probe.queryExecutor().submit(()->{work.countDown();if(!workRelease.await(5,TimeUnit.SECONDS))throw new AssertionError("metadata query timeout");return null;});
            try{
                assertThat(work.await(2,TimeUnit.SECONDS)).isTrue();assertThat(member.snapshot().data().runs()).isZero();assertThat(member.snapshot().processors()).isZero();
                assertThat(member.snapshot().processReserved()).isEqualTo(new ValidationLocalBudget.Amount(16L*1024*1024,2,1,1));assertThat(member.snapshot().metadataFrames()).isOne();
                probe.close();assertThat(cleanup.await(2,TimeUnit.SECONDS)).isTrue();workers.complete(null);rows.complete(null);
                assertThat(member.snapshot().processReserved().isEmpty()).isFalse();workRelease.countDown();actual.get(2,TimeUnit.SECONDS);
                assertThat(member.snapshot().processReserved().isEmpty()).isFalse();assertThat(member.snapshot().metadataFrames()).isOne();
                cleanupRelease.countDown();org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).until(()->member.snapshot().processReserved().isEmpty()&&member.snapshot().metadataFrames()==0);
                assertThat(member.snapshot().metadataFrames()).isZero();assertThatThrownBy(probe::queryExecutor).isInstanceOf(TapstateException.class);
            }finally{workRelease.countDown();cleanupRelease.countDown();probe.close();}
        }finally{workRelease.countDown();cleanupRelease.countDown();}
    }
    @Test void cancellingMetadataFuturesDoesNotReleaseAnIgnoringNativeBodyOrPermitAThirdWorker()throws Exception{
        var entered=new CountDownLatch(2);var release=new CountDownLatch(1);
        try(var process=new ValidationMemberResources()){
            var member=process.bind("cluster","node","boot",4,()->{});var probe=member.admitProbe();
            Runnable blocked=()->{entered.countDown();while(release.getCount()!=0)try{release.await();}catch(InterruptedException ignored){/* Native calls can ignore an interruption. */}};
            var first=probe.queryExecutor().submit(blocked);var second=probe.decodeExecutor().submit(blocked);
            try{
                assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(()->probe.queryExecutor().submit(()->{})).isInstanceOf(TapstateException.class).hasMessageContaining("metadataWorkers");
                assertThat(first.cancel(true)).isTrue();assertThat(first.isDone()).isTrue();probe.close();
                assertThat(member.snapshot().processReserved().isEmpty()).isFalse();assertThat(member.snapshot().metadataFrames()).isOne();
                release.countDown();second.get(2,TimeUnit.SECONDS);
                org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).until(()->member.snapshot().processReserved().isEmpty()&&member.snapshot().metadataFrames()==0);assertThat(member.snapshot().metadataFrames()).isZero();
            }finally{release.countDown();probe.close();}
        }finally{release.countDown();}
    }
    @Test void replacingAMemberCannotReturnItsOldMetadataReadOrDropARejectedLifetime()throws Exception{
        try(var process=new ValidationMemberResources()){
            var old=process.bind("cluster","node","boot",4,()->{});var probe=old.admitProbe();var nativeExit=new CompletableFuture<Void>();var rowExit=new CompletableFuture<Void>();var closed=new CountDownLatch(1);
            probe.retain(closed::countDown,nativeExit,rowExit);old.close();
            var fresh=process.bind("cluster","node","boot",4,()->{});var next=fresh.admitProbe();
            assertThat(fresh.snapshot().data().runs()).isZero();assertThat(fresh.snapshot().processReserved().bytes()).isEqualTo(32L*1024*1024);assertThat(fresh.snapshot().metadataFrames()).isEqualTo(2);
            assertThatThrownBy(probe::queryExecutor).isInstanceOf(TapstateException.class);probe.close();assertThat(closed.await(2,TimeUnit.SECONDS)).isTrue();
            nativeExit.complete(null);assertThat(fresh.snapshot().processReserved().bytes()).isEqualTo(32L*1024*1024);
            rowExit.complete(null);org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(2)).until(()->fresh.snapshot().processReserved().bytes()==16L*1024*1024&&fresh.snapshot().metadataFrames()==1);
            next.close();assertThat(fresh.snapshot().processReserved().isEmpty()).isTrue();assertThat(fresh.snapshot().metadataFrames()).isZero();
        }
    }
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
