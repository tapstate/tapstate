package io.tapstate.adapters.mongostore;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoStructureProofTest {
    @Test void aPhysicalProofExpiresTenSecondsAfterVerificationStartedAndFailureDropsOtherProofs(){
        var now=new AtomicLong();var calls=new AtomicInteger();var proof=new MongoStructureProof(now::get);
        var srs=proof.check(MongoStructureProof.Kind.SRS,calls::incrementAndGet);
        var claims=proof.check(MongoStructureProof.Kind.CLAIMS,calls::incrementAndGet);
        srs.run();claims.run();assertThat(calls).hasValue(2);
        now.set(TimeUnit.SECONDS.toNanos(10)-1);srs.run();claims.run();assertThat(calls).hasValue(2);
        now.incrementAndGet();srs.run();assertThat(calls).hasValue(3);
        var failed=proof.check(MongoStructureProof.Kind.PIPELINE_STATE,()->{throw new IllegalStateException("metadata unavailable");});
        assertThatThrownBy(failed::run).isInstanceOf(IllegalStateException.class);
        srs.run();assertThat(calls).hasValue(4);
        MongoStructureProof.invalidate(srs);claims.run();assertThat(calls).hasValue(5);
    }
    @Test void aNewSrsLeaseCannotExtendAnOlderPhysicalProofAndInvalidationEndsItsTrust()throws Exception{
        var now=new AtomicLong(System.nanoTime());var proof=new MongoStructureProof(now::get);
        var physical=proof.check(MongoStructureProof.Kind.SRS,()->{});physical.run();
        now.addAndGet(TimeUnit.SECONDS.toNanos(9));
        var lease=MongoSrsLease.selected(physical);var before=lease.observe();
        var acquired=lease.proved(before,System.nanoTime(),30000);assertThat(acquired).isNotNull();assertThat(lease.stands(acquired)).isTrue();
        Thread.sleep(1100);assertThat(lease.stands(acquired)).isFalse();
        var next=lease.proved(lease.observe(),System.nanoTime(),30000);assertThat(lease.stands(next)).isTrue();
        physical.invalidate();assertThat(lease.stands(next)).isFalse();
        assertThat(lease.fork().enabled()).isEqualTo(lease.enabled());
    }
    @Test void aSlowCheckCannotPublishTenMoreSecondsOfTrustWhenItReturns(){
        var now=new AtomicLong();var calls=new AtomicInteger();var proof=new MongoStructureProof(now::get);
        var check=proof.check(MongoStructureProof.Kind.SRS,()->{calls.incrementAndGet();now.addAndGet(TimeUnit.SECONDS.toNanos(11));});
        check.run();check.run();assertThat(calls).hasValue(2);
    }
    @Test void refreshedPhysicalProofMustNotReviveAnInvalidatedLease(){
        var proof = new MongoStructureProof();
        var physical = proof.check(MongoStructureProof.Kind.SRS, () -> {});
        physical.run();
        var leases = MongoSrsLease.selected(physical);
        var lease = leases.proved(leases.observe(), System.nanoTime(), 30000);
        assertThat(leases.stands(lease)).isTrue();
        physical.invalidate();
        assertThat(leases.stands(lease)).isFalse();
        physical.run();
        assertThat(leases.stands(lease)).as("new proof must not revive the old lease").isFalse();
    }

    @Test void aBlockedCheckpointAuditMustNotBlockWarmClaimProof() throws Exception {
        var proof = new MongoStructureProof();
        var claims = proof.check(MongoStructureProof.Kind.CLAIMS, () -> {});
        claims.run();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var cold = proof.check(MongoStructureProof.Kind.CHECKPOINT_READ, () -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("probe not released");
            } catch (InterruptedException e) { throw new AssertionError(e); }
        });
        try (var workers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            var blocked = workers.submit(cold);
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                var warm = workers.submit(claims);
                warm.get(500, TimeUnit.MILLISECONDS);
            } finally {
                release.countDown();
                blocked.get(2, TimeUnit.SECONDS);
            }
        }
    }
    @Test void invalidationDuringVerificationPreventsPublishingTheProof(){
        var proof=new MongoStructureProof();var calls=new AtomicInteger();
        var check=proof.check(MongoStructureProof.Kind.SRS,()->{calls.incrementAndGet();proof.clear();});
        check.run();check.run();assertThat(calls).hasValue(2);assertThat(check.remainingNanos()).isZero();
    }
    @Test void aPhysicalInvalidationDuringReferenceValidationCannotPublishANewLease(){
        var proof=new MongoStructureProof();var physical=proof.check(MongoStructureProof.Kind.SRS,()->{});
        physical.run();var owner=MongoSrsLease.selected(physical);var before=owner.observe();long started=System.nanoTime();
        physical.invalidate();physical.run();
        assertThat(owner.proved(before,started,30000)).isNull();
    }
}
