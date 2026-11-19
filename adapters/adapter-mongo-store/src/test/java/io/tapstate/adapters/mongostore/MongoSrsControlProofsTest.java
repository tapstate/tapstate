package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.CacheBusinessScope;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoSrsControlProofsTest {
    @Test void scopeAndReferenceSetsStayIsolatedAndPhysicalLossInvalidatesSharedProofs(){
        var now=new AtomicLong();var physical=new MongoStructureProof(now::get);var check=physical.check(MongoStructureProof.Kind.SRS,()->{});check.run();
        var owner=MongoSrsLease.selected(check);var lease=owner.proved(owner.observe(),System.nanoTime(),30000);var proofs=new MongoSrsControlProofs();
        var scope=new CacheBusinessScope("cluster",1,"cache");proofs.remember(scope,"chain","root",owner,lease,proofs.generation());proofs.remember(scope,"chain","root-and-pins",owner,lease,proofs.generation());
        assertThat(proofs.matches(scope,"chain","root")).isTrue();assertThat(proofs.matches(scope,"chain","root-and-pins")).isTrue();
        assertThat(proofs.matches(new CacheBusinessScope("cluster",1,"replacement"),"chain","root")).isFalse();
        assertThat(proofs.matches(scope,"other-chain","root")).isFalse();assertThat(proofs.matches(scope,"chain","changed-root")).isFalse();
        now.set(TimeUnit.SECONDS.toNanos(10));assertThat(proofs.matches(scope,"chain","root")).isFalse();
    }
    @Test void theBoundEvictsOldProofsAndLossOfTheirOriginalLeaseEndsReuse(){
        var owner=MongoSrsLease.selected();var lease=owner.proved(owner.observe(),System.nanoTime(),30000);
        var proofs=new MongoSrsControlProofs();var scope=new CacheBusinessScope("cluster",1,"cache");
        for(int i=0;i<256;i++)proofs.remember(scope,"chain-"+i,"root",owner,lease,proofs.generation());
        assertThat(proofs.matches(scope,"chain-0","root")).isTrue();
        proofs.remember(scope,"chain-256","root",owner,lease,proofs.generation());
        assertThat(proofs.matches(scope,"chain-0","root")).isTrue();
        assertThat(proofs.matches(scope,"chain-1","root")).isFalse();
        assertThat(proofs.matches(scope,"chain-256","root")).isTrue();
        owner.drop();assertThat(proofs.matches(scope,"chain-0","root")).isFalse();
        assertThat(proofs.matches(scope,"chain-256","root")).isFalse();
    }
    @org.junit.jupiter.api.Test void aReferenceCheckStartedBeforeInvalidationCannotPublish(){
        var proofs=new MongoSrsControlProofs();
        var owner=MongoSrsLease.selected();
        var lease=owner.proved(owner.observe(),System.nanoTime(),30000);
        var scope=new CacheBusinessScope("cluster",1,"cache");
        long started=proofs.generation();
        proofs.clear();
        proofs.remember(scope,"chain","root",owner,lease,started);
        assertThat(proofs.matches(scope,"chain","root")).isFalse();
    }
}
