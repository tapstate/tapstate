package io.tapstate.app;

import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.runtime.engine.validation.ValidationProblemQuota;
import io.tapstate.spi.store.ValidationClaims;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Composition of local tenure expiry and quota sealing, not durable report handover or a real cluster. */
class ValidationOwnerQuotaTest {
    @Test void expirySealsAllLocalRunPublicationsBeforeAnyOldCallbackCanResume() {
        var ttl = Duration.ofSeconds(30);
        var store = new InMemoryWorkloadClaimStore();
        var owner = new WorkloadOwner("node-a", "boot-a");
        WorkloadClaim claim = store.acquire(ValidationClaims.key("cluster", "pipeline"), owner, 1, ttl).claim();
        claim = store.advanceExecution(claim, 1, Set.of(owner.nodeId())).orElseThrow();
        var productionKey = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "pipeline");
        var production = store.acquire(productionKey, owner, 1, Duration.ofMinutes(5)).claim();
        var time = new AtomicLong();
        var quotaReference = new AtomicReference<ValidationProblemQuota>();
        var cancellations = new AtomicInteger();
        var publications = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("pipeline", claim, 0, ttl, time::get, () -> true,
                previous -> store.renew(previous, ttl), store::release, reason -> {
                    quotaReference.get().close();
                    cancellations.incrementAndGet();
                })) {
            var quota = new ValidationProblemQuota(lease.owner(), 1, 4, 1_000_000, lease::require);
            quotaReference.set(quota);
            var full = new ValidationRunIdentity(lease.owner(), "task", "full", ValidationMode.FULL, "spec", 1, "rules");
            var inc = new ValidationRunIdentity(lease.owner(), "task", "inc", ValidationMode.INCREMENTAL, "spec", 1, "rules");
            quota.join(full, () -> { });
            quota.join(inc, () -> { });
            var root = new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(1))), List.of());
            var first = quota.reserve(full, root).permit().orElseThrow();
            var second = quota.reserve(inc, root).permit().orElseThrow();
            assertThat(quota.publish(first, publications::incrementAndGet)).isTrue();
            store.elapse(ttl.plusMillis(1));
            time.set(ttl.toNanos() + 1);
            lease.checkDeadline();
            assertThat(quota.state()).isEqualTo(ValidationProblemQuota.State.CLOSED);
            assertThat(cancellations).hasValue(1);
            // Claim acquisition alone does not authorize a replacement validation job: the controller
            // must still seal old durable results before admitting the new owner's runs.
            var successor = store.acquire(ValidationClaims.key("cluster", "pipeline"),
                    new WorkloadOwner("node-b", "boot-b"), 1, ttl);
            assertThat(successor.acquired()).isTrue();
            lease.renew();
            assertThat(quota.publish(first, publications::incrementAndGet)).isFalse();
            assertThat(quota.publish(second, publications::incrementAndGet)).isFalse();
            assertThat(quota.reserve(full, root).permit()).isEmpty();
            assertThat(quota.reservedRoots()).isEqualTo(2);
            assertThat(publications).hasValue(1);
            assertThat(store.read(productionKey).orElseThrow().claim()).isEqualTo(production);
            assertThat(store.read(productionKey).orElseThrow().leased()).isTrue();
        }
        assertThat(cancellations).hasValue(1);
    }
}
