package io.tapstate.control.core;

import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterIdentityServiceTest {

    @Test
    void issuerSurvivesAServiceRestartBecauseTheGeneratedIdentityIsPersisted() {
        MemoryStore persisted = new MemoryStore();

        ClusterIdentity first = new ClusterIdentityService(persisted, () -> "01J5FIRST").identity();
        ClusterIdentity afterRestart = new ClusterIdentityService(persisted, () -> "01J5SECOND").identity();

        assertThat(first).isEqualTo(new ClusterIdentity("01J5FIRST"));
        assertThat(afterRestart).isEqualTo(first);
        assertThat(afterRestart.issuer()).isEqualTo("urn:tapstate:cluster:01J5FIRST");
    }

    @Test
    void exposesAControlCoreProjectionWithoutLeakingTheStorageIdentity() {
        ClusterIdentityView view = new ClusterIdentityService(new MemoryStore(), () -> "01J5VIEW")
                .identityView();

        assertThat(view.clusterId()).isEqualTo("01J5VIEW");
        assertThat(view.issuer()).isEqualTo("urn:tapstate:cluster:01J5VIEW");
    }

    private static final class MemoryStore implements ClusterIdentityStore {
        private final AtomicReference<ClusterIdentity> identity = new AtomicReference<>();

        @Override
        public Optional<ClusterIdentity> find() {
            return Optional.ofNullable(identity.get());
        }

        @Override
        public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
            identity.compareAndSet(null, proposed);
            return identity.get();
        }
    }
}
