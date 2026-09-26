package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.RegistrationSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The real connector provisioner: {@link RegistryConnectorProvisioner} resolves a connector id against
 * the distribution store, stages its registered bytes into a content-addressed on-disk cache
 * ({@code <hash>.jar}), and introspects the staged jar into the {@link ConnectorRef} the bridge loads
 * from. Staging is content-addressed, so a second resolve reuses the cached jar rather than fetching the
 * bytes again. An id that resolves to no artifact — or to more than one — is refused with a coded
 * connector-domain exception rather than loaded blindly. Synthetic connector jars compiled at test time
 * stand in for real connector dist jars; an in-memory registry stands in for the Mongo/GridFS store.
 */
class RegistryConnectorProvisionerTest {

    @Test
    void resolvesARegisteredConnectorToARefStagedInTheContentAddressedCache(@TempDir Path dir) throws IOException {
        Path cacheDir = dir.resolve("plugins");
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);

        ConnectorRef ref = new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir)
                .resolve("orders");

        assertThat(ref.className()).isEqualTo("synthetic.OrdersConnector");
        assertThat(ref.pdkApiVersion()).isEqualTo("1.3.5");
        assertThat(ref.requiredLevel()).isNull();
        assertThat(ref.classpath()).hasSize(1);
        Path staged = ref.classpath().get(0);
        assertThat(staged.getParent()).isEqualTo(cacheDir);
        assertThat(staged.getFileName().toString()).endsWith(".jar");
        assertThat(Files.readAllBytes(staged)).isEqualTo(artifact);
    }

    /**
     * An artifact certified for one instance to serve several writers resolves to a ref that says so and names
     * its bytes; an artifact with no certification, or another's, runs one instance per writer.
     */
    @Test
    void resolvesAnArtifactCertifiedForSharingToARefThatSharesIt(@TempDir Path dir) throws IOException {
        Path cacheDir = dir.resolve("plugins");
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        String hash = registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact)
                .registration().contentHash();
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        ConnectorRef uncertified = provisioner.resolve("orders");
        registry.shareSafe.put("some-other-bytes", "v1");
        ConnectorRef certifiedForOthers = provisioner.resolve("orders");
        registry.shareSafe.put(hash, "v1");
        ConnectorRef certified = provisioner.resolve("orders");

        assertThat(uncertified.shareSafe()).isFalse();
        assertThat(certifiedForOthers.shareSafe()).isFalse();
        assertThat(certified.shareSafe()).isTrue();
        assertThat(certified.contentHash()).isEqualTo(hash);
    }

    @Test
    void reusesTheStagedArtifactOnASecondResolveInsteadOfFetchingBytesAgain(@TempDir Path dir) throws IOException {
        Path cacheDir = dir.resolve("plugins");
        byte[] artifact = Files.readAllBytes(Synthetic.annotatedConnector(dir));
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED, artifact);
        RegistryConnectorProvisioner provisioner =
                new RegistryConnectorProvisioner(registry, new ConnectorIntrospector(), cacheDir);

        Path first = provisioner.resolve("orders").classpath().get(0);
        Path second = provisioner.resolve("orders").classpath().get(0);

        assertThat(second).isEqualTo(first);
        assertThat(registry.artifactCalls).isEqualTo(1);
    }

    @Test
    void refusesAConnectorIdWithNoRegisteredArtifact(@TempDir Path dir) {
        RegistryConnectorProvisioner provisioner = new RegistryConnectorProvisioner(
                new InMemoryConnectorRegistry(), new ConnectorIntrospector(), dir.resolve("plugins"));

        assertThatThrownBy(() -> provisioner.resolve("ghost"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException ce = (TapstateException) e;
                    assertThat(ce.code()).isEqualTo(ConnectorError.NOT_REGISTERED);
                    assertThat(ce.args()).containsEntry("connector", "ghost");
                });
    }

    @Test
    void refusesAConnectorIdThatResolvesToMoreThanOneRegisteredArtifact(@TempDir Path dir) throws IOException {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.register("orders", "1.3.5", RegistrationSource.SEED,
                Files.readAllBytes(Synthetic.annotatedConnector(dir)));
        registry.register("orders", "2.0.8", RegistrationSource.REGISTER,
                Files.readAllBytes(Synthetic.annotatedEmittingConnector(dir)));
        RegistryConnectorProvisioner provisioner = new RegistryConnectorProvisioner(
                registry, new ConnectorIntrospector(), dir.resolve("plugins"));

        assertThatThrownBy(() -> provisioner.resolve("orders"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> {
                    TapstateException ce = (TapstateException) e;
                    assertThat(ce.code()).isEqualTo(ConnectorError.AMBIGUOUS_REGISTRATION);
                    assertThat(ce.args()).containsEntry("connector", "orders").containsKey("artifacts");
                });
    }

    @Test
    void refusesARegistrationWhoseArtifactBytesAreMissing(@TempDir Path dir) {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        registry.addDanglingRegistration("orders", "0000feedface", "1.3.5");
        RegistryConnectorProvisioner provisioner = new RegistryConnectorProvisioner(
                registry, new ConnectorIntrospector(), dir.resolve("plugins"));

        assertThatThrownBy(() -> provisioner.resolve("orders"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(ConnectorError.LOAD_FAILED));
    }

    @Test
    void requiresItsCollaborators(@TempDir Path dir) {
        InMemoryConnectorRegistry registry = new InMemoryConnectorRegistry();
        ConnectorIntrospector introspector = new ConnectorIntrospector();
        Path cacheDir = dir.resolve("plugins");

        assertThatNullPointerException().isThrownBy(
                () -> new RegistryConnectorProvisioner(null, introspector, cacheDir));
        assertThatNullPointerException().isThrownBy(
                () -> new RegistryConnectorProvisioner(registry, null, cacheDir));
        assertThatNullPointerException().isThrownBy(
                () -> new RegistryConnectorProvisioner(registry, introspector, null));
    }

}
