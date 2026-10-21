package io.tapstate.app;

import io.tapstate.adapters.pdk.*;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Verifies pinning and trust-material lifetime, without claiming a real Mongo connector TLS handshake. */
class RegisteredMongoDefaultTargetTest {
    private static String ca() {
        try (var input = RegisteredMongoDefaultTargetTest.class.getResourceAsStream("/tls/ca.pem")) {
            return new String(java.util.Objects.requireNonNull(input).readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException failed) { throw new AssertionError(failed); }
    }
    private static final String SPEC = "{\"connector\":\"mongo-fixture\"}";
    private static final String ARTIFACT = "a".repeat(64);
    private static final class Fixture {
        final ConnectorRegistry registry = mock(ConnectorRegistry.class);
        final ConnectorCatalogView catalog = mock(ConnectorCatalogView.class);
        final ConnectorProvisioner provisioner = mock(ConnectorProvisioner.class);
        final AtomicReference<Map<String,Object>> observed = new AtomicReference<>();
        final ConnectorRef ref = new ConnectorRef(List.of(),"fixture.Mongo",null,null,SPEC,ARTIFACT,false);
        boolean fail;
        Fixture() {
            when(registry.findAll("mongodb")).thenReturn(List.of(new ConnectorRegistration("mongodb",ARTIFACT,null,RegistrationSource.SEED)));
            when(catalog.detail("mongodb")).thenReturn(new ConnectorDetail("mongodb","Mongo",null,null,List.of(),null,true,false,"registered",List.of(),SpecSource.of(CanonicalHash.ofText(SPEC),SPEC),true));
            when(provisioner.resolve("mongodb",ARTIFACT)).thenReturn(ref);
        }
        RegisteredMongoDefaultTarget target() {
            return new RegisteredMongoDefaultTarget(registry,catalog,provisioner,pinned -> config -> {
                assertThat(pinned.resolve(config.connectorId())).isSameAs(ref); observed.set(config.settings());
                if (config.settings().containsKey("sslCA")) {
                    try { assertThat(Files.readString(Path.of((String) config.settings().get("sslCA")))).isEqualTo(ca()); }
                    catch (java.io.IOException missing) { throw new AssertionError(missing); }
                }
                if (fail) throw new TapstateException(ConnectorError.TEST_FAILED,Map.of("connector","mongodb","detail","sentinel-driver-secret"),null);
                return new ConnectionTestResult(config.id(),config.connectorId(),ConnectionTestResult.Outcome.PASSED,
                        List.of(new ConnectionTestItem("connect",ConnectionTestItem.Status.PASSED,"ok",null,null,null)),0);
            });
        }
        StorageBinding.Connection master(String ca) { return new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,"mongodb://user:sentinel-password@host/master?tls=true","master",ca); }
    }
    @Test void theFormalTesterUsesThePinnedArtifactAndOnlyTemporaryCaPaths() {
        var f = new Fixture(); var target = f.target(); var master = f.master(ca()); var source = target.definition(master,"managed");
        assertThat(source.config()).containsEntry("tlsCaPem",ca()).doesNotContainKey("sslCA");
        target.test(master,source,target.identity(master));
        assertThat(f.observed.get()).containsEntry("ssl",true).containsEntry("sslValidate",true).doesNotContainKeys("tlsCaPem","sslKey","sslPass");
        assertThat(Path.of((String)f.observed.get().get("sslCA"))).doesNotExist();
        verify(provisioner(f)).resolve("mongodb",ARTIFACT);
    }
    private static ConnectorProvisioner provisioner(Fixture f) { return f.provisioner; }
    @Test void failedConnectorTestsStillCloseTheirOwnTrustMaterial() {
        var f = new Fixture(); f.fail = true; var target = f.target(); var master = f.master(ca());
        assertThatThrownBy(() -> target.test(master,target.definition(master,"managed"),target.identity(master))).isInstanceOf(TapstateException.class);
        assertThat(Path.of((String)f.observed.get().get("sslCA"))).doesNotExist();
    }
    @Test void aBundledRowOrDifferentSpecCannotStandInForTheActualRegisteredArtifact() {
        var f = new Fixture(); var target = f.target(); var master = f.master(null); var identity = target.identity(master);
        when(f.provisioner.resolve("mongodb",ARTIFACT)).thenReturn(new ConnectorRef(List.of(),"fixture.Mongo",null,null,"{}",ARTIFACT,false));
        assertThatThrownBy(() -> target.test(master,target.definition(master,"managed"),identity)).isInstanceOf(TapstateException.class);
        assertThat(f.observed.get()).isNull();
        when(f.registry.findAll("mongodb")).thenReturn(List.of());
        assertThatThrownBy(() -> target.identity(master)).isInstanceOf(TapstateException.class);
    }
}
