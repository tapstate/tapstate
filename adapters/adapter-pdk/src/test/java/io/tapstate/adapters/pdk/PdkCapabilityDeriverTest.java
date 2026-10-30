package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The capability-derivation PDK bridge: {@link PdkCapabilityDeriver} reading back the capabilities a
 * connector's {@code registerCapabilities} declares, without initing it or opening a connection. An
 * un-loadable or level-incompatible connector is refused with a code up front. Synthetic connectors
 * compiled at test time prove the derivation and the coded-error path without a real connector jar or
 * the PDK runtime.
 */
class PdkCapabilityDeriverTest {
    @Test void exactArtifactDerivationRefusesAnotherHashWithoutOpeningAConnection(@TempDir Path dir) {
        var ref=new ConnectorRef(List.of(Synthetic.initHostileSource(dir)),"synthetic.InitHostile","2.0.8",null,null,"a".repeat(64),false);
        var derived=new PdkCapabilityDeriver(id -> ref);
        assertThat(derived.derive("demo","a".repeat(64)).capabilityIds()).containsExactly("batch_read_function");
        assertThatThrownBy(() -> derived.derive("demo","b".repeat(64))).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(ConnectorError.LOAD_FAILED));
    }

    /** A deriver over a provisioner that hands back one fixed connector ref, whatever id is asked for. */
    private static CapabilityDeriver deriver(Path jar, String className) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), className, "2.0.8", null);
        return new PdkCapabilityDeriver(connectorId -> ref);
    }

    @Test
    void derivesTheSourceCapabilitiesAConnectorRegisters(@TempDir Path dir) {
        // emittingSource registers batch read and stream read.
        CapabilityDeriver deriver = deriver(Synthetic.emittingSource(dir), "synthetic.EmittingSource");

        ConnectorCapabilities caps = deriver.derive("demo");

        assertThat(caps.capabilityIds())
                .containsExactly("batch_read_function", "stream_read_function");
    }

    @Test
    void derivesTheSinkCapabilityAConnectorRegisters(@TempDir Path dir) {
        // countingSink registers write record only.
        CapabilityDeriver deriver = deriver(Synthetic.countingSink(dir), "synthetic.CountingSink");

        assertThat(deriver.derive("demo").capabilityIds()).containsExactly("write_record_function");
    }

    @Test
    void aConnectorThatRegistersNothingHasNoCapabilities(@TempDir Path dir) {
        // passingTest registers no read/write functions.
        CapabilityDeriver deriver = deriver(Synthetic.passingTest(dir), "synthetic.PassingTest");

        assertThat(deriver.derive("demo").capabilityIds()).isEmpty();
    }

    @Test
    void derivesWithoutInitingOrConnecting(@TempDir Path dir) {
        // The connector's every lifecycle method throws; a successful derive proves none was called —
        // capability derivation reads the registered ids and must open no connection.
        CapabilityDeriver deriver = deriver(Synthetic.initHostileSource(dir), "synthetic.InitHostile");

        assertThat(deriver.derive("demo").capabilityIds()).containsExactly("batch_read_function");
    }

    @Test
    void aConnectorThatWillNotLoadIsACodedLoadFailure(@TempDir Path dir) {
        // ctorThrows throws from its constructor: the connector cannot be built.
        CapabilityDeriver deriver = deriver(Synthetic.ctorThrowsSource(dir), "synthetic.CtorThrows");

        assertThatThrownBy(() -> deriver.derive("demo"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code()).isEqualTo(ConnectorError.LOAD_FAILED));
    }

    @Test
    void anIncompatibleConnectorIsRefusedBeforeDerivation(@TempDir Path dir) {
        // A declared required level the bridge does not provide is refused up front, like the other ports.
        ConnectorRef ref = new ConnectorRef(
                List.of(Synthetic.emittingSource(dir)), "synthetic.EmittingSource", "2.0.8", "99");
        CapabilityDeriver deriver = new PdkCapabilityDeriver(connectorId -> ref);

        assertThatThrownBy(() -> deriver.derive("demo"))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code())
                        .isEqualTo(ConnectorError.API_LEVEL_INCOMPATIBLE));
    }
}
