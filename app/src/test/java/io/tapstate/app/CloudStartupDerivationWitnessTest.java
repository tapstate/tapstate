package io.tapstate.app;

import com.mongodb.MongoException;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.GenericApplicationContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudStartupDerivationWitnessTest {
    private static final String SENTINEL = "untrusted-connection-and-password-sentinel";
    private final List<Map<String, Object>> events = new ArrayList<>();
    private final CloudStartupDerivationWitness witness = new CloudStartupDerivationWitness(events::add);

    @Test
    void repeatedDerivationReturnsTheOriginalCapabilities() {
        var capabilities = new ConnectorCapabilities(Set.of("batch_read_function"));
        CapabilityDeriver observed = witness.observe((CapabilityDeriver) id -> capabilities);
        assertThat(observed.derive("mysql")).isSameAs(capabilities);
        assertThat(observed.derive("mysql")).isSameAs(capabilities);
        assertThat(events).extracting(event -> event.get("call")).containsExactly(1, 2);
        assertThat(events).allSatisfy(event -> assertThat(event).containsEntry("outcome", "ok"));
    }

    @Test
    void aCodedRefusalRetainsIdentityAndOnlySafeFields() {
        TapstateException failure = coded();
        CapabilityDeriver observed = witness.observe((CapabilityDeriver) id -> { throw failure; });
        assertThatThrownBy(() -> observed.derive("mysql")).isSameAs(failure);
        assertThat(events).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("phase", "derive")
                .containsEntry("code", ConnectorError.LOAD_FAILED.code())
                .containsEntry("driver_code", 112));
        assertThat(JsonWriter.write(events)).doesNotContain(SENTINEL);
    }

    @Test
    void aProvisioningRefusalKeepsItsMoreSpecificStage() {
        TapstateException failure = coded();
        ConnectorProvisioner provisioner = witness.observe((ConnectorProvisioner) id -> { throw failure; });
        CapabilityDeriver observed = witness.observe((CapabilityDeriver) id -> {
            provisioner.resolve(id);
            throw new AssertionError("the failing provisioner returned");
        });
        assertThatThrownBy(() -> observed.derive("mysql")).isSameAs(failure);
        assertThat(events).singleElement().satisfies(event -> assertThat(event).containsEntry("phase", "provision"));
        assertThat(JsonWriter.write(events)).doesNotContain(SENTINEL);
    }

    @Test
    void anIoRefusalDoesNotPublishItsMessageOrBecomeACodedError() {
        var failure = new UncheckedIOException(new IOException(SENTINEL));
        CapabilityDeriver observed = witness.observe((CapabilityDeriver) id -> { throw failure; });
        assertThatThrownBy(() -> observed.derive("mysql")).isSameAs(failure);
        assertThat(events).singleElement().satisfies(event -> assertThat(event).doesNotContainKey("code"));
        assertThat(JsonWriter.write(events)).doesNotContain(SENTINEL);
    }

    @Test
    void nonMysqlDerivationHasNoObserver() {
        TapstateException failure = coded();
        CapabilityDeriver observed = witness.observe((CapabilityDeriver) id -> { throw failure; });
        assertThatThrownBy(() -> observed.derive("oracle")).isSameAs(failure);
        assertThat(events).isEmpty();
    }

    @Test
    void aNonMysqlProvisionInsideADerivationIsNotObservedAsAMysqlProvision() {
        TapstateException failure = coded();
        ConnectorProvisioner provisioner = witness.observe((ConnectorProvisioner) id -> { throw failure; });
        CapabilityDeriver observed = witness.observe((CapabilityDeriver) id -> {
            provisioner.resolve("oracle");
            throw new AssertionError("the failing provisioner returned");
        });
        assertThatThrownBy(() -> observed.derive("mysql")).isSameAs(failure);
        assertThat(events).singleElement().satisfies(event -> assertThat(event).containsEntry("phase", "derive"));
    }

    @Test
    void programmerFailuresStayBareAndClearTheAttempt() {
        var failure = new IllegalStateException(SENTINEL);
        CapabilityDeriver observed = witness.observe((CapabilityDeriver) id -> { throw failure; });
        assertThatThrownBy(() -> observed.derive("mysql")).isSameAs(failure);
        ConnectorProvisioner provisioner = witness.observe((ConnectorProvisioner) id -> { throw coded(); });
        assertThatThrownBy(() -> provisioner.resolve("mysql")).isInstanceOf(TapstateException.class);
        assertThat(events).isEmpty();
    }

    @Test
    void theProvisionersOwnCacheIdentityIsUnchanged() {
        ConnectorProvisioner observed = witness.observe(new ConnectorProvisioner() {
            @Override public ConnectorRef resolve(String id) { throw new AssertionError("not needed for identity"); }
            @Override public String cacheIdentity(String id) { return SENTINEL; }
        });
        assertThat(observed.cacheIdentity("mysql")).isEqualTo(SENTINEL);
        assertThat(events).isEmpty();
    }

    @Test
    void theInitializerObservesTheSameInterfaceBeansUsedByStartup() {
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(ConnectorProvisioner.class, () -> id -> { throw coded(); });
            context.registerBean(CapabilityDeriver.class, () -> id -> {
                context.getBean(ConnectorProvisioner.class).resolve(id);
                throw new AssertionError("the failing provisioner returned");
            });
            witness.initialize(context);
            context.refresh();
            assertThatThrownBy(() -> context.getBean(CapabilityDeriver.class).derive("mysql"))
                    .isInstanceOf(TapstateException.class);
            assertThat(events).singleElement().satisfies(event -> assertThat(event).containsEntry("phase", "provision"));
            assertThat(JsonWriter.write(events)).doesNotContain(SENTINEL);
        }
    }

    private static TapstateException coded() {
        return new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", SENTINEL),
                new MongoException(112, SENTINEL));
    }
}
