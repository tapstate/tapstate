package io.tapstate.app;

import com.mongodb.MongoException;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.SeedConnectorSweep;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.context.support.GenericApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudSeedOutcomeWitnessTest {
    private static final String HASH = "a".repeat(64);
    private static final String PDK = "2.0.5-SNAPSHOT";
    private static final String SENTINEL = "untrusted-path-connection-and-password-sentinel";
    @TempDir Path temporary;
    private final List<Map<String, Object>> events = new ArrayList<>();

    @Test
    void reportsTypedFailureWithoutReadingItsMessageOrArgumentsAndReturnsTheSameList() {
        TapstateException failure = new TapstateException(ConnectorError.LOAD_FAILED,
                Map.of("connector", SENTINEL), new MongoException(8000, SENTINEL)) {
            @Override public String getMessage() { throw new AssertionError("must not read the failure message"); }
        };
        List<SeedOutcome> original = List.of(new SeedOutcome.Failed(mysql(), failure));
        assertThat(witness().record(original)).isSameAs(original);
        assertThat(events).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("outcome", "failed").containsEntry("driver_code", 8000)
                .containsEntry("code", ConnectorError.LOAD_FAILED.code()));
        assertThat(JsonWriter.write(events)).doesNotContain(SENTINEL);
        assertThat(((SeedOutcome.Failed) original.getFirst()).cause()).isSameAs(failure);
    }

    @Test
    void distinguishesRegistrationMetadataAndPathMismatchesUsingBooleansOnly() {
        var outcome = new SeedOutcome.Seeded(temporary.resolve(SENTINEL).resolve("mysql-connector.jar"),
                registration("mysql", SENTINEL, SENTINEL));
        witness().record(List.of(outcome));
        assertThat(events).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("outcome", "seeded").containsEntry("path_match", false)
                .containsEntry("id_match", true).containsEntry("hash_match", false).containsEntry("pdk_match", false));
        assertThat(JsonWriter.write(events)).doesNotContain(SENTINEL);
    }

    @Test
    void absentAndAmbiguousOutcomesAreNotFabricatedAsSuccessfulSeeds() {
        CloudSeedOutcomeWitness observed = witness();
        observed.record(List.of());
        var value = new SeedOutcome.Seeded(mysql(), registration("mysql", HASH, PDK));
        observed.record(List.of(value, value));
        assertThat(events).extracting(event -> event.get("outcome")).containsExactly("absent", "ambiguous");
        assertThat(events).extracting(event -> event.get("matching_count")).containsExactly(0, 2);
    }

    @Test
    void aNonMysqlFailureIsNotInspectedOrRetained() {
        var failure = new UncheckedIOException(new IOException(SENTINEL));
        witness().record(List.of(new SeedOutcome.Failed(temporary.resolve("postgres-connector.jar"), failure)));
        assertThat(events).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("outcome", "absent").doesNotContainKey("exception_type"));
        assertThat(JsonWriter.write(events)).doesNotContain(SENTINEL);
    }

    @Test
    void observesTheRealSweepOnceBeforeTheUnchangedCloudGateAndPreservesItsFailure() throws Exception {
        Files.writeString(mysql(), "synthetic registrar input");
        ConnectorArtifactRegistrar registrar = mock(ConnectorArtifactRegistrar.class);
        RegistrationOutcome registration = registration("mysql", HASH, PDK);
        when(registrar.register(mysql(), RegistrationSource.SEED)).thenReturn(registration);
        CloudConnectorSeedReadiness readiness = mock(CloudConnectorSeedReadiness.class);
        IllegalStateException gateFailure = new IllegalStateException(SENTINEL);
        doAnswer(call -> {
            assertThat(events).singleElement().satisfies(event -> assertThat(event).containsEntry("outcome", "seeded"));
            List<SeedOutcome> outcomes = call.getArgument(0);
            assertThat(outcomes).singleElement().satisfies(outcome ->
                    assertThat(((SeedOutcome.Seeded) outcome).outcome()).isSameAs(registration));
            throw gateFailure;
        }).when(readiness).verifyRegistrations(anyList());
        var observed = witness().observe(new SeedConnectorSweep(registrar));
        var runner = new SeedSweepRunner(observed, temporary, readiness);
        assertThatThrownBy(() -> runner.run(new DefaultApplicationArguments())).isSameAs(gateFailure);
        verify(registrar, times(1)).register(mysql(), RegistrationSource.SEED);
        verify(readiness, times(1)).verifyRegistrations(anyList());
    }

    @Test
    void aProgrammerFailureStillEscapesTheRealSweepUnchanged() throws Exception {
        Files.writeString(mysql(), "synthetic registrar input");
        ConnectorArtifactRegistrar registrar = mock(ConnectorArtifactRegistrar.class);
        IllegalStateException failure = new IllegalStateException(SENTINEL);
        when(registrar.register(any(Path.class), eq(RegistrationSource.SEED))).thenThrow(failure);
        var observed = witness().observe(new SeedConnectorSweep(registrar));
        assertThatThrownBy(() -> observed.sweep(temporary)).isSameAs(failure);
        assertThat(events).isEmpty();
    }

    @Test
    void springInjectsTheObservedConcreteSweepWithNoAdditionalRegistrationCalls() throws Exception {
        Files.writeString(mysql(), "synthetic registrar input");
        ConnectorArtifactRegistrar registrar = mock(ConnectorArtifactRegistrar.class);
        when(registrar.register(mysql(), RegistrationSource.SEED)).thenReturn(registration("mysql", HASH, PDK));
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(SeedConnectorSweep.class, () -> new SeedConnectorSweep(registrar));
            witness().initialize(context);
            context.refresh();
            assertThat(context.getBean(SeedConnectorSweep.class).getClass()).isEqualTo(SeedConnectorSweep.class);
            assertThat(context.getBean(SeedConnectorSweep.class).sweep(temporary)).hasSize(1);
            verify(registrar, times(1)).register(mysql(), RegistrationSource.SEED);
            assertThat(events).singleElement().satisfies(event -> assertThat(event)
                    .containsEntry("id_match", true).containsEntry("hash_match", true).containsEntry("pdk_match", true));
        }
    }

    @Test
    void bothInitializersObserveTheSameContainedRefusalBeforeTheOriginalGate() throws Exception {
        Files.writeString(mysql(), "synthetic registrar input");
        ConnectorArtifactRegistrar registrar = mock(ConnectorArtifactRegistrar.class);
        TapstateException refusal = new TapstateException(ConnectorError.LOAD_FAILED,
                Map.of("connector", SENTINEL), new MongoException(8000, SENTINEL));
        List<String> order = new ArrayList<>();
        List<Map<String, Object>> derivations = new ArrayList<>();
        var seedWitness = new CloudSeedOutcomeWitness(mysql(), HASH, PDK, event -> {
            events.add(event);
            order.add("seed");
        });
        var derivationWitness = new CloudStartupDerivationWitness(event -> {
            derivations.add(event);
            order.add("derive");
        });
        CloudConnectorSeedReadiness readiness = mock(CloudConnectorSeedReadiness.class);
        IllegalStateException gateFailure = new IllegalStateException("original gate refusal");
        doAnswer(call -> {
            order.add("gate");
            List<SeedOutcome> outcomes = call.getArgument(0);
            assertThat(outcomes).singleElement().satisfies(outcome ->
                    assertThat(((SeedOutcome.Failed) outcome).cause()).isSameAs(refusal));
            throw gateFailure;
        }).when(readiness).verifyRegistrations(anyList());
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(CapabilityDeriver.class, () -> id -> { throw refusal; });
            when(registrar.register(mysql(), RegistrationSource.SEED)).thenAnswer(call -> {
                context.getBean(CapabilityDeriver.class).derive("mysql");
                throw new AssertionError("the refusing deriver returned");
            });
            context.registerBean(SeedConnectorSweep.class, () -> new SeedConnectorSweep(registrar));
            derivationWitness.initialize(context);
            seedWitness.initialize(context);
            context.refresh();
            var runner = new SeedSweepRunner(context.getBean(SeedConnectorSweep.class), temporary, readiness);
            assertThatThrownBy(() -> runner.run(new DefaultApplicationArguments())).isSameAs(gateFailure);
            verify(registrar, times(1)).register(mysql(), RegistrationSource.SEED);
        }
        assertThat(order).containsExactly("derive", "seed", "gate");
        assertThat(events).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("outcome", "failed").containsEntry("driver_code", 8000));
        assertThat(derivations).hasSize(1);
        assertThat(JsonWriter.write(events) + JsonWriter.write(derivations)).doesNotContain(SENTINEL);
    }

    private Path mysql() { return temporary.resolve("mysql-connector.jar"); }

    private CloudSeedOutcomeWitness witness() {
        return new CloudSeedOutcomeWitness(mysql(), HASH, PDK, events::add);
    }

    private static RegistrationOutcome registration(String id, String hash, String pdk) {
        return new RegistrationOutcome(new ConnectorRegistration(id, hash, pdk, RegistrationSource.SEED), true);
    }
}
