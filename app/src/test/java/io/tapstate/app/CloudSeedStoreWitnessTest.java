package io.tapstate.app;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoException;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.IntrospectedConnector;
import io.tapstate.adapters.pdk.SeedConnectorSweep;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.springframework.context.support.GenericApplicationContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudSeedStoreWitnessTest {
    private static final String SENTINEL = "untrusted-seed-store-message-and-key";
    private static final IntrospectedConnector MYSQL = new IntrospectedConnector(
            "synthetic.MySqlConnector", "api", "mysql.json", "{\"properties\":{\"id\":\"mysql\"}}");
    @TempDir Path directory;

    @Test
    void theRealPathOverloadRunsOnceAndReturnsTheOriginalOutcome() throws IOException {
        Fixture f = new Fixture(directory);
        Path original = directory.resolve(".").resolve("mysql-connector.jar");
        assertThat(f.registrar.register(original, RegistrationSource.SEED)).isSameAs(f.registered);
        ArgumentCaptor<List<Path>> paths = ArgumentCaptor.captor();
        verify(f.introspector).introspect(paths.capture());
        assertThat(paths.getValue()).singleElement().isSameAs(original);
        verify(f.registry).register(eq("mysql"), eq("api"), eq(RegistrationSource.SEED), eq(new byte[] {4, 7}));
        assertThat(f.events).extracting(e -> e.get("operation")).containsExactly(
                "registry-find-all", "spec-has", "registry-register", "catalog-get", "registration");
        assertThat(f.events).allSatisfy(e -> assertThat(e).containsEntry("stage", "seed")
                .containsEntry("connector", "mysql").containsEntry("path_match", true).containsEntry("outcome", "ok"));
        assertThat(JsonWriter.write(f.events)).doesNotContain(directory.toString(), SENTINEL, "mysql.json");
    }

    @Test
    void aSameBasenameElsewhereDoesNotBorrowTheLockedSeedIdentity() throws IOException {
        Fixture f = new Fixture(directory);
        Path other = Files.createDirectory(directory.resolve("other")).resolve("mysql-connector.jar");
        Files.write(other, new byte[] {4, 7});
        assertThat(f.registrar.register(other, RegistrationSource.SEED)).isSameAs(f.registered);
        assertThat(f.events).isEmpty();
        verify(f.introspector).introspect(List.of(other));
    }

    @Test
    void anExplicitRuntimeRegistrationIsNotASeedObservation() throws IOException {
        Fixture f = new Fixture(directory);
        assertThat(f.registrar.register(f.artifact, RegistrationSource.REGISTER)).isSameAs(f.registered);
        verify(f.registry).register(eq("mysql"), eq("api"), eq(RegistrationSource.REGISTER), eq(new byte[] {4, 7}));
        assertThat(f.events).isEmpty();
    }

    @Test
    void theMappedFailureKeepsItsIdentityAndTheSeedScopeIsRemoved() throws IOException {
        Fixture f = new Fixture(directory);
        when(f.registry.findAll("mysql")).thenThrow(f.failure);
        assertThatThrownBy(f::seed).isSameAs(f.failure);
        assertThat(f.events).extracting(e -> e.get("operation")).containsExactly("registry-find-all", "registration");
        assertThat(f.events).allSatisfy(e -> assertThat(e).containsEntry("outcome", "mapped-refusal")
                .containsEntry("code", IoError.STORE_UNAVAILABLE.code()));
        assertThatThrownBy(() -> f.observedRegistry.findAll("mysql")).isSameAs(f.failure);
        assertThat(f.events).hasSize(2);
        assertThat(JsonWriter.write(f.events)).doesNotContain(SENTINEL);
        verify(f.registry, times(2)).findAll("mysql");
    }

    @Test
    void specPresenceUsesTheOriginalOptimizedMethodAndWritesOnlyOnce() throws IOException {
        Fixture f = new Fixture(directory);
        when(f.specs.has(anyString())).thenReturn(false);
        when(f.specs.get(anyString())).thenThrow(new AssertionError("presence must not fetch the spec"));
        assertThat(f.seed()).isSameAs(f.registered);
        verify(f.specs).has(anyString());
        verify(f.specs).put(anyString(), any(byte[].class));
        verify(f.specs, never()).get(anyString());
        assertThat(f.events).extracting(e -> e.get("operation")).containsExactly(
                "registry-find-all", "spec-has", "spec-put", "registry-register", "catalog-get", "registration");
    }

    @Test
    void unrelatedInterfaceMethodsKeepOriginalArgumentsAndReturnObjects() throws IOException {
        Fixture f = new Fixture(directory);
        byte[] bytes = {2, 3};
        Optional<byte[]> stored = Optional.of(bytes);
        ConnectorCatalogEntry entry = mock(ConnectorCatalogEntry.class);
        List<ConnectorCatalogEntry> rows = new ArrayList<>();
        when(f.specs.get(SENTINEL)).thenReturn(stored);
        when(f.catalog.list()).thenReturn(rows);
        assertThat(f.observedSpecs.get(SENTINEL)).isSameAs(stored);
        f.observedSpecs.put(SENTINEL, bytes);
        f.observedCatalog.upsert(entry);
        assertThat(f.observedCatalog.list()).isSameAs(rows);
        verify(f.specs).get(SENTINEL);
        verify(f.specs).put(eq(SENTINEL), eq(bytes));
        verify(f.catalog).upsert(eq(entry));
        verify(f.catalog).list();
        assertThat(f.events).isEmpty();
    }

    @Test
    void aWriteFailureProjectsOnlyItsClassAndNumericCodeInTheExactOperation() throws IOException {
        Fixture f = new Fixture(directory);
        CommandListener listener = listener(f);
        when(f.specs.has(anyString())).thenReturn(false);
        doAnswer(call -> {
            listener.commandFailed(driverFailure("update"));
            throw f.failure;
        }).when(f.specs).put(anyString(), any(byte[].class));
        assertThatThrownBy(f::seed).isSameAs(f.failure);
        assertThat(f.events).filteredOn(e -> "command-failed".equals(e.get("kind"))).singleElement()
                .satisfies(e -> assertThat(e).containsEntry("stage", "seed").containsEntry("operation", "spec-put")
                        .containsEntry("command", "update").containsEntry("driver_code", 8000)
                        .containsEntry("exception_type", UnreadableMessage.class.getName()));
        assertThat(JsonWriter.write(f.events)).doesNotContain(SENTINEL, directory.toString());
    }

    @Test
    void theDefaultWitnessNeverReadsTheServerErrorField() throws IOException {
        Fixture f = new Fixture(directory);
        CommandListener listener = listener(f);
        MongoCommandException driver = commandFailure(8000);
        when(driver.getErrorMessage()).thenThrow(new AssertionError("the default witness must not read"));
        when(f.specs.has(anyString())).thenReturn(false);
        doAnswer(call -> {
            listener.commandFailed(driverFailure("update", driver));
            throw f.failure;
        }).when(f.specs).put(anyString(), any(byte[].class));
        assertThatThrownBy(f::seed).isSameAs(f.failure);
        verify(driver, never()).getErrorMessage();
        assertThat(f.events).allSatisfy(event -> assertThat(event).doesNotContainKey("refusal_category"));
    }

    @Test
    void anEnabledExactWriteClassifiesOnceWithoutChangingTheOriginalFailure() throws IOException {
        Fixture f = new Fixture(directory, true);
        CommandListener listener = listener(f);
        MongoCommandException driver = commandFailure(8000);
        when(driver.getErrorMessage()).thenReturn("you are over your space quota, using 513 MB of 512 MB");
        when(f.specs.has(anyString())).thenReturn(false);
        doAnswer(call -> {
            listener.commandFailed(driverFailure("update", driver));
            listener.commandFailed(driverFailure("update", driver));
            throw f.failure;
        }).when(f.specs).put(anyString(), any(byte[].class));
        assertThatThrownBy(f::seed).isSameAs(f.failure);
        verify(f.specs).put(anyString(), any(byte[].class));
        verify(driver).getErrorMessage();
        verify(driver, never()).getMessage();
        verify(driver, never()).getResponse();
        verify(driver, never()).getCause();
        assertThat(f.events).filteredOn(event -> event.containsKey("refusal_category")).singleElement()
                .satisfies(event -> assertThat(event).containsEntry("refusal_category", "SPACE_QUOTA")
                        .containsEntry("operation", "spec-put").containsEntry("command", "update"));
        assertThat(JsonWriter.write(f.events.stream().map(event -> event.values().stream()
                .filter(String.class::isInstance).toList()).toList()))
                .doesNotContain("513", "512", "space quota", SENTINEL);
    }

    @Test
    void otherOperationsAndCodesCannotConsumeTheSingleClassification() throws IOException {
        Fixture f = new Fixture(directory, true);
        CommandListener listener = listener(f);
        MongoCommandException other = commandFailure(13);
        MongoCommandException driver = commandFailure(8000);
        when(driver.getErrorMessage()).thenReturn(SENTINEL);
        when(f.registry.findAll("mysql")).thenAnswer(call -> {
            listener.commandFailed(driverFailure("find", driver));
            return List.of();
        });
        when(f.specs.has(anyString())).thenReturn(false);
        doAnswer(call -> {
            listener.commandFailed(driverFailure("update", other));
            listener.commandFailed(driverFailure("find", driver));
            listener.commandFailed(driverFailure("update", driver));
            throw f.failure;
        }).when(f.specs).put(anyString(), any(byte[].class));
        assertThatThrownBy(f::seed).isSameAs(f.failure);
        verify(other, never()).getErrorMessage();
        verify(driver).getErrorMessage();
        assertThat(f.events).filteredOn(event -> event.containsKey("refusal_category")).singleElement()
                .satisfies(event -> assertThat(event).containsEntry("refusal_category", "UNCLASSIFIED")
                        .containsEntry("operation", "spec-put").containsEntry("command", "update"));
        assertThat(JsonWriter.write(f.events)).doesNotContain(SENTINEL);
    }

    @Test
    void anUnavailableFieldStillPreservesTheMappedFailure() throws IOException {
        Fixture f = new Fixture(directory, true);
        CommandListener listener = listener(f);
        MongoCommandException driver = commandFailure(8000);
        when(driver.getErrorMessage()).thenThrow(new IllegalArgumentException(SENTINEL));
        when(f.specs.has(anyString())).thenReturn(false);
        doAnswer(call -> {
            listener.commandFailed(driverFailure("update", driver));
            throw f.failure;
        }).when(f.specs).put(anyString(), any(byte[].class));
        assertThatThrownBy(f::seed).isSameAs(f.failure);
        assertThat(f.events).filteredOn(event -> event.containsKey("refusal_category")).singleElement()
                .satisfies(event -> assertThat(event).containsEntry("refusal_category", "UNCLASSIFIED"));
        assertThat(JsonWriter.write(f.events)).doesNotContain(SENTINEL);
    }

    @Test
    void unknownCommandsAndCallbacksOutsideTheSeedStayUnobserved() throws IOException {
        Fixture f = new Fixture(directory);
        CommandListener listener = listener(f);
        listener.commandFailed(driverFailure("find"));
        when(f.registry.findAll("mysql")).thenAnswer(call -> {
            listener.commandFailed(driverFailure("aggregate"));
            return List.of();
        });
        f.seed();
        assertThat(f.events).noneMatch(e -> "command-failed".equals(e.get("kind")));
    }

    @Test
    void aCallbackOnAnotherThreadIsNotAttributedToTheSeed() throws IOException {
        Fixture f = new Fixture(directory);
        CommandListener listener = listener(f);
        when(f.registry.findAll("mysql")).thenAnswer(call -> {
            Thread other = new Thread(() -> listener.commandFailed(driverFailure("find")));
            other.start();
            other.join();
            return List.of();
        });
        f.seed();
        assertThat(f.events).noneMatch(e -> "command-failed".equals(e.get("kind")));
    }

    @Test
    void aNestedNonMysqlDerivationDoesNotBorrowTheSeedScope() throws IOException {
        Fixture f = new Fixture(directory);
        CapabilityDeriver other = f.witness.observe((CapabilityDeriver) id -> {
            f.observedSpecs.has(SENTINEL);
            return new ConnectorCapabilities(Set.of());
        });
        when(f.specs.has(anyString())).thenAnswer(call -> {
            if (!SENTINEL.equals(call.getArgument(0))) other.derive("oracle");
            return true;
        });
        f.seed();
        assertThat(f.events).filteredOn(e -> "spec-has".equals(e.get("operation"))).hasSize(1);
        verify(f.specs, times(2)).has(anyString());
    }

    @Test
    void aNestedDifferentArtifactRestoresTheOuterSeedScope() throws IOException {
        Fixture f = new Fixture(directory);
        Path other = directory.resolve("other.jar");
        when(f.introspector.introspect(anyList())).thenAnswer(call -> {
            List<Path> paths = call.getArgument(0);
            if (paths.getFirst().equals(other)) {
                f.observedSpecs.has(SENTINEL);
                throw f.failure;
            }
            return MYSQL;
        });
        when(f.registry.findAll("mysql")).thenAnswer(call -> {
            assertThatThrownBy(() -> f.registrar.register(other, RegistrationSource.SEED)).isSameAs(f.failure);
            return List.of();
        });
        f.seed();
        assertThat(f.events).filteredOn(e -> "spec-has".equals(e.get("operation"))).hasSize(1);
        assertThat(f.events).filteredOn(e -> "registration".equals(e.get("operation"))).hasSize(1);
    }

    @Test
    void theInitializerObservesTheRegistrarActuallyInjectedIntoTheSingleSweep() throws IOException {
        Fixture f = new Fixture(directory);
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(ConnectorRegistry.class, () -> f.registry);
            context.registerBean(ConnectorSpecStore.class, () -> f.specs);
            context.registerBean(ConnectorCatalogStore.class, () -> f.catalog);
            context.registerBean(CapabilityDeriver.class, () -> id -> new ConnectorCapabilities(Set.of()));
            context.registerBean(ConnectorArtifactRegistrar.class, () -> new ConnectorArtifactRegistrar(
                    context.getBean(ConnectorRegistry.class), f.introspector, context.getBean(CapabilityDeriver.class),
                    context.getBean(ConnectorCatalogStore.class), context.getBean(ConnectorSpecStore.class)));
            context.registerBean(SeedConnectorSweep.class, () -> new SeedConnectorSweep(
                    context.getBean(ConnectorArtifactRegistrar.class)));
            f.witness.initialize(context);
            context.refresh();
            List<SeedOutcome> outcomes = context.getBean(SeedConnectorSweep.class).sweep(directory);
            assertThat(outcomes).singleElement().isInstanceOfSatisfying(SeedOutcome.Seeded.class,
                    seeded -> assertThat(seeded.outcome()).isSameAs(f.registered));
        }
        verify(f.introspector).introspect(List.of(f.artifact));
        verify(f.registry).findAll("mysql");
        assertThat(f.events).extracting(e -> e.get("operation")).containsExactly(
                "registry-find-all", "spec-has", "registry-register", "catalog-get", "registration");
    }

    @Test
    void unavailableDiagnosticOutputDoesNotChangeTheOriginalFailure() throws IOException {
        Fixture f = new Fixture(directory);
        CloudStartupDerivationWitness unavailable = new CloudStartupDerivationWitness(event -> {}, event -> {
            throw new IllegalStateException(SENTINEL);
        }, f.artifact);
        when(f.registry.findAll("mysql")).thenThrow(f.failure);
        ConnectorArtifactRegistrar registrar = unavailable.observe(new ConnectorArtifactRegistrar(
                unavailable.observe(f.registry), f.introspector, id -> new ConnectorCapabilities(Set.of()),
                unavailable.observe(f.catalog), unavailable.observe(f.specs)));
        assertThatThrownBy(() -> registrar.register(f.artifact, RegistrationSource.SEED)).isSameAs(f.failure);
        verify(f.registry).findAll("mysql");
    }

    private static CommandListener listener(Fixture f) {
        AtomicReference<CommandListener> captured = new AtomicReference<>();
        try (var scope = f.witness.observeClientCreation()) {
            scope.mockedFactory().when(() -> MongoClients.create(any(MongoClientSettings.class),
                    nullable(MongoDriverInformation.class))).thenAnswer(call -> {
                        MongoClientSettings settings = call.getArgument(0);
                        captured.set(settings.getCommandListeners().getLast());
                        return mock(MongoClient.class);
                    });
            MongoClients.create(MongoClientSettings.builder().build());
        }
        f.events.clear();
        return captured.get();
    }

    private static CommandFailedEvent driverFailure(String command) {
        return driverFailure(command, new UnreadableMessage());
    }

    private static CommandFailedEvent driverFailure(String command, Throwable failure) {
        CommandFailedEvent event = mock(CommandFailedEvent.class);
        when(event.getCommandName()).thenReturn(command);
        when(event.getThrowable()).thenReturn(failure);
        return event;
    }

    private static MongoCommandException commandFailure(int code) {
        MongoCommandException failure = mock(MongoCommandException.class);
        when(failure.getCode()).thenReturn(code);
        return failure;
    }

    private static final class UnreadableMessage extends MongoException {
        private UnreadableMessage() { super(8000, SENTINEL); }
        @Override public String getMessage() { throw new AssertionError("the observer must not read the message"); }
    }

    private static final class Fixture {
        private final List<Map<String, Object>> events = new ArrayList<>();
        private final Path artifact;
        private final CloudStartupDerivationWitness witness;
        private final ConnectorRegistry registry = mock(ConnectorRegistry.class);
        private final ConnectorIntrospector introspector = mock(ConnectorIntrospector.class);
        private final ConnectorSpecStore specs = mock(ConnectorSpecStore.class);
        private final ConnectorCatalogStore catalog = mock(ConnectorCatalogStore.class);
        private final ConnectorRegistry observedRegistry;
        private final ConnectorSpecStore observedSpecs;
        private final ConnectorCatalogStore observedCatalog;
        private final RegistrationOutcome registered = new RegistrationOutcome(
                new ConnectorRegistration("mysql", "retained-hash", "api", RegistrationSource.SEED), false);
        private final TapstateException failure = new TapstateException(
                IoError.STORE_UNAVAILABLE, Map.of("detail", SENTINEL), null);
        private final ConnectorArtifactRegistrar registrar;

        private Fixture(Path directory) throws IOException {
            this(directory, false);
        }

        private Fixture(Path directory, boolean classify) throws IOException {
            artifact = Files.write(directory.resolve("mysql-connector.jar"), new byte[] {4, 7});
            witness = new CloudStartupDerivationWitness(event -> {}, events::add, artifact, classify);
            observedRegistry = witness.observe(registry);
            observedSpecs = witness.observe(specs);
            observedCatalog = witness.observe(catalog);
            when(introspector.introspect(anyList())).thenReturn(MYSQL);
            when(registry.findAll("mysql")).thenReturn(List.of());
            when(registry.register(eq("mysql"), eq("api"), any(RegistrationSource.class), any(byte[].class)))
                    .thenReturn(registered);
            when(specs.has(anyString())).thenReturn(true);
            when(catalog.get("mysql")).thenReturn(Optional.of(mock(ConnectorCatalogEntry.class)));
            registrar = witness.observe(new ConnectorArtifactRegistrar(observedRegistry, introspector,
                    id -> { throw new AssertionError("an existing row must skip derivation"); },
                    observedCatalog, observedSpecs));
        }

        private RegistrationOutcome seed() { return registrar.register(artifact, RegistrationSource.SEED); }
    }
}
