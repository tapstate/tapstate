package io.tapstate.app;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.FindIterable;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import io.tapstate.adapters.mongostore.MongoConnectorRegistry;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.junit.jupiter.api.Test;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.context.support.GenericApplicationContext;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Answers.RETURNS_SELF;

class CloudMongoReadWitnessTest {
    private static final String SENTINEL = "untrusted-driver-message-and-connection-sentinel";
    private final List<Map<String, Object>> derivations = new ArrayList<>();
    private final List<Map<String, Object>> reads = new ArrayList<>();
    private final CloudStartupDerivationWitness witness =
            new CloudStartupDerivationWitness(derivations::add, reads::add);
    private final ConnectorCapabilities capabilities = new ConnectorCapabilities(Set.of("batch_read_function"));

    @Test
    void factoryCopiesSettingsAndAddsOneListenerWithOneRealFactoryCall() {
        CommandListener existing = new CommandListener() {};
        MongoClientSettings original = MongoClientSettings.builder().applicationName("original-test-client")
                .credential(MongoCredential.createCredential("synthetic", "admin", "synthetic".toCharArray()))
                .retryReads(false).retryWrites(false)
                .applyToSocketSettings(value -> value.readTimeout(47, TimeUnit.MILLISECONDS))
                .addCommandListener(existing).build();
        MongoClient client = mock(MongoClient.class);
        AtomicReference<MongoClientSettings> actual = new AtomicReference<>();
        AtomicInteger creates = new AtomicInteger();
        try (var scope = witness.observeClientCreation()) {
            scope.mockedFactory().when(() -> MongoClients.create(any(MongoClientSettings.class),
                    nullable(MongoDriverInformation.class))).thenAnswer(call -> {
                        creates.incrementAndGet();
                        actual.set(call.getArgument(0));
                        return client;
                    });
            assertThat(MongoClients.create(original)).isSameAs(client);
        }
        assertThat(creates).hasValue(1);
        MongoClientSettings copied = actual.get();
        assertThat(copied.getApplicationName()).isEqualTo(original.getApplicationName());
        assertThat(copied.getCredential()).isSameAs(original.getCredential());
        assertThat(copied.getClusterSettings()).isEqualTo(original.getClusterSettings());
        assertThat(copied.getSocketSettings()).isEqualTo(original.getSocketSettings());
        assertThat(copied.getConnectionPoolSettings()).isEqualTo(original.getConnectionPoolSettings());
        assertThat(copied.getSslSettings()).isEqualTo(original.getSslSettings());
        assertThat(copied.getRetryReads()).isFalse();
        assertThat(copied.getRetryWrites()).isFalse();
        assertThat(copied.getCommandListeners()).hasSize(2).startsWith(existing);
        assertThat(reads).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("kind", "factory").containsEntry("intercepted_calls", 1)
                .containsEntry("real_factory_calls", 1));
        verify(client, never()).close();
    }

    @Test
    void factoryFailurePreservesIdentityAndClearsItsReentryGuard() {
        RuntimeException failure = new IllegalStateException(SENTINEL);
        MongoClient client = mock(MongoClient.class);
        try (var scope = witness.observeClientCreation()) {
            scope.mockedFactory().when(() -> MongoClients.create(any(MongoClientSettings.class),
                    nullable(MongoDriverInformation.class))).thenThrow(failure).thenReturn(client);
            assertThatThrownBy(() -> MongoClients.create(MongoClientSettings.builder().build())).isSameAs(failure);
            assertThat(MongoClients.create(MongoClientSettings.builder().build())).isSameAs(client);
        }
        assertThat(reads).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("intercepted_calls", 2).containsEntry("real_factory_calls", 2));
        assertThat(JsonWriter.write(reads)).doesNotContain(SENTINEL);
        try (var next = mockStatic(MongoClients.class)) {
            assertThat(next).isNotNull();
        }
    }

    @Test
    void registryReadsReturnOriginalObjectsAndDelegateOnlyOnce() {
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        List<ConnectorRegistration> registrations = new ArrayList<>();
        Optional<byte[]> bytes = Optional.of(new byte[] {1, 2});
        when(delegate.list()).thenReturn(registrations);
        when(delegate.artifact("opaque-hash")).thenReturn(bytes);
        ConnectorRegistry observed = witness.observe(delegate);
        mysql(() -> {
            assertThat(observed.list()).isSameAs(registrations);
            assertThat(observed.artifact("opaque-hash")).isSameAs(bytes);
            return null;
        });
        verify(delegate).list();
        verify(delegate).artifact("opaque-hash");
        assertThat(reads).extracting(event -> event.get("operation")).containsExactly("registry-list", "artifact");
        assertThat(reads).allSatisfy(event -> assertThat(event).containsEntry("outcome", "ok"));
    }

    @Test
    void unrelatedRegistryMethodsKeepTheirDelegatesAndArguments() {
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        byte[] artifact = {4, 7};
        RegistrationOutcome registered = mock(RegistrationOutcome.class);
        List<ConnectorRegistration> found = new ArrayList<>();
        Optional<String> certification = Optional.of("retained-certification");
        when(delegate.register("mysql", "api", RegistrationSource.SEED, artifact)).thenReturn(registered);
        when(delegate.findAll("mysql")).thenReturn(found);
        when(delegate.shareSafeCertification("mysql", "opaque-hash")).thenReturn(certification);
        when(delegate.hasArtifact("opaque-hash")).thenReturn(true);
        ConnectorRegistry observed = witness.observe(delegate);
        assertThat(observed.register("mysql", "api", RegistrationSource.SEED, artifact)).isSameAs(registered);
        assertThat(observed.findAll("mysql")).isSameAs(found);
        assertThat(observed.shareSafeCertification("mysql", "opaque-hash")).isSameAs(certification);
        assertThat(observed.hasArtifact("opaque-hash")).isTrue();
        verify(delegate).register(eq("mysql"), eq("api"), eq(RegistrationSource.SEED), eq(artifact));
        verify(delegate).findAll("mysql");
        verify(delegate).shareSafeCertification("mysql", "opaque-hash");
        verify(delegate).hasArtifact("opaque-hash");
        assertThat(reads).isEmpty();
    }

    @Test
    void mappedRefusalPreservesTheOriginalExceptionAndCleansTheScope() {
        TapstateException failure = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", SENTINEL), null);
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        when(delegate.list()).thenThrow(failure);
        ConnectorRegistry observed = witness.observe(delegate);
        assertThatThrownBy(() -> mysql(observed::list)).isSameAs(failure);
        assertThat(reads).singleElement().satisfies(event -> assertThat(event)
                .containsEntry("operation", "registry-list").containsEntry("outcome", "mapped-refusal")
                .containsEntry("code", IoError.STORE_UNAVAILABLE.code()));
        assertThatThrownBy(observed::list).isSameAs(failure);
        assertThat(reads).hasSize(1);
        assertThat(JsonWriter.write(reads)).doesNotContain(SENTINEL);
    }

    @Test
    void driverFailureShowsOnlyTypeAndCodeInsideTheExactMysqlRead() {
        CommandListener listener = listener();
        CommandFailedEvent failure = driverFailure("getMore");
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        Optional<byte[]> bytes = Optional.of(new byte[] {1});
        when(delegate.artifact("opaque-hash")).thenAnswer(call -> {
            listener.commandFailed(failure);
            return bytes;
        });
        ConnectorRegistry observed = witness.observe(delegate);
        mysql(() -> observed.artifact("opaque-hash"));
        assertThat(reads).filteredOn(event -> "command-failed".equals(event.get("kind")))
                .singleElement().satisfies(event -> assertThat(event)
                        .containsEntry("operation", "artifact").containsEntry("command", "getMore")
                        .containsEntry("artifact_step", "unclassified")
                        .containsEntry("driver_code", 8000).containsEntry("exception_type", UnreadableMessage.class.getName()));
        assertThat(JsonWriter.write(reads)).doesNotContain(SENTINEL, "opaque-hash");
    }

    @Test
    void aRegistryListCommandFailureHasNoArtifactStep() {
        CommandListener listener = listener();
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        when(delegate.list()).thenAnswer(call -> {
            listener.commandFailed(driverFailure("find"));
            return List.of();
        });
        mysql(() -> witness.observe(delegate).list());
        verify(delegate).list();
        assertThat(reads).filteredOn(event -> "command-failed".equals(event.get("kind")))
                .singleElement().satisfies(event -> assertThat(event)
                        .containsEntry("operation", "registry-list").doesNotContainKey("artifact_step"));
        assertThat(JsonWriter.write(reads)).doesNotContain(SENTINEL);
    }

    @Test
    void aRealDriverMetadataCallStackIsIdentifiedWithoutADatabase() {
        CommandListener listener = listener();
        DriverReadFixture fixture = driverReadFixture();
        when(fixture.find().into(any())).thenAnswer(call -> {
            listener.commandFailed(driverFailure("find"));
            return call.getArgument(0);
        });
        ConnectorRegistry observed = witness.observe(fixture.registry());
        mysql(() -> {
            assertThat(observed.artifact("opaque-hash")).isEmpty();
            return null;
        });
        verify(fixture.files()).find();
        verify(fixture.find()).filter(any(Bson.class));
        verify(fixture.find()).into(any());
        verify(fixture.find(), never()).first();
        assertThat(reads).filteredOn(event -> "command-failed".equals(event.get("kind")))
                .singleElement().satisfies(event -> assertThat(event)
                        .containsEntry("artifact_step", "metadata-find"));
        assertThat(JsonWriter.write(reads)).doesNotContain(SENTINEL, "opaque-hash");
    }

    @Test
    void aRealDriverDownloadFileLookupIsIdentifiedWithoutADatabase() {
        CommandListener listener = listener();
        DriverReadFixture fixture = driverReadFixture();
        GridFSFile file = new GridFSFile(new BsonObjectId(new ObjectId()), "synthetic-file", 0, 255,
                new Date(0), new Document());
        RuntimeException stop = new IllegalStateException(SENTINEL);
        when(fixture.find().into(any())).thenAnswer(call -> {
            java.util.Collection<GridFSFile> target = call.getArgument(0);
            target.add(file);
            return target;
        });
        when(fixture.find().first()).thenAnswer(call -> {
            listener.commandFailed(driverFailure("find"));
            throw stop;
        });
        ConnectorRegistry observed = witness.observe(fixture.registry());
        assertThatThrownBy(() -> mysql(() -> observed.artifact("opaque-hash"))).isSameAs(stop);
        verify(fixture.files(), times(2)).find();
        verify(fixture.find(), times(2)).filter(any(Bson.class));
        verify(fixture.find()).into(any());
        verify(fixture.find()).first();
        assertThat(reads).filteredOn(event -> "command-failed".equals(event.get("kind")))
                .singleElement().satisfies(event -> assertThat(event)
                        .containsEntry("artifact_step", "download"));
        assertThat(JsonWriter.write(reads)).doesNotContain(SENTINEL, "opaque-hash");
    }

    @Test
    void nonMysqlAndUnknownCommandsDoNotProduceDriverEvents() {
        CommandListener listener = listener();
        CommandFailedEvent failure = driverFailure("find");
        listener.commandFailed(failure);
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        when(delegate.list()).thenAnswer(call -> {
            listener.commandFailed(failure);
            listener.commandFailed(driverFailure("unknown-command"));
            return List.of();
        });
        ConnectorRegistry observed = witness.observe(delegate);
        ConnectorProvisioner other = witness.observe((ConnectorProvisioner) id -> {
            observed.list();
            return null;
        });
        CapabilityDeriver mysql = witness.observe((CapabilityDeriver) id -> {
            other.resolve("oracle");
            return capabilities;
        });
        mysql.derive("mysql");
        observed.list();
        assertThat(reads).isEmpty();
    }

    @Test
    void anEventOnAnotherThreadIsNotAttributedToTheMysqlCall() {
        CommandListener listener = listener();
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        when(delegate.list()).thenAnswer(call -> {
            Thread outside = new Thread(() -> listener.commandFailed(driverFailure("find")));
            outside.start();
            outside.join();
            return List.of();
        });
        mysql(() -> witness.observe(delegate).list());
        assertThat(reads).singleElement().satisfies(event -> assertThat(event).containsEntry("kind", "operation"));
    }

    @Test
    void unknownCommandsInsideAMysqlReadStayUnobserved() {
        CommandListener listener = listener();
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        when(delegate.list()).thenAnswer(call -> {
            listener.commandFailed(driverFailure("unknown-command"));
            return List.of();
        });
        mysql(() -> witness.observe(delegate).list());
        assertThat(reads).singleElement().satisfies(event -> assertThat(event).containsEntry("kind", "operation"));
    }

    @Test
    void aNestedNonMysqlProvisionRestoresTheOuterMysqlScope() {
        CommandListener listener = listener();
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        when(delegate.list()).thenAnswer(call -> {
            listener.commandFailed(driverFailure("find"));
            return List.of();
        });
        ConnectorRegistry observed = witness.observe(delegate);
        ConnectorProvisioner other = witness.observe((ConnectorProvisioner) id -> {
            observed.list();
            return null;
        });
        mysql(() -> {
            observed.list();
            other.resolve("oracle");
            observed.list();
            return null;
        });
        verify(delegate, times(3)).list();
        assertThat(reads).filteredOn(event -> "command-failed".equals(event.get("kind"))).hasSize(2);
        assertThat(reads).filteredOn(event -> "operation".equals(event.get("kind"))).hasSize(2);
    }

    @Test
    void theInitializerKeepsTheOriginalInterfaceBeanCalls() {
        CommandListener listener = listener();
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        when(delegate.list()).thenAnswer(call -> {
            listener.commandFailed(driverFailure("find"));
            return List.of();
        });
        try (GenericApplicationContext context = new GenericApplicationContext()) {
            context.registerBean(ConnectorRegistry.class, () -> delegate);
            context.registerBean(ConnectorProvisioner.class, () -> id -> {
                context.getBean(ConnectorRegistry.class).list();
                return null;
            });
            context.registerBean(CapabilityDeriver.class, () -> id -> {
                context.getBean(ConnectorProvisioner.class).resolve(id);
                return capabilities;
            });
            witness.initialize(context);
            context.refresh();
            assertThat(context.getBean(CapabilityDeriver.class).derive("mysql")).isSameAs(capabilities);
        }
        verify(delegate).list();
        assertThat(reads).extracting(event -> event.get("kind")).containsExactly("command-failed", "operation");
    }

    @Test
    void missingCommandEventsStayUnknownAndDiagnosticOutputCannotBreakTheRead() {
        CloudStartupDerivationWitness unavailable = new CloudStartupDerivationWitness(event -> {}, event -> {
            throw new IllegalStateException(SENTINEL);
        });
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        List<ConnectorRegistration> original = new ArrayList<>();
        when(delegate.list()).thenReturn(original);
        ConnectorRegistry observed = unavailable.observe(delegate);
        ConnectorProvisioner provisioner = unavailable.observe((ConnectorProvisioner) id -> {
            assertThat(observed.list()).isSameAs(original);
            return null;
        });
        unavailable.observe((CapabilityDeriver) id -> {
            provisioner.resolve(id);
            return capabilities;
        }).derive("mysql");
        assertThat(reads).isEmpty();
    }

    private CommandListener listener() {
        AtomicReference<CommandListener> listener = new AtomicReference<>();
        try (var scope = witness.observeClientCreation()) {
            scope.mockedFactory().when(() -> MongoClients.create(any(MongoClientSettings.class),
                    nullable(MongoDriverInformation.class))).thenAnswer(call -> {
                        MongoClientSettings settings = call.getArgument(0);
                        listener.set(settings.getCommandListeners().getLast());
                        return mock(MongoClient.class);
                    });
            MongoClients.create(MongoClientSettings.builder().build());
        }
        reads.clear();
        return listener.get();
    }

    private void mysql(Supplier<?> operation) {
        ConnectorProvisioner provisioner = witness.observe((ConnectorProvisioner) id -> {
            operation.get();
            return null;
        });
        witness.observe((CapabilityDeriver) id -> {
            provisioner.resolve(id);
            return capabilities;
        }).derive("mysql");
    }

    private static CommandFailedEvent driverFailure(String command) {
        CommandFailedEvent event = mock(CommandFailedEvent.class);
        when(event.getCommandName()).thenReturn(command);
        when(event.getThrowable()).thenReturn(new UnreadableMessage());
        return event;
    }

    @SuppressWarnings("unchecked")
    static DriverReadFixture driverReadFixture() {
        MongoDatabase database = mock(MongoDatabase.class);
        MongoCollection<GridFSFile> files = mock(MongoCollection.class, RETURNS_SELF);
        MongoCollection<BsonDocument> chunks = mock(MongoCollection.class, RETURNS_SELF);
        FindIterable<GridFSFile> find = mock(FindIterable.class, RETURNS_SELF);
        when(database.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(database.getCollection("connector_artifacts.files", GridFSFile.class)).thenReturn(files);
        when(database.getCollection("connector_artifacts.chunks", BsonDocument.class)).thenReturn(chunks);
        when(files.find()).thenReturn(find);
        MongoCollection<Document> metadata = mock(MongoCollection.class, RETURNS_SELF);
        when(database.getCollection("connector_artifacts.files")).thenReturn(metadata);
        when(database.getCollection("connector_artifacts.chunks")).thenReturn(mock(MongoCollection.class));
        when(metadata.getNamespace()).thenReturn(new MongoNamespace("synthetic", "connector_artifacts.files"));
        ListIndexesIterable<Document> indexes = mock(ListIndexesIterable.class);
        when(metadata.listIndexes()).thenReturn(indexes);
        when(indexes.iterator()).thenAnswer(call -> {
            MongoCursor<Document> cursor = mock(MongoCursor.class);
            when(cursor.hasNext()).thenReturn(true, false);
            when(cursor.next()).thenReturn(new Document("name", "filename_idx")
                    .append("key", new Document("filename", 1)).append("unique", true));
            return cursor;
        });
        return new DriverReadFixture(new MongoConnectorRegistry(mock(MongoClient.class), database), files, find);
    }

    record DriverReadFixture(MongoConnectorRegistry registry, MongoCollection<GridFSFile> files,
                                     FindIterable<GridFSFile> find) {}

    private static final class UnreadableMessage extends MongoException {
        private UnreadableMessage() { super(8000, SENTINEL); }
        @Override public String getMessage() { throw new AssertionError("the observer must not read a driver message"); }
        @Override public synchronized Throwable getCause() { throw new AssertionError("the observer must not read a driver cause"); }
        @Override public StackTraceElement[] getStackTrace() { throw new AssertionError("the observer must not read a throwable stack"); }
    }
}
