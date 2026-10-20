package io.tapstate.app;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoSocketReadException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import io.tapstate.adapters.mongostore.MongoConnectorRegistry;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorRegistry;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Answers.RETURNS_SELF;

@ResourceLock(Resources.SYSTEM_OUT)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class CloudArtifactReadFailureWitnessTest {
    private static final String PROPERTY = CloudStartupDerivationWitness.ARTIFACT_READ_FAILURE_PROPERTY;
    private static final String SENTINEL = "synthetic-private-text-must-stay-local";
    private enum Mode { OLD_CONSTRUCTOR, EXPLICIT_OFF, ENABLED, PROPERTY_DEFAULT,
                        PROPERTY_OFF, PROPERTY_ON, UNREADABLE, OUTPUT_FAILURE, RETURNED }

    @Test void oldConstructorStaysDisabled() { runQuietly(Mode.OLD_CONSTRUCTOR); }
    @Test void explicitOffStaysDisabled() { runQuietly(Mode.EXPLICIT_OFF); }
    @Test void enabledReadsOnceAcrossCallbacks() { runQuietly(Mode.ENABLED); }
    @Test void absentPropertyStaysDisabled() { runQuietly(Mode.PROPERTY_DEFAULT); }
    @Test void falsePropertyStaysDisabled() { runQuietly(Mode.PROPERTY_OFF); }
    @Test void truePropertyEnablesOnlyOneRead() { runQuietly(Mode.PROPERTY_ON); }
    @Test void unreadableCauseStaysUnclassified() { runQuietly(Mode.UNREADABLE); }
    @Test void outputFailureKeepsOriginalException() { runQuietly(Mode.OUTPUT_FAILURE); }
    @Test void originalResultAndCapabilitiesKeepIdentity() { runQuietly(Mode.RETURNED); }

    private static void runQuietly(Mode mode) {
        PrintStream original = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try (PrintStream quiet = new PrintStream(captured)) {
            System.setOut(quiet);
            run(mode);
        } finally {
            System.setOut(original);
        }
        assertThat(captured.toString()).doesNotContain(SENTINEL);

    }

    @SuppressWarnings("unchecked")
    private static void run(Mode mode) {
        List<Map<String, Object>> events = new ArrayList<>();
        Consumer<Map<String, Object>> output = mode == Mode.OUTPUT_FAILURE
                ? event -> { throw new IllegalStateException(SENTINEL); } : events::add;
        boolean propertyMode = mode == Mode.PROPERTY_DEFAULT || mode == Mode.PROPERTY_OFF
                || mode == Mode.PROPERTY_ON;
        CloudStartupDerivationWitness witness;
        String previous = System.getProperty(PROPERTY);
        try {
            if (mode == Mode.PROPERTY_DEFAULT) System.clearProperty(PROPERTY);
            if (mode == Mode.PROPERTY_OFF) System.setProperty(PROPERTY, "false");
            if (mode == Mode.PROPERTY_ON) System.setProperty(PROPERTY, "true");
            if (propertyMode) {
                try (var inputs = mockStatic(CloudConnectorTestInputs.class)) {
                    inputs.when(CloudConnectorTestInputs::seedDirectory).thenReturn(Path.of("synthetic-release"));
                    witness = CloudStartupDerivationWitness.initializer();
                }
            } else if (mode == Mode.OLD_CONSTRUCTOR) {
                witness = new CloudStartupDerivationWitness(event -> {}, output, null, true);
            } else {
                witness = new CloudStartupDerivationWitness(event -> {}, output, null, false,
                        mode != Mode.EXPLICIT_OFF);
            }
        } finally {
            if (previous == null) System.clearProperty(PROPERTY);
            else System.setProperty(PROPERTY, previous);
        }

        AtomicReference<CommandListener> listener = new AtomicReference<>();
        AtomicInteger factories = new AtomicInteger();
        MongoClient client = mock(MongoClient.class);
        try (var observation = witness.observeClientCreation()) {
            observation.mockedFactory().when(() -> MongoClients.create(any(MongoClientSettings.class),
                    nullable(MongoDriverInformation.class))).thenAnswer(call -> {
                        factories.incrementAndGet();
                        listener.set(((MongoClientSettings) call.getArgument(0)).getCommandListeners().getLast());
                        return client;
                    });
            assertThat(MongoClients.create(MongoClientSettings.builder().build())).isSameAs(client);
        }
        events.clear();
        assertThat(factories).hasValue(1);

        MongoSocketReadException failure = mock(MongoSocketReadException.class);
        assertThat(failure.getClass()).isEqualTo(MongoSocketReadException.class);
        when(failure.getCode()).thenReturn(-2);
        AtomicInteger causeReads = new AtomicInteger();
        doAnswer(call -> {
            causeReads.incrementAndGet();
            if (mode == Mode.UNREADABLE) throw new IllegalStateException(SENTINEL);
            return new SocketTimeoutException(SENTINEL);
        }).when(failure).getCause();
        doAnswer(call -> { throw new AssertionError("No driver message read"); }).when(failure).getMessage();
        doAnswer(call -> { throw new AssertionError("No driver stack read"); }).when(failure).getStackTrace();
        CommandFailedEvent failed = mock(CommandFailedEvent.class);
        when(failed.getCommandName()).thenReturn("find");
        when(failed.getThrowable()).thenReturn(failure);

        MongoDatabase database = mock(MongoDatabase.class);
        MongoCollection<GridFSFile> files = mock(MongoCollection.class, RETURNS_SELF);
        MongoCollection<BsonDocument> chunks = mock(MongoCollection.class, RETURNS_SELF);
        FindIterable<GridFSFile> find = mock(FindIterable.class, RETURNS_SELF);
        when(database.getCodecRegistry()).thenReturn(MongoClientSettings.getDefaultCodecRegistry());
        when(database.getCollection("synthetic.files", GridFSFile.class)).thenReturn(files);
        when(database.getCollection("synthetic.chunks", BsonDocument.class)).thenReturn(chunks);
        when(files.find()).thenReturn(find);
        MongoConnectorRegistry realRegistry = new MongoConnectorRegistry(GridFSBuckets.create(database, "synthetic"));
        GridFSFile empty = new GridFSFile(new BsonObjectId(new ObjectId()), "synthetic-file", 0, 255,
                new Date(0), new Document());
        AtomicInteger findCalls = new AtomicInteger();
        RuntimeException stop = new IllegalStateException(SENTINEL);
        when(find.first()).thenAnswer(call -> {
            if (findCalls.incrementAndGet() == 1) return empty;
            listener.get().commandFailed(failed);
            listener.get().commandFailed(failed);
            if (mode == Mode.RETURNED) return empty;
            throw stop;
        });
        ConnectorRegistry delegate = mock(ConnectorRegistry.class);
        AtomicReference<Optional<byte[]>> originalResult = new AtomicReference<>();
        when(delegate.artifact("synthetic-hash")).thenAnswer(call -> {
            Optional<byte[]> value = realRegistry.artifact("synthetic-hash");
            originalResult.set(value);
            return value;
        });
        ConnectorRegistry observed = witness.observe(delegate);
        ConnectorCapabilities capabilities = new ConnectorCapabilities(Set.of());
        ConnectorProvisioner provisioner = witness.observe((ConnectorProvisioner) id -> {
            Optional<byte[]> value = observed.artifact("synthetic-hash");
            assertThat(value).isSameAs(originalResult.get());
            return null;
        });
        CapabilityDeriver deriver = witness.observe((CapabilityDeriver) id -> {
            provisioner.resolve(id);
            return capabilities;
        });
        if (mode == Mode.RETURNED) {
            assertThat(deriver.derive("mysql")).isSameAs(capabilities);
            assertThat(originalResult.get()).isPresent();
            assertThat(originalResult.get().orElseThrow()).isEmpty();
        } else {
            assertThatThrownBy(() -> deriver.derive("mysql")).isSameAs(stop);
        }
        verify(delegate, times(1)).artifact("synthetic-hash");
        verify(files, times(2)).find();
        verify(find, times(2)).filter(any(Bson.class));
        verify(find, times(2)).first();
        boolean enabled = !(mode == Mode.OLD_CONSTRUCTOR || mode == Mode.EXPLICIT_OFF
                || mode == Mode.PROPERTY_DEFAULT || mode == Mode.PROPERTY_OFF);
        assertThat(causeReads).hasValue(enabled ? 1 : 0);
        verify(failure, times(enabled ? 1 : 0)).getCause();
        if (!propertyMode && mode != Mode.OUTPUT_FAILURE) {
            List<Map<String, Object>> failedEvents = events.stream()
                    .filter(event -> "command-failed".equals(event.get("kind"))).toList();
            assertThat(failedEvents).hasSize(2).allSatisfy(event -> assertThat(event)
                    .containsEntry("artifact_step", "download"));
            assertThat(failedEvents.stream().filter(event -> event.containsKey("read_failure_category")).count())
                    .isEqualTo(enabled ? 1L : 0L);
            if (enabled) assertThat(failedEvents.getFirst()).containsEntry("read_failure_category",
                    mode == Mode.UNREADABLE ? "UNCLASSIFIED" : "READ_TIMEOUT");
        }
    }
}
