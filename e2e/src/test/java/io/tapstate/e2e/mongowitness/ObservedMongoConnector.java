package io.tapstate.e2e.mongowitness;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.utils.StateListener;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.exception.TapPdkOffsetOutOfLogEx;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Test-only observation around an unchanged real Mongo connector artifact. The manifest's class path
 * gives the real implementation and its resources the same product-owned loader as this observer.
 */
@TapConnectorClass("observed-mongo-spec.json")
public final class ObservedMongoConnector implements TapConnector {
    private final TapConnector delegate;

    public ObservedMongoConnector() {
        try {
            delegate = getClass().getClassLoader().loadClass("io.tapdata.mongodb.MongodbConnector")
                    .asSubclass(TapConnector.class).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("cannot load the real Mongo connector witness", failure);
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
        delegate.registerCapabilities(functions, codecs);
        Path witness;
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("mongo-write-witness-path.txt")) {
            if (in == null) throw new AssertionError("missing write witness path");
            witness = Path.of(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (java.io.IOException failure) {
            throw new AssertionError("cannot read write witness path", failure);
        }
        var original = functions.getWriteRecordFunction();
        if (original == null) throw new AssertionError("real Mongo connector registered no writer");
        functions.supportWriteRecord(WriteTableWitness.record(witness, original));
        var stream = functions.getStreamReadFunction();
        if (stream != null) {
            Path readerWitness = optionalReaderWitness();
            functions.supportStreamRead((context, tables, offset, size, consumer) -> {
                Path tail = witness.resolveSibling(witness.getFileName() + ".tail");
                String start = "START " + offset;
                appendTailObservation(tail, start);
                String selected = String.join(",", tables);
                if (readerWitness != null) {
                    appendTailObservation(readerWitness, offsetReading("START", selected, offset));
                }
                Throwable reported = null;
                try {
                    stream.streamRead(context, tables, offset, size, readerWitness == null ? consumer
                            : observe(consumer, readerWitness, selected));
                } catch (Throwable failure) {
                    reported = failure;
                    if (readerWitness != null) {
                        try {
                            if (failure instanceof TapPdkOffsetOutOfLogEx refused) {
                                appendTailObservation(readerWitness,
                                        offsetReading("REFUSED", selected, refused.getOffset()) + "\t"
                                                + refused.getCode() + "\t"
                                                + Arrays.toString(refused.getDynamicDescriptionParameters()));
                            } else {
                                appendTailObservation(readerWitness, "ERROR\t" + selected + "\t"
                                        + failure.getClass().getName());
                            }
                        } catch (IOException observationFailure) {
                            failure.addSuppressed(observationFailure);
                        }
                    }
                    throw failure;
                } finally {
                    try {
                        appendTailObservation(tail, "END");
                        if (readerWitness != null) appendTailObservation(readerWitness, "END\t" + selected);
                    } catch (IOException observationFailure) {
                        if (readerWitness == null || reported == null) throw observationFailure;
                        reported.addSuppressed(observationFailure);
                    }
                }
            });
        }
    }

    private Path optionalReaderWitness() {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("mongo-read-witness-path.txt")) {
            return in == null ? null : Path.of(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException failure) {
            throw new AssertionError("cannot read reader witness path", failure);
        }
    }

    /** Records the exact SDK object, whose byte identity is compared with the product's durable token. */
    private static String offsetReading(String event, String selected, Object offset) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(offset);
        }
        return event + "\t" + selected + "\t" + Base64.getEncoder().encodeToString(bytes.toByteArray())
                + "\t" + offset;
    }

    /** Every SDK callback/state operation still reaches the original consumer unchanged. */
    private static StreamReadConsumer observe(StreamReadConsumer original, Path witness, String selected) {
        return new StreamReadConsumer() {
            @Override public void accept(List<TapEvent> events, Object offset) {
                original.accept(events, offset);
                try {
                    appendTailObservation(witness, offsetReading("DELIVERY", selected, offset));
                } catch (IOException failure) {
                    throw new AssertionError("cannot record completed reader delivery", failure);
                }
            }
            @Override public void streamReadStarted() { original.streamReadStarted(); }
            @Override public void streamReadEnded() { original.streamReadEnded(); }
            @Override public int getState() { return original.getState(); }
            @Override public boolean isAsyncMethodAndNoRetry() { return original.isAsyncMethodAndNoRetry(); }
            @Override public void asyncMethodAndNoRetry() { original.asyncMethodAndNoRetry(); }
            @Override public StreamReadConsumer consumer(BiConsumer<List<TapEvent>, Object> consumer) {
                original.consumer(consumer); return this;
            }
            @Override public StreamReadConsumer stateListener(StateListener<Integer> listener) {
                original.stateListener(listener); return this;
            }
        };
    }

    private static void appendTailObservation(Path tail, String observation) throws IOException {
        // Stop interrupts the stream thread. Record its return with non-interruptible file I/O so
        // observing teardown neither clears that interrupt nor turns it into a channel failure.
        try (var out = new FileOutputStream(tail.toFile(), true)) {
            out.write((observation + "\n").getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void init(TapConnectionContext context) throws Throwable {
        delegate.init(context);
    }

    @Override
    public void lightInit(TapConnectionContext context) throws Throwable {
        delegate.lightInit(context);
    }

    @Override
    public void stop(TapConnectionContext context) throws Throwable {
        delegate.stop(context);
    }

    @Override
    public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
                               Consumer<List<TapTable>> consumer) throws Throwable {
        delegate.discoverSchema(context, tables, size, consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) throws Throwable {
        return delegate.connectionTest(context, consumer);
    }

    @Override
    public int tableCount(TapConnectionContext context) throws Throwable {
        return delegate.tableCount(context);
    }
}
