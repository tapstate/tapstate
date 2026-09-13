package io.tapstate.e2e.mongowitness;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
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
            functions.supportStreamRead((context, tables, offset, size, consumer) -> {
                Path tail = witness.resolveSibling(witness.getFileName() + ".tail");
                String start = "START " + offset;
                appendTailObservation(tail, start);
                try {
                    stream.streamRead(context, tables, offset, size, consumer);
                } finally {
                    appendTailObservation(tail, "END");
                }
            });
        }
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
