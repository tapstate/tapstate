package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.FieldSchema;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.capture.TableSchema;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The PDK implementation of the read-side capture port: it provisions a connector, refuses it with a
 * code if its declared API level is incompatible, drives its registered read functions through the
 * frozen PDK contract, and projects the PDK events to the tapstate envelope. Connector-side read
 * failures and unprojectable events surface as coded connector-domain exceptions; asking a connector
 * for a read capability it does not provide is a caller invariant violation (the DSL validated the
 * connector's modes upstream) and crashes bare rather than being laundered into a code.
 *
 * <p>Snapshot reads are collected eagerly as a bounded batch. The cdc stream runs on a background
 * thread that delivers each decoded change to the listener; how a stream failure reaches the caller
 * and the backpressure that bounds the stream belong to the runtime that owns stream execution, not
 * to this port.
 */
public final class PdkCapturePort implements CapturePort {

    private static final Logger LOG = LoggerFactory.getLogger(PdkCapturePort.class);

    private static final int BATCH_SIZE = 1000;
    private static final int SAMPLE_SIZE = 10;
    private static final long SHUTDOWN_JOIN_MILLIS = 2000;

    private final ConnectorProvisioner provisioner;

    public PdkCapturePort(ConnectorProvisioner provisioner) {
        this.provisioner = provisioner;
    }

    @Override
    public CaptureBatch snapshot(CaptureConfig config) {
        PdkConnector connector = open(config);
        try {
            // Resolve the read capability before entering the drive: a non-source connector is a caller
            // invariant violation (the modes were validated upstream) and crashes bare here rather than
            // being laundered into a coded capture failure.
            BatchReadFunction batch = requireFunction(connector.functions().getBatchReadFunction());
            List<TapEvent> raw = read(connector, () -> batchRead(connector, config, batch));
            List<Envelope> rows = decodeSnapshot(connector.connectorId(), raw);
            return new PdkCaptureBatch(rows, connector);
        } catch (RuntimeException e) {
            connector.stopQuietly();
            connector.close();
            throw e;
        }
    }

    @Override
    public Subscription cdc(CaptureConfig config, CaptureListener listener) {
        PdkConnector connector = open(config);
        StreamReadFunction stream;
        try {
            stream = requireFunction(connector.functions().getStreamReadFunction());
        } catch (RuntimeException e) {
            connector.close();
            throw e;
        }
        Thread thread = new Thread(() -> streamLoop(connector, config, listener, stream),
                "tapstate-cdc-" + connector.connectorId());
        thread.setDaemon(true);
        thread.start();
        AtomicBoolean closed = new AtomicBoolean();
        return () -> {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            thread.interrupt();
            connector.stopQuietly();
            joinQuietly(thread);
            connector.close();
        };
    }

    @Override
    public ConnectionReport testConnection(CaptureConfig config) {
        PdkConnector connector = openUnscoped(config);
        try {
            Probe probe = read(connector, () -> probe(connector, config));
            DiscoveredSchema schema = toDiscoveredSchema(probe.tables());
            List<Envelope> sample = decodeSnapshot(connector.connectorId(), probe.sample());
            return new ConnectionReport(schema, sample);
        } finally {
            connector.stopQuietly();
            connector.close();
        }
    }

    @Override
    public DiscoveredSchema discoverSchema(CaptureConfig config) {
        PdkConnector connector = openUnscoped(config);
        try {
            List<TapTable> tables = read(connector, () -> discover(connector, config.streams()));
            return toDiscoveredSchema(tables);
        } finally {
            connector.stopQuietly();
            connector.close();
        }
    }

    // ---- drive helpers ---------------------------------------------------------------------------

    /**
     * Opens the connector for a drive that keeps notes: the node on the config says where they belong,
     * so the full load and the change tail of one run file under one name and read each other's.
     */
    private PdkConnector open(CaptureConfig config) {
        return open(config, ConnectorStateNamespace.of(config.node()));
    }

    /**
     * Opens the connector for a drive that keeps nothing. A connection test and a schema discovery each
     * live for the single call that made them, so there is no later drive for anything they wrote to be
     * read back by — filing it would leave a record with no reader. The node is ignored here rather than
     * assumed absent: whether these two scope state is decided at this seam, not by what a caller
     * happened to put on the config.
     */
    private PdkConnector openUnscoped(CaptureConfig config) {
        return open(config, null);
    }

    private PdkConnector open(CaptureConfig config, String stateNamespace) {
        return PdkConnector.open(config.connectorId(), provisioner.resolve(config.connectorId()), config.settings(),
                stateNamespace);
    }

    /** Inits the connector once, then batch-reads the configured streams (or every discovered stream). */
    private List<TapEvent> batchRead(PdkConnector connector, CaptureConfig config, BatchReadFunction batch) throws Throwable {
        connector.connector().init(connector.context());
        // A connector builds its read from the table's own columns, so it is handed the table as
        // discovered - with its fields - not a bare name. Discovery does not re-init: init has run.
        Map<String, TapTable> discovered = byId(discoverTables(connector, config.streams()));
        List<String> streams = config.streams().isEmpty()
                ? new ArrayList<>(discovered.keySet()) : config.streams();
        List<TapEvent> raw = new ArrayList<>();
        for (String stream : streams) {
            TapTable table = discovered.get(stream);
            if (table == null) {
                throw new IllegalStateException(
                        "stream " + stream + " was requested but the connector did not discover it");
            }
            // The connector reads by each field's PDK type, which discovery leaves unset; fill it from the
            // connector's own type mapping before the read, or the read meets a null field type.
            connector.fillFieldTypes(table);
            batch.batchRead(connector.context(), table, null, BATCH_SIZE, (events, offset) -> raw.addAll(events));
        }
        return raw;
    }

    /** Indexes discovered tables by id, keeping discovery order. */
    private static Map<String, TapTable> byId(List<TapTable> tables) {
        Map<String, TapTable> byId = new LinkedHashMap<>();
        for (TapTable table : tables) {
            byId.put(table.getId(), table);
        }
        return byId;
    }

    /** Inits the connector and discovers the given streams (empty = all). */
    private List<TapTable> discover(PdkConnector connector, List<String> streams) throws Throwable {
        connector.connector().init(connector.context());
        return discoverTables(connector, streams);
    }

    /** Discovers the given streams without initializing — the caller has already inited the connector. */
    private List<TapTable> discoverTables(PdkConnector connector, List<String> streams) throws Throwable {
        List<TapTable> tables = new ArrayList<>();
        connector.connector().discoverSchema(connector.context(), streams, Integer.MAX_VALUE, tables::addAll);
        return tables;
    }

    /** One init, then discover the schema and read a small sample — the connection-test probe. */
    private Probe probe(PdkConnector connector, CaptureConfig config) throws Throwable {
        connector.connector().init(connector.context());
        List<TapTable> tables = new ArrayList<>();
        connector.connector().discoverSchema(connector.context(), config.streams(), Integer.MAX_VALUE, tables::addAll);

        List<TapEvent> sample = new ArrayList<>();
        BatchReadFunction batch = connector.functions().getBatchReadFunction();
        if (batch != null) {
            List<String> streams = config.streams().isEmpty() ? names(tables) : config.streams();
            for (String stream : streams) {
                if (sample.size() >= SAMPLE_SIZE) {
                    break;
                }
                // The discovered table, not a bare name. A connector builds its read from the table's
                // own columns, so a descriptor carrying none reads nothing - and one whose column map
                // was never created answers a connector asking for it by throwing, inside the connector,
                // where it reads as a broken connection rather than as a descriptor we failed to pass.
                // Discovery ran above and the table is already in hand.
                TapTable descriptor = discovered(tables, stream);
                // And fill its field types, as the snapshot read does. Discovery reports the database's
                // own type name and leaves the PDK type unset, so a descriptor that skips this step
                // carries its columns with every type null - the same shape of failure one step later,
                // and thrown from inside the connector just the same.
                connector.fillFieldTypes(descriptor);
                batch.batchRead(connector.context(), descriptor, null, SAMPLE_SIZE, (events, offset) -> {
                    for (TapEvent event : events) {
                        if (sample.size() < SAMPLE_SIZE) {
                            sample.add(event);
                        }
                    }
                });
            }
        }
        return new Probe(tables, sample);
    }

    private void streamLoop(PdkConnector connector, CaptureConfig config, CaptureListener listener, StreamReadFunction stream) {
        try {
            connector.underLoader(() -> {
                connector.connector().init(connector.context());
                // streamRead is handed only stream names, so the connector reads each changed table's
                // schema off the context's table map and its resume position off the offset argument -
                // neither is assembled by open(). Discover-and-fill the tables onto the context the way the
                // snapshot read does, and derive a current stream position, or the tail cannot decode a
                // change (null table map) or even position (a null offset drives a schema-only recovery
                // that has no stored offset to recover from).
                Map<String, TapTable> tables = byId(discoverTables(connector, config.streams()));
                tables.values().forEach(connector::fillFieldTypes);
                connector.context().setTableMap(tableMap(tables));
                Object startOffset = startOffset(connector);
                StreamReadConsumer consumer = StreamReadConsumer.create((events, offset) -> {
                    for (TapEvent event : events) {
                        // A change stream also carries control events (heartbeats and the like) that signal
                        // the tail is alive but carry no row; they are not decodable changes, so skip them.
                        if (event instanceof ControlEvent) {
                            continue;
                        }
                        listener.onEvent(TapEventCodec.decodeChange(event));
                    }
                });
                stream.streamRead(connector.context(), config.streams(), startOffset, BATCH_SIZE, consumer);
                return null;
            });
        } catch (Throwable t) {
            // The cdc stream runs on this daemon thread; its failure cannot be returned to the caller, so it
            // is delivered through the listener's error channel for the runtime to observe and drive the
            // pipeline into an error state. It is logged as well, so a dead stream is visible in the logs.
            //
            // Coded on the way out, the way the snapshot side of this port already codes what it catches.
            // This is the last place that knows what the failure was: nothing above understands a
            // connector's own exception types, and the code a user is finally shown is built by walking the
            // cause chain for something coded. Handed on uncoded, a connector that refused to start for a
            // reason it stated precisely arrives as "the job died", with the sentence naming what to
            // reconfigure surviving only in a log line.
            LOG.warn("cdc stream for connector {} stopped on a failure", connector.connectorId(), t);
            listener.onError(t instanceof TapstateException coded
                    ? coded
                    : new TapstateException(ConnectorError.CAPTURE_FAILED,
                            Map.of("connector", connector.connectorId(), "detail", detail(t)), t));
        }
    }

    /**
     * The discovered tables, in the shape a connector reads them off its context: by name, and by a walk
     * over every entry.
     *
     * <p>Handing over a lookup alone is not a smaller version of this - it is a map that throws. The
     * frozen contract implements the walk as a default that raises, so a connector which expands what it
     * was asked to watch, rather than asking for one name at a time, dies at the very start of its stream
     * with nothing decoded. A snapshot over the same source is unaffected, because it never walks; the
     * pair reads from outside as "this source cannot do change data capture at all", which is why a
     * witness admitting only snapshot rows cannot see it.
     *
     * <p>Both readings are views of the one map, so what the walk yields cannot drift from what the
     * lookup answers.
     */
    private static KVReadOnlyMap<TapTable> tableMap(Map<String, TapTable> tables) {
        return new KVReadOnlyMap<>() {
            @Override
            public TapTable get(String name) {
                return tables.get(name);
            }

            @Override
            public Iterator<Entry<TapTable>> iterator() {
                java.util.Iterator<Map.Entry<String, TapTable>> entries = tables.entrySet().iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return entries.hasNext();
                    }

                    @Override
                    public Entry<TapTable> next() {
                        Map.Entry<String, TapTable> entry = entries.next();
                        return new TableEntry(entry.getKey(), entry.getValue());
                    }
                };
            }
        };
    }

    /** One entry of the table map, in the shape the walk yields. */
    private record TableEntry(String key, TapTable table) implements Entry<TapTable> {

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public TapTable getValue() {
            return table;
        }
    }

    /**
     * The connector's current stream position, so the tail resumes from now rather than driving a
     * schema-only recovery that has no stored offset. Null when the connector declares no offset function.
     */
    private static Object startOffset(PdkConnector connector) throws Throwable {
        TimestampToStreamOffsetFunction offset = connector.functions().getTimestampToStreamOffsetFunction();
        return offset == null ? null : offset.timestampToStreamOffset(connector.context(), null);
    }

    /** Runs a read action under the connector loader, mapping a connector-side failure to a code. */
    private static <T> T read(PdkConnector connector, PdkConnector.Action<T> action) {
        try {
            return connector.underLoader(action);
        } catch (TapstateException e) {
            throw e;
        } catch (Throwable t) {
            throw new TapstateException(ConnectorError.CAPTURE_FAILED,
                    Map.of("connector", connector.connectorId(), "detail", detail(t)), t);
        }
    }

    /** Projects raw snapshot rows to envelopes; a codec refusal is a projection failure, not a read failure. */
    private static List<Envelope> decodeSnapshot(String connectorId, List<TapEvent> raw) {
        List<Envelope> rows = new ArrayList<>(raw.size());
        try {
            for (TapEvent event : raw) {
                rows.add(TapEventCodec.decodeSnapshotRow(event));
            }
        } catch (RuntimeException e) {
            throw new TapstateException(ConnectorError.PROJECTION_FAILED,
                    Map.of("connector", connectorId, "detail", detail(e)), e);
        }
        return rows;
    }

    private static DiscoveredSchema toDiscoveredSchema(List<TapTable> tables) {
        List<TableSchema> mapped = new ArrayList<>(tables.size());
        for (TapTable table : tables) {
            List<FieldSchema> fields = new ArrayList<>();
            if (table.getNameFieldMap() != null) {
                table.getNameFieldMap().forEach((name, field) -> fields.add(new FieldSchema(name, field.getDataType())));
            }
            mapped.add(new TableSchema(table.getId(), fields));
        }
        return new DiscoveredSchema(mapped);
    }

    private static List<String> names(List<TapTable> tables) {
        List<String> names = new ArrayList<>(tables.size());
        for (TapTable table : tables) {
            names.add(table.getId());
        }
        return names;
    }

    private static <T> T requireFunction(T function) {
        if (function == null) {
            throw new IllegalStateException("connector does not provide the requested read capability");
        }
        return function;
    }

    /** Waits a bounded time for the stream thread to exit before its loader is closed. */
    private static void joinQuietly(Thread thread) {
        try {
            thread.join(SHUTDOWN_JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String detail(Throwable t) {
        return t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName();
    }

    /** The connection-test probe result: the discovered schema tables and a small raw sample. */
    /**
     * The discovered table for {@code stream}, or a descriptor declaring no columns when discovery did
     * not report it. Declaring no columns is a statement a connector can read; being unable to answer
     * what columns there are is not, which is why the fallback is never a raw descriptor.
     */
    private static TapTable discovered(List<TapTable> tables, String stream) {
        for (TapTable table : tables) {
            if (stream.equals(table.getId())) {
                return table;
            }
        }
        return TargetTapTable.bare(stream);
    }

    private record Probe(List<TapTable> tables, List<TapEvent> sample) {
    }
}
