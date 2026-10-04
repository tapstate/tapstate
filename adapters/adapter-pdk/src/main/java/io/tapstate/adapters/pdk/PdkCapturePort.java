package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.FieldSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.SharedNotes;
import io.tapstate.spi.capture.SnapshotSession;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.capture.TableSchema;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.connector.common.ReleaseExternalFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.apis.functions.connector.target.FlushOffsetFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * The PDK implementation of the read-side capture port: it provisions a connector, refuses it with a
 * code if its declared API level is incompatible, drives its registered read functions through the
 * frozen PDK contract, and projects the PDK events to the tapstate envelope. Connector-side read
 * failures and unprojectable events surface as coded connector-domain exceptions; asking a connector
 * for a read capability it does not provide is a caller invariant violation (the DSL validated the
 * connector's modes upstream) and crashes bare rather than being laundered into a code.
 *
 * <p>A snapshot read runs on a background thread of its own, staying a few of the connector's batches ahead
 * of whoever takes the rows, who decodes each batch as it takes it; the connector's read loop waits when it
 * gets further ahead than that, so a table of any size holds the same few thousand rows at once. The cdc
 * stream runs on a background thread that delivers each decoded change to the listener; how a
 * stream failure reaches the caller and the backpressure that bounds the stream belong to the runtime that
 * owns stream execution, not to this port.
 */
public final class PdkCapturePort implements CapturePort, SnapshotSession.Provider {

    private static final Logger LOG = LoggerFactory.getLogger(PdkCapturePort.class);

    private static final int BATCH_SIZE = 1000;
    private static final int SAMPLE_SIZE = 10;
    private static final long SHUTDOWN_JOIN_MILLIS = 2000;
    private static final long CDC_SHUTDOWN_GRACE_MILLIS = 5000;

    /** The longest an Oracle LogMiner start waits for connector initialization and schema discovery. */
    public static final Duration DEFAULT_PREFLIGHT_TIMEOUT = Duration.ofSeconds(30);

    /**
     * How often a cdc subscription hands its connector the latest position it was told the source may
     * release. Often enough that what a source keeps past that position stays a few seconds' worth, and
     * seldom enough that re-handing an unchanged position -- which is what most intervals do -- costs the
     * source one small write every few seconds.
     */
    private static final Duration DEFAULT_ACKNOWLEDGE_INTERVAL = Duration.ofSeconds(5);

    /**
     * The least time between two warnings about one subscription's acknowledgements failing. A source that
     * refuses one usually refuses each one after it, once an interval, and a line per refusal would bury
     * the log under a single fact; the listener still hears every one of them.
     */
    private static final long ACKNOWLEDGE_WARNING_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();

    /**
     * The longest a release waits for its connector to let go of what it set up on the source. Clearing a
     * pipeline waits on it, on the thread that also keeps renewing this member's claims on the pipelines it
     * drives -- every 10 s on a 30 s lease by default -- so a release has to leave that room. Ten seconds is
     * long enough for a source that answers, where letting go of a slot takes well under one, and for a
     * PostgreSQL driver's own 10 s connect timeout to answer for a source that cannot be reached.
     */
    private static final Duration DEFAULT_RELEASE_TIMEOUT = Duration.ofSeconds(10);

    /**
     * The notes a connector keeps to name something it set up on its source that its release function lets go
     * of: the postgres connector, and the connectors built on it, keep their replication slot's name under
     * this key. Read for any connector, only to say what a release asks to let go of, and what is left there
     * when it fails.
     */
    static final List<String> NOTES_NAMING_SOURCE_RESOURCES = List.of("tapdata_pg_slot");

    private final ConnectorProvisioner provisioner;
    private final KeyedStateStore stateStore;
    private final Duration preflightTimeout;
    private final long acknowledgeIntervalNanos;
    private final LongSupplier nanoClock;
    private final Duration releaseTimeout;

    /** For the drives that keep nothing: no store, so nothing a connector writes is filed anywhere. */
    public PdkCapturePort(ConnectorProvisioner provisioner) {
        this(provisioner, null, DEFAULT_PREFLIGHT_TIMEOUT);
    }

    public PdkCapturePort(ConnectorProvisioner provisioner, KeyedStateStore stateStore) {
        this(provisioner, stateStore, DEFAULT_PREFLIGHT_TIMEOUT);
    }

    public PdkCapturePort(
            ConnectorProvisioner provisioner, KeyedStateStore stateStore, Duration preflightTimeout) {
        this(provisioner, stateStore, preflightTimeout, DEFAULT_ACKNOWLEDGE_INTERVAL, System::nanoTime);
    }

    /**
     * As above, with how often a cdc subscription hands its connector the latest acknowledged position, and
     * the clock that interval is measured on -- for a case that has to cross the interval without waiting
     * it out.
     */
    PdkCapturePort(ConnectorProvisioner provisioner, KeyedStateStore stateStore, Duration preflightTimeout,
            Duration acknowledgeInterval, LongSupplier nanoClock) {
        this(provisioner, stateStore, preflightTimeout, acknowledgeInterval, nanoClock, DEFAULT_RELEASE_TIMEOUT);
    }

    /** As above, with how long a release waits for its connector -- for a case that cannot wait a minute. */
    PdkCapturePort(ConnectorProvisioner provisioner, KeyedStateStore stateStore, Duration preflightTimeout,
            Duration acknowledgeInterval, LongSupplier nanoClock, Duration releaseTimeout) {
        this.provisioner = provisioner;
        this.stateStore = stateStore;
        this.preflightTimeout = requirePositive(preflightTimeout, "preflightTimeout");
        this.acknowledgeIntervalNanos = requirePositive(acknowledgeInterval, "acknowledgeInterval").toNanos();
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.releaseTimeout = requirePositive(releaseTimeout, "releaseTimeout");
    }

    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    @Override
    public CaptureBatch snapshot(CaptureConfig config) {
        PdkConnector connector = open(config);
        BatchReadFunction batch;
        try {
            // Resolve the read capability before entering the drive: a non-source connector is a caller
            // invariant violation (the modes were validated upstream) and crashes bare here rather than
            // being laundered into a coded capture failure.
            batch = requireFunction(connector.functions().getBatchReadFunction());
        } catch (RuntimeException e) {
            connector.stopQuietly();
            connector.close();
            throw e;
        }
        // The batch stops and closes the connector itself when the read fails before its seam is taken, and
        // on close afterwards.
        return PdkCaptureBatch.start(
                connector,
                reading -> read(connector, () -> batchRead(connector, config, batch, reading)),
                "tapstate-snapshot-" + connector.connectorId());
    }

    /** One initialized connector serves every table of a chained snapshot round. */
    @Override
    public SnapshotSession snapshotSession(CaptureConfig config) {
        PdkConnector connector = open(config);
        try {
            BatchReadFunction batch = requireFunction(connector.functions().getBatchReadFunction());
            return new SharedSnapshotSession(connector, config, batch);
        } catch (RuntimeException | Error failure) {
            connector.close();
            throw failure;
        }
    }

    private final class SharedSnapshotSession implements SnapshotSession {

        private final PdkConnector connector;
        private final CaptureConfig config;
        private final BatchReadFunction batch;
        private final AtomicReference<PreparedSnapshot> prepared = new AtomicReference<>();
        private PdkCaptureBatch active;
        private volatile boolean closed;

        private SharedSnapshotSession(PdkConnector connector, CaptureConfig config, BatchReadFunction batch) {
            this.connector = connector;
            this.config = config;
            this.batch = batch;
        }

        @Override
        public CaptureBatch read(String table) {
            if (closed) {
                throw new java.util.concurrent.CancellationException("the snapshot session was closed");
            }
            PdkCaptureBatch opened = PdkCaptureBatch.start(connector,
                    reading -> PdkCapturePort.read(connector, () -> {
                        PreparedSnapshot snapshot = prepared.get();
                        if (snapshot == null) {
                            snapshot = prepareSnapshot(connector, config);
                            prepared.set(snapshot);
                        }
                        reading.seamSampled(snapshot.seam());
                        readTable(connector, snapshot, table, batch, reading);
                        return null;
                    }),
                    "tapstate-snapshot-" + connector.connectorId(), false);
            synchronized (this) {
                if (!closed) {
                    active = opened;
                    return opened;
                }
            }
            opened.close();
            throw new java.util.concurrent.CancellationException("the snapshot session was closed");
        }

        @Override
        public void close() {
            PdkCaptureBatch reading;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                reading = active;
            }
            if (reading != null) {
                reading.requestClose();
            }
            connector.stopQuietly();
            try {
                if (reading != null) {
                    reading.close();
                }
            } finally {
                connector.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>A {@link CaptureStart.Resume} is read back into the object the connector issued and handed to its
     * stream read as the position to pick up from; a {@link CaptureStart.Present} samples the connector's
     * current position instead, so the tail begins at now because the caller asked for now. The token
     * itself never reaches a connector: it is this adapter's rendering of the connector's own offset, and
     * a connector only accepts one of those back.
     *
     * <p>A recorded position this connector can no longer read is a coded refusal, raised before anything
     * is opened. Beginning at the present instead would be the silent form of the same failure — every
     * change made since the position was recorded dropped, with nothing thrown and nothing logged.
     *
     * <p>The subscription's {@link Subscription#acknowledge acknowledge} records the position and returns;
     * the stream's next delivery hands it to the connector's flush function, on the connector's own delivery
     * thread, and reports the outcome to {@code listener} -- see {@code Acknowledgements}.
     */
    @Override
    public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
        OracleLogMinerIdentifiers.validateConfigured(config);
        PdkConnector connector = open(config);
        StreamReadFunction stream;
        Object resumeAt;
        try {
            stream = requireFunction(connector.functions().getStreamReadFunction());
            // A start named as an instant is one only the source can resolve, and whether it can at all is
            // knowable here, from the functions it registered, before a connection is opened. Refusing now
            // is the point: the two ways of carrying on without that function -- begin at the present, or
            // begin at the source's oldest -- both yield a stream that runs, reports healthy and reads a
            // different span than the caller asked for, which is the failure this whole start exists to
            // make impossible. A connector that lacks it is not a source that broke, so this is a refusal
            // to whoever asked rather than an error delivered later on the stream's channel.
            if (startAt(start) != null && connector.functions().getTimestampToStreamOffsetFunction() == null) {
                throw new TapstateException(ConnectorError.CAPABILITY_MISSING,
                        Map.of("connector", connector.connectorId(),
                                "capability", "timestamp-to-stream-offset"), null);
            }
            // Read the recorded position back here, on the caller's thread, rather than inside the stream
            // loop: the loop's catch-all delivers everything it catches as a coded connector read failure,
            // which would report "the source failed" for what is really a position this build cannot read.
            // It needs the connector's loader, not an inited connector, so this is the earliest point.
            resumeAt = start instanceof CaptureStart.Resume resume
                    ? ConnectorOffsetCodec.fromToken(connector.connectorId(), resume.position().token(),
                            connector.connector().getClass().getClassLoader())
                    : null;
        } catch (RuntimeException e) {
            connector.close();
            throw e;
        }
        Long startAt = startAt(start);
        // Keep connector initialization and streamRead on the same worker, while waiting for LogMiner's
        // discovery-based preflight before returning a subscription. An unsupported table or column then
        // refuses the start instead of letting the pipeline report RUNNING before its tail fails.
        CompletableFuture<Void> preflight = OracleLogMinerIdentifiers.appliesTo(config)
                ? new CompletableFuture<>() : null;
        CdcDelivery delivery = new CdcDelivery();
        Acknowledgements acknowledgements = new Acknowledgements(connector, listener,
                connector.functions().getFlushOffsetFunction(), acknowledgeIntervalNanos, nanoClock);
        Thread thread = new Thread(
                () -> streamLoop(connector, config, resumeAt, startAt, listener, stream, preflight,
                        acknowledgements, delivery),
                "tapstate-cdc-" + connector.connectorId());
        thread.setDaemon(true);
        thread.start();
        awaitPreflight(preflight, connector, thread, delivery);
        return new Subscription() {
            @Override
            public void acknowledge(SourcePosition durable) {
                acknowledgements.offer(durable);
            }

            @Override
            public void close() {
                // Before anything else: a source may still deliver on its way down, and a delivery made
                // after close must hand it nothing.
                acknowledgements.close();
                if (!delivery.cancel()) {
                    return;
                }
                shutDown(connector, thread, CDC_SHUTDOWN_GRACE_MILLIS);
            }
        };
    }

    /** Waits until LogMiner's worker has accepted its discovered identifiers, cleaning up a refusal. */
    private void awaitPreflight(
            CompletableFuture<Void> preflight, PdkConnector connector, Thread thread, CdcDelivery delivery) {
        if (preflight == null) {
            return;
        }
        try {
            preflight.get(preflightTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException failure) {
            delivery.cancel();
            shutDown(connector, thread, 0);
            throw new TapstateException(ConnectorError.LOGMINER_PREFLIGHT_TIMEOUT,
                    Map.of("connector", connector.connectorId(),
                            "timeout", preflightTimeout.toMillis() + "ms"), failure);
        } catch (InterruptedException failure) {
            delivery.cancel();
            shutDown(connector, thread, 0);
            Thread.currentThread().interrupt();
            throw new TapstateException(ConnectorError.CAPTURE_FAILED,
                    Map.of("connector", connector.connectorId(),
                            "detail", "change-capture preflight was interrupted"), failure);
        } catch (ExecutionException failure) {
            delivery.cancel();
            shutDown(connector, thread, 0);
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException(cause);
        }
    }

    /** Lets a cancelled read release its cursor before stopping the client; aborts remain bounded. */
    private static void shutDown(PdkConnector connector, Thread thread, long graceMillis) {
        if (graceMillis > 0) {
            joinQuietly(thread, graceMillis);
        }
        if (thread.isAlive()) {
            thread.interrupt();
        }
        connector.stopQuietly();
        joinQuietly(thread, SHUTDOWN_JOIN_MILLIS);
        connector.close();
    }

    /** Cancels at the consumer boundary without interrupting the source cursor's cleanup. */
    private static final class CdcDelivery {
        private final Set<Thread> active = new HashSet<>();
        private volatile boolean closed;

        synchronized boolean cancel() {
            if (closed) {
                return false;
            }
            closed = true;
            // Wake a listener blocked on downstream capacity, including connector-owned delivery threads.
            active.forEach(Thread::interrupt);
            return true;
        }

        void accept(Runnable batch) {
            Thread current = Thread.currentThread();
            synchronized (this) {
                if (closed) {
                    throw new CancellationException("the change capture was closed");
                }
                active.add(current);
            }
            try {
                batch.run();
                if (closed) {
                    throw new CancellationException("the change capture was closed");
                }
            } finally {
                synchronized (this) {
                    active.remove(current);
                    if (closed) {
                        // Our listener wake-up must not prevent a connector's finally block doing I/O.
                        Thread.interrupted();
                    }
                }
            }
        }
    }

    @Override
    public ConnectionReport testConnection(CaptureConfig config) {
        PdkConnector connector = openUnscoped(config);
        try {
            Probe probe = read(connector, () -> probe(connector, config));
            DiscoveredSchema schema = toDiscoveredSchema(probe.tables());
            List<Envelope> sample = decodeSnapshot(connector, probe.sample(), byId(probe.tables()));
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

    /**
     * {@inheritDoc}
     *
     * <p>The connector is opened over the notes a read over {@code config} opens -- the physical capture's for
     * a shared one, carried over from its earlier nodes' where it never took them over, and the node's own for
     * a source read directly -- so its release function finds what it recorded there, a replication slot's
     * name, say, and lets go of that on the source. A connector that registered no release function set
     * nothing up there that it knows to let go of.
     *
     * <p>What the notes name on the source is read before the connector is asked, so that the release can be
     * said, and a release the source refuses can still say what is left there to remove by hand. Which notes
     * name such a thing is the connector's own knowledge; {@link #NOTES_NAMING_SOURCE_RESOURCES} lists what is
     * known of it, and a connector keeping nothing under it is still released and answered for, only without
     * the names.
     *
     * <p>The connector is driven on a thread of its own and waited for a bounded time. One that has not
     * answered by then is answered for as a refusal and left to finish, or not, by itself -- without its notes:
     * from then on they refuse it, because they are the caller's to drop, and a run started over the same
     * source since may be keeping its own in them.
     */
    @Override
    public Optional<TapstateException> release(CaptureConfig config) {
        Objects.requireNonNull(config, "config");
        if (stateStore == null || (config.node() == null && config.sharedNotes() == null)) {
            // Nothing was kept anywhere a later drive could read it, so nothing was set up through it either.
            return Optional.empty();
        }
        FencedStateStore notes = new FencedStateStore(stateStore);
        AtomicReference<List<String>> named = new AtomicReference<>();
        CompletableFuture<Void> released = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                PdkConnector connector = PdkConnector.open(config.connectorId(),
                        provisioner.resolve(config.connectorId()), config.settings(), config.node(), notes,
                        config.sharedNotes());
                try {
                    List<String> names = namedOnTheSource(connector);
                    named.set(names);
                    ReleaseExternalFunction release = connector.functions().getReleaseExternalFunction();
                    if (release != null) {
                        LOG.info("connector {} is letting go of what it set up on its source to read changes ({})",
                                config.connectorId(),
                                names.isEmpty() ? "nothing its notes name" : String.join(", ", names));
                        connector.underLoader(() -> {
                            release.release(connector.context());
                            return null;
                        });
                    }
                } finally {
                    connector.stopQuietly();
                    connector.close();
                }
                released.complete(null);
            } catch (Throwable failure) {
                released.completeExceptionally(failure);
                if (failure instanceof VirtualMachineError fatal) {
                    throw fatal;
                }
            }
        }, "tapstate-release-" + config.connectorId());
        thread.setDaemon(true);
        thread.start();
        Throwable failure;
        try {
            released.get(releaseTimeout.toNanos(), TimeUnit.NANOSECONDS);
            return Optional.empty();
        } catch (ExecutionException refused) {
            failure = refused.getCause();
            if (failure instanceof VirtualMachineError fatal) {
                throw fatal;
            }
        } catch (TimeoutException silent) {
            notes.fence();
            thread.interrupt();
            failure = new TimeoutException("the source did not answer within " + releaseTimeout.toMillis() + " ms");
        } catch (InterruptedException interrupted) {
            notes.fence();
            thread.interrupt();
            Thread.currentThread().interrupt();
            failure = interrupted;
        }
        List<String> left = named.get();
        return Optional.of(new TapstateException(ConnectorError.RELEASE_FAILED,
                Map.of("connector", config.connectorId(), "detail", detail(failure),
                        "resources", left == null ? "whatever its notes name, which could not be read"
                                : left.isEmpty() ? "nothing its notes name" : String.join(", ", left)),
                failure));
    }

    /**
     * What the notes kept under {@code namespaces} name on the source, for a caller that has no connector to open
     * over them -- one clearing a capture nothing defined reads any more. Best effort: a value that cannot be read
     * is passed over, because this only ever feeds what is said.
     */
    public static List<String> namedIn(KeyedStateStore store, List<String> namespaces) {
        Objects.requireNonNull(store, "store");
        java.util.LinkedHashSet<String> named = new java.util.LinkedHashSet<>();
        for (String namespace : namespaces) {
            for (String key : NOTES_NAMING_SOURCE_RESOURCES) {
                try {
                    store.load(namespace, key).map(ConnectorStateCodec::decode).map(String::valueOf)
                            .ifPresent(named::add);
                } catch (RuntimeException unreadable) {
                    // Only the warning loses this name.
                }
            }
        }
        return List.copyOf(named);
    }

    /** What {@code connector}'s notes name on its source, read as its own drive would read them. */
    private static List<String> namedOnTheSource(PdkConnector connector) {
        List<String> named = new ArrayList<>();
        for (String key : NOTES_NAMING_SOURCE_RESOURCES) {
            try {
                Object value = connector.context().getStateMap().get(key);
                if (value != null) {
                    named.add(String.valueOf(value));
                }
            } catch (RuntimeException unreadable) {
                // Only the refusal loses this name; the release itself goes ahead either way.
            }
        }
        return List.copyOf(named);
    }

    /**
     * The notes a release reads and writes through, for as long as its caller waits on it. Once the caller has
     * given up, every access refuses, so a connector still running past that point can neither bring back
     * notes the caller has dropped since nor read ones a later run has started keeping.
     */
    private static final class FencedStateStore implements KeyedStateStore {

        private final KeyedStateStore store;
        private volatile boolean fenced;

        private FencedStateStore(KeyedStateStore store) {
            this.store = store;
        }

        void fence() {
            fenced = true;
        }

        private KeyedStateStore store() {
            if (fenced) {
                throw new IllegalStateException("the release was given up on, and its notes are no longer its own");
            }
            return store;
        }

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            return store().load(namespace, key);
        }

        @Override
        public Map<String, byte[]> loadAll(String namespace, java.util.Collection<String> keys) {
            return store().loadAll(namespace, keys);
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            store().save(namespace, key, state);
        }

        @Override
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
            return store().saveIfAbsent(namespace, key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            store().delete(namespace, key);
        }

        @Override
        public void dropNamespace(String namespace) {
            store().dropNamespace(namespace);
        }

        @Override
        public long count(String namespace) {
            return store().count(namespace);
        }
    }

    // ---- drive helpers ---------------------------------------------------------------------------

    /**
     * Opens the connector for a drive that keeps notes: the node on the config says where they belong,
     * so the full load and the change tail of one run file under one name and read each other's.
     */
    private PdkConnector open(CaptureConfig config) {
        return open(config, config.node(), config.sharedNotes());
    }

    /**
     * Opens the connector for a drive that keeps nothing. A connection test and a schema discovery each
     * live for the single call that made them, so there is no later drive for anything they wrote to be
     * read back by — filing it would leave a record with no reader. The node is ignored here rather than
     * assumed absent: whether these two scope state is decided at this seam, not by what a caller
     * happened to put on the config.
     */
    private PdkConnector openUnscoped(CaptureConfig config) {
        return open(config, null, null);
    }

    private PdkConnector open(CaptureConfig config, PipelineNode node, SharedNotes notes) {
        return PdkConnector.open(config.connectorId(), provisioner.resolve(config.connectorId()), config.settings(),
                node, stateStore, notes);
    }

    /**
     * Inits the connector once, discovers its tables, samples the seam, then batch-reads the configured
     * streams (or every discovered stream) into {@code reading}, a batch at a time as the connector hands
     * them over.
     *
     * <p>The seam is sampled <em>before</em> the first row is read, which is what makes the join to the
     * change tail gapless. A change made while the snapshot runs then falls after the seam and is
     * re-delivered by the tail, and the idempotent write downstream absorbs that overlap. Sampled after
     * the read instead, every change made during it would fall before the seam and never be delivered at
     * all -- the same shape of loss, but silent. It is handed over the moment it is taken, rather than
     * with the rows, because it is only the right position while it is the one taken before the first row.
     *
     * <p>Each of the connector's batches is decoded when it is taken, against the tables it was read from --
     * not here, inside the connector's own read loop, where a refusal would be the connector's to wrap or to
     * swallow before it reached anybody. A connector's own way back from a converted value reads the column's
     * declared type to decide what to rebuild, so a row decoded without its table can be written to a target
     * of the same kind and still land as text.
     */
    private Void batchRead(PdkConnector connector, CaptureConfig config, BatchReadFunction batch,
            PdkCaptureBatch reading) throws Throwable {
        PreparedSnapshot snapshot = prepareSnapshot(connector, config);
        reading.seamSampled(snapshot.seam());
        List<String> streams = config.streams().isEmpty()
                ? new ArrayList<>(snapshot.discovered().keySet()) : config.streams();
        for (String stream : streams) {
            readTable(connector, snapshot, stream, batch, reading);
        }
        return null;
    }

    private PreparedSnapshot prepareSnapshot(PdkConnector connector, CaptureConfig config) throws Throwable {
        connector.connector().init(connector.context());
        // A connector builds its read from the table's own columns, so it is handed the table as
        // discovered - with its fields - not a bare name. Discovery does not re-init: init has run.
        Map<String, TapTable> discovered = byId(discoverTables(connector, config.streams()));
        discovered.values().forEach(connector::fillFieldTypes);
        connector.context().setTableMap(tableMap(discovered));
        // Position discovery may inspect the selected tables too. Populate their context first, while
        // still sampling before any snapshot row is read so the snapshot-to-stream transition has no gap.
        return new PreparedSnapshot(discovered, declaredTypes(discovered),
                position(connector, startOffset(connector, null)));
    }

    private void readTable(PdkConnector connector, PreparedSnapshot snapshot, String stream,
            BatchReadFunction batch, PdkCaptureBatch reading) throws Throwable {
        TapTable table = snapshot.discovered().get(stream);
        if (table == null) {
            throw new IllegalStateException(
                    "stream " + stream + " was requested but the connector did not discover it");
        }
        batch.batchRead(connector.context(), table, null, BATCH_SIZE, (events, offset) -> {
            if (events.isEmpty()) {
                return;
            }
            // A copy: the list is the connector's, and is decoded only once somebody takes it.
            List<TapEvent> handed = new ArrayList<>(events);
            reading.rowsRead(() -> decodeSnapshotRows(connector, handed, snapshot.declared()));
        });
    }

    private record PreparedSnapshot(Map<String, TapTable> discovered,
            Map<String, Map<String, String>> declared, Optional<SourcePosition> seam) {
    }

    /**
     * What the source's own schema calls each column of the table this event came from, or empty where it
     * describes none.
     *
     * <p>This is the one thing a connector's way back from a converted value has to go on. Its own way in
     * turned a driver type into something portable; the way back is handed the portable value and this
     * name, and rebuilds the driver type from the pair. Empty is a real answer - a table nothing
     * discovered, a connector whose schema names no types - and it means the target writes the portable
     * value, which is what it would have been handed anyway.
     *
     * <p><b>Read off the tables once, not once per row.</b> It depends on the table alone, and both loops
     * that consult it run once per event: worked out inside them, a wide table's whole field map is walked
     * and copied for every row read, on the hottest path this adapter has.
     */
    private static Map<String, Map<String, String>> declaredTypes(Map<String, TapTable> tables) {
        Map<String, Map<String, String>> byTable = new LinkedHashMap<>();
        tables.forEach((id, table) -> {
            if (table == null || table.getNameFieldMap() == null) {
                return;
            }
            Map<String, String> declared = new LinkedHashMap<>();
            table.getNameFieldMap().forEach((column, field) -> {
                if (field != null && field.getDataType() != null) {
                    declared.put(column, field.getDataType());
                }
            });
            byTable.put(id, declared);
        });
        return byTable;
    }

    /** What that reading says about the table this event came from, or empty where it describes none. */
    private static Map<String, String> declaredTypes(
            Map<String, Map<String, String>> byTable, TapEvent event) {
        String tableId = event instanceof TapBaseEvent based ? based.getTableId() : null;
        return tableId == null ? Map.of() : byTable.getOrDefault(tableId, Map.of());
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

    /** Initializes a stream connector and places its discovered, typed tables on its context. */
    private Map<String, TapTable> prepareStream(PdkConnector connector, CaptureConfig config) throws Throwable {
        connector.connector().init(connector.context());
        List<TapTable> discovered = discoverTables(connector, config.streams());
        OracleLogMinerIdentifiers.validateDiscovered(config, discovered);
        Map<String, TapTable> tables = byId(discovered);
        tables.values().forEach(connector::fillFieldTypes);
        connector.context().setTableMap(tableMap(tables));
        return tables;
    }

    private void streamLoop(PdkConnector connector, CaptureConfig config, Object resumeAt, Long startAt,
            CaptureListener listener, StreamReadFunction stream, CompletableFuture<Void> preflight,
            Acknowledgements acknowledgements, CdcDelivery delivery) {
        try {
            connector.underLoader(() -> {
                // streamRead is handed only stream names, so the connector reads each changed table's
                // schema off the context's table map and its resume position off the offset argument -
                // neither is assembled by open(). Discover-and-fill the tables onto the context the way the
                // snapshot read does, and derive a current stream position, or the tail cannot decode a
                // change (null table map) or even position (a null offset drives a schema-only recovery
                // that has no stored offset to recover from).
                Map<String, TapTable> tables = prepareStream(connector, config);
                if (preflight != null) {
                    preflight.complete(null);
                }
                // Resuming uses the position the caller recorded; every other start asks the connector to
                // name one, which also keeps a null offset out of the connector -- that drives a
                // schema-only recovery with no stored offset to recover from. Which position it names is
                // the instant it is handed: none for the present, the caller's for an instant start.
                Object startOffset = resumeAt != null ? resumeAt : startOffset(connector, startAt);
                if (listener instanceof CaptureStartedListener started) {
                    position(connector, startOffset).ifPresent(started::onStart);
                }
                Map<String, Map<String, String>> declared = declaredTypes(tables);
                StreamReadConsumer consumer = StreamReadConsumer.create((events, offset) -> {
                    // Every delivery is the moment to hand the connector what its source may release: this
                    // is the connector's own delivery thread, the one thread any connector can safely be
                    // told on. A delivery of heartbeats alone counts too, which keeps a quiet stream
                    // releasing.
                    // Before the batch rather than after it, because handing a batch over can hold this
                    // thread for as long as the recipient needs room, and the release is due regardless --
                    // and outside the delivery a close cancels, so a close wakes the hand-over and never the
                    // source's own call.
                    acknowledgements.applyIfDue();
                    delivery.accept(() -> {
                        // A change stream also carries control events (heartbeats and the like) that signal
                        // the tail is alive but carry no row; they are not decodable changes, so skip them.
                        List<TapEvent> changes = new ArrayList<>(events.size());
                        for (TapEvent event : events) {
                            if (!(event instanceof ControlEvent)) {
                                changes.add(event);
                            }
                        }
                        // The batch goes over whole, with the one offset the source named for it. The offset
                        // means the source had read to here once this entire batch was handed over, so it
                        // belongs to the batch and not to any change inside it; and the batch itself is worth
                        // keeping, because everything downstream that costs per act rather than per change --
                        // writing the changes down above all -- costs one act per batch only while the batch
                        // still exists.
                        List<Envelope> decoded = new ArrayList<>(changes.size());
                        for (TapEvent change : changes) {
                            decoded.add(TapEventCodec.decodeChange(
                                    change, connector.codecs(), declaredTypes(declared, change)));
                        }
                        listener.onBatch(decoded, position(connector, offset));
                    });
                });
                Object readerOffset = MysqlResumeOffset.forReader(connector.connectorId(), startOffset,
                        connector.context().getStateMap(), () -> InstanceFactory.instance(JsonParser.class));
                stream.streamRead(connector.context(), config.streams(), readerOffset, BATCH_SIZE, consumer);
                return null;
            });
        } catch (Throwable t) {
            if (t instanceof CancellationException && delivery.closed) {
                // Consumer cancellation is the requested stop, not a connector failure to publish.
                return;
            }
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
            TapstateException reported = t instanceof TapstateException coded
                    ? coded
                    : new TapstateException(ConnectorError.CAPTURE_FAILED,
                            Map.of("connector", connector.connectorId(), "detail", detail(t)), t);
            if (preflight != null && preflight.completeExceptionally(reported)) {
                return;
            }
            LOG.warn("cdc stream for connector {} stopped on a failure", connector.connectorId(), t);
            listener.onError(reported);
        }
    }

    /**
     * What one cdc subscription has been told its source may release, and when that was last handed to the
     * connector.
     *
     * <p><b>One slot, holding the latest.</b> A durable position covers every one before it, so a newer one
     * replaces one not yet handed over, and a queue would only replay positions already superseded.
     *
     * <p><b>Handed over on the connector's own delivery thread, and on no other.</b> A source releases its log
     * over the connection it reads it from -- Postgres confirms a position on the very replication stream it
     * is polling -- and that connection is not safe to drive from a second thread while the first reads it.
     * The thread that acknowledges is the runtime's, which must not wait on a source either. So
     * acknowledging only fills the slot, and the next delivery hands it over. A quiet source still delivers
     * heartbeats, so a stream with no changes to pass on does not stop releasing.
     *
     * <p><b>As the connector's own offset.</b> A connector recognizes only a position of its own making and
     * passes over anything else in silence, so the token is read back into the object it was made from,
     * exactly as a resume reads it.
     *
     * <p><b>Again every interval, even unchanged.</b> A connector gives no sign that it acted on a position,
     * and some drop one without a word -- handed over before the source has started streaming, say -- so the
     * latest is handed over again every interval rather than once. That is harmless: it names the same
     * place. An attempt is timed whether it went through or failed, so a source that refuses is asked once
     * an interval rather than on every delivery.
     *
     * <p><b>Never at the stream's expense.</b> A position that cannot be handed over is reported, coded, to
     * the listener and in a rate-limited warning, and the delivery goes on as if nothing had happened: a
     * source that has not released yet keeps some log a while longer, and failing the stream over that
     * would make an outage of a delay.
     */
    private static final class Acknowledgements {

        private final PdkConnector connector;
        private final CaptureListener listener;
        /** The connector's flush function, or null when it registered none: then nothing is ever handed over. */
        private final FlushOffsetFunction flush;
        private final long intervalNanos;
        private final LongSupplier clock;

        /** The latest position acknowledged: set by whoever acknowledges, read by the delivery. */
        private final AtomicReference<SourcePosition> latest = new AtomicReference<>();

        /** Set by close and read by the delivery, so a delivery the source makes on its way down does nothing. */
        private volatile boolean closed;

        // The delivery's own bookkeeping, guarded by this so that it holds even for a source that delivers
        // from more than one thread over its life.
        private boolean attempted;
        private long lastAttemptNanos;
        private boolean warned;
        private long lastWarningNanos;
        private long failuresSinceWarning;

        Acknowledgements(PdkConnector connector, CaptureListener listener, FlushOffsetFunction flush,
                long intervalNanos, LongSupplier clock) {
            this.connector = connector;
            this.listener = listener;
            this.flush = flush;
            this.intervalNanos = intervalNanos;
            this.clock = clock;
        }

        /** Makes {@code durable} the latest, for the next delivery to hand over; does nothing once closed. */
        void offer(SourcePosition durable) {
            Objects.requireNonNull(durable, "durable");
            if (!closed) {
                latest.set(durable);
            }
        }

        void close() {
            closed = true;
        }

        /**
         * Hands the latest position to the connector when one is due: the subscription is open, the connector
         * registered a flush function, a position has been acknowledged, and an interval has passed since the
         * last attempt. Called on the delivery thread, before the batch is passed on.
         */
        synchronized void applyIfDue() {
            if (flush == null || closed) {
                return;
            }
            SourcePosition position = latest.get();
            if (position == null) {
                return;
            }
            long now = clock.getAsLong();
            if (attempted && now - lastAttemptNanos < intervalNanos) {
                return;
            }
            attempted = true;
            lastAttemptNanos = now;
            try {
                // Through the connector's own seam, for its loader and its pipeline attribution. A delivery
                // on the stream's own thread already carries both; one from a thread the connector started
                // for itself may not, and whatever the connector logs while it releases is this pipeline's.
                connector.underLoader(() -> {
                    flush.flushOffset(connector.context(), ConnectorOffsetCodec.fromToken(
                            connector.connectorId(), position.token(),
                            connector.connector().getClass().getClassLoader()));
                    return null;
                });
            } catch (VirtualMachineError fatal) {
                // Not a position the source refused but a process in trouble, which this bridge lets crash
                // bare wherever it surfaces.
                throw fatal;
            } catch (Throwable failure) {
                if (failure instanceof InterruptedException) {
                    // The stream is being closed, and the interrupt that says so is the connector's to see.
                    Thread.currentThread().interrupt();
                }
                failed(failure, now);
                return;
            }
            listener.onAcknowledged(position);
        }

        private void failed(Throwable failure, long now) {
            TapstateException coded = new TapstateException(ConnectorError.ACKNOWLEDGE_FAILED,
                    Map.of("connector", connector.connectorId(), "detail", detail(failure)), failure);
            failuresSinceWarning++;
            if (!warned || now - lastWarningNanos >= ACKNOWLEDGE_WARNING_INTERVAL_NANOS) {
                LOG.warn("cdc stream for connector {} could not tell its source how far it may release its change "
                        + "log; the stream carries on and retries (failures since the last warning: {})",
                        connector.connectorId(), failuresSinceWarning, coded);
                warned = true;
                lastWarningNanos = now;
                failuresSinceWarning = 0;
            }
            listener.onAcknowledgeFailed(coded);
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
     * The stream position the connector names for {@code atEpochMilli}, or for its present moment when
     * that is null — so the tail begins somewhere the source chose rather than driving a schema-only
     * recovery that has no stored offset. Null when the connector declares no offset function, which only
     * a present start reaches: an instant start is refused before this, where the caller can still hear it.
     */
    private static Object startOffset(PdkConnector connector, Long atEpochMilli) throws Throwable {
        TimestampToStreamOffsetFunction offset = connector.functions().getTimestampToStreamOffsetFunction();
        return offset == null ? null : offset.timestampToStreamOffset(connector.context(), atEpochMilli);
    }

    /**
     * The instant a start asks the source to resolve, as the epoch milliseconds the frozen contract takes,
     * or null when the start names no instant. The oldest change a source retains is asked for as the
     * beginning of the timeline: the contract has one way to ask about a point in time and no separate way
     * to ask for the earliest, so the two forms differ here only in which instant they name — while
     * staying distinct above, where "as far back as you go" and "back to 1970" are different asks and
     * fail differently.
     */
    private static Long startAt(CaptureStart start) {
        return switch (start) {
            case CaptureStart.At at -> at.instant().toEpochMilli();
            case CaptureStart.Earliest ignored -> 0L;
            case CaptureStart.Resume ignored -> null;
            case CaptureStart.Present ignored -> null;
        };
    }

    /**
     * A connector's own offset rendered as the position it stands for, or empty when the connector named
     * none. Empty is a statement, not a gap: this source did not say where it had read to, so a caller
     * needing a position has to refuse rather than invent one that no connector could be started from.
     */
    private static Optional<SourcePosition> position(PdkConnector connector, Object offset) {
        if (offset == null) {
            return Optional.empty();
        }
        // A snapshot renders its seam after the read's loader scope has ended. The
        // connector's native JSON service must still resolve under that same loader.
        Object represented = read(connector, () -> SqlServerOffset.forStorage(
                connector.connectorId(), offset, () -> InstanceFactory.instance(JsonParser.class)));
        return Optional.of(new SourcePosition(
                ConnectorOffsetCodec.toToken(connector.connectorId(), represented)));
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
    private static List<Envelope> decodeSnapshot(
            PdkConnector connector, List<TapEvent> raw, Map<String, TapTable> tables) {
        return decodeSnapshotRows(connector, raw, declaredTypes(tables));
    }

    /** The same, against the declared types already read off the tables -- once per read, not per batch. */
    private static List<Envelope> decodeSnapshotRows(
            PdkConnector connector, List<TapEvent> raw, Map<String, Map<String, String>> declared) {
        List<Envelope> rows = new ArrayList<>(raw.size());
        try {
            for (TapEvent event : raw) {
                rows.add(TapEventCodec.decodeSnapshotRow(
                        event, connector.codecs(), declaredTypes(declared, event)));
            }
        } catch (RuntimeException e) {
            throw new TapstateException(ConnectorError.PROJECTION_FAILED,
                    Map.of("connector", connector.connectorId(), "detail", detail(e)), e);
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
    private static void joinQuietly(Thread thread, long millis) {
        try {
            thread.join(millis);
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
