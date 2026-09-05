package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.JsonReader;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.conversion.impl.TableFieldTypesGeneratorImpl;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectorCapabilities;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An opened, constructed connector plus its registered functions and a driving context — the shared
 * handle the read and write ports drive. Opening resolves the load-time, structural diagnoses into
 * coded connector-domain exceptions: an incompatible API level is refused (never downgraded), and a
 * missing / non-connector entry class and an un-instantiable connector each carry a code. The
 * per-operation drive (init / read / write / stop) and its own coded failures belong to the ports.
 *
 * <p>The connector is driven with its own class loader installed as the thread context loader, the way
 * a real connector reaches its isolated runtime; {@link #close()} closes that loader. Constructing a
 * real connector bootstraps the PDK runtime through the host loader, so the host must carry it: the
 * assembly root does, so a real connector constructs here. A build whose host omits the runtime drives
 * only synthetic connectors that bind to the frozen contract alone.
 */
final class PdkConnector implements AutoCloseable {

    /** The runtime env key naming the host's deployment identity, which a connector's runtime reads. */
    private static final String DEPLOYMENT_IDENTITY_KEY = "app_type";

    /** Tapstate hosts connectors as a standalone, on-premise deployment (not a cloud tenant). */
    private static final String DEPLOYMENT_IDENTITY = "DAAS";

    /** A connector-driving action that may throw the connector's own {@code Throwable}. */
    interface Action<T> {
        T run() throws Throwable;
    }

    private final String connectorId;
    private final ConnectorClassLoader loader;
    private final TapConnector connector;
    private final ConnectorFunctions functions;
    private final TapConnectorContext context;
    private final DefaultExpressionMatchingMap dataTypesMap;
    private final String stateNamespace;
    /** Volatile because the thread that stops an instance is rarely the thread that drove it. */
    private volatile boolean stopped;

    private PdkConnector(String connectorId, ConnectorClassLoader loader, TapConnector connector,
                         ConnectorFunctions functions, TapConnectorContext context,
                         DefaultExpressionMatchingMap dataTypesMap, String stateNamespace) {
        this.connectorId = connectorId;
        this.loader = loader;
        this.connector = connector;
        this.functions = functions;
        this.context = context;
        this.dataTypesMap = dataTypesMap;
        this.stateNamespace = stateNamespace;
    }

    /**
     * Loads, level-gates and constructs the connector named by {@code ref} for a drive that scopes
     * nothing it keeps for itself — the read-only drives, which live for a single call and so have no
     * later drive for anything they wrote to be read back by.
     */
    static PdkConnector open(String connectorId, ConnectorRef ref, Map<String, Object> settings) {
        return open(connectorId, ref, settings, null);
    }

    /**
     * Loads, level-gates and constructs the connector named by {@code ref}, returning a drivable
     * handle. Throws a coded connector-domain exception for the structural failures: an incompatible
     * API level, a missing / non-connector class, or an un-instantiable connector.
     *
     * <p>{@code stateNamespace} is where whatever this connector keeps for itself belongs — derived
     * from the pipeline node the drive is for, and null for a drive that names no node.
     */
    static PdkConnector open(String connectorId, ConnectorRef ref, Map<String, Object> settings,
                             String stateNamespace) {
        ensureDeploymentIdentity();
        gateApiLevel(connectorId, ref);

        ConnectorClassLoader loader;
        try {
            loader = ConnectorClassLoader.open(ref.classpath());
        } catch (RuntimeException e) {
            throw loadFailed(connectorId, e);
        }
        boolean opened = false;
        try {
            Class<? extends TapConnector> connectorClass = loadConnectorClass(connectorId, loader, ref.className());
            ClassLoader restore = Thread.currentThread().getContextClassLoader();
            TapConnector connector;
            ConnectorFunctions functions = new ConnectorFunctions();
            try {
                // Construct under the connector's own loader so any context-loader-based PDK lookup
                // resolves against the connector's classpath rather than the host.
                Thread.currentThread().setContextClassLoader(connectorClass.getClassLoader());
                connector = connectorClass.getDeclaredConstructor().newInstance();
                connector.registerCapabilities(functions, new TapCodecsRegistry());
            } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
                // A connector that will not construct, register, or link (e.g. a missing bundled
                // dependency surfacing as NoClassDefFoundError, or a throwing static initializer) is a
                // diagnosable load failure, not a bare crash. A VM error (out of memory) is not a
                // LinkageError and still crashes bare.
                throw loadFailed(connectorId, e);
            } finally {
                Thread.currentThread().setContextClassLoader(restore);
            }
            TapConnectorContext context = new TapConnectorContext(
                    new TapNodeSpecification(), DataMap.create(settings), null, new SilentLog());
            // A connector reaches what it keeps for itself through the context's state maps during init,
            // discovery and the drive; the context leaves them null, so give it live ones or the first
            // touch NPEs. The map handed over here is the same reference for as long as this handle
            // lives: a connector may compare the map it was bound to against the one it is later handed
            // and refuse to run when they differ, and that expectation is nowhere in the signatures.
            context.setStateMap(new InMemoryStateMap());
            context.setGlobalStateMap(new InMemoryStateMap());
            // A connector reads its capability alternatives off the context during the drive; the context
            // leaves them null, so give it an empty set or the first read NPEs. Empty means no overrides:
            // the connector uses its own default capability behaviour, which is the L1 intent.
            context.setConnectorCapabilities(ConnectorCapabilities.create());
            PdkConnector result = new PdkConnector(
                    connectorId, loader, connector, functions, context, dataTypesFrom(ref.spec()), stateNamespace);
            opened = true;
            return result;
        } finally {
            // Close the loader on every failure path — a coded load/class error, an escaping VM error,
            // anything — so a construction failure never leaks the connector's open jar handle.
            if (!opened) {
                closeQuietly(loader);
            }
        }
    }

    /**
     * Declares the host's deployment identity to the PDK runtime before any connector is driven. A real
     * connector's runtime reads {@code app_type} to choose its on-premise vs cloud behaviour and crashes
     * constructing its writer when it finds it blank; a synthetic connector never reads it. The host
     * declares itself a standalone deployment, set once and only when neither an environment variable nor a
     * system property already chose one, so an operator override (or a cloud host) stands. The runtime reads
     * an environment variable ahead of the property, so a set property is only consulted when no variable is
     * present, exactly as this gap-fill assumes.
     */
    private static void ensureDeploymentIdentity() {
        if (System.getenv(DEPLOYMENT_IDENTITY_KEY) == null
                && System.getProperty(DEPLOYMENT_IDENTITY_KEY) == null) {
            System.setProperty(DEPLOYMENT_IDENTITY_KEY, DEPLOYMENT_IDENTITY);
        }
    }

    private static void gateApiLevel(String connectorId, ConnectorRef ref) {
        LevelResolution level = PdkLevelResolver.resolve(ref.pdkApiVersion(), ref.requiredLevel());
        // A switch expression so a new outcome fails to compile here rather than falling through to
        // loadable — this gate is the one place an unrecognized level must never silently pass.
        TapstateException refusal = switch (level.outcome()) {
            case INCOMPATIBLE -> new TapstateException(ConnectorError.API_LEVEL_INCOMPATIBLE,
                    Map.of("connector", connectorId, "required", level.requiredLevel(), "provided", level.engineLevel()),
                    null);
            // The version resolves to no row: a Tapstate-side registry gap, refused with a code that names
            // the version rather than the bare crash the strict level lookup would raise. resolve() only
            // returns this when a version was declared, so ref.pdkApiVersion() is present to name.
            case UNKNOWN_VERSION -> new TapstateException(ConnectorError.API_LEVEL_UNKNOWN,
                    Map.of("connector", connectorId, "version", ref.pdkApiVersion()),
                    null);
            // Loadable: the declared requirement fits, or nothing was declared to judge against.
            case COMPATIBLE, UNDECLARED -> null;
        };
        if (refusal != null) {
            throw refusal;
        }
    }

    private static Class<? extends TapConnector> loadConnectorClass(
            String connectorId, ConnectorClassLoader loader, String className) {
        try {
            return loader.loadConnectorClass(className);
        } catch (ClassNotFoundException | IllegalStateException e) {
            throw new TapstateException(ConnectorError.CLASS_NOT_FOUND,
                    Map.of("connector", connectorId, "class", className), e);
        } catch (LinkageError e) {
            // The class is present but does not link (a missing referenced type): a load failure, not
            // an absent class.
            throw loadFailed(connectorId, e);
        }
    }

    private static TapstateException loadFailed(String connectorId, Throwable cause) {
        return new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", connectorId), cause);
    }

    String connectorId() {
        return connectorId;
    }

    /**
     * Where whatever this connector keeps for itself belongs, or null when the drive names no node.
     * Derived from the pipeline node that opened it, so the full load and the change tail of one run
     * answer with the same name while two pipelines reading the same database answer with different
     * ones. What is filed under it is the state maps handed to the context above.
     */
    String stateNamespace() {
        return stateNamespace;
    }

    TapConnector connector() {
        return connector;
    }

    ConnectorFunctions functions() {
        return functions;
    }

    /**
     * Fills each field's PDK type ({@code tapType}) from its declared database type using the connector's
     * {@code dataTypes} mapping. A connector's read builds on the field's tapType - a mysql snapshot reads
     * its date columns by it - and a discovered field carries only its database type string until this
     * runs. A connector with no spec, or none declaring dataTypes, leaves the fields as discovered.
     */
    void fillFieldTypes(TapTable table) {
        if (dataTypesMap == null || table.getNameFieldMap() == null) {
            return;
        }
        new TableFieldTypesGeneratorImpl().autoFill(table.getNameFieldMap(), dataTypesMap);
    }

    /**
     * Builds the connector's database-type-to-PDK-type mapping from its spec's {@code dataTypes} object,
     * or null when the spec is absent or declares none. Runs on the host with the host's json reader over
     * the raw spec text, not under the connector loader.
     */
    private static DefaultExpressionMatchingMap dataTypesFrom(String spec) {
        if (spec == null || !(JsonReader.parse(spec) instanceof Map<?, ?> root)
                || !(root.get("dataTypes") instanceof Map<?, ?> dataTypes)) {
            return null;
        }
        Map<String, DataMap> byExpression = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : dataTypes.entrySet()) {
            if (entry.getKey() instanceof String expression && entry.getValue() instanceof Map<?, ?> config) {
                @SuppressWarnings("unchecked")
                Map<String, Object> configMap = (Map<String, Object>) config;
                byExpression.put(expression, DataMap.create(configMap));
            }
        }
        return byExpression.isEmpty() ? null : new DefaultExpressionMatchingMap(byExpression);
    }

    TapConnectorContext context() {
        return context;
    }

    /** Runs {@code action} with the connector's own loader installed as the thread context loader. */
    <T> T underLoader(Action<T> action) throws Throwable {
        ClassLoader restore = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(connector.getClass().getClassLoader());
            return action.run();
        } finally {
            Thread.currentThread().setContextClassLoader(restore);
        }
    }

    /** Stops the connector, swallowing failures — used on the cleanup path where the real error already won. */
    void stopQuietly() {
        stopped = true;
        try {
            underLoader(() -> {
                connector.stop(context);
                return null;
            });
        } catch (Throwable ignore) {
            // best-effort cleanup: a stop failure must not mask the outcome that is already being returned or thrown
        }
    }

    /**
     * Whether this connector would still consider itself running, as judged from the two facts the
     * host owns: that it has not been stopped, and that the thread driving it has not been interrupted.
     *
     * <p>Those are the same two facts a connector reads to answer the question for itself — a running
     * read asks between batches and abandons quietly when either has changed — but that answer is not
     * on the frozen interface and cannot be asked for. Reconstructing it here reaches the same
     * judgement without reflecting into a connector's own class, and stays true of every connector
     * rather than of one.
     *
     * <p>Only meaningful on the thread that drove the read: the interrupt half is that thread's.
     */
    boolean isAlive() {
        return !stopped && !Thread.currentThread().isInterrupted();
    }

    @Override
    public void close() {
        closeQuietly(loader);
    }

    private static void closeQuietly(ConnectorClassLoader loader) {
        try {
            loader.close();
        } catch (RuntimeException ignore) {
            // best-effort: the loader is being discarded; a close failure must not mask the real error
        }
    }
}
