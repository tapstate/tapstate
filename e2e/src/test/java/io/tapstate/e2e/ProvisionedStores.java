package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The stores a specification asked for, brought up and reported back in the three shapes a run needs
 * them: an address to interpolate, a driver to read them back with, and a handle to release them.
 *
 * <p>This is the seam the specification never sees. It says {@code src: { kind: mysql }} and gets a real
 * MySQL; it does not name a container, a port, or a driver class. Which is the point - the same
 * specification has to keep working when the store comes from somewhere else entirely (a pooled remote
 * instance rather than a local container), and it can only do that if it never said where from.
 *
 * <p>Addresses are published as environment references named after the store: a request named
 * {@code src} publishes {@code SRC_HOST}, {@code SRC_PORT}, … and the resource writes
 * {@code host: ${SRC_HOST}}. The shape differs by kind because stores differ - a JDBC endpoint has no
 * single string that is its address, a Mongo one does - and that difference is the store's, not the
 * specification's, so it lives here.
 */
final class ProvisionedStores implements AutoCloseable {

    /** Mongo refuses a database name past this many characters, and only says so once a run is under way. */
    private static final int NAME_LIMIT = 63;

    /** Digest width in bytes: 48 bits of hex, enough that a collision is not something one arranges. */
    private static final int DIGEST_BYTES = 6;

    private final Map<String, String> environment = new LinkedHashMap<>();
    private final Map<String, Endpoints> driversByConnector = new LinkedHashMap<>();
    private final Map<String, Store> storesByName = new LinkedHashMap<>();
    private final List<StreamGate> gates = new ArrayList<>();

    /**
     * One store as this run brought it up: the database that is its identity, a handle of our own, and
     * the gate its traffic reaches it through. The gate is null for a store whose address is a single
     * uri - there is nothing there to hold a stream for, because those are targets here.
     */
    private record Store(String database, Endpoints driver, EndpointAddress address, StreamGate gate) {}

    private ProvisionedStores() {
    }

    /**
     * Brings up every store the specification asked for.
     *
     * <p>{@code runId} keeps concurrent runs off each other's data: it is folded into the database name,
     * so two runs of the same specification share a server and nothing else. Sharing the server is
     * deliberate - a database per run costs a name, a server per run costs a start-up.
     */
    static ProvisionedStores provision(Map<String, DatabaseRequest> requests, String runId) {
        ProvisionedStores stores = new ProvisionedStores();
        try {
            requests.forEach((name, request) -> stores.bring(name, request, runId));
        } catch (RuntimeException bringingUpFailed) {
            // Whatever came up before the failure still has to come down; a half-provisioned run must not
            // leak the half that succeeded.
            stores.close();
            throw bringingUpFailed;
        }
        return stores;
    }

    /** What a specification's {@code ${...}} references resolve to for these stores. */
    Map<String, String> environment() {
        return Map.copyOf(environment);
    }

    /** The independent reader per connector, for the stores this run brought up. */
    Map<String, Endpoints> driversByConnector() {
        return Map.copyOf(driversByConnector);
    }

    /**
     * The store an address actually lands on, told by the one thing that is a store's identity here:
     * its database name. Every database this run makes carries the run id, so the name appears in a
     * resource's settings if and only if the resource interpolated that store's published address -
     * whether as a {@code database} setting or inside a uri. This is how a guard learns where a
     * resource really points without trusting the interpolation it is there to check.
     */
    Optional<String> storeHolding(EndpointAddress address) {
        for (Map.Entry<String, Store> named : storesByName.entrySet()) {
            String database = named.getValue().database();
            boolean holds = address.settings().values().stream()
                    .anyMatch(value -> value != null && value.toString().contains(database));
            if (holds) {
                return Optional.of(named.getKey());
            }
        }
        return Optional.empty();
    }

    /**
     * Counts a table in a named store by the handle this run kept for itself when it brought the store
     * up - an address no resource supplies and no example can name. The last word on where rows ended
     * up has to be read over an address the specification could not have influenced.
     */
    long count(String store, String table) {
        Store named = storesByName.get(store);
        if (named == null) {
            throw new EnvelopeException(
                    "no store named " + store + " was provisioned; this run brought up " + storesByName.keySet());
        }
        return named.driver().count(named.address(), table);
    }

    @Override
    public void close() {
        driversByConnector.values().forEach(Endpoints::close);
        driversByConnector.clear();
        gates.forEach(StreamGate::close);
        gates.clear();
        environment.clear();
    }

    private void bring(String name, DatabaseRequest request, String runId) {
        String database = database(name, runId);
        String prefix = name.toUpperCase(Locale.ROOT);
        // One driver per connector however many stores share it: the driver holds connections, and a
        // second instance for a second store of the same kind would leave the first unclosed.
        Endpoints driver = driversByConnector.computeIfAbsent(
                request.kind().connectorId(), connectorId -> newDriver(request.kind()));
        switch (request.kind()) {
            case MYSQL -> gated(name, prefix, database, driver, SharedMySql.settings(database));
            // Same five settings as MySQL and published the same way: both are JDBC stores a resource
            // addresses by host, port, database and credentials, so a specification that swaps one engine
            // for the other changes the kind and nothing else.
            case POSTGRES -> gated(name, prefix, database, driver, SharedPostgres.settings(database));
            case ORACLE -> {
                Map<String, Object> settings = SharedOracle.settings(database);
                gated(name, prefix, String.valueOf(settings.get("schema")), driver, settings);
            }
            case SQLSERVER -> gated(name, prefix, database, driver, SharedSqlServer.settings(database));
            case MONGO -> {
                String url = SharedMongo.replicaSetUrl(database);
                environment.put(prefix + "_URI", url);
                storesByName.put(
                        name, new Store(database, driver, new EndpointAddress(name, Map.of("uri", url)), null));
            }
        }
    }

    /**
     * Brings up one JDBC store with a gate in front of it, and publishes the gate's address rather than
     * the store's own.
     *
     * <p>The two addresses are the point. What a resource interpolates - and therefore what the product
     * dials - is the gate, so a run can hold that store's traffic without the product being told
     * anything. What this class keeps for itself is the store's own address, so the harness can still
     * seed and read a store whose stream is held; a harness that dialled through the gate would block on
     * its own hold, and writing rows into a held source is exactly what a specification about arrival
     * order has to do.
     *
     * <p>Both addresses reach the same server. The gate is a relay and nothing else, so this is not the
     * harness composing an address of its own - the property that matters, that a count is taken from
     * the store the product wrote to, is unchanged.
     */
    private void gated(
            String name, String prefix, String database, Endpoints driver, Map<String, Object> settings) {
        StreamGate gate = StreamGate.inFrontOf(
                String.valueOf(settings.get("host")), Integer.parseInt(String.valueOf(settings.get("port"))));
        gates.add(gate);
        Map<String, Object> published = new LinkedHashMap<>(settings);
        published.put("host", gate.host());
        published.put("port", gate.port());
        published.forEach((setting, value) ->
                environment.put(prefix + "_" + setting.toUpperCase(Locale.ROOT), String.valueOf(value)));
        storesByName.put(name, new Store(database, driver, new EndpointAddress(name, settings), gate));
    }

    /**
     * Holds or releases the stream of whichever store the given address lands on.
     *
     * <p>The address is a source's own settings, so the store is found the way {@link #storeHolding}
     * finds it: by the database name this run gave it. A store with no gate refuses rather than doing
     * nothing - a specification that holds a stream and is silently obeyed by nobody would go green
     * having tested the opposite of what it says.
     */
    void driveStream(EndpointAddress address, StreamVerb verb) {
        String name = storeHolding(address).orElseThrow(() -> new EnvelopeException(
                "no store this run brought up holds " + address.resourceId()
                        + ", so its stream cannot be held; this run brought up " + storesByName.keySet()));
        StreamGate gate = storesByName.get(name).gate();
        if (gate == null) {
            throw new EnvelopeException(
                    "the store " + name + " is reached by a uri and has no stream gate, so "
                            + verb.word() + " cannot be scoped to it");
        }
        switch (verb) {
            case PAUSE -> gate.hold();
            case RESUME -> gate.release();
        }
    }

    /**
     * The store's own address behind whatever gate publishes it, for the harness's own dialling.
     *
     * <p>An address that is not a gate's comes back as it was. This is the one place the harness reads
     * an address rather than passing it through, and it is bounded to undoing something this class did:
     * the settings handed back are the ones this class brought the store up with, not a composition.
     */
    EndpointAddress behindTheGate(EndpointAddress address) {
        return storeHolding(address)
                .map(storesByName::get)
                .filter(store -> store.gate() != null)
                .map(store -> new EndpointAddress(address.resourceId(), merged(address, store.address())))
                .orElse(address);
    }

    /** The resource's own settings with the host and port the store really answers on. */
    private static Map<String, Object> merged(EndpointAddress declared, EndpointAddress store) {
        Map<String, Object> settings = new LinkedHashMap<>(declared.settings());
        settings.put("host", store.settings().get("host"));
        settings.put("port", store.settings().get("port"));
        return settings;
    }

    private static Endpoints newDriver(DatabaseKind kind) {
        return switch (kind) {
            case MYSQL -> new MySqlEndpoints();
            case POSTGRES -> new PostgresEndpoints();
            case MONGO -> new MongoEndpoints();
            case ORACLE -> new OracleEndpoints();
            case SQLSERVER -> new SqlServerEndpoints();
        };
    }

    /**
     * A database name of this store's own. Mongo refuses names past 63 characters and only says so once a
     * run is under way, so the name is brought under the limit rather than allowed to overflow - but it
     * has to stay a name of this run's own while doing it, which a plain cut does not.
     */
    static String database(String name, String runId) {
        String candidate = ("e2e_" + name + "_" + runId).toLowerCase(Locale.ROOT).replace('-', '_');
        if (candidate.length() <= NAME_LIMIT) {
            return candidate;
        }
        // What distinguishes two runs sits at the end of the run id, so a plain cut takes exactly the
        // part that separates them - two tiers of one example are told apart by a suffix that is the
        // first thing to go. A digest of the whole name put back on the end keeps them apart, and it
        // fails visibly rather than silently: two runs sharing a database would drop and rewrite each
        // other's tables in turn, which no assertion here would report as anything but a wrong count.
        String digest = digest(candidate);
        return candidate.substring(0, NAME_LIMIT - digest.length() - 1) + "_" + digest;
    }

    /**
     * A short digest of the whole text, from a hash built to make collisions hard to produce.
     *
     * <p>{@code String.hashCode} is not one: it is 32 bits, and pairs that agree on it are easy to
     * construct rather than merely unlikely - {@code aan} and {@code ac0} already do. Used to keep
     * truncated names apart, it would let two runs share a database, and sharing one is silent here
     * because the runs go in turn and every seed drops its table before writing.
     */
    static String digest(String text) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash, 0, DIGEST_BYTES);
        } catch (NoSuchAlgorithmException everyJvmHasIt) {
            throw new IllegalStateException("SHA-256 is required of every Java runtime", everyJvmHasIt);
        }
    }
}
