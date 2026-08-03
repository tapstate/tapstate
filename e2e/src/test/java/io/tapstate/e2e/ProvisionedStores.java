package io.tapstate.e2e;

import java.util.LinkedHashMap;
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

    private final Map<String, String> environment = new LinkedHashMap<>();
    private final Map<String, Endpoints> driversByConnector = new LinkedHashMap<>();
    private final Map<String, Store> storesByName = new LinkedHashMap<>();

    /** One store as this run brought it up: the database that is its identity, and a handle of our own. */
    private record Store(String database, Endpoints driver, EndpointAddress address) {}

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
            case MYSQL -> {
                Map<String, Object> settings = SharedMySql.settings(database);
                settings.forEach((setting, value) ->
                        environment.put(prefix + "_" + setting.toUpperCase(Locale.ROOT), String.valueOf(value)));
                storesByName.put(name, new Store(database, driver, new EndpointAddress(name, settings)));
            }
            case MONGO -> {
                String url = SharedMongo.replicaSetUrl(database);
                environment.put(prefix + "_URI", url);
                storesByName.put(name, new Store(database, driver, new EndpointAddress(name, Map.of("uri", url))));
            }
        }
    }

    private static Endpoints newDriver(DatabaseKind kind) {
        return switch (kind) {
            case MYSQL -> new MySqlEndpoints();
            case MONGO -> new MongoEndpoints();
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
        String digest = Integer.toHexString(candidate.hashCode());
        return candidate.substring(0, NAME_LIMIT - digest.length() - 1) + "_" + digest;
    }
}
