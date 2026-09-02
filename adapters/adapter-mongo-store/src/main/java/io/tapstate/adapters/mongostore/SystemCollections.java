package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import org.bson.Document;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Every collection this product keeps, written out one row per collection — and the only place in the
 * repository that turns a database into a collection handle.
 *
 * <p>Both halves of that sentence are load-bearing. The rows are what lets "a collection nobody
 * declared" be noticed at all: without them, a store that has grown an eighteenth collection and a
 * store that has not look exactly alike from in here. And making this the only place a handle is taken
 * is what keeps the rows honest — a second {@code getCollection} elsewhere would be a collection that
 * exists without a row, which is the state the rows exist to rule out.
 *
 * <p>That is also why rows sit here for collections this class has no other business with. The two
 * operator-state collections live in a different database and are not versioned with the rest
 * ({@link Strategy#NEST_EXCLUDED}), but their handles are taken here anyway: the architecture rule that
 * guards the closure matches a call by the method being called, not by the database it happens to
 * reach, so an exception written by hand would leave nothing closed at all.
 *
 * <p>Three gates read this class, and each catches what the others cannot: an architecture rule
 * catches a handle taken somewhere else, an integration test catches a live database holding a
 * collection with no row, and a checked-in rendering catches a row that changed meaning. Rows are
 * appended, not edited: an existing row's meaning is a thing already-deployed stores depend on.
 */
public enum SystemCollections {

    // ---- the store database: what the product keeps about itself ----

    ARTIFACTS(MongoStorePort.ARTIFACTS, Database.STORE, MongoArtifactStore.class, Strategy.MIGRATED, 0),
    PIPELINE_STATE(MongoStorePort.PIPELINE_STATE, Database.STORE, MongoStateStore.class, Strategy.MIGRATED, 0),
    PIPELINE_DESIRED(MongoStorePort.PIPELINE_DESIRED, Database.STORE, MongoDesiredStore.class, Strategy.MIGRATED, 0),
    PIPELINE_OBSERVATION(
            MongoStorePort.PIPELINE_OBSERVATION, Database.STORE, MongoObservationStore.class, Strategy.MIGRATED, 0),
    CONNECTIONS(MongoStorePort.CONNECTIONS, Database.STORE, MongoCatalogStore.class, Strategy.MIGRATED, 0),
    SOURCE_SCHEMAS(MongoStorePort.SOURCE_SCHEMAS, Database.STORE, MongoSchemaStore.class, Strategy.MIGRATED, 0),
    CONNECTOR_CATALOG(
            MongoStorePort.CONNECTOR_CATALOG, Database.STORE, MongoConnectorCatalogStore.class, Strategy.MIGRATED, 0),
    CONNECTOR_SPECS(
            MongoStorePort.CONNECTOR_SPECS, Database.STORE, MongoConnectorSpecStore.class, Strategy.MIGRATED, 0),
    CONNECTION_TEST_RESULTS(
            MongoStorePort.CONNECTION_TEST_RESULTS, Database.STORE, MongoConnectionTestResultStore.class,
            Strategy.MIGRATED, 0),

    // ---- the store database: who may talk to it ----

    USERS(MongoAuthStores.USERS, Database.STORE, MongoAuthStores.class, Strategy.MIGRATED, 0),
    TOKENS(MongoAuthStores.TOKENS, Database.STORE, MongoAuthStores.class, Strategy.MIGRATED, 0),
    /**
     * The one collection with an index that is not optional: every request carrying a session credential
     * looks a session up by the pair below, so without it each of those requests is a collection scan.
     */
    SESSIONS(MongoAuthStores.SESSIONS, Database.STORE, MongoAuthStores.class, Strategy.MIGRATED, 0,
            new IndexSpec(List.of("secretHash", "issuer"), false)),
    AUDIT(MongoAuthStores.AUDIT, Database.STORE, MongoAuthStores.class, Strategy.MIGRATED, 0),
    CLUSTER_IDENTITY(MongoAuthStores.CLUSTER_IDENTITY, Database.STORE, MongoAuthStores.class, Strategy.MIGRATED, 0),

    /**
     * Registered connector artifacts, addressed by the hash of what is in them. A GridFS bucket rather
     * than a collection, so it is two physical collections; the index below lands on the metadata one,
     * which is the half a lookup by connector id reads. Nothing rewrites a document here — a different
     * artifact is a different hash — so it is outside the version scheme.
     */
    CONNECTOR_ARTIFACTS(MongoStorePort.CONNECTOR_ARTIFACTS, Database.STORE, MongoConnectorRegistry.class,
            Strategy.IMMUTABLE, 0, Kind.GRIDFS_BUCKET,
            new IndexSpec(List.of("metadata.connectorId"), false)),

    /** One coordination record per mining chain. Another line owns how this one evolves. */
    SRS_META(MongoStorePort.SRS_META, Database.STORE, MongoSrsMetaStore.class, Strategy.OWNED_ELSEWHERE, 0),

    /**
     * The one document recording which schema version the store has been brought to, and the lock the
     * member doing the bringing holds while it works. Created by the first changeset, which is why this
     * is the only row that was introduced by one rather than predating the whole scheme.
     */
    SYSTEM_META("system_meta", Database.STORE, MongoConnection.class, Strategy.MIGRATED, 1),

    // ---- the operator-state database: not versioned here, but still taken from here ----

    OPERATOR_STATE(MongoStorePort.OPERATOR_STATE, Database.NEST, MongoKeyedStateStore.class,
            Strategy.NEST_EXCLUDED, 0),
    NEST_DEAD_LETTERS(MongoStorePort.NEST_DEAD_LETTERS, Database.NEST, MongoNestDeadLetterStore.class,
            Strategy.NEST_EXCLUDED, 0);

    /** What happens to a collection's documents as the product's shape moves on. */
    public enum Strategy {
        /** Reshaped by changesets as the model changes; the version scheme covers it. */
        MIGRATED,
        /** Addressed by the hash of its content, so nothing in it is ever rewritten. */
        IMMUTABLE,
        /** Another line of work owns how this one evolves; changesets here leave it alone. */
        OWNED_ELSEWHERE,
        /**
         * Outside the version scheme entirely: operator state is rebuilt from its sources when its shape
         * no longer fits, rather than being carried forward.
         */
        NEST_EXCLUDED
    }

    /** Whether a row is one collection or a GridFS bucket, which is two. */
    public enum Kind { COLLECTION, GRIDFS_BUCKET }

    /** Which of the two databases a row lives in. */
    public enum Database { STORE, NEST }

    /**
     * An index a query shape needs. Keys are in order and all ascending: nothing here sorts, and a
     * descending key would only matter to a query that did.
     */
    public record IndexSpec(List<String> keys, boolean unique) {
        public IndexSpec {
            keys = List.copyOf(keys);
        }

        /** The index name, derived from the keys so two declarations of the same index cannot differ. */
        public String indexName() {
            return String.join("_", keys) + "_idx";
        }
    }

    /** The GridFS suffixes, in the order the driver creates them. */
    private static final List<String> GRIDFS_SUFFIXES = List.of(".files", ".chunks");

    private final String collectionName;
    private final Database database;
    private final Class<?> owner;
    private final Strategy strategy;
    private final int introducedIn;
    private final Kind kind;
    private final List<IndexSpec> indexes;

    SystemCollections(String collectionName, Database database, Class<?> owner, Strategy strategy,
            int introducedIn, IndexSpec... indexes) {
        this(collectionName, database, owner, strategy, introducedIn, Kind.COLLECTION, indexes);
    }

    SystemCollections(String collectionName, Database database, Class<?> owner, Strategy strategy,
            int introducedIn, Kind kind, IndexSpec... indexes) {
        this.collectionName = collectionName;
        this.database = database;
        this.owner = owner;
        this.strategy = strategy;
        this.introducedIn = introducedIn;
        this.kind = kind;
        this.indexes = List.of(indexes);
    }

    /**
     * The handle on this collection. The one call to {@code getCollection} in the repository, which is
     * what makes the rows above exhaustive rather than merely well-intentioned.
     */
    public MongoCollection<Document> on(MongoDatabase database) {
        if (kind != Kind.COLLECTION) {
            // Asking a bucket for a plain collection handle would silently give back its metadata half.
            throw new IllegalStateException(collectionName + " is a GridFS bucket; ask for the bucket");
        }
        return database.getCollection(collectionName);
    }

    /** The handle on this GridFS bucket — the one call to {@code GridFSBuckets.create}, for the same reason. */
    public GridFSBucket bucketOn(MongoDatabase database) {
        if (kind != Kind.GRIDFS_BUCKET) {
            throw new IllegalStateException(collectionName + " is a plain collection; ask for the collection");
        }
        return GridFSBuckets.create(database, collectionName);
    }

    public String collectionName() {
        return collectionName;
    }

    public Database database() {
        return database;
    }

    public Strategy strategy() {
        return strategy;
    }

    public int introducedIn() {
        return introducedIn;
    }

    public List<IndexSpec> indexes() {
        return indexes;
    }

    /**
     * The physical collection names this row accounts for. A GridFS bucket is two of them — the driver
     * creates {@code <name>.files} and {@code <name>.chunks} itself, and a reconciliation that did not
     * expand the row would report the driver's own two collections as undeclared.
     */
    public List<String> physicalNames() {
        return kind == Kind.GRIDFS_BUCKET
                ? GRIDFS_SUFFIXES.stream().map(suffix -> collectionName + suffix).toList()
                : List.of(collectionName);
    }

    /**
     * The collection an index declared on this row lands on: the row's own collection, or — for a
     * bucket — its metadata half, which is the only one anything queries by anything but {@code _id}.
     */
    public String indexTarget() {
        return physicalNames().get(0);
    }

    /** Every physical collection name expected in {@code database}. */
    public static Set<String> physicalNamesIn(Database database) {
        Set<String> names = new LinkedHashSet<>();
        for (SystemCollections row : values()) {
            if (row.database == database) {
                names.addAll(row.physicalNames());
            }
        }
        return names;
    }

    /**
     * The rows as text, for the checked-in rendering that pins them. Declaration order, not sorted: a
     * row is appended when a collection is introduced, so the order is the order they came into being
     * and a re-sort would make every later diff unreadable.
     */
    public static String render() {
        StringBuilder out = new StringBuilder();
        out.append("# collection | database | owner | strategy | introducedIn | indexes\n");
        for (SystemCollections row : values()) {
            out.append(row.collectionName)
                    .append(row.kind == Kind.GRIDFS_BUCKET ? " (gridfs)" : "")
                    .append(" | ").append(row.database.name().toLowerCase(Locale.ROOT))
                    .append(" | ").append(row.owner.getSimpleName())
                    .append(" | ").append(row.strategy.name().toLowerCase(Locale.ROOT).replace('_', '-'))
                    .append(" | ").append(row.introducedIn)
                    .append(" | ").append(renderIndexes(row.indexes))
                    .append('\n');
        }
        return out.toString();
    }

    private static String renderIndexes(List<IndexSpec> indexes) {
        if (indexes.isEmpty()) {
            return "-";
        }
        return indexes.stream()
                .map(index -> String.join("+", index.keys()) + (index.unique() ? " (unique)" : ""))
                .reduce((a, b) -> a + ", " + b)
                .orElseThrow();
    }
}
