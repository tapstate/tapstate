package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Server-side command evidence for one uniquely owned Testcontainers control database.
 *
 * <p>The helper writes only its owned profiler collection and profiler settings. Its anchor is a
 * tagged read of an id no workload claim can have. The caller creates the real application client
 * from {@link #nativeUri()} and separately proves authority, submission and target correctness.
 * A profiler record, including a transaction's record, is never a proof of commit.
 */
final class NativeCoordinationProfile implements AutoCloseable {
    enum Family {
        READ, CONTROL, SCHEMA_WRITE, ADVANCE_STANDALONE, ADVANCE_UNDER_CLAIM,
        ACQUIRE, RENEW, RELEASE, RECORD_EXECUTION_FAILURE, CLAIM_WRITE_GUARD
    }

    enum TransactionScope { UNKNOWN, EXPLICIT_TRANSACTION }

    record Limits(long cappedBytes, int maxRecords) {
        static final Limits DEFAULT = new Limits(4L * 1024 * 1024, 2048);

        Limits {
            if (cappedBytes < 4096 || cappedBytes > 16L * 1024 * 1024
                    || cappedBytes % 256 != 0 || maxRecords < 1 || maxRecords > 8192) {
                throw new IllegalArgumentException("invalid bounded profiler limits");
            }
        }
    }

    record Key(String clusterId, String resourceType, String resourceId) { }

    record Operation(Family family, String command, Key key, boolean upsert,
                     TransactionScope transactionScope, Long guardedExecutionGeneration,
                     Map<String, Long> serverCounts, Long errorCode) {
        Operation { serverCounts = Map.copyOf(serverCounts); }
    }

    record Boundary(String action, List<Operation> operations, Map<Family, Long> delta,
                    Map<Family, Long> totals, int retainedRecords) {
        Boundary {
            operations = List.copyOf(operations);
            delta = Map.copyOf(delta);
            totals = Map.copyOf(totals);
        }

        long count(Family family) { return delta.getOrDefault(family, 0L); }

        long count(Family family, Key key) {
            return operations.stream().filter(operation -> operation.family() == family && key.equals(operation.key())).count();
        }

        Map<String, Object> evidence() {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("action", action);
            result.put("source", "MONGO_DATABASE_PROFILER");
            result.put("profileRecords", retainedRecords);
            result.put("anchorRetained", true);
            result.put("prefixAppendOnly", true);
            result.put("transactionCommitProven", false);
            result.put("performanceAcceptanceEligible", false);
            result.put("counts", names(delta));
            result.put("totals", names(totals));
            result.put("operations", operations.stream().map(operation -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("family", operation.family().name());
                row.put("command", operation.command());
                if (operation.key() != null) {
                    row.put("key", Map.of("clusterId", operation.key().clusterId(),
                            "resourceType", operation.key().resourceType(),
                            "resourceId", operation.key().resourceId()));
                }
                row.put("upsert", operation.upsert());
                row.put("transactionScope", operation.transactionScope().name());
                if (operation.guardedExecutionGeneration() != null) {
                    row.put("guardedExecutionGeneration", operation.guardedExecutionGeneration());
                    row.put("guardShape", "STANDALONE_CURRENT_AUTHORITY");
                }
                row.put("serverCounts", operation.serverCounts());
                if (operation.errorCode() != null) { row.put("errorCode", operation.errorCode()); }
                return row;
            }).toList());
            return result;
        }

        private static Map<String, Long> names(Map<Family, Long> counts) {
            Map<String, Long> result = new LinkedHashMap<>();
            for (Family family : Family.values()) { result.put(family.name(), counts.getOrDefault(family, 0L)); }
            return result;
        }
    }

    private static final String PROFILE = "system.profile";
    private static final String CLAIMS = MongoStorePort.WORKLOAD_CLAIMS;
    private static final Set<String> READS = Set.of("find", "aggregate", "getMore", "count", "distinct",
            "listIndexes", "listCollections");
    private static final Set<String> SCHEMA = Set.of("create", "createIndexes", "drop", "dropIndexes", "collMod");
    private static final Set<String> MUTATIONS = Set.of("findAndModify", "findandmodify", "insert", "update", "delete", "bulkWrite");
    private static final Set<String> CONTROL = Set.of("killCursors");
    private static final Set<String> ID_FIELDS = Set.of("clusterId", "resourceType", "resourceId");
    private static final Set<String> EXPECTED_FIELDS = Set.of("_id", "ownerNodeId", "ownerBootId", "claimGeneration", "executionGeneration");
    private final MongoClient observer;
    private final MongoDatabase database;
    private final Limits limits;
    private final String nativeUri;
    private final String nativeAppName;
    private final String observerAppName;
    private final String anchor;
    private final String namespace;
    private final Document filter;
    private final Map<Family, Long> totals = zeros();
    private Document previousSettings;
    private Object profileUuid;
    private List<String> prefix = List.of();
    private int anchorIndex = -1;
    private boolean collectionCreated;
    private boolean filterInstalled;
    private boolean closed;

    private NativeCoordinationProfile(String baseUri, Limits limits) {
        ConnectionString connection = new ConnectionString(Objects.requireNonNull(baseUri, "owned control URI"));
        String name = connection.getDatabase();
        if (!baseUri.startsWith("mongodb://") || name == null
                || !name.matches("[A-Za-z][A-Za-z0-9_]{0,110}")
                || !name.matches(".*_[0-9a-f]{12,32}(?:_.*)?")
                || connection.getApplicationName() != null) {
            throw new IllegalArgumentException("profile requires a uniquely named owned control database and an untagged URI");
        }
        if (connection.getHosts().stream().anyMatch(host -> !(host.equals("localhost")
                || host.startsWith("localhost:") || host.equals("127.0.0.1")
                || host.startsWith("127.0.0.1:") || host.startsWith("[::1]")))) {
            throw new IllegalArgumentException("profile is restricted to a loopback Testcontainers endpoint");
        }
        this.limits = Objects.requireNonNull(limits);
        String nonce = UUID.randomUUID().toString().replace("-", "");
        nativeAppName = "tapstate-native-profile-" + nonce;
        observerAppName = "tapstate-profile-reader-" + nonce;
        anchor = "tapstate-profile-anchor-" + nonce;
        namespace = name + "." + CLAIMS;
        nativeUri = baseUri + (baseUri.contains("?") ? "&" : "?") + "appName=" + nativeAppName;
        observer = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connection)
                .applicationName(observerAppName).build());
        database = observer.getDatabase(name);
        filter = new Document("$and", List.of(equalTo("ns", namespace),
                new Document("$or", List.of(equalTo("appName", nativeAppName),
                        new Document("$and", List.of(equalTo("appName", observerAppName),
                                equalTo("command.find", CLAIMS), equalTo("command.comment", anchor)))))));
    }

    static NativeCoordinationProfile openOwned(String baseUri) {
        return openOwned(baseUri, Limits.DEFAULT);
    }

    static NativeCoordinationProfile openOwned(String baseUri, Limits limits) {
        NativeCoordinationProfile result = new NativeCoordinationProfile(baseUri, limits);
        try {
            result.open();
            return result;
        } catch (RuntimeException | Error failure) {
            try { result.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    String nativeUri() { return nativeUri; }
    String nativeAppName() { return nativeAppName; }
    String databaseName() { return database.getName(); }

    private static Document equalTo(String field, String value) {
        return new Document(field, new Document("$eq", value));
    }

    private void open() {
        previousSettings = settings();
        require(number(previousSettings.get("was")) == 0, "an existing active profiler must not be taken over");
        require(description() == null, "an existing profile collection must not be replaced");
        database.createCollection(PROFILE, new CreateCollectionOptions().capped(true).sizeInBytes(limits.cappedBytes()));
        collectionCreated = true;
        profileUuid = uuid(Objects.requireNonNull(description()));
        filterInstalled = true;
        database.runCommand(new Document("profile", 1).append("filter", filter));
        database.getCollection(CLAIMS).find(new Document("_id", new Document("nativeProfileAbsent", anchor)))
                .comment(anchor).limit(1).first();
        List<Document> beginning = Await.answered("the owned profiler's completed read anchor", Duration.ofSeconds(5), () -> {
            List<Document> rows = rows();
            return rows.stream().anyMatch(this::isAnchor) ? Optional.of(rows) : Optional.empty();
        });
        accept(beginning, "baseline");
    }

    synchronized Boundary boundary(String action) {
        require(!closed, "profile boundary requested after close");
        require(action != null && !action.isBlank() && action.length() <= 128, "invalid profile action");
        verifyOwner();
        return accept(rows(), action);
    }

    private List<Document> rows() {
        return database.getCollection(PROFILE).find().sort(new Document("$natural", 1))
                .limit(limits.maxRecords() + 1).into(new ArrayList<>());
    }

    private Boundary accept(List<Document> rows, String action) {
        require(rows.size() <= limits.maxRecords(), "owned profile record limit exceeded");
        int foundAnchor = -1;
        List<String> nextPrefix = new ArrayList<>(rows.size());
        List<Operation> operations = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Document row = rows.get(i);
            require(namespace.equals(row.getString("ns")), "profile namespace escaped its strict filter");
            Document command = document(row.get("command"));
            require(!command.containsKey("$truncated"), "profile command was truncated");
            if (isAnchor(row)) {
                require(foundAnchor < 0, "owned profile anchor was duplicated");
                foundAnchor = i;
            } else {
                require(nativeAppName.equals(row.getString("appName")), "profile appName escaped its strict filter");
                if (i >= prefix.size()) { operations.add(classify(row, command)); }
            }
            nextPrefix.add(fingerprint(row));
        }
        require(foundAnchor >= 0, "owned profile anchor was lost: capped rollover or collection replacement");
        require(anchorIndex < 0 || anchorIndex == foundAnchor, "owned profile anchor moved");
        require(nextPrefix.size() >= prefix.size(), "owned profile prefix shrank");
        for (int i = 0; i < prefix.size(); i++) {
            require(prefix.get(i).equals(nextPrefix.get(i)), "owned profile prefix changed: capped rollover or rewriting");
        }
        Map<Family, Long> delta = zeros();
        for (Operation operation : operations) { delta.merge(operation.family(), 1L, Math::addExact); }
        delta.forEach((family, count) -> totals.merge(family, count, Math::addExact));
        anchorIndex = foundAnchor;
        prefix = List.copyOf(nextPrefix);
        return new Boundary(action, operations, delta, totals, rows.size());
    }

    private boolean isAnchor(Document row) {
        Object raw = row.get("command");
        return observerAppName.equals(row.getString("appName")) && raw instanceof Document command
                && CLAIMS.equals(command.get("find")) && anchor.equals(command.get("comment"));
    }

    private static Operation classify(Document row, Document command) {
        require(!command.isEmpty(), "required profiled command was empty");
        if ("update".equals(row.get("op")) && (command.containsKey("q") || command.containsKey("u"))) {
            return standaloneClaimWriteGuard(row, command);
        }
        // The first field names the operation; findAndModify also has an update parameter.
        String name = command.keySet().iterator().next();
        if (!(READS.contains(name) || SCHEMA.contains(name) || MUTATIONS.contains(name) || CONTROL.contains(name))) {
            // Individual bulk writes can be profiled as q/u documents instead of the wire command.
            String operation = row.getString("op");
            if (Set.of("insert", "update", "remove").contains(operation)) {
                throw new AssertionError("unsupported workload mutation command: " + operation
                        + "; actual profile=" + row.toJson());
            }
            throw new AssertionError("unsupported profiled command");
        }
        Family family;
        Key key = null;
        boolean upsert = false;
        if (READS.contains(name)) {
            if (name.equals("aggregate")) {
                List<?> pipeline = list(command.get("pipeline"));
                require(pipeline.size() <= 64, "profiled aggregation stage limit exceeded");
                for (Object raw : pipeline) {
                    Document stage = document(raw);
                    require(stage.size() == 1, "unknown profiled aggregation stage shape");
                    require(!stage.containsKey("$out") && !stage.containsKey("$merge"),
                            "unsupported aggregate workload mutation");
                }
            }
            family = Family.READ;
        }
        else if (SCHEMA.contains(name)) { family = Family.SCHEMA_WRITE; }
        else if (CONTROL.contains(name)) { family = Family.CONTROL; }
        else {
            require(name.equals("findAndModify") || name.equals("findandmodify"),
                    "unsupported workload mutation command: " + name);
            require(CLAIMS.equals(command.get(name)), "mutation collection changed");
            require(!command.containsKey("remove"), "unsupported workload remove mutation");
            Document query = document(command.get("query"));
            Document id = findId(query);
            key = key(id);
            List<?> pipeline = list(command.get("update"));
            require(pipeline.size() == 1, "unknown workload mutation pipeline length");
            Document stage = document(pipeline.getFirst());
            require(stage.keySet().equals(Set.of("$set")), "unknown workload mutation operator");
            Document set = document(stage.get("$set"));
            Object rawUpsert = command.get("upsert");
            require(rawUpsert == null || rawUpsert instanceof Boolean, "invalid workload upsert option");
            upsert = Boolean.TRUE.equals(rawUpsert);
            family = mutation(query, id, set);
            require(!upsert || family == Family.ADVANCE_STANDALONE || family == Family.ACQUIRE,
                    "unexpected upsert on a guarded workload mutation");
        }
        return operation(row, command, family, name, key, upsert, null);
    }

    private static Operation standaloneClaimWriteGuard(Document row, Document command) {
        String rejection = "unsupported workload mutation command: update";
        require(command.keySet().equals(Set.of("q", "u", "multi", "upsert")), rejection);
        require(Boolean.FALSE.equals(command.get("multi")) && Boolean.FALSE.equals(command.get("upsert")), rejection);
        Document update = document(command.get("u"));
        require(update.keySet().equals(Set.of("$inc")), rejection);
        Document increment = document(update.get("$inc"));
        require(increment.keySet().equals(Set.of("fencedAppends")) && number(increment.get("fencedAppends")) == 1,
                rejection);
        Document query = document(command.get("q"));
        require(query.keySet().equals(Set.of("_id", "executionGeneration", "$or")),
                "unknown standalone claim write guard query");
        Document id = document(query.get("_id"));
        Key key = key(id);
        require(key.resourceType().equals(WorkloadClaimType.PIPELINE_ACTUATION.name()),
                "standalone claim write guard names another workload type");
        long generation = number(query.get("executionGeneration"));
        require(generation > 0, "standalone claim write guard lacks an admitted generation");
        List<Document> eligible = List.of(new Document("ownerNodeId", new Document("$exists", false)),
                new Document("$and", List.of(new Document("leaseUntil", new Document("$type", "date")),
                        new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW"))))));
        require(eligible.equals(query.get("$or")), "standalone claim write guard authority predicate changed");
        return operation(row, command, Family.CLAIM_WRITE_GUARD, "update", key, false, generation);
    }

    private static Operation operation(Document row, Document command, Family family, String name, Key key,
            boolean upsert, Long guardedGeneration) {
        Map<String, Long> counts = new LinkedHashMap<>();
        for (String field : List.of("nMatched", "nModified", "nUpserted", "ninserted", "nInserted", "nreturned", "nReturned")) {
            if (row.containsKey(field)) { counts.put(field, number(row.get(field))); }
        }
        Long error = row.containsKey("errCode") ? number(row.get("errCode")) : null;
        return new Operation(family, name, key, upsert, transactionScope(row, command), guardedGeneration, counts, error);
    }

    private static TransactionScope transactionScope(Document row, Document command) {
        boolean commandHasScope = command.containsKey("autocommit");
        boolean rowHasScope = row.containsKey("autocommit");
        if (!commandHasScope && !rowHasScope) { return TransactionScope.UNKNOWN; }
        Object scope = commandHasScope ? command.get("autocommit") : row.get("autocommit");
        if (commandHasScope && rowHasScope) {
            require(Objects.equals(scope, row.get("autocommit")), "profiled transaction metadata conflicts");
        }
        require(Boolean.FALSE.equals(scope), "unsupported explicit profiled transaction metadata");
        return TransactionScope.EXPLICIT_TRANSACTION;
    }

    private static Family mutation(Document query, Document id, Document set) {
        Set<String> fields = set.keySet();
        if (fields.equals(Set.of("clusterId", "resourceType", "resourceId", "executionGeneration"))) {
            require(id.entrySet().stream().allMatch(entry -> Objects.equals(set.get(entry.getKey()), entry.getValue())),
                    "standalone generation identity changed");
            require(increment(set.get("executionGeneration"), "executionGeneration"), "unknown generation increment");
            require(standaloneGuard(query, id), "standalone generation guard changed");
            return Family.ADVANCE_STANDALONE;
        }
        if (fields.equals(Set.of("clusterId", "resourceType", "resourceId", "ownerNodeId", "ownerBootId",
                "claimGeneration", "executionGeneration", "topologyRevision", "leaseUntil"))) {
            require(id.entrySet().stream().allMatch(entry -> Objects.equals(set.get(entry.getKey()), entry.getValue())),
                    "acquire identity changed");
            String node = text(set.get("ownerNodeId"));
            String boot = text(set.get("ownerBootId"));
            require(number(set.get("topologyRevision")) >= 0 && lease(set.get("leaseUntil")), "unknown acquire lease");
            require(ifNull(set.get("executionGeneration"), "executionGeneration", 0), "acquire unexpectedly advances execution");
            List<?> condition = operator(set.get("claimGeneration"), "$cond", 3);
            Document same = new Document("$and", List.of(new Document("$eq", List.of("$ownerNodeId", node)),
                    new Document("$eq", List.of("$ownerBootId", boot))));
            require(condition.getFirst().equals(same) && ifNull(condition.get(1), "claimGeneration", 1)
                    && increment(condition.get(2), "claimGeneration"), "unknown acquire generation shape");
            Document eligible = new Document("$and", List.of(new Document("_id", id), new Document("$or", List.of(
                    new Document("ownerNodeId", node).append("ownerBootId", boot),
                    new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))));
            Document absent = new Document("$and", List.of(new Document("_id", id),
                    new Document("ownerNodeId", new Document("$exists", false))));
            require(query.equals(eligible) || query.equals(absent), "acquire ownership guard changed");
            return Family.ACQUIRE;
        }
        if (fields.equals(Set.of("leaseUntil"))) {
            if ("$$NOW".equals(set.get("leaseUntil"))) {
                require(expected(query, id), "release ownership guard changed");
                return Family.RELEASE;
            }
            require(lease(set.get("leaseUntil")) && liveExpected(query, id), "renew ownership guard changed");
            return Family.RENEW;
        }
        Set<String> context = Set.of("executionGeneration", "contextExecutionGeneration", "executionClaimGeneration",
                "executionNodeIds", "failureClaimGeneration", "failureAfterMemberLoss");
        if (fields.equals(Set.of("executionGeneration")) || fields.equals(context)) {
            require(increment(set.get("executionGeneration"), "executionGeneration") && liveExpected(query, id),
                    "claim-guarded execution mutation changed");
            if (fields.equals(context)) {
                require(increment(set.get("contextExecutionGeneration"), "executionGeneration")
                        && "$claimGeneration".equals(set.get("executionClaimGeneration"))
                        && number(set.get("failureClaimGeneration")) == 0
                        && Boolean.FALSE.equals(set.get("failureAfterMemberLoss")), "execution context mutation changed");
                List<?> nodes = list(set.get("executionNodeIds"));
                require(!nodes.isEmpty() && nodes.size() <= 128 && nodes.stream().allMatch(value -> value instanceof String)
                        && nodes.stream().distinct().count() == nodes.size(), "invalid planned execution members");
            }
            return Family.ADVANCE_UNDER_CLAIM;
        }
        if (fields.equals(Set.of("failureClaimGeneration", "failureAfterMemberLoss"))) {
            List<?> clauses = operator(query, "$and", 2);
            require(liveExpected(document(clauses.getFirst()), id)
                    && document(clauses.get(1)).equals(new Document("$expr", new Document("$eq",
                    List.of("$contextExecutionGeneration", "$executionGeneration")))), "failure record guard changed");
            List<?> code = operator(set.get("failureClaimGeneration"), "$cond", 3);
            List<?> loss = operator(set.get("failureAfterMemberLoss"), "$cond", 3);
            List<?> unrecorded = operator(code.getFirst(), "$eq", 2);
            require(ifNull(unrecorded.getFirst(), "failureClaimGeneration", 0)
                    && number(unrecorded.get(1)) == 0 && "$claimGeneration".equals(code.get(1))
                    && "$failureClaimGeneration".equals(code.get(2)) && loss.getFirst().equals(code.getFirst())
                    && loss.get(1) instanceof Boolean && "$failureAfterMemberLoss".equals(loss.get(2)),
                    "unknown failure record mutation");
            return Family.RECORD_EXECUTION_FAILURE;
        }
        throw new AssertionError("unknown workload mutation shape");
    }

    private static boolean standaloneGuard(Document query, Document id) {
        Document absent = new Document("_id", id).append("ownerNodeId", new Document("$exists", false));
        Document eligible = new Document("$and", List.of(new Document("_id", id), new Document("$or", List.of(
                new Document("ownerNodeId", new Document("$exists", false)), new Document("$and", List.of(
                new Document("leaseUntil", new Document("$type", "date")),
                new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))))));
        return query.equals(absent) || query.equals(eligible);
    }

    private static boolean expected(Document query, Document id) {
        return query.keySet().equals(EXPECTED_FIELDS) && query.get("_id").equals(id)
                && !text(query.get("ownerNodeId")).isBlank() && !text(query.get("ownerBootId")).isBlank()
                && number(query.get("claimGeneration")) > 0 && number(query.get("executionGeneration")) >= 0;
    }

    private static boolean liveExpected(Document query, Document id) {
        List<?> clauses = operator(query, "$and", 3);
        Document topology = document(clauses.get(1));
        return expected(document(clauses.getFirst()), id) && topology.keySet().equals(Set.of("topologyRevision"))
                && number(topology.get("topologyRevision")) >= 0
                && document(clauses.get(2)).equals(new Document("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW"))));
    }

    private static boolean lease(Object value) {
        Document root = document(value);
        if (!root.keySet().equals(Set.of("$dateAdd"))) { return false; }
        Document date = document(root.get("$dateAdd"));
        return date.keySet().equals(Set.of("startDate", "unit", "amount")) && "$$NOW".equals(date.get("startDate"))
                && "millisecond".equals(date.get("unit")) && number(date.get("amount")) > 0;
    }

    private static boolean increment(Object value, String field) {
        List<?> add = operator(value, "$add", 2);
        return ifNull(add.getFirst(), field, 0) && number(add.get(1)) == 1;
    }

    private static boolean ifNull(Object value, String field, long fallback) {
        List<?> values = operator(value, "$ifNull", 2);
        return ("$" + field).equals(values.getFirst()) && number(values.get(1)) == fallback;
    }

    private static Document findId(Document query) {
        List<Document> ids = new ArrayList<>();
        collectIds(query, ids, 0);
        require(ids.size() == 1, "workload query needs one factual claim id");
        return ids.getFirst();
    }

    private static void collectIds(Document query, List<Document> ids, int depth) {
        require(depth <= 8 && query.size() <= 16, "workload query exceeded its structural bound");
        if (query.containsKey("_id")) { ids.add(document(query.get("_id"))); }
        for (String conjunction : List.of("$and", "$or")) {
            if (!query.containsKey(conjunction)) { continue; }
            List<?> clauses = list(query.get(conjunction));
            require(clauses.size() <= 16, "workload query clause limit exceeded");
            for (Object clause : clauses) { collectIds(document(clause), ids, depth + 1); }
        }
    }

    private static Key key(Document id) {
        require(id.keySet().equals(ID_FIELDS), "unknown workload id shape");
        String kind = text(id.get("resourceType"));
        try { WorkloadClaimType.valueOf(kind); } catch (IllegalArgumentException unknown) {
            throw new AssertionError("unknown workload resource type");
        }
        return new Key(text(id.get("clusterId")), kind, text(id.get("resourceId")));
    }

    private void verifyOwner() {
        Document description = description();
        require(description != null && Objects.equals(profileUuid, uuid(description)), "owned profile collection was replaced");
        Document options = document(description.get("options"));
        require(Boolean.TRUE.equals(options.get("capped")) && number(options.get("size")) == limits.cappedBytes(),
                "owned profile capped capacity changed");
        Document settings = settings();
        require(number(settings.get("was")) == 1 && filter.equals(settings.get("filter")),
                "owned profiler settings changed: expected=" + filter.toJson() + "; actual=" + settings.toJson());
    }

    private Document description() {
        return database.listCollections().filter(new Document("name", PROFILE)).first();
    }

    private static Object uuid(Document description) {
        Object value = document(description.get("info")).get("uuid");
        require(value != null, "owned profile collection identity unavailable");
        return value;
    }

    private Document settings() { return database.runCommand(new Document("profile", -1)); }

    @Override
    public synchronized void close() {
        if (closed) { return; }
        closed = true;
        Throwable failure = null;
        boolean ownSettings = !filterInstalled;
        if (filterInstalled) {
            try {
                Document status = settings();
                require(filter.equals(status.get("filter")), "owned profiler filter was replaced; foreign settings preserved");
                ownSettings = true;
                database.runCommand(new Document("profile", 0));
            } catch (RuntimeException | Error problem) { failure = problem; }
        }
        if (collectionCreated && ownSettings) {
            try {
                Document description = description();
                require(description != null && profileUuid != null && Objects.equals(profileUuid, uuid(description)),
                        "owned profile collection was replaced; replacement preserved");
                database.getCollection(PROFILE).drop();
            } catch (RuntimeException | Error problem) { failure = combine(failure, problem); }
        }
        if (previousSettings != null && ownSettings && (filterInstalled || collectionCreated)) {
            try {
                Document restore = new Document("profile", Math.toIntExact(number(previousSettings.get("was"))));
                for (String setting : List.of("slowms", "sampleRate")) {
                    if (previousSettings.containsKey(setting)) { restore.append(setting, previousSettings.get(setting)); }
                }
                restore.append("filter", previousSettings.get("filter") == null ? "unset" : previousSettings.get("filter"));
                database.runCommand(restore);
            } catch (RuntimeException | Error problem) { failure = combine(failure, problem); }
        }
        try { observer.close(); } catch (RuntimeException | Error problem) { failure = combine(failure, problem); }
        if (failure instanceof Error error) { throw error; }
        if (failure instanceof RuntimeException runtime) { throw runtime; }
    }

    private static Throwable combine(Throwable first, Throwable next) {
        if (first == null) { return next; }
        first.addSuppressed(next);
        return first;
    }

    private static String fingerprint(Document row) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(row.toJson().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException unavailable) { throw new AssertionError("SHA-256 unavailable", unavailable); }
    }

    private static Map<Family, Long> zeros() {
        Map<Family, Long> values = new EnumMap<>(Family.class);
        for (Family family : Family.values()) { values.put(family, 0L); }
        return values;
    }

    private static Document document(Object value) {
        require(value instanceof Document, "required profiled document shape unavailable");
        return (Document) value;
    }

    private static List<?> list(Object value) {
        require(value instanceof List<?>, "required profiled array shape unavailable");
        return (List<?>) value;
    }

    private static List<?> operator(Object value, String name, int size) {
        Document document = document(value);
        require(document.keySet().equals(Set.of(name)), "unknown workload expression operator");
        List<?> list = list(document.get(name));
        require(list.size() == size, "unknown workload expression arity");
        return list;
    }

    private static long number(Object value) {
        require(value instanceof Integer || value instanceof Long, "required integral profiler value unavailable");
        return ((Number) value).longValue();
    }

    private static String text(Object value) {
        require(value instanceof String text && !text.isBlank() && text.length() <= 512,
                "required bounded profiler identity unavailable");
        return (String) value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) { throw new AssertionError(message); }
    }
}
