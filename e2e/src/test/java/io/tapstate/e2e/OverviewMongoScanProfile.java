package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import io.tapstate.adapters.mongostore.MongoStorePort;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/** Optional namespace evidence; a profiler cannot distinguish console and background callers. */
final class OverviewMongoScanProfile implements AutoCloseable {
    private static final String PREFIX = "tapstate.e2e.overview-attribution.";
    private static final String PROFILE = "system.profile";
    private static final int MAX_RECORDS = 2048;
    private static final int MAX_RECORD_BYTES = 65536;
    private static final int MAX_BYTES = 4 * 1024 * 1024;
    private static final List<String> READS = List.of("find", "aggregate", "getMore", "count", "distinct",
            "listIndexes", "listCollections");

    private final Path jar;
    private final String jarSha;
    private final Path harnessRoot;
    private final Document initialHarness;
    private final Path output;
    private final MongoClient observer;
    private final MongoDatabase database;
    private final String appName;
    private final String observerName;
    private final String nativeUri;
    private final String anchor;
    private final Document filter;
    private final Document report = new Document("kind", "OVERVIEW_SCAN_ATTRIBUTION_DIAGNOSTIC")
            .append("performanceAcceptanceEligible", false).append("callerAttribution", "UNATTRIBUTED");
    private final List<Document> windows = new ArrayList<>();
    private List<String> prefix = List.of();
    private int anchorIndex = -1;
    private Document previousSettings;
    private Object profileUuid;
    private boolean collectionCreated;
    private boolean filterInstalled;
    private boolean begun;
    private boolean closed;
    private String lastReportDigest;
    private OverviewHistoryCallerJdiSession caller;

    static boolean requested(Tiers tier) {
        return tier == Tiers.REAL_PROCESS && List.of("output-dir", "jar", "sha256").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null);
    }

    static String databaseStem(Tiers tier) {
        return requested(tier) ? "overview_history_" + UUID.randomUUID().toString().replace("-", "")
                : "overview_history";
    }

    static OverviewMongoScanProfile openIfRequested(Tiers tier, String baseUri) throws Exception {
        return requested(tier) ? new OverviewMongoScanProfile(baseUri) : null;
    }

    private OverviewMongoScanProfile(String baseUri) throws Exception {
        jar = Path.of(required("jar")).toRealPath();
        jarSha = required("sha256");
        require(jarSha.matches("[0-9a-f]{64}") && Files.isRegularFile(jar)
                && jarSha.equals(PipelineBenchmarkLiveRunIT.sha256(jar)), "immutable diagnostic JAR mismatch");
        Path directory = Path.of(required("output-dir"));
        require(directory.isAbsolute(), "diagnostic output directory must be absolute");
        output = directory.resolve("real-process.json");
        Path root = PipelineBenchmarkLiveRunIT.harnessRoot();
        harnessRoot = root;
        initialHarness = harnessInputs(root);
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, root);
        require(!Files.exists(output), "diagnostic output already exists; preserve the earlier run");

        ConnectionString connection = new ConnectionString(baseUri);
        String name = connection.getDatabase();
        require(name != null && name.matches("overview_history_[0-9a-f]{32}_real_process")
                && connection.getApplicationName() == null, "diagnostic needs its uniquely owned database");
        require(connection.getHosts().stream().allMatch(host -> host.equals("localhost")
                || host.startsWith("localhost:") || host.equals("127.0.0.1")
                || host.startsWith("127.0.0.1:") || host.startsWith("[::1]")), "diagnostic endpoint is not loopback");
        String nonce = UUID.randomUUID().toString().replace("-", "");
        appName = "tapstate-overview-native-" + nonce;
        observerName = "tapstate-overview-profile-" + nonce;
        anchor = "tapstate-overview-anchor-" + nonce;
        nativeUri = baseUri + (baseUri.contains("?") ? "&" : "?") + "appName=" + appName;
        observer = MongoClients.create(MongoClientSettings.builder().applyConnectionString(connection)
                .applicationName(observerName).build());
        database = observer.getDatabase(name);
        List<Document> reads = READS.stream().map(command -> new Document("command." + command,
                new Document("$exists", true))).toList();
        filter = new Document("$and", List.of(new Document("ns", new Document("$regex", "^" + name + "\\.")),
                new Document("$or", List.of(new Document("$and", List.of(eq("appName", appName),
                        new Document("$or", reads))), new Document("$and", List.of(eq("appName", observerName),
                        eq("command.find", MongoStorePort.WORKLOAD_CLAIMS), eq("command.comment", anchor)))))));
        report.append("database", name).append("nativeAppName", appName)
                .append("observerAppName", observerName).append("profileFilter", filter)
                .append("profileCappedBytes", MAX_BYTES).append("maxProfileRecords", MAX_RECORDS)
                .append("inputs", new Document("jar", PipelineBenchmarkLiveRunIT.artifact(jar))
                        .append("expectedJarSha256", jarSha).append("harness", initialHarness))
                .append("windows", windows).append("status", "PREPARED")
                .append("unverified", List.of("API_VS_BACKGROUND_CALLER", "OTHER_DATABASE_WORK",
                        "NON_READ_COMMAND_WORK", "EXACT_GLOBAL_PROFILE_BOUNDARY_JOIN"));
    }

    ServerHandle launch() {
        String selected = System.getProperty(PREFIX + "caller-stack");
        require(selected == null || selected.equals("true") || selected.equals("false"), "invalid caller-stack selection");
        if (!"true".equals(selected)) { return RealProcessServer.start(nativeUri, jar); }
        try {
            caller = OverviewHistoryCallerJdiSession.start(nativeUri, jar, jarSha);
            return new ServerHandle() {
                @Override public java.net.URI baseUrl() { return caller.server().baseUrl(); }
                @Override public void close() {
                    try { caller.close(); }
                    catch (RuntimeException | Error problem) { throw problem; }
                    catch (Exception problem) {
                        if (problem instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                        throw new AssertionError("owned caller observer close failed", problem);
                    }
                }
            };
        } catch (RuntimeException | Error problem) { throw problem; }
        catch (Exception problem) { throw new AssertionError("owned caller observer launch failed", problem); }
    }

    void begin(String pipelineId, int historyDocuments, int readsPerEndpoint) {
        require(!begun && !closed, "diagnostic profile can begin only once");
        report.append("pipelineId", pipelineId).append("originalHistoryDocuments", historyDocuments)
                .append("originalReadsPerEndpoint", readsPerEndpoint);
        previousSettings = settings();
        require(number(previousSettings.get("was")) == 0, "do not take over an active profiler");
        require(description() == null, "do not replace an existing profile collection");
        database.createCollection(PROFILE, new CreateCollectionOptions().capped(true).sizeInBytes(MAX_BYTES));
        collectionCreated = true;
        profileUuid = uuid(Objects.requireNonNull(description()));
        filterInstalled = true;
        database.runCommand(new Document("profile", 1).append("filter", filter));
        database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("_id", new Document("overviewAbsentAnchor", anchor)))
                .comment(anchor).limit(1).first();
        List<Document> beginning = Await.answered("the owned overview profiler anchor", Duration.ofSeconds(5), () -> {
            List<Document> rows = rows();
            return rows.stream().anyMatch(this::isAnchor) ? java.util.Optional.of(rows) : java.util.Optional.empty();
        });
        accept(beginning);
        begun = true;
        report.append("status", "COLLECTING");
    }

    <T> T round(String action, Supplier<T> body, ToLongFunction<T> globalExamined, ToLongFunction<T> millis) {
        require(begun && !closed && windows.size() < 2, "invalid diagnostic round");
        verifyOwner();
        accept(rows());
        int first = prefix.size();
        Instant startedAt = Instant.now();
        long started = System.nanoTime();
        T answer = body.get();
        long durationNanos = System.nanoTime() - started;
        Instant returnedAt = Instant.now();
        List<Document> after = rows();
        accept(after);
        List<Document> operations = new ArrayList<>();
        Map<String, Document> byNamespace = new LinkedHashMap<>();
        long knownExamined = 0;
        int unknownCounts = 0;
        for (Document row : after.subList(first, after.size())) {
            require(!isAnchor(row), "unexpected second diagnostic anchor");
            Document operation = evidence(row);
            operations.add(operation);
            String ns = row.getString("ns");
            Document summary = byNamespace.computeIfAbsent(ns, ignored -> new Document("profileRecords", 0)
                    .append("knownExamined", 0L).append("unknownExaminedRecords", 0));
            summary.put("profileRecords", summary.getInteger("profileRecords") + 1);
            if (row.containsKey("docsExamined") && row.containsKey("keysExamined")) {
                long examined = Math.addExact(number(row.get("docsExamined")), number(row.get("keysExamined")));
                knownExamined = Math.addExact(knownExamined, examined);
                summary.put("knownExamined", Math.addExact(summary.getLong("knownExamined"), examined));
            } else {
                unknownCounts++;
                summary.put("unknownExaminedRecords", summary.getInteger("unknownExaminedRecords") + 1);
            }
        }
        Map<String, Object> callerEvidence = null;
        if (caller != null) {
            try { callerEvidence = caller.boundary(); }
            catch (Exception problem) { throw new AssertionError("caller boundary unavailable", problem); }
            attachCallers(operations, callerEvidence);
        }
        long global = globalExamined.applyAsLong(answer);
        require(global >= 0, "global examined counters moved backward");
        require(!operations.isEmpty(), "no actual native reads were profiled; namespace attribution is unverified");
        windows.add(new Document("action", action).append("startedAt", startedAt.toString())
                .append("returnedAt", returnedAt.toString()).append("elapsedNanos", durationNanos)
                .append("originalRoundMillis", millis.applyAsLong(answer)).append("originalGlobalExamined", global)
                .append("profileReadRecords", operations.size()).append("profileKnownExamined", knownExamined)
                .append("profileUnknownExaminedRecords", unknownCounts)
                .append("globalMinusKnownProfile", Math.subtractExact(global, knownExamined))
                .append("residualAttribution", "UNATTRIBUTED")
                .append("boundaryJoin", "NON_ATOMIC_PROFILE_PREFIX_AND_GLOBAL_COUNTER_WINDOWS")
                .append("byNamespace", byNamespace).append("operations", operations)
                .append("callerEvidence", callerEvidence));
        report.append("status", "COLLECTED");
        write();
        return answer;
    }

    void failed(Throwable failure) {
        report.append("status", "FAILED").append("failure", new Document("type", failure.getClass().getName())
                .append("message", failure.getMessage()));
        write();
    }

    /** Only the actual initial scoped query shape is joined; other shapes remain explicitly unattributed. */
    private static void attachCallers(List<Document> operations, Map<String, Object> boundary) {
        require(boundary.get("entries") instanceof List<?>, "caller entries unavailable");
        List<?> entries = (List<?>) boundary.get("entries");
        for (Document operation : operations) {
            if (!(operation.get("command") instanceof Document command)
                    || !"pipeline_rate_history".equals(command.get("find"))) { continue; }
            List<?> matches = entries.stream().filter(value -> value instanceof Map<?, ?> entry
                    && initialQueryMatches(operation, command, entry)).toList();
            operation.append("callerEntryMatches", matches.size());
            if (matches.size() != 1) { continue; }
            Map<?, ?> entry = (Map<?, ?>) matches.getFirst();
            long sameShapeRows = operations.stream().filter(row -> row.get("command") instanceof Document actual
                    && "pipeline_rate_history".equals(actual.get("find")) && initialQueryMatches(row, actual, entry)).count();
            operation.append("sameShapeProfileRows", sameShapeRows);
            if (sameShapeRows != 1) { continue; }
            String kind = (String) entry.get("callerKind");
            if (!Set.of("ROLLUP", "OVERVIEW_API").contains(kind)) { continue; }
            operation.append("callerAttribution", kind).append("callerEntry", entry)
                    .append("callerJoin", "UNIQUE_ACTUAL_NAMESPACE_AND_COMPLETE_INITIAL_QUERY_SHAPE");
        }
    }
    private static boolean initialQueryMatches(Document operation, Document command, Map<?, ?> entry) {
        if (!Objects.equals(operation.get("ns"), entry.get("database") + "." + entry.get("collection"))
                || !Boolean.FALSE.equals(entry.get("includeLegacy")) || entry.get("after") != null
                || !(entry.get("requestedLimit") instanceof Number limit)
                || !(command.get("limit") instanceof Number wireLimit) || wireLimit.longValue() != limit.longValue() + 1
                || !new Document("observedAt", 1).append("_id", 1).equals(command.get("sort"))
                || !(command.get("filter") instanceof Document filter)
                || !(filter.get("$and") instanceof List<?> clauses) || clauses.size() != 4) { return false; }
        Map<String, Object> expected = Map.of("pipelineId", entry.get("pipelineId"),
                "pipelineIncarnationId", entry.get("incarnationId"));
        Map<String, Object> actual = new LinkedHashMap<>();
        Long from = null, to = null;
        for (Object raw : clauses) {
            if (!(raw instanceof Document clause) || clause.size() != 1) { return false; }
            if (clause.get("observedAt") instanceof Document range && range.size() == 1) {
                if (range.get("$gte") instanceof java.util.Date at) { if (from != null) { return false; } from = at.getTime(); }
                else if (range.get("$lt") instanceof java.util.Date at) { if (to != null) { return false; } to = at.getTime(); }
                else { return false; }
            } else {
                var field = clause.entrySet().iterator().next();
                if (actual.putIfAbsent(field.getKey(), field.getValue()) != null) { return false; }
            }
        }
        return expected.equals(actual) && Objects.equals(from, entry.get("fromBsonMillis"))
                && Objects.equals(to, entry.get("toBsonMillis"));
    }

    private Document evidence(Document row) {
        Document result = new Document("rowSha256", fingerprint(row)).append("callerAttribution", "UNATTRIBUTED");
        for (String field : List.of("ts", "op", "ns", "appName", "client", "command", "originatingCommand",
                "cursorid", "millis", "nreturned", "docsExamined", "keysExamined", "planSummary", "errCode", "lsid")) {
            if (row.containsKey(field)) { result.append(field, row.get(field)); }
        }
        result.append("examinedComplete", row.containsKey("docsExamined") && row.containsKey("keysExamined"));
        return result;
    }

    private List<Document> rows() {
        return database.getCollection(PROFILE).find().sort(new Document("$natural", 1))
                .limit(MAX_RECORDS + 1).into(new ArrayList<>());
    }

    private void accept(List<Document> rows) {
        require(rows.size() <= MAX_RECORDS, "profile record budget exceeded");
        verifyOwner();
        List<String> next = new ArrayList<>(rows.size());
        int foundAnchor = -1;
        for (int i = 0; i < rows.size(); i++) {
            Document row = rows.get(i);
            require(row.get("ns") instanceof String ns && ns.startsWith(database.getName() + "."),
                    "profile namespace escaped its owned database");
            require(row.get("ts") instanceof java.util.Date, "profile timestamp is unavailable");
            require(row.get("command") instanceof Document, "profile command is unavailable");
            require(!truncated(row), "profile record contains a truncated command");
            require(row.toJson().getBytes(StandardCharsets.UTF_8).length <= MAX_RECORD_BYTES,
                    "profile record byte budget exceeded");
            if (isAnchor(row)) {
                require(foundAnchor < 0, "profile anchor duplicated");
                foundAnchor = i;
            } else {
                require(appName.equals(row.getString("appName")), "foreign profile application name");
                Document command = row.get("command", Document.class);
                require(READS.stream().anyMatch(command::containsKey), "unexpected non-read profiled command");
            }
            for (String counter : List.of("docsExamined", "keysExamined")) {
                if (row.containsKey(counter)) { require(number(row.get(counter)) >= 0, "negative profile examined count"); }
            }
            next.add(fingerprint(row));
        }
        require(foundAnchor >= 0 && (anchorIndex < 0 || foundAnchor == anchorIndex), "profile anchor lost or moved");
        require(next.size() >= prefix.size(), "profile prefix shrank");
        for (int i = 0; i < prefix.size(); i++) { require(prefix.get(i).equals(next.get(i)), "profile prefix changed"); }
        anchorIndex = foundAnchor;
        prefix = List.copyOf(next);
    }

    private boolean isAnchor(Document row) {
        return observerName.equals(row.getString("appName")) && row.get("command") instanceof Document command
                && MongoStorePort.WORKLOAD_CLAIMS.equals(command.get("find")) && anchor.equals(command.get("comment"));
    }

    private void verifyOwner() {
        Document current = settings();
        require(filterInstalled && number(current.get("was")) == 1 && filter.equals(current.get("filter")),
                "owned profile filter or active level changed");
        Document found = description();
        require(found != null && Objects.equals(profileUuid, uuid(found)), "owned profile collection changed");
    }

    private Document settings() { return database.runCommand(new Document("profile", -1)); }
    private Document description() { return database.listCollections().filter(new Document("name", PROFILE)).first(); }
    private static Object uuid(Document description) {
        require(description.get("info") instanceof Document, "profile collection metadata unavailable");
        Object value = description.get("info", Document.class).get("uuid");
        require(value != null, "profile collection UUID unavailable");
        return value;
    }

    private void write() {
        try {
            byte[] bytes = report.toJson().getBytes(StandardCharsets.UTF_8);
            require(bytes.length <= MAX_BYTES, "diagnostic report byte budget exceeded");
            Files.createDirectories(output.getParent());
            if (lastReportDigest == null) {
                Files.write(output, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            } else {
                require(Files.size(output) <= MAX_BYTES && lastReportDigest.equals(digest(Files.readAllBytes(output))),
                        "diagnostic report was replaced; foreign output preserved");
                Files.write(output, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            }
            lastReportDigest = digest(bytes);
        } catch (IOException failure) { throw new java.io.UncheckedIOException(failure); }
    }

    @Override public void close() throws Exception {
        if (closed) { return; }
        closed = true;
        Throwable failure = null;
        boolean ownSettings = false;
        if (filterInstalled) {
            try { verifyOwner(); ownSettings = true; database.runCommand(new Document("profile", 0)); }
            catch (RuntimeException | Error problem) { failure = problem; }
        } else if (collectionCreated) {
            try {
                Document current = settings();
                require(number(current.get("was")) == 0
                        && Objects.equals(current.get("filter"), previousSettings.get("filter")),
                        "profiler changed before filter installation; foreign settings preserved");
                ownSettings = true;
            } catch (RuntimeException | Error problem) { failure = problem; }
        }
        if (collectionCreated && ownSettings) {
            try {
                Document found = description();
                require(found != null && Objects.equals(profileUuid, uuid(found)), "replacement profile collection preserved");
                database.getCollection(PROFILE).drop();
            } catch (RuntimeException | Error problem) { failure = combine(failure, problem); }
        }
        if (previousSettings != null && ownSettings && (collectionCreated || filterInstalled)) {
            try {
                Document restore = new Document("profile", Math.toIntExact(number(previousSettings.get("was"))));
                for (String key : List.of("slowms", "sampleRate")) {
                    if (previousSettings.containsKey(key)) { restore.append(key, previousSettings.get(key)); }
                }
                restore.append("filter", previousSettings.get("filter") == null ? "unset" : previousSettings.get("filter"));
                database.runCommand(restore);
            } catch (RuntimeException | Error problem) { failure = combine(failure, problem); }
        }
        try {
            if (caller != null) { caller.close(); report.append("callerShutdown", caller.afterCloseEvidence()); }
            require(jarSha.equals(PipelineBenchmarkLiveRunIT.sha256(jar)), "diagnostic JAR changed during run");
            require(initialHarness.equals(harnessInputs(harnessRoot)), "diagnostic source or executing classes changed");
            write();
        } catch (Exception | Error problem) { failure = combine(failure, problem); }
        try { observer.close(); } catch (RuntimeException | Error problem) { failure = combine(failure, problem); }
        if (failure instanceof Exception exception) { throw exception; }
        if (failure instanceof Error error) { throw error; }
    }

    private static Document harnessInputs(Path root) throws Exception {
        Document inputs = new Document();
        List<Class<?>> types = new ArrayList<>(List.of(OverviewMongoScanProfile.class, OverviewQueriesDoNotScanTheHistoryIT.class));
        if ("true".equals(System.getProperty(PREFIX + "caller-stack"))) {
            types.add(OverviewHistoryCallerJdiSession.class); types.add(NativeTelemetryMirror.class);
        }
        for (Class<?> type : types) {
            inputs.append(type.getSimpleName() + "Source", PipelineBenchmarkLiveRunIT.artifact(root.resolve(
                    "e2e/src/test/java/io/tapstate/e2e/" + type.getSimpleName() + ".java")));
            try (InputStream input = type.getResourceAsStream(type.getSimpleName() + ".class")) {
                require(input != null, "executing diagnostic class unavailable");
                byte[] bytes = input.readNBytes(1024 * 1024 + 1);
                require(bytes.length <= 1024 * 1024, "executing class byte budget exceeded");
                inputs.append(type.getSimpleName() + "ExecutingClassSha256", digest(bytes));
            }
        }
        return inputs;
    }
    private static boolean truncated(Object value) { return truncated(value, 0); }
    private static boolean truncated(Object value, int depth) {
        require(depth <= 16, "profile command nesting budget exceeded");
        if (value instanceof Map<?, ?> map) {
            return map.containsKey("$truncated") || map.values().stream().anyMatch(item -> truncated(item, depth + 1));
        }
        if (value instanceof List<?> list) { return list.stream().anyMatch(item -> truncated(item, depth + 1)); }
        return false;
    }
    private static String fingerprint(Document row) { return digest(row.toJson().getBytes(StandardCharsets.UTF_8)); }
    private static String digest(byte[] value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)); }
        catch (java.security.NoSuchAlgorithmException missing) { throw new AssertionError("SHA-256 unavailable", missing); }
    }
    private static Document eq(String key, String value) { return new Document(key, new Document("$eq", value)); }
    private static long number(Object value) {
        require(value instanceof Integer || value instanceof Long, "integral profiler count unavailable");
        return ((Number) value).longValue();
    }
    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        require(value != null && !value.isBlank(), "missing diagnostic property " + PREFIX + name);
        return value;
    }
    private static Throwable combine(Throwable first, Throwable next) {
        if (first == null) { return next; }
        if (first != next) { first.addSuppressed(next); }
        return first;
    }
    private static void require(boolean condition, String message) { if (!condition) { throw new AssertionError(message); } }
}
