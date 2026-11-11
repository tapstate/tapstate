package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.bson.types.Binary;
import io.tapstate.core.common.PreparedTargetModel;

/** The original CACHE client owns both target preparation facts and their Pipeline claim competition. */
final class MongoSinkPreparationStore implements SinkPreparationStore {
    private static final String TARGETS = "targets";
    private static final String FORMAT_VERSION = "formatVersion";
    private static final String PREPARATION_ID = "preparationId";
    private static final String SINK_PREPARATIONS = "sinkPreparations";
    private static final String TARGET = "target";
    private static final String INPUT = "input";
    private static final String CANONICAL = "canonical";
    private final MongoCachePipelineAccess access;

    MongoSinkPreparationStore(MongoCachePipelineAccess access) {
        this.access = Objects.requireNonNull(access, "access");
    }

    static Document initial(String preparationId) {
        if (!UUID.fromString(preparationId).toString().equals(preparationId))
            throw new IllegalArgumentException("preparation id must be canonical");
        return new Document(FORMAT_VERSION, 1)
                .append(PREPARATION_ID, preparationId)
                .append(TARGETS, null);
    }

    static void requireInitial(Document claim, String preparationId, PipelineIntentScope scope) {
        if (!initial(preparationId).equals(claim.get(SINK_PREPARATIONS))) throw rebuild(scope);
    }

    @Override
    public void prepareTargets(List<Input> originalTargets) {
        var captured = List.copyOf(originalTargets);
        var targets = new TreeMap<String, Document>();
        for (var input : captured) {
            requireScope(input);
            var encoded = input(input);
            if (targets.put(id(input.key()).toJson(), encoded) != null)
                throw new IllegalArgumentException(
                        "target preparation cannot repeat a physical target");
        }
        var manifest = List.copyOf(targets.values());
        requireWriter();
        access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                true,
                session -> {
                    var claim = claim(session);
                    var before = marker(claim);
                    String operation = before.getString(PREPARATION_ID);
                    if (before.get(TARGETS) != null) {
                        if (!manifest.equals(before.get(TARGETS))) throw rebuild(access.scope);
                        verifyClosure(session, manifest, operation);
                        return null;
                    }
                    if (SystemCollections.CACHE_SINK_PREPARATIONS
                                    .on(access.database)
                                    .find(session, scopeFilter(access.scope))
                                    .first()
                            != null) throw rebuild(access.scope);
                    for (var input : captured)
                        SystemCollections.CACHE_SINK_PREPARATIONS
                                .on(access.database)
                                .insertOne(
                                        session,
                                        row(
                                                new Snapshot(
                                                        input,
                                                        operation,
                                                        0,
                                                        Phase.PREPARING,
                                                        null,
                                                        null)));
                    var next = new Document(before).append(TARGETS, manifest);
                    if (SystemCollections.CACHE_WORKLOAD_CLAIMS
                                    .on(access.database)
                                    .updateOne(
                                            session,
                                            new Document(
                                                            "_id",
                                                            MongoWorkloadClaimStore.id(
                                                                    access.originalClaim().key()))
                                                    .append(SINK_PREPARATIONS, before),
                                            new Document(
                                                    "$set", new Document(SINK_PREPARATIONS, next)))
                                    .getMatchedCount()
                            != 1) throw fenced();
                    return null;
                });
    }

    @Override
    public List<Prepared> resolveModels(
            List<Prepared> candidates, PipelinePreparationInputs authoring) {
        var captured = List.copyOf(candidates);
        captured.forEach(this::validateModel);
        requireWriter();
        return access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                false,
                session -> {
                    var control = marker(claim(session));
                    if (control.get(TARGETS) == null) return captured;
                    var manifest = documents(control.get(TARGETS));
                    verifyClosure(session, manifest, control.getString(PREPARATION_ID));
                    var answer = new ArrayList<Prepared>();
                    for (var candidate : captured) {
                        answer.add(resolveModel(session, control, manifest, candidate, authoring));
                    }
                    return List.copyOf(answer);
                });
    }

    private Prepared resolveModel(
            ClientSession session,
            Document control,
            List<Document> manifest,
            Prepared candidate,
            PipelinePreparationInputs authoring) {
        var raw =
                SystemCollections.CACHE_SINK_PREPARATIONS
                        .on(access.database)
                        .find(session, new Document("_id", id(candidate.input().key())))
                        .first();
        var held = decode(raw);
        var original = held.input();
        if (!held.operationId().equals(control.getString(PREPARATION_ID))
                || !manifest.contains(input(original))
                || !original.key().equals(candidate.input().key())
                || original.policy() != candidate.input().policy()
                || original.fullLoad() != candidate.input().fullLoad()) throw rebuild(access.scope);
        var body = storedModel(raw);
        if (body.isEmpty()) {
            if (!original.equals(candidate.input())) throw rebuild(access.scope);
            return candidate;
        }
        if (!original.modelHash().equals(candidate.input().modelHash())) {
            var retained =
                    MongoPipelineSourcePreparations.retained(
                            session, access.database, access.scope);
            if (authoring == null || !retained.equals(authoring)) throw rebuild(access.scope);
        }
        return new Prepared(original, body.orElseThrow());
    }

    @Override
    public boolean hasPreparedModels() {
        return access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                false,
                session -> {
                    var control = marker(claim(session));
                    if (control.get(TARGETS) == null) return false;
                    var manifest = documents(control.get(TARGETS));
                    verifyClosure(session, manifest, control.getString(PREPARATION_ID));
                    if (manifest.isEmpty()) return false;
                    for (var input : manifest) {
                        var raw =
                                SystemCollections.CACHE_SINK_PREPARATIONS
                                        .on(access.database)
                                        .find(
                                                session,
                                                new Document("_id", id(decodeInput(input).key())))
                                        .first();
                        if (storedModel(raw).isPresent()) return true;
                    }
                    return false;
                });
    }

    @Override
    public Optional<Prepared> retainedModel(
            Key key, OnFullLoad policy, boolean fullLoad, PipelinePreparationInputs authoring) {
        return Optional.ofNullable(
                retainedModels(List.of(key), policy, fullLoad, authoring).get(key));
    }

    @Override
    public Map<Key, Prepared> retainedModels(
            Collection<Key> keys,
            OnFullLoad policy,
            boolean fullLoad,
            PipelinePreparationInputs authoring) {
        var captured = List.copyOf(keys);
        for (var key : captured) {
            if (!access.scope.equals(key.scope())) throw fenced();
        }
        requireWriter();
        return access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                false,
                session -> {
                    var control = marker(claim(session));
                    if (control.get(TARGETS) == null) return Map.of();
                    var manifest = documents(control.get(TARGETS));
                    var rows = verifyClosure(session, manifest, control.getString(PREPARATION_ID));
                    boolean hasBody =
                            captured.stream()
                                    .map(rows::get)
                                    .filter(Objects::nonNull)
                                    .anyMatch(row -> storedModel(row).isPresent());
                    boolean sameAuthoring =
                            hasBody
                                    && authoring != null
                                    && MongoPipelineSourcePreparations.retained(
                                                    session, access.database, access.scope)
                                            .equals(authoring);
                    return retainedModels(captured, rows, policy, fullLoad, sameAuthoring);
                });
    }

    private Map<Key, Prepared> retainedModels(
            List<Key> captured,
            Map<Key, Document> rows,
            OnFullLoad policy,
            boolean fullLoad,
            boolean sameAuthoring) {
        var result = new LinkedHashMap<Key, Prepared>();
        for (var key : captured) {
            var raw = rows.get(key);
            if (raw == null) continue;
            var original = decode(raw).input();
            if (original.policy() != policy || original.fullLoad() != fullLoad)
                throw rebuild(access.scope);
            var body = storedModel(raw);
            if (body.isPresent() && sameAuthoring)
                result.put(key, new Prepared(original, body.orElseThrow()));
        }
        return Map.copyOf(result);
    }

    @Override
    public void prepareModels(List<Prepared> targets) {
        prepareTargets(targets.stream().map(Prepared::input).toList(), targets);
    }

    @Override
    public void prepareTargets(List<Input> originalTargets, List<Prepared> originalModels) {
        var captured = List.copyOf(originalTargets);
        var sorted = sortedTargets(captured);
        var bodies = modelBodies(originalModels, sorted);
        requireWriter();
        var manifest = sorted.values().stream().map(MongoSinkPreparationStore::input).toList();
        access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                true,
                session -> {
                    var table = SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database);
                    var before = marker(claim(session));
                    String operation = before.getString(PREPARATION_ID);
                    if (before.get(TARGETS) != null) {
                        if (!manifest.equals(before.get(TARGETS))) throw rebuild(access.scope);
                        verifyClosure(session, manifest, operation);
                        backfillModels(session, table, operation, sorted, bodies);
                        return null;
                    }
                    if (table.find(session, scopeFilter(access.scope)).first() != null)
                        throw rebuild(access.scope);
                    var replacements = new LinkedHashMap<Document, Document>();
                    for (var entry : sorted.entrySet()) {
                        var input = entry.getValue();
                        var held = new Snapshot(input, operation, 0, Phase.PREPARING, null, null);
                        var bytes = bodies.get(entry.getKey());
                        if (bytes != null) requirePublicationBudget(held, bytes);
                        replacements.put(id(input.key()), row(held, bytes));
                    }
                    for (var encoded : replacements.values()) {
                        table.insertOne(session, encoded);
                    }
                    publishManifest(session, before, manifest);
                    return null;
                });
    }

    private Map<String, Input> sortedTargets(List<Input> targets) {
        var sorted = new TreeMap<String, Input>();
        for (var input : targets) {
            requireScope(input);
            if (sorted.put(id(input.key()).toJson(), input) != null)
                throw new IllegalArgumentException(
                        "target preparation cannot repeat a physical target");
        }
        return sorted;
    }

    private Map<String, byte[]> modelBodies(
            List<Prepared> originalModels, Map<String, Input> sorted) {
        var bodies = new HashMap<String, byte[]>();
        long total = 0;
        for (var model : List.copyOf(originalModels)) {
            validateModel(model);
            String key = id(model.input().key()).toJson();
            if (!model.input().equals(sorted.get(key))
                    || bodies.put(key, model.canonical()) != null)
                throw new IllegalArgumentException(
                        "prepared body does not match the complete roster");
            total += model.canonical().length;
        }
        requireScopeBudget(total);
        return bodies;
    }

    private void backfillModels(
            ClientSession session,
            MongoCollection<Document> table,
            String operation,
            Map<String, Input> sorted,
            Map<String, byte[]> bodies) {
        var originals = new LinkedHashMap<Document, Document>();
        var replacements = new LinkedHashMap<Document, Document>();
        long retainedTotal = 0;
        for (var entry : sorted.entrySet()) {
            var input = entry.getValue();
            var raw = table.find(session, new Document("_id", id(input.key()))).first();
            var held = decode(raw);
            if (!held.input().equals(input) || !held.operationId().equals(operation))
                throw rebuild(access.scope);
            var bytes = retainedBody(raw, bodies.get(entry.getKey())).orElse(null);
            if (bytes != null) retainedTotal += bytes.length;
            var upgraded = row(held, bytes);
            if (bytes != null) requirePublicationBudget(held, bytes);
            originals.put(id(input.key()), raw);
            replacements.put(id(input.key()), upgraded);
        }
        requireScopeBudget(retainedTotal);
        for (var entry : replacements.entrySet()) {
            var before = originals.get(entry.getKey());
            if (!entry.getValue().equals(before)
                    && table.replaceOne(session, before, entry.getValue()).getMatchedCount() != 1)
                throw fenced();
        }
    }

    private Optional<byte[]> retainedBody(Document raw, byte[] proposed) {
        var existing = storedModel(raw);
        if (existing.isPresent()
                && proposed != null
                && !Arrays.equals(existing.orElseThrow(), proposed)) throw rebuild(access.scope);
        return existing.isPresent() ? existing : Optional.ofNullable(proposed);
    }

    private void publishManifest(ClientSession session, Document before, List<Document> manifest) {
        var next = new Document(before).append(TARGETS, manifest);
        if (SystemCollections.CACHE_WORKLOAD_CLAIMS
                        .on(access.database)
                        .updateOne(
                                session,
                                new Document(
                                                "_id",
                                                MongoWorkloadClaimStore.id(
                                                        access.originalClaim().key()))
                                        .append(SINK_PREPARATIONS, before),
                                new Document("$set", new Document(SINK_PREPARATIONS, next)))
                        .getMatchedCount()
                != 1) throw fenced();
    }

    private void requirePublicationBudget(Snapshot held, byte[] bytes) {
        requireRowBudget(row(held, bytes), held.input().key().target());
        if (held.phase() == Phase.PREPARING) {
            var originalClaim =
                    held.revision() == 0 ? access.originalClaim() : held.originalClaim();
            if (held.revision() == 0)
                requireRowBudget(
                        row(
                                new Snapshot(
                                        held.input(),
                                        held.operationId(),
                                        1,
                                        Phase.PREPARING,
                                        originalClaim,
                                        null),
                                bytes),
                        held.input().key().target());
            requireRowBudget(
                    row(
                            new Snapshot(
                                    held.input(),
                                    held.operationId(),
                                    2,
                                    Phase.READY,
                                    originalClaim,
                                    new Result(true)),
                            bytes),
                    held.input().key().target());
        }
    }

    private void validateModel(Prepared model) {
        requireScope(model.input());
        var bytes = model.canonical();
        if (bytes.length > MAX_MODEL_ROW_BYTES)
            throw capacity(model.input().key().target(), bytes.length, MAX_MODEL_ROW_BYTES);
        try {
            var decoded = PreparedTargetModel.decode(bytes);
            if (!decoded.name().equals(model.input().key().target())
                    || !PreparedTargetModel.hash(bytes).equals(model.input().modelHash()))
                throw unreadable();
        } catch (IllegalArgumentException invalid) {
            throw unreadable();
        }
    }

    private void requireRowBudget(Document row, String target) {
        int bytes = new RawBsonDocument(row, new DocumentCodec()).getByteBuffer().remaining();
        if (bytes > MAX_MODEL_ROW_BYTES) throw capacity(target, bytes, MAX_MODEL_ROW_BYTES);
    }

    private void requireScopeBudget(long bytes) {
        if (bytes > MAX_SCOPE_MODEL_BYTES)
            throw capacity("all-targets", bytes, MAX_SCOPE_MODEL_BYTES);
    }

    private TapstateException capacity(String target, long bytes, long limit) {
        return new TapstateException(
                IoError.SINK_MODEL_CAPACITY,
                Map.of(
                        "pipeline",
                        access.scope.pipelineId(),
                        TARGET,
                        target,
                        "bytes",
                        bytes,
                        "limit",
                        limit),
                null);
    }

    private static List<Document> documents(Object value) {
        if (!(value instanceof List<?> list)) throw unreadable();
        var result = new ArrayList<Document>();
        for (Object entry : list) {
            if (!(entry instanceof Document document)) throw unreadable();
            result.add(document);
        }
        return result;
    }

    @Override
    public Snapshot load(Input original) {
        requireScope(original);
        return access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                false,
                session -> require(session, original));
    }

    @Override
    public List<Snapshot> loadMany(List<Input> originals) {
        var inputs = List.copyOf(originals);
        inputs.forEach(this::requireScope);
        if (inputs.isEmpty()) return List.of();
        return access.metadataTransaction(SystemCollections.CACHE_SINK_PREPARATIONS, false, session -> {
            var control = marker(claim(session));
            if (!(control.get(TARGETS) instanceof List<?> targets)) throw rebuild(access.scope);
            var manifest = new HashSet<>(targets);
            var requested = new LinkedHashMap<Key, Input>();
            for (var expected : inputs) {
                if (!manifest.contains(input(expected))) throw rebuild(access.scope);
                var previous = requested.put(expected.key(), expected);
                if (previous != null && !previous.equals(expected)) throw rebuild(access.scope);
            }
            var found = new HashMap<Key, Snapshot>();
            var query = new Document("_id", new Document("$in", requested.keySet().stream().map(MongoSinkPreparationStore::id).toList()));
            try (var cursor = SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database).find(session, query).batchSize(32).iterator()) {
                while (cursor.hasNext()) {
                    var raw = cursor.next();
                    var held = decode(raw);
                    if (!held.input().equals(requested.get(held.input().key()))
                            || !held.operationId().equals(control.getString(PREPARATION_ID))) throw rebuild(access.scope);
                    if (storedModel(raw).isPresent()) requireRowBudget(raw, held.input().key().target());
                    if (found.put(held.input().key(), held) != null) throw rebuild(access.scope);
                }
            }
            if (found.size() != requested.size()) throw rebuild(access.scope);
            return inputs.stream().map(input -> found.get(input.key())).toList();
        });
    }

    @Override
    public Attempt begin(Input original) {
        requireScope(original);
        requireWriter();
        return access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                true,
                session -> {
                    Snapshot held = require(session, original);
                    if (held.phase() != Phase.PREPARING || held.revision() != 0)
                        throw rebuild(access.scope);
                    var started =
                            new Snapshot(
                                    original,
                                    held.operationId(),
                                    1,
                                    Phase.PREPARING,
                                    access.originalClaim(),
                                    null);
                    replace(session, held, started);
                    return new Issued(this, started);
                });
    }

    @Override
    public Snapshot complete(Attempt originalAttempt, Result originalResult) {
        Objects.requireNonNull(originalResult, "originalResult");
        if (!(originalAttempt instanceof Issued issued) || issued.owner() != this) throw fenced();
        Snapshot original = issued.original();
        requireWriter();
        if (!access.originalClaim().equals(original.originalClaim())) throw fenced();
        return access.metadataTransaction(
                SystemCollections.CACHE_SINK_PREPARATIONS,
                true,
                session -> {
                    var held = require(session, original.input());
                    var ready =
                            new Snapshot(
                                    original.input(),
                                    original.operationId(),
                                    2,
                                    Phase.READY,
                                    access.originalClaim(),
                                    originalResult);
                    if (held.equals(ready)) return held;
                    if (!held.equals(original)) throw rebuild(access.scope);
                    replace(session, original, ready);
                    return ready;
                });
    }

    private record Issued(MongoSinkPreparationStore owner, Snapshot original) implements Attempt {}

    private void replace(ClientSession session, Snapshot before, Snapshot next) {
        var table = SystemCollections.CACHE_SINK_PREPARATIONS.on(access.database);
        var raw = table.find(session, new Document("_id", id(before.input().key()))).first();
        if (!decode(raw).equals(before)) throw fenced();
        var bytes = storedModel(raw).orElse(null);
        var replacement = row(next, bytes);
        if (bytes != null) requireRowBudget(replacement, next.input().key().target());
        if (table.replaceOne(session, raw, replacement).getMatchedCount() != 1) throw fenced();
    }

    private Snapshot require(ClientSession session, Input expected) {
        var control = marker(claim(session));
        if (!(control.get(TARGETS) instanceof List<?> targets)
                || !targets.contains(input(expected))) throw rebuild(access.scope);
        var raw =
                SystemCollections.CACHE_SINK_PREPARATIONS
                        .on(access.database)
                        .find(session, new Document("_id", id(expected.key())))
                        .first();
        Snapshot held = decode(raw);
        if (!held.input().equals(expected)
                || !held.operationId().equals(control.getString(PREPARATION_ID)))
            throw rebuild(access.scope);
        if (storedModel(raw).isPresent()) requireRowBudget(raw, held.input().key().target());
        return held;
    }

    private Map<Key, Document> verifyClosure(
            ClientSession session, List<Document> targets, String operation) {
        var expected = new HashSet<>(targets);
        long total = 0;
        var result = new LinkedHashMap<Key, Document>();
        try (var rows =
                SystemCollections.CACHE_SINK_PREPARATIONS
                        .on(access.database)
                        .find(session, scopeFilter(access.scope))
                        .iterator()) {
            while (rows.hasNext()) {
                var raw = rows.next();
                var held = decode(raw);
                var bytes = storedModel(raw).orElse(null);
                if (bytes != null) {
                    requireRowBudget(raw, held.input().key().target());
                    total += bytes.length;
                }
                if (!held.operationId().equals(operation) || !expected.remove(input(held.input())))
                    throw rebuild(access.scope);
                result.put(held.input().key(), raw);
            }
        }
        if (!expected.isEmpty()) throw rebuild(access.scope);
        requireScopeBudget(total);
        return result;
    }

    private Document claim(ClientSession session) {
        var key =
                new WorkloadClaimKey(
                        access.scope.clusterId(),
                        WorkloadClaimType.PIPELINE_ACTUATION,
                        access.scope.pipelineId());
        var claim =
                SystemCollections.CACHE_WORKLOAD_CLAIMS
                        .on(access.database)
                        .find(session, new Document("_id", MongoWorkloadClaimStore.id(key)))
                        .first();
        if (claim == null
                || !MongoPipelinePreparationFacts.scope(access.scope)
                        .equals(claim.get("pipelineScope"))) throw rebuild(access.scope);
        return claim;
    }

    private Document marker(Document claim) {
        try {
            if (!(claim.get("pipelinePreparation") instanceof Document prepared)
                    || !(claim.get(SINK_PREPARATIONS) instanceof Document marker)
                    || !marker.keySet().equals(Set.of(FORMAT_VERSION, PREPARATION_ID, TARGETS))
                    || !Integer.valueOf(1).equals(marker.get(FORMAT_VERSION))
                    || !prepared.getString(PREPARATION_ID).equals(marker.getString(PREPARATION_ID))
                    || !UUID.fromString(marker.getString(PREPARATION_ID))
                            .toString()
                            .equals(marker.getString(PREPARATION_ID))) throw rebuild(access.scope);
            Object entries = marker.get(TARGETS);
            if (entries != null) {
                if (!(entries instanceof List<?> values)) throw rebuild(access.scope);
                String previous = null;
                for (Object value : values) {
                    if (!(value instanceof Document encoded)) throw rebuild(access.scope);
                    var input = decodeInput(encoded);
                    requireScope(input);
                    String key = id(input.key()).toJson();
                    if (previous != null && previous.compareTo(key) >= 0)
                        throw rebuild(access.scope);
                    previous = key;
                }
            }
            return marker;
        } catch (IllegalArgumentException | ClassCastException | NullPointerException bad) {
            throw unreadable();
        }
    }

    private void requireScope(Input original) {
        if (!access.scope.equals(Objects.requireNonNull(original, "originalTarget").key().scope()))
            throw fenced();
    }

    private void requireWriter() {
        if (access.originalClaim() == null || access.originalClaim().executionGeneration() < 1)
            throw fenced();
    }

    static Document scopeFilter(PipelineIntentScope scope) {
        return new Document("_id.scope.clusterId", scope.clusterId())
                .append("_id.scope.bindingVersion", scope.bindingVersion())
                .append("_id.scope.cacheIncarnation", scope.cacheIncarnation())
                .append("_id.scope.pipelineId", scope.pipelineId())
                .append("_id.scope.stateGeneration", scope.stateGeneration());
    }

    static void requireEmpty(
            ClientSession session, MongoDatabase database, PipelineIntentScope scope) {
        if (SystemCollections.CACHE_SINK_PREPARATIONS
                        .on(database)
                        .find(session, scopeFilter(scope))
                        .first()
                != null) throw rebuild(scope);
    }

    static Document id(Key key) {
        return new Document("scope", MongoPipelinePreparationFacts.scope(key.scope()))
                .append("purpose", key.purpose().segment())
                .append("node", OperatorStateText.encode(key.nodeId()))
                .append(TARGET, OperatorStateText.encode(key.target()));
    }

    private static Document input(Input value) {
        return new Document("key", id(value.key()))
                .append("modelHash", value.modelHash())
                .append("policy", value.policy().yaml())
                .append("fullLoad", value.fullLoad());
    }

    private static Input decodeInput(Document value) {
        var key = value.get("key", Document.class);
        var purpose =
                switch (key.getString("purpose")) {
                    case "view" -> PipelineNode.Purpose.VIEW;
                    case "sync" -> PipelineNode.Purpose.SYNC;
                    default -> throw unreadable();
                };
        var policy =
                switch (value.getString("policy")) {
                    case "clear" -> OnFullLoad.CLEAR;
                    case "append" -> OnFullLoad.APPEND;
                    case "fail" -> OnFullLoad.FAIL;
                    default -> throw unreadable();
                };
        if (!(value.get("fullLoad") instanceof Boolean full)) throw unreadable();
        var result =
                new Input(
                        new Key(
                                MongoPipelinePreparationFacts.scope(
                                        key.get("scope", Document.class)),
                                purpose,
                                OperatorStateText.decode(key.getString("node")),
                                OperatorStateText.decode(key.getString(TARGET))),
                        value.getString("modelHash"),
                        policy,
                        full);
        if (!input(result).equals(value)) throw unreadable();
        return result;
    }

    private static Document row(Snapshot value) {
        return new Document("_id", id(value.input().key()))
                .append(FORMAT_VERSION, 1)
                .append(INPUT, input(value.input()))
                .append("operationId", value.operationId())
                .append("revision", value.revision())
                .append("phase", value.phase().name())
                .append(
                        "originalClaim",
                        value.originalClaim() == null
                                ? null
                                : WorkloadClaimDocuments.stored(value.originalClaim()))
                .append(
                        "result",
                        value.result() == null
                                ? null
                                : new Document("created", value.result().created()));
    }

    private static Document row(Snapshot value, byte[] canonical) {
        var row = row(value);
        if (canonical != null)
            row.put(
                    "targetModel",
                    new Document(FORMAT_VERSION, 1).append(CANONICAL, new Binary(canonical)));
        if (canonical != null) row.put(FORMAT_VERSION, 2);
        return row;
    }

    private static Optional<byte[]> storedModel(Document row) {
        if (Integer.valueOf(1).equals(row.get(FORMAT_VERSION))) return Optional.empty();
        try {
            var body = row.get("targetModel", Document.class);
            if (body == null
                    || !body.keySet().equals(Set.of(FORMAT_VERSION, CANONICAL))
                    || !Integer.valueOf(1).equals(body.get(FORMAT_VERSION))
                    || !(body.get(CANONICAL) instanceof Binary binary)
                    || binary.getType() != 0) throw unreadable();
            var bytes = binary.getData();
            var model = PreparedTargetModel.decode(bytes);
            var input = decodeInput(row.get(INPUT, Document.class));
            if (!model.name().equals(input.key().target())
                    || !PreparedTargetModel.hash(bytes).equals(input.modelHash()))
                throw unreadable();
            return Optional.of(bytes.clone());
        } catch (IllegalArgumentException | ClassCastException invalid) {
            throw unreadable();
        }
    }

    private static Snapshot decode(Document value) {
        if (value == null) throw unreadable();
        try {
            if ((!Integer.valueOf(1).equals(value.get(FORMAT_VERSION))
                            && !Integer.valueOf(2).equals(value.get(FORMAT_VERSION)))
                    || !(value.get("revision") instanceof Long revision)) throw unreadable();
            Phase phase =
                    switch (value.getString("phase")) {
                        case "PREPARING" -> Phase.PREPARING;
                        case "READY" -> Phase.READY;
                        default -> throw unreadable();
                    };
            var original = value.get("originalClaim", Document.class);
            var result = value.get("result", Document.class);
            if (result != null && !(result.get("created") instanceof Boolean)) throw unreadable();
            var decoded =
                    new Snapshot(
                            decodeInput(value.get(INPUT, Document.class)),
                            value.getString("operationId"),
                            revision,
                            phase,
                            original == null ? null : WorkloadClaimDocuments.readPipeline(original),
                            result == null ? null : new Result(result.getBoolean("created")));
            if (!row(decoded, storedModel(value).orElse(null)).equals(value)) throw unreadable();
            return decoded;
        } catch (IllegalArgumentException | ClassCastException | NullPointerException bad) {
            throw unreadable();
        }
    }

    private static TapstateException fenced() {
        return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }

    private static TapstateException rebuild(PipelineIntentScope scope) {
        return new TapstateException(
                IoError.PIPELINE_STATE_REBUILD_REQUIRED,
                Map.of("pipeline", scope.pipelineId()),
                null);
    }

    private static TapstateException unreadable() {
        return new TapstateException(
                IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "sink-preparation", "field", "preparation"),
                null);
    }
}
