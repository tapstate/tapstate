package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsCaptureCheckpoint;
import io.tapstate.spi.store.SrsChainState;
import io.tapstate.spi.store.SrsChainStore;
import io.tapstate.spi.store.SrsTableCheckpoint;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/** Chain control and exact logged-capture checkpoints share the original capture claim transaction. */
public final class MongoSrsChainStore implements SrsChainStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final Runnable requireStructure;
    private final MongoSrsModelStore models;
    private final MongoSrsLogStore log;

    public MongoSrsChainStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsChainStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this(client, database, scope, requireActive, () -> {});
    }
    MongoSrsChainStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive, Runnable requireStructure) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.requireStructure = Objects.requireNonNull(requireStructure, "requireStructure");
        this.models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
        this.log = new MongoSrsLogStore(client, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models);
    }
    @Override public MongoSrsChainStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoSrsChainStore(client, database, scope, () -> { requireActive.run(); additional.run(); }, requireStructure);
    }
    @Override public SrsChainState prepare(String chain, String retention, WorkloadClaimFence fence) {
        requireName(chain);
        return transaction(session -> {
            prove(session, fence);
            Document existing = requireRoot(session, chain);
            var capture = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,
                    new Document("_id",MongoWorkloadClaimStore.id(fence.key()))).first();
            var preparation=MongoCaptureCachePreparations.requirePreparation(capture,fence.key(),scope);
            if(preparation.mode()!=io.tapstate.spi.store.CapturePreparation.Mode.SHARED_SRS || !chain.equals(preparation.chainId())) throw fenced();
            requireCapture(existing,fence);
            return state(session,existing);
        });
    }
    static Document initialRow(CacheBusinessScope scope,String chain,String retention,WorkloadClaimFence fence) {
        return new Document("_id", chain).append("formatVersion", 1).append("cacheIncarnation", scope.cacheIncarnation())
                .append("captureId", fence.key().resourceId()).append("captureFence", WorkloadClaimDocuments.stored(fence))
                .append("retention", retention).append("epoch", 0L).append("requestedTables", List.of())
                .append("servingTables", List.of()).append("modelContextHeads", List.of()).append("modelWriteFence", 0L)
                .append("checkpointRevision", 0L).append("checkpoint", null).append("checkpointAt", null).append("consumerCount", 0L).append("consumerRevision", 0L)
                .append("resumeOverride", null);
    }
    @Override public Optional<SrsChainState> load(String chain) {
        requireName(chain);
        return transaction(session -> {
            Document row = root(session, chain);
            if (row == null) {
                if (hasRetainedFacts(session, chain)) throw unproven(chain);
                return Optional.empty();
            }
            return Optional.of(state(session, row));
        });
    }
    @Override public SrsChainState requestTables(String chain, Set<String> tables, WorkloadClaimFence fence) {
        requireName(chain); Set<String> captured = tables(tables);
        return transaction(session -> {
            prove(session, fence); Document row = requireRoot(session, chain); requireCapture(row, fence);
            SrsChainState held = state(session, row); var union = new TreeSet<>(held.requestedTables()); union.addAll(captured);
            if (union.equals(held.requestedTables())) return held;
            var replacement = new Document(row).append("requestedTables", new ArrayList<>(union));
            replace(session, row, replacement); return decode(replacement);
        });
    }
    @Override public OptionalLong openEpoch(String chain, long expectedEpoch, WorkloadClaimFence fence) {
        requireName(chain);
        if (expectedEpoch < 0 || expectedEpoch >= Long.MAX_VALUE - 1) throw new IllegalArgumentException("invalid expected capture epoch");
        return transaction(session -> {
            prove(session, fence); Document row = requireRoot(session, chain); requireCapture(row, fence);
            SrsChainState held = state(session, row);
            if (held.epoch() == expectedEpoch + 1 && WorkloadClaimDocuments.readCapture(row.get("captureFence", Document.class)).equals(fence)) {
                return OptionalLong.of(held.epoch());
            }
            if (held.epoch() != expectedEpoch) return OptionalLong.empty();
            var replacement = new Document(row).append("epoch", expectedEpoch + 1).append("captureFence", WorkloadClaimDocuments.stored(fence))
                    .append("servingTables", List.of());
            replace(session, row, replacement); return OptionalLong.of(expectedEpoch + 1);
        });
    }
    @Override public boolean publishServingTables(String chain, long epoch, Set<String> tables, WorkloadClaimFence fence) {
        requireName(chain); Set<String> captured = tables(tables);
        return transaction(session -> {
            prove(session, fence); Document row = requireRoot(session, chain); requireCurrent(row, epoch, fence);
            SrsChainState held = state(session, row);
            if (!captured.containsAll(held.requestedTables())) return false;
            if (!held.servingTables().isEmpty()) return held.servingTables().equals(captured);
            var replacement = new Document(row).append("servingTables", sorted(captured));
            replace(session, row, replacement); return true;
        });
    }
    @Override public Optional<SrsChainState> checkpoint(String chain, long expectedRevision, SrsCaptureCheckpoint checkpoint, WorkloadClaimFence fence) {
        requireName(chain); Objects.requireNonNull(checkpoint, "checkpoint");
        if (expectedRevision < 0 || expectedRevision == Long.MAX_VALUE) throw new IllegalArgumentException("invalid checkpoint revision");
        return transaction(session -> {
            prove(session, fence); Document row = requireRoot(session, chain);
            requireCurrent(row, checkpoint.position().order().epoch(), fence); SrsChainState held = state(session, row);
            if (held.checkpointRevision() == expectedRevision + 1 && checkpoint.equals(held.checkpoint())) return Optional.of(held);
            if (held.checkpointRevision() != expectedRevision) return Optional.empty();
            if (checkpoint.equals(held.checkpoint())) return Optional.of(held);
            if (expectedRevision >= Long.MAX_VALUE - 1 || !held.servingTables().equals(checkpoint.tables().keySet())
                    || !held.servingTables().containsAll(held.requestedTables())) throw unproven(chain);
            if ((held.pendingFreshFull() != null || held.checkpoint() != null && held.checkpoint().positionEditsRestricted())
                    && !checkpoint.positionEditsRestricted()) throw unproven(chain);
            if (held.pendingFreshFull() != null) {
                if (checkpoint.position().order().seq() < 1
                        || !checkpoint.tables().keySet().equals(held.pendingFreshFull().models().keySet())) throw unproven(chain);
                for (var entry : checkpoint.tables().entrySet()) {
                    var baseline = models.requireReference(session, held.pendingFreshFull().models().get(entry.getKey())).model();
                    var actual = models.requireReference(session, entry.getValue().model()).model();
                    if (!baseline.table().equals(actual.table())) throw unproven(chain);
                }
            }
            if (held.checkpoint() != null) {
                if (checkpoint.position().order().compareTo(held.checkpoint().position().order()) <= 0) throw unproven(chain);
                for (var before : held.checkpoint().tables().entrySet()) {
                    SrsTableCheckpoint next = checkpoint.tables().get(before.getKey());
                    if (next == null || next.sequence() < before.getValue().sequence()) throw unproven(chain);
                }
            }
            validateCheckpoint(session, row, checkpoint, fence, true);
            var replacement = new Document(row).append("checkpointRevision", expectedRevision + 1)
                    .append("checkpoint", checkpointDocument(checkpoint)).append("checkpointAt", Date.from(Instant.now()));
            if (held.resumeOverride() != null && checkpoint.position().order().epoch() > held.resumeOverride().fromEpoch()) replacement.put("resumeOverride", null);
            replace(session, row, replacement); return Optional.of(decode(replacement));
        });
    }

    private void validateCheckpoint(ClientSession session, Document root, SrsCaptureCheckpoint checkpoint,
                                    WorkloadClaimFence fence, boolean writing) {
        String chain = root.getString("_id");
        for (var at : checkpoint.tables().values()) {
            var key = at.model();
            if (!chain.equals(key.miningChainId()) || !scope.cacheIncarnation().equals(key.cacheIncarnation())
                    || key.captureEpoch() != checkpoint.position().order().epoch()) throw unproven(chain);
        }
        var keys = checkpoint.tables().values().stream().map(SrsTableCheckpoint::model).collect(java.util.stream.Collectors.toSet());
        var current = models.currentReferences(session, root, keys);
        var boundsByRing = current.isEmpty() ? Map.<String, io.tapstate.spi.store.SrsLogBounds>of()
                : log.boundsForProvenRings(session, current.orElseThrow().values().stream().map(reference -> reference.model().ringName())
                        .collect(java.util.stream.Collectors.toSet()));
        for (var entry : checkpoint.tables().entrySet()) {
            var at = entry.getValue(); var key = at.model();
            var reference = current.isPresent() ? current.orElseThrow().get(key) : models.requireReference(session, key);
            var range = reference.range(); String ring = reference.model().ringName();
            if ((writing && !fence.equals(reference.fence())) || at.sequence() < range.firstSequence() - 1
                    || (range.nextSequence() != null && at.sequence() >= range.nextSequence())) throw unproven(chain);
            var bounds = boundsByRing.get(ring);
            if (bounds == null) bounds = log.boundsInSession(session, ring);
            if (at.sequence() > bounds.largestSequence()) throw unproven(chain);
            if (at.sequence() >= 0 && at.sequence() > bounds.trimmedThrough()) {
                var event = log.loadInSession(session, ring, at.sequence()).orElseThrow(() -> unproven(chain));
                if (at.sequence() >= range.firstSequence() && (event.epoch() != key.captureEpoch() || event.schemaVer() != key.modelVersion()
                        || !reference.fence().equals(event.captureFence()))) throw unproven(chain);
            }
        }
    }
    SrsChainState state(ClientSession session, Document row) {
        SrsChainState state = decode(row);
        if (state.checkpoint() != null) validateCheckpoint(session, row, state.checkpoint(), null, false);
        if (state.pendingFreshFull() != null)
            models.requireSnapshotReferences(session, Set.copyOf(state.pendingFreshFull().models().values()));
        return state;
    }
    private Document root(ClientSession session, String chain) {
        return SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", chain)).first();
    }
    Document requireRoot(ClientSession session, String chain) {
        Document row = root(session, chain); if (row == null) throw unproven(chain); return row;
    }
    private boolean hasRetainedFacts(ClientSession session, String chain) {
        for (var table : List.of(SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_CONSUMERS, SystemCollections.CACHE_SRS_CONSUMER_OFFSETS)) {
            if (table.on(database).find(session, new Document("_id.chain", chain)).limit(1).first() != null) return true;
        }
        return false;
    }
    void prove(ClientSession session, WorkloadClaimFence fence) {
        Objects.requireNonNull(fence, "originalCaptureFence");
        if (fence.key().type() != WorkloadClaimType.CAPTURE || !scope.clusterId().equals(fence.key().clusterId())
                || SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session, WorkloadClaimDocuments.live(fence),
                new Document("$inc", new Document("fencedChainWrites", 1L))).getMatchedCount() != 1) throw fenced();
    }
    void requireCapture(Document row, WorkloadClaimFence fence) {
        decode(row);
        if (!fence.key().resourceId().equals(row.get("captureId"))) throw fenced();
    }
    private void requireCurrent(Document row, long epoch, WorkloadClaimFence fence) {
        requireCapture(row, fence);
        if (!Long.valueOf(epoch).equals(row.get("epoch")) || !fence.equals(WorkloadClaimDocuments.readCapture(row.get("captureFence", Document.class)))) throw fenced();
    }
    void replace(ClientSession session, Document expected, Document replacement) {
        long revision = expected.getLong("modelWriteFence");
        if (revision >= Long.MAX_VALUE - 1) throw unproven(expected.getString("_id"));
        replacement.put("modelWriteFence", revision + 1);
        if (SystemCollections.SRS_CHAINS.on(database).replaceOne(session, new Document("_id", expected.get("_id"))
                .append("epoch", expected.get("epoch")).append("modelWriteFence", revision), replacement).getMatchedCount() != 1) throw unproven(expected.getString("_id"));
    }
    private SrsChainState decode(Document row) { return decode(row, scope, new MongoSrsModelContexts(database, scope)); }
    static SrsChainState decode(Document row, CacheBusinessScope scope, MongoSrsModelContexts contexts) {
        try {
            if (!row.keySet().equals(Set.of("_id", "formatVersion", "cacheIncarnation", "captureId", "captureFence", "retention", "epoch",
                    "requestedTables", "servingTables", "modelContextHeads", "modelWriteFence", "checkpointRevision", "checkpoint", "checkpointAt", "consumerCount", "consumerRevision", "resumeOverride"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion")) || !scope.cacheIncarnation().equals(row.get("cacheIncarnation"))
                    || !(row.get("epoch") instanceof Long epoch) || !(row.get("checkpointRevision") instanceof Long revision)
                    || !(row.get("consumerCount") instanceof Long consumerCount) || !(row.get("consumerRevision") instanceof Long consumerRevision)
                    || !(row.get("modelWriteFence") instanceof Long write) || write < 0 || write == Long.MAX_VALUE
                    || (row.get("retention") != null && !(row.get("retention") instanceof String))) throw unreadable();
            var fence = WorkloadClaimDocuments.readCapture(row.get("captureFence", Document.class));
            if (!scope.clusterId().equals(fence.key().clusterId()) || !fence.key().resourceId().equals(row.get("captureId"))) throw unreadable();
            var heads = contexts.heads(row);
            if (epoch == 0 && (!heads.isEmpty() || !tableSet(row.get("servingTables")).isEmpty())) throw unreadable();
            SrsCaptureCheckpoint checkpoint = row.get("checkpoint") == null ? null : checkpointFrom(row.get("checkpoint", Document.class));
            if (row.get("checkpointAt") != null && !(row.get("checkpointAt") instanceof Date)) throw unreadable();
            var request = MongoSrsSourceRequests.read(row.get("resumeOverride"));
            if (request.full() != null) {
                var target = request.full().origin().targetScope();
                if (!scope.equals(new CacheBusinessScope(target.clusterId(), target.bindingVersion(), target.cacheIncarnation())))
                    throw unreadable();
            }
            return new SrsChainState(row.getString("_id"), row.getString("captureId"), row.getString("retention"), epoch,
                    tableSet(row.get("requestedTables")), tableSet(row.get("servingTables")), revision, checkpoint,
                    row.getDate("checkpointAt") == null ? null : row.getDate("checkpointAt").toInstant(), consumerCount, consumerRevision,
                    request.request(), request.full());
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }
    private static Document checkpointDocument(SrsCaptureCheckpoint checkpoint) {
        var order = checkpoint.position().order(); var positions = new ArrayList<Document>();
        for (String table : sorted(checkpoint.tables().keySet())) {
            var at = checkpoint.tables().get(table);
            positions.add(new Document("table", table).append("model", MongoSrsModelStore.key(at.model())).append("sequence", at.sequence()));
        }
        Document row = new Document("epoch", order.epoch()).append("sequence", order.seq()).append("token", checkpoint.position().token()).append("tables", positions);
        if (checkpoint.positionEditsRestricted()) row.append("positionEditsRestricted", true);
        return row.append("contentHash", CanonicalHash.ofText(row.toJson()));
    }
    private static SrsCaptureCheckpoint checkpointFrom(Document row) {
        boolean restricted = row.containsKey("positionEditsRestricted");
        if (!(restricted ? row.keySet().equals(Set.of("epoch", "sequence", "token", "tables", "contentHash", "positionEditsRestricted"))
                && Boolean.TRUE.equals(row.get("positionEditsRestricted"))
                : row.keySet().equals(Set.of("epoch", "sequence", "token", "tables", "contentHash"))) || !(row.get("epoch") instanceof Long epoch)
                || !(row.get("sequence") instanceof Long sequence) || !(row.get("token") instanceof String token)
                || !(row.get("tables") instanceof List<?> entries)) throw unreadable();
        var positions = new LinkedHashMap<String, SrsTableCheckpoint>();
        for (Object value : entries) {
            if (!(value instanceof Document entry) || !entry.keySet().equals(Set.of("table", "model", "sequence"))
                    || !(entry.get("table") instanceof String table) || !(entry.get("sequence") instanceof Long at)) throw unreadable();
            var position = new SrsTableCheckpoint(MongoSrsModelStore.readKey(entry.get("model", Document.class)), at);
            if (positions.put(table, position) != null) throw unreadable();
        }
        var result = new SrsCaptureCheckpoint(new ChainPosition(new SourceOrder(epoch, sequence), token), positions, restricted);
        if (!checkpointDocument(result).equals(row)) throw unreadable();
        return result;
    }
    private static Set<String> tableSet(Object value) {
        if (!(value instanceof List<?> entries)) throw unreadable();
        var tables = new TreeSet<String>();
        for (Object entry : entries) if (!(entry instanceof String table) || table.isBlank() || !tables.add(table)) throw unreadable();
        if (!new ArrayList<>(tables).equals(entries)) throw unreadable();
        return Collections.unmodifiableSet(tables);
    }
    private static Set<String> tables(Set<String> tables) {
        Set<String> captured = Set.copyOf(Objects.requireNonNull(tables, "tables"));
        if (captured.isEmpty() || captured.stream().anyMatch(String::isBlank)) throw new IllegalArgumentException("a capture requires nonblank tables");
        return captured;
    }
    private static List<String> sorted(Set<String> tables) { return new ArrayList<>(new TreeSet<>(tables)); }
    private static void requireName(String chain) { if (chain == null || chain.isBlank()) throw new IllegalArgumentException("blank mining chain"); }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> {
                        requireActive.run(); requireStructure.run(); requireActive.run(); new MongoCacheBusinessIdentity(database, scope).requireReady(session);
                        T result = action.apply(session); requireActive.run(); return result;
                    }, TRANSACTION);
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        });
    }
    private static TapstateException unproven(String chain) { return new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", chain), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "srs-chain", "field", "chain-control"), null); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
