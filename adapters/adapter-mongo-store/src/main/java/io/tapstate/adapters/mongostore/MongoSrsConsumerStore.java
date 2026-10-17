package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsChainState;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsConsumerKey;
import io.tapstate.spi.store.SrsConsumerMembership;
import io.tapstate.spi.store.SrsConsumerStore;
import io.tapstate.spi.store.SrsConsumerTopology;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Membership, its cursor and minimal chain dependency facts change in one cache transaction. */
public final class MongoSrsConsumerStore implements SrsConsumerStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final MongoSrsChainStore chains;
    private final MongoSrsConsumerRecords records;

    public MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = requireActive;
        this.chains = new MongoSrsChainStore(client, database, scope); this.records = new MongoSrsConsumerRecords(scope);
    }
    @Override public MongoSrsConsumerStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoSrsConsumerStore(client, database, scope, () -> { requireActive.run(); additional.run(); });
    }
    @Override public Optional<SrsConsumerMembership> find(SrsConsumerKey key) {
        Objects.requireNonNull(key, "key");
        return transaction(session -> Optional.ofNullable(view(session, key.miningChainId()).members().get(key.consumerId())));
    }
    @Override public SrsConsumerTopology attached(String chain) {
        requireChainName(chain);
        return transaction(session -> {
            var view = view(session, chain);
            List<SrsConsumerMembership> active = view.members().values().stream().filter(member -> !member.detached())
                    .sorted(Comparator.comparing(member -> member.context().key().consumerId())).toList();
            return new SrsConsumerTopology(scope, chain, view.state().consumerRevision(), active);
        });
    }
    @Override public Optional<ConsumerOffset> cursor(SrsConsumerContext context) {
        Objects.requireNonNull(context, "context");
        return transaction(session -> {
            var view = view(session, context.key().miningChainId()); var member = view.members().get(context.key().consumerId());
            if (member == null) return Optional.empty();
            if (!member.context().equals(context)) throw unavailable(context.key().miningChainId());
            return member.detached() ? Optional.empty() : Optional.of(view.cursors().get(context.key().consumerId()).value());
        });
    }
    @Override public Optional<SrsConsumerMembership> attach(SrsConsumerContext context, long expectedRevision, Set<String> tables, WorkloadClaimFence fence) {
        Objects.requireNonNull(context, "context"); requireRevision(expectedRevision); Set<String> selected = Set.copyOf(Objects.requireNonNull(tables, "tables"));
        if (selected.isEmpty() || selected.stream().anyMatch(String::isBlank)) throw new IllegalArgumentException("an attached consumer requires tables");
        String chain = context.key().miningChainId();
        return transaction(session -> {
            prove(session, context.key(), fence); var view = view(session, chain); var held = view.members().get(context.key().consumerId());
            if (replayed(held, context, expectedRevision, false, selected, fence)) return Optional.of(held);
            long revision = held == null ? 0 : held.revision();
            if (revision != expectedRevision) return Optional.empty();
            if (revision >= Long.MAX_VALUE - 1 || (held != null && (held.context().stateGeneration() > context.stateGeneration()
                    || (!held.detached() && !held.context().equals(context))))) throw unavailable(chain);
            var next = new SrsConsumerMembership(context, revision + 1, false, selected, fence);
            var previousCursor = held == null || held.detached() ? null : view.cursors().get(context.key().consumerId());
            ConsumerOffset cursor = previousCursor == null ? records.initial(context.key(), selected) : records.include(previousCursor.value(), selected);
            Document cursorRow = records.cursor(next, cursor, previousCursor == null ? 0 : previousCursor.revision());
            writeMember(session, held, next);
            if (previousCursor == null) SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).insertOne(session, cursorRow);
            else if (SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).replaceOne(session,
                    new Document("_id", records.id(context.key())).append("membershipRevision", held.revision()), cursorRow).getMatchedCount() != 1) throw unavailable(chain);
            changeRoot(session, view, held == null || held.detached() ? 1 : 0);
            return Optional.of(next);
        });
    }
    @Override public Optional<SrsConsumerMembership> detach(SrsConsumerContext context, long expectedRevision, WorkloadClaimFence fence) {
        Objects.requireNonNull(context, "context"); requireRevision(expectedRevision); String chain = context.key().miningChainId();
        return transaction(session -> {
            prove(session, context.key(), fence); var view = view(session, chain); var held = view.members().get(context.key().consumerId());
            if (replayed(held, context, expectedRevision, true, Set.of(), fence)) return Optional.of(held);
            long revision = held == null ? 0 : held.revision();
            if (revision != expectedRevision) return Optional.empty();
            if (held != null && !held.context().equals(context)) throw unavailable(chain);
            if (revision >= Long.MAX_VALUE - 1) throw unavailable(chain);
            var next = new SrsConsumerMembership(context, revision + 1, true, Set.of(), fence);
            writeMember(session, held, next);
            if (held != null && !held.detached() && SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).deleteOne(session,
                    new Document("_id", records.id(context.key())).append("membershipRevision", held.revision())).getDeletedCount() != 1) throw unavailable(chain);
            changeRoot(session, view, held == null || held.detached() ? 0 : -1);
            return Optional.of(next);
        });
    }

    private static boolean replayed(SrsConsumerMembership held, SrsConsumerContext context, long expected, boolean detached,
                                     Set<String> tables, WorkloadClaimFence fence) {
        return held != null && held.revision() == expected + 1 && held.context().equals(context) && held.detached() == detached
                && held.tables().equals(tables) && held.commandFence().equals(fence);
    }
    private void writeMember(ClientSession session, SrsConsumerMembership before, SrsConsumerMembership next) {
        Document row = records.membership(next);
        if (before == null) SystemCollections.SRS_CONSUMERS.on(database).insertOne(session, row);
        else if (SystemCollections.SRS_CONSUMERS.on(database).replaceOne(session,
                new Document("_id", records.id(before.context().key())).append("revision", before.revision()), row).getMatchedCount() != 1) {
            throw unavailable(next.context().key().miningChainId());
        }
    }
    private void changeRoot(ClientSession session, View view, long countDelta) {
        long count = view.state().consumerCount(); long revision = view.state().consumerRevision(); String chain = view.state().miningChainId();
        if (revision >= Long.MAX_VALUE - 1 || count + countDelta < 0 || count + countDelta >= Long.MAX_VALUE) throw unavailable(chain);
        Document next = new Document(view.root()).append("consumerCount", count + countDelta).append("consumerRevision", revision + 1);
        chains.replace(session, view.root(), next);
    }
    private record View(Document root, SrsChainState state, Map<String, SrsConsumerMembership> members,
                        Map<String, MongoSrsConsumerRecords.Cursor> cursors) {}

    private View view(ClientSession session, String chain) {
        Document root = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", chain)).first();
        if (root == null) throw unavailable(chain);
        SrsChainState state = chains.state(session, root);
        var members = new HashMap<String, SrsConsumerMembership>();
        long revisions = 0;
        try (var cursor = SystemCollections.SRS_CONSUMERS.on(database).find(session, records.chain(chain)).iterator()) {
            while (cursor.hasNext()) {
                SrsConsumerMembership member = records.membership(cursor.next());
                if (!chain.equals(member.context().key().miningChainId()) || members.put(member.context().key().consumerId(), member) != null) throw unavailable(chain);
                try { revisions = Math.addExact(revisions, member.revision()); }
                catch (ArithmeticException invalid) { throw unavailable(chain); }
            }
        }
        var cursors = new HashMap<String, MongoSrsConsumerRecords.Cursor>();
        try (var cursor = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(session, records.chain(chain)).iterator()) {
            while (cursor.hasNext()) {
                Document row = cursor.next();
                if (!(row.get("_id") instanceof Document id) || !(id.get("consumer") instanceof String consumer)) throw unavailable(chain);
                SrsConsumerMembership member = members.get(consumer);
                if (member == null || member.detached() || cursors.put(consumer, records.cursor(member, row)) != null) throw unavailable(chain);
            }
        }
        long active = members.values().stream().filter(member -> !member.detached()).count();
        if (active != state.consumerCount() || cursors.size() != active || revisions != state.consumerRevision()) throw unavailable(chain);
        return new View(root, state, members, cursors);
    }
    private void prove(ClientSession session, SrsConsumerKey key, WorkloadClaimFence fence) {
        Objects.requireNonNull(fence, "originalPipelineFence");
        if (fence.executionGeneration() < 1 || fence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION || !scope.clusterId().equals(fence.key().clusterId())
                || !key.pipelineId().equals(fence.key().resourceId()) || SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,
                WorkloadClaimDocuments.live(fence), new Document("$inc", new Document("fencedConsumerWrites", 1L))).getMatchedCount() != 1) throw fenced();
    }
    private <T> T transaction(Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try (var session = client.startSession()) {
                return session.withTransaction(() -> {
                    requireActive.run(); requireStructure(); new MongoCacheBusinessIdentity(database, scope).requireReady(session);
                    T result = action.apply(session); requireActive.run(); return result;
                }, TRANSACTION);
            }
        });
    }
    private void requireStructure() {
        var tables = List.of(SystemCollections.SRS_CHAINS, SystemCollections.SRS_CONSUMERS, SystemCollections.CACHE_SRS_CONSUMER_OFFSETS);
        var names = tables.stream().map(SystemCollections::collectionName).toList();
        var present = database.listCollections().filter(new Document("name", new Document("$in", names))).into(new ArrayList<>());
        if (present.size() != tables.size() || present.stream().anyMatch(row -> !"collection".equals(row.get("type")))) throw unreadableStructure();
        for (var table : tables) {
            var indexes = table.on(database).listIndexes().into(new ArrayList<>());
            if (indexes.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) throw unreadableStructure();
            for (var declared : table.indexes()) {
                Document keys = new Document(); declared.keys().forEach(key -> keys.put(key, 1));
                var found = indexes.stream().filter(index -> declared.indexName().equals(index.get("name"))).findFirst().orElse(null);
                if (found == null || !keys.equals(found.get("key")) || Boolean.TRUE.equals(found.get("unique"))
                        || Boolean.TRUE.equals(found.get("sparse")) || Boolean.TRUE.equals(found.get("hidden")) || found.containsKey("partialFilterExpression")) throw unreadableStructure();
            }
        }
    }
    private static TapstateException unreadableStructure() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "srs-consumers", "field", "structure"), null);
    }

    private static void requireRevision(long revision) {
        if (revision < 0 || revision == Long.MAX_VALUE) throw new IllegalArgumentException("invalid expected consumer revision");
    }
    private static void requireChainName(String chain) { if (chain == null || chain.isBlank()) throw new IllegalArgumentException("blank mining chain"); }
    private static TapstateException unavailable(String chain) { return new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE, Map.of("chain", chain), null); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
