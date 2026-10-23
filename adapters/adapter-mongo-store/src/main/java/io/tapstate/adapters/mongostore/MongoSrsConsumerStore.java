package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.SourcePositionEdits;
import io.tapstate.spi.store.SourceResumeOverride;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsChainState;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsConsumerProgress;
import io.tapstate.spi.store.SrsSnapshotPin;
import io.tapstate.spi.store.SrsConsumerWriterRun;
import io.tapstate.spi.store.SrsConsumerAcknowledgements;
import io.tapstate.spi.store.WriterProgress;
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
    private final Runnable requireStructure;
    private final MongoSrsChainStore chains;
    private final MongoSrsConsumerRecords records;

    public MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this(client, database, scope, requireActive, null);
    }
    MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive, Runnable requireStructure) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.requireStructure = requireStructure == null ? this::verifyConsumerStructure : requireStructure;
        this.chains = new MongoSrsChainStore(client, database, scope, requireActive, this.requireStructure); this.records = new MongoSrsConsumerRecords(scope);
    }
    @Override public MongoSrsConsumerStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoSrsConsumerStore(client, database, scope, () -> { requireActive.run(); additional.run(); }, requireStructure);
    }

    SourcePositionEdits positions(WorkloadClaimFence node, java.time.Duration retention) {
        Objects.requireNonNull(node, "originalNode");
        return (moves, readers, editedAt) -> {
            var fixed = List.copyOf(moves); var originals = Map.copyOf(readers);
            var at = Objects.requireNonNull(editedAt).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
            if (fixed.isEmpty()) throw new IllegalArgumentException("a position edit requires selected sources");
            transaction(session -> {
                new MongoCacheBusinessIdentity(database, scope).admitMetadataWrite(session);
                MongoCachePipelinePreparations.proveNode(session, database, scope, node);
                for (var reader : originals.entrySet()) {
                    var physical = reader.getKey();
                    if (!scope.equals(new CacheBusinessScope(physical.clusterId(), physical.bindingVersion(), physical.cacheIncarnation()))) throw fenced();
                    var access = new MongoCachePipelineAccess(client, database, physical, reader.getValue(), requireActive);
                    access.verifyStateStructure(); var state = access.state(session);
                    access.proveConsumerClaim(session); access.updateState(session, state, new Document(), false);
                }
                var selected = new java.util.HashSet<String>();
                for (var move : fixed) {
                    requireActive.run();
                    if (move instanceof SourcePositionEdits.Logged logged) {
                        String chain = logged.original().miningChainId();
                        if (!selected.add(chain)) throw unavailable(chain);
                        var held = view(session, chain);
                        if (!held.state().equals(logged.original()) || held.state().checkpoint() == null) throw unavailable(chain);
                        chains.prove(session, logged.capture()); chains.requireCapture(held.root(), logged.capture());
                        var attached = new java.util.HashSet<SrsConsumerContext>();
                        for (var member : held.members().values()) {
                            if (member.detached()) continue;
                            var context = member.context(); var cursor = held.cursors().get(context.key().consumerId());
                            var before = new SrsConsumerProgress(context, member.revision(), cursor.revision(), cursor.value(),
                                    cursor.snapshot(), cursor.writerRun(), cursor.ringDoneThrough());
                            var physical = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), context.key().pipelineId(), context.stateGeneration());
                            var claim = originals.get(physical);
                            if (!before.equals(logged.consumers().get(context)) || claim == null || !MongoPositionFences.retained(member.commandFence(), claim)) throw unavailable(chain);
                            attached.add(context); prove(session, context, claim); validateSnapshot(session, context, cursor.snapshot());
                            var old = cursor.value();
                            var cleared = new ConsumerOffset(old.pipelineId(), old.perTableSeq(), null, old.snapshotCompletedTables(),
                                    old.cdcStartPosition(), old.snapshotEpoch(), Map.of(), old.progressKind());
                            replaceProgress(session, new Own(member, cursor), cleared, null, cursor.ringDoneThrough());
                        }
                        if (!attached.equals(logged.consumers().keySet())) throw unavailable(chain);
                        var next = new Document(held.root()).append("resumeOverride", MongoResumeOverrides.stored(new SourceResumeOverride(logged.token(), held.state().epoch(), at)));
                        chains.replace(session, held.root(), next);
                    } else if (move instanceof SourcePositionEdits.Direct direct) {
                        String channel = direct.original().context().channelId();
                        if (!selected.add(channel)) throw fenced();
                        new MongoCacheMetadataStructure(database, retention).verifyDirect();
                        var pipeline = originals.get(direct.original().context().scope());
                        if (pipeline == null) throw fenced();
                        new MongoDirectChannelStore(client, database, scope, requireActive, retention)
                                .editPosition(session, direct.original(), direct.token(), at, direct.capture(), pipeline);
                    }
                }
                requireActive.run(); MongoCachePipelinePreparations.proveNode(session, database, scope, node);
                // End the original tenures in this same transaction: a delayed writer cannot recreate its old run roster.
                for (var reader : originals.entrySet()) {
                    var filter = WorkloadClaimDocuments.live(reader.getValue()).append("pipelineScope", MongoPipelinePreparationFacts.scope(reader.getKey()))
                            .append("pipelinePreparation.phase", "PREPARED");
                    if (SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session, filter,
                            List.of(new Document("$set", new Document("leaseUntil", "$$NOW")))).getMatchedCount() != 1) throw fenced();
                }
                return null;
            });
        };
    }
    @Override public List<SrsConsumerMembership> forPipeline(io.tapstate.spi.store.PipelineIntentScope original) {
        Objects.requireNonNull(original,"original");
        if(!scope.equals(new CacheBusinessScope(original.clusterId(),original.bindingVersion(),original.cacheIncarnation())))throw MongoCacheBusinessIdentity.businessFenced();
        return transaction(session -> {
            var result=new ArrayList<SrsConsumerMembership>();var names=new java.util.TreeSet<String>();
            for(var row:SystemCollections.SRS_CONSUMERS.on(database).find(session,new Document("pipelineId",original.pipelineId())
                    .append("_id.cacheIncarnation",scope.cacheIncarnation())))names.add(records.membership(row).context().key().miningChainId());
            for(String name:names)for(var member:view(session,name).members().values())if(member.context().key().pipelineId().equals(original.pipelineId())) {
                if(member.context().stateGeneration()>original.stateGeneration())throw MongoCachePipelinePreparations.rebuild(original);
                if(member.context().stateGeneration()==original.stateGeneration())result.add(member);
            }
            return List.copyOf(result);
        });
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
    /** Limits a fixed requested cut and serializes actual deletion with membership replacement. */
    long limitTrimToConfirmed(ClientSession session,String chain,String table,long requested) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("consumer trim protection requires the original CACHE transaction");
        requireActive.run(); requireStructure.run(); new MongoCacheBusinessIdentity(database,scope).requireReady(session);
        var held = view(session,chain); long safe = requested;
        for (var member : held.members().values()) {
            if (member.detached() || !member.tables().contains(table)) continue;
            var cursor = held.cursors().get(member.context().key().consumerId());
            safe = Math.min(safe,cursor.ringDoneThrough().getOrDefault(table,-1L));
        }
        if (safe >= 0) chains.replace(session,held.root(),new Document(held.root()));
        requireActive.run(); return safe;
    }

    /** Serializes model reclamation with attach, detach and exact snapshot-pin publication. */
    Set<io.tapstate.spi.store.SrsModelKey> retainedModels(ClientSession session,String chain) {
        var held=view(session,chain);var result=new java.util.HashSet<io.tapstate.spi.store.SrsModelKey>();
        for(var member:held.members().values()) {
            if(member.detached())continue;
            var snapshot=held.cursors().get(member.context().key().consumerId()).snapshot();
            validateSnapshot(session,member.context(),snapshot);
            if(snapshot!=null)result.addAll(snapshot.models().values());
        }
        chains.replace(session,held.root(),new Document(held.root()));return Set.copyOf(result);
    }

    /**
     * Read-only closure check for a caller that fences the original claim and checkpoint in this same
     * CACHE transaction. It cannot detach, repair missing history or establish retirement by itself.
     */
    void requireDetachedPipeline(ClientSession session, io.tapstate.spi.store.PipelineIntentScope original) {
        Objects.requireNonNull(session, "session"); Objects.requireNonNull(original, "original");
        if (!session.hasActiveTransaction()) throw new IllegalStateException("consumer retirement preflight requires its original CACHE transaction");
        if (!scope.equals(new CacheBusinessScope(original.clusterId(), original.bindingVersion(), original.cacheIncarnation())))
            throw MongoCacheBusinessIdentity.businessFenced();
        requireActive.run(); requireStructure.run();
        new MongoCacheBusinessIdentity(database, scope).requireReady(session);
        var names = new java.util.TreeSet<String>();
        try (var rows = SystemCollections.SRS_CHAINS.on(database).find(session).iterator()) {
            while (rows.hasNext()) {
                requireActive.run(); Document row = rows.next();
                if (!(row.get("_id") instanceof String id) || id.isBlank()) throw unreadableStructure();
                names.add(id);
            }
        }
        try (var rows = SystemCollections.SRS_CONSUMERS.on(database).find(session).iterator()) {
            while (rows.hasNext()) { requireActive.run(); names.add(records.membership(rows.next()).context().key().miningChainId()); }
        }
        try (var rows = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(session).iterator()) {
            while (rows.hasNext()) {
                requireActive.run(); Document row = rows.next();
                if (!(row.get("_id") instanceof Document id)
                        || !id.keySet().equals(Set.of("cacheIncarnation", "chain", "consumer"))
                        || !scope.cacheIncarnation().equals(id.get("cacheIncarnation"))
                        || !(id.get("chain") instanceof String chain) || chain.isBlank()
                        || !(id.get("consumer") instanceof String consumer) || consumer.isBlank()) throw unreadableStructure();
                names.add(chain);
            }
        }
        for (String name : names) {
            requireActive.run(); var retained = view(session, name);
            for (var member : retained.members().values()) if (member.context().key().pipelineId().equals(original.pipelineId())
                    && (!member.detached() || member.context().stateGeneration() > original.stateGeneration()))
                throw MongoCachePipelinePreparations.rebuild(original);
        }
        requireActive.run();
    }

    @Override public Optional<ConsumerOffset> cursor(SrsConsumerContext context) { return progress(context).map(SrsConsumerProgress::cursor); }
    @Override public Optional<SrsConsumerProgress> progress(SrsConsumerContext context) {
        Objects.requireNonNull(context, "context");
        return transaction(session -> {
            var own = own(session,context);
            if (own == null || own.member().detached()) return Optional.empty();
            var held = own.cursor(); validateSnapshot(session, context, held.snapshot());
            return Optional.of(new SrsConsumerProgress(context,own.member().revision(),held.revision(),held.value(),held.snapshot(),held.writerRun(),held.ringDoneThrough()));
        });
    }

    @Override public boolean beginWriterRun(SrsConsumerContext context,long membershipRevision,String runId,
            Map<String,List<String>> expected,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision);
        // Freeze caller-owned collections before a transaction callback can be retried.
        var requested = new SrsConsumerWriterRun(membershipRevision,fence,runId,expected,Map.of());
        return transaction(session -> {
            prove(session,context,fence); var own = requireOwn(session,context);
            if (!bound(own.member(),membershipRevision,fence)) return false;
            if (!own.member().tables().equals(requested.expected().keySet())) throw unavailable(context.key().miningChainId());
            var held = own.cursor(); var before = held.writerRun();
            if (requested.equals(before)) return true;
            if (before != null && before.membershipRevision() == membershipRevision && before.commandFence().equals(fence)
                    && before.runId().equals(runId) && before.expected().equals(requested.expected())) return true;
            if (before != null && (before.runId().equals(runId) || before.commandFence().equals(fence))) return false;
            replaceProgress(session,own,held.value(),requested);
            return true;
        });
    }

    @Override public boolean advanceWriter(SrsConsumerContext context,long membershipRevision,String runId,String table,String writerId,
            WriterProgress report,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); Objects.requireNonNull(report,"progress"); requireRevision(membershipRevision);
        if (runId == null || runId.isBlank() || table == null || table.isBlank() || writerId == null || writerId.isBlank())
            throw new IllegalArgumentException("an original writer report requires named run, table and writer");
        return transaction(session -> {
            prove(session,context,fence); var own = requireOwn(session,context);
            if (!bound(own.member(),membershipRevision,fence)) return false;
            var held = own.cursor(); var run = held.writerRun();
            if (run == null || run.membershipRevision() != membershipRevision || !run.commandFence().equals(fence) || !run.runId().equals(runId)) return false;
            if (!run.expected().getOrDefault(table,List.of()).contains(writerId)) throw unavailable(context.key().miningChainId());
            var next = SrsConsumerAcknowledgements.advance(run,held.value(),table,writerId,report);
            var done = MongoSrsConsumerRecords.reportedFloor(next.cursor(),held.ringDoneThrough(),next.writerRun(),table,report);
            if (!next.writerRun().equals(run) || !next.cursor().equals(held.value()) || !done.equals(held.ringDoneThrough()))
                replaceProgress(session,own,next.cursor(),next.writerRun(),done);
            return true;
        });
    }

    @Override public boolean startReadingAfter(SrsConsumerContext context,long membershipRevision,long expectedProgressRevision,
            String table,long sequence,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision); requireRevision(expectedProgressRevision);
        if (table == null || table.isBlank() || sequence < -1 || sequence == Long.MAX_VALUE) throw new IllegalArgumentException("invalid original consumer arrival");
        return startReadingAfter(context,membershipRevision,expectedProgressRevision,Map.of(table,sequence),fence);
    }

    @Override public boolean startReadingAfter(SrsConsumerContext context,long membershipRevision,long expectedProgressRevision,
            Map<String,Long> sequences,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision); requireRevision(expectedProgressRevision);
        Objects.requireNonNull(sequences,"sequences");
        if (sequences.isEmpty() || sequences.entrySet().stream().anyMatch(entry -> entry.getKey() == null || entry.getKey().isBlank()
                || entry.getValue() == null || entry.getValue() < -1 || entry.getValue() == Long.MAX_VALUE))
            throw new IllegalArgumentException("invalid original consumer arrivals");
        var captured = Map.copyOf(sequences);
        return transaction(session -> {
            prove(session,context,fence); var own = requireOwn(session,context);
            if (!bound(own.member(),membershipRevision,fence)) return false;
            if (!own.member().tables().containsAll(captured.keySet())) throw unavailable(context.key().miningChainId());
            var held = own.cursor(); var old = held.value();
            var missing = new HashMap<String,Long>();
            for (var entry : captured.entrySet()) {
                var confirmed = held.ringDoneThrough().get(entry.getKey());
                if (confirmed == null) missing.put(entry.getKey(),entry.getValue());
                else if (entry.getValue() > confirmed) return false;
            }
            if (missing.isEmpty()) return true;
            if (held.revision() != expectedProgressRevision) return false;
            // Validate every new arrival before replacing any part of the consumer's original cursor.
            if (missing.keySet().stream().anyMatch(table -> old.perTableSeq().get(table) != -1L))
                throw unavailable(context.key().miningChainId());
            var done = new HashMap<>(held.ringDoneThrough()); done.putAll(missing);
            var read = new HashMap<>(old.perTableSeq()); read.putAll(missing);
            var next = new ConsumerOffset(old.pipelineId(),read,old.sinkAcked(),old.snapshotCompletedTables(),old.cdcStartPosition(),
                    old.snapshotEpoch(),old.sinkAckedByTable(),old.progressKind());
            replaceProgress(session,own,next,held.writerRun(),done); return true;
        });
    }

    @Override public boolean advanceReadSequence(SrsConsumerContext context,long membershipRevision,String table,long sequence,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision);
        if (table == null || table.isBlank() || sequence < -1 || sequence == Long.MAX_VALUE) throw new IllegalArgumentException("invalid original consumer read sequence");
        return transaction(session -> {
            prove(session,context,fence); var own = requireOwn(session,context);
            if (!bound(own.member(),membershipRevision,fence)) return false;
            if (!own.member().tables().contains(table) || !own.cursor().ringDoneThrough().containsKey(table)) throw unavailable(context.key().miningChainId());
            var held = own.cursor(); var old = held.value();
            if (sequence <= old.perTableSeq().get(table)) return true;
            var read = new HashMap<>(old.perTableSeq()); read.put(table,sequence);
            var next = new ConsumerOffset(old.pipelineId(),read,old.sinkAcked(),old.snapshotCompletedTables(),old.cdcStartPosition(),
                    old.snapshotEpoch(),old.sinkAckedByTable(),old.progressKind());
            replaceProgress(session,own,next,held.writerRun());
            return true;
        });
    }

    private static boolean bound(SrsConsumerMembership member,long revision,WorkloadClaimFence fence) {
        if (member.revision() != revision) return false;
        if (!member.commandFence().equals(fence)) throw fenced();
        return true;
    }
    private void replaceProgress(ClientSession session,Own own,ConsumerOffset value,SrsConsumerWriterRun run) {
        replaceProgress(session,own,value,run,own.cursor().ringDoneThrough());
    }
    private void replaceProgress(ClientSession session,Own own,ConsumerOffset value,SrsConsumerWriterRun run,Map<String,Long> done) {
        var held = own.cursor(); var member = own.member(); String chain = member.context().key().miningChainId();
        if (held.revision() >= Long.MAX_VALUE - 1) throw unavailable(chain);
        var next = records.cursor(member,value,held.revision()+1,held.snapshot(),run,MongoSrsConsumerRecords.confirmedFloors(value,done,run));
        if (SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).replaceOne(session,
                new Document("_id",records.id(member.context().key())).append("membershipRevision",member.revision())
                        .append("progressRevision",held.revision()),next).getMatchedCount() != 1) throw unavailable(chain);
    }

    @Override public boolean pinSnapshot(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision,
                                          SrsSnapshotPin snapshot, WorkloadClaimFence fence) {
        Objects.requireNonNull(context, "context"); Objects.requireNonNull(snapshot, "snapshot");
        requireRevision(membershipRevision); requireRevision(expectedProgressRevision); String chain = context.key().miningChainId();
        return transaction(session -> {
            prove(session,context,fence); var view = view(session, chain); var member = view.members().get(context.key().consumerId());
            if (member == null || member.detached() || !member.context().equals(context)) throw unavailable(chain);
            if (member.revision() != membershipRevision) return false;
            if (!member.commandFence().equals(fence)) throw fenced();
            var held = view.cursors().get(context.key().consumerId()); var before = held.snapshot();
            if (!snapshot.equals(before) && held.revision() != expectedProgressRevision) return false;
            var required = new java.util.HashSet<>(member.tables()); if (before != null) required.addAll(before.models().keySet());
            if (!snapshot.models().keySet().equals(required)) throw unavailable(chain);
            if (before != null && (before.snapshotEpoch() != snapshot.snapshotEpoch() || !before.sourcePosition().equals(snapshot.sourcePosition())
                    || before.models().entrySet().stream().anyMatch(entry -> !entry.getValue().equals(snapshot.models().get(entry.getKey()))))) throw unavailable(chain);
            validateSnapshot(session, context, snapshot);
            if (snapshot.equals(before)) return true;
            if (held.revision() >= Long.MAX_VALUE - 1) throw unavailable(chain);
            ConsumerOffset old = held.value();
            var progress = new ConsumerOffset(old.pipelineId(), old.perTableSeq(), old.sinkAcked(), old.snapshotCompletedTables(),
                    snapshot.sourcePosition(), snapshot.snapshotEpoch(), old.sinkAckedByTable(), old.progressKind());
            Document row = records.cursor(member, progress, expectedProgressRevision + 1, snapshot,held.writerRun(),held.ringDoneThrough());
            if (SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).replaceOne(session,
                    new Document("_id", records.id(context.key())).append("membershipRevision", membershipRevision)
                            .append("progressRevision", expectedProgressRevision), row).getMatchedCount() != 1) throw unavailable(chain);
            chains.replace(session, view.root(), new Document(view.root()));
            return true;
        });
    }
    private void validateSnapshot(ClientSession session, SrsConsumerContext context, SrsSnapshotPin snapshot) {
        if (snapshot == null) return;
        var models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
        for (var key : snapshot.models().values()) {
            if (!context.key().miningChainId().equals(key.miningChainId()) || !scope.cacheIncarnation().equals(key.cacheIncarnation())) {
                throw unavailable(context.key().miningChainId());
            }
        }
        models.requireSnapshotReferences(session, Set.copyOf(snapshot.models().values()));
    }
    @Override public Optional<SrsConsumerMembership> attach(SrsConsumerContext context, long expectedRevision, Set<String> tables, WorkloadClaimFence fence) {
        Objects.requireNonNull(context, "context"); requireRevision(expectedRevision); Set<String> selected = Set.copyOf(Objects.requireNonNull(tables, "tables"));
        if (selected.isEmpty() || selected.stream().anyMatch(String::isBlank)) throw new IllegalArgumentException("an attached consumer requires tables");
        String chain = context.key().miningChainId();
        return transaction(session -> {
            prove(session,context,fence); var view = view(session, chain); var held = view.members().get(context.key().consumerId());
            if (replayed(held, context, expectedRevision, false, selected, fence)) return Optional.of(held);
            long revision = held == null ? 0 : held.revision();
            if (revision != expectedRevision) return Optional.empty();
            if (revision >= Long.MAX_VALUE - 1 || (held != null && (held.context().stateGeneration() > context.stateGeneration()
                    || (!held.detached() && !held.context().equals(context))))) throw unavailable(chain);
            var next = new SrsConsumerMembership(context, revision + 1, false, selected, fence);
            var previousCursor = held == null || held.detached() ? null : view.cursors().get(context.key().consumerId());
            ConsumerOffset cursor = previousCursor == null ? records.initial(context.key(), selected) : records.include(previousCursor.value(), selected);
            Document cursorRow = records.cursor(next, cursor, previousCursor == null ? 0 : previousCursor.revision(), previousCursor == null ? null : previousCursor.snapshot(),previousCursor == null ? null : previousCursor.writerRun(),previousCursor == null ? Map.of() : previousCursor.ringDoneThrough());
            writeMember(session, held, next);
            if (previousCursor == null) SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).insertOne(session, cursorRow);
            else if (SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).replaceOne(session,
                    new Document("_id", records.id(context.key())).append("membershipRevision", held.revision()), cursorRow).getMatchedCount() != 1) throw unavailable(chain);
            changeRoot(session, view, held == null || held.detached() ? 1 : 0);
            return Optional.of(next);
        });
    }
    @Override public Optional<SrsConsumerMembership> detach(SrsConsumerContext context, long expectedRevision, WorkloadClaimFence fence) {
        Objects.requireNonNull(context, "context"); requireRevision(expectedRevision);
        return transaction(session -> { prove(session,context,fence);return detachInTransaction(session,context,expectedRevision,fence); });
    }
    @Override public Optional<SrsConsumerMembership> detachForRebuild(io.tapstate.spi.store.PipelineRebuildPreparation original,
            SrsConsumerMembership membership,WorkloadClaimFence currentNode) {
        MongoPipelineStateRebuilds.requireOriginalPending(original);Objects.requireNonNull(membership,"membership");Objects.requireNonNull(currentNode,"currentNode");
        var request=original.request();var physical=request.originalScope();var context=membership.context();
        if(!scope.equals(new CacheBusinessScope(physical.clusterId(),physical.bindingVersion(),physical.cacheIncarnation()))
                || !physical.pipelineId().equals(context.key().pipelineId()) || physical.stateGeneration()!=context.stateGeneration())throw fenced();
        return transaction(session -> {
            MongoPipelineStateRebuilds.requireOriginalActive(original);
            MongoCachePipelinePreparations.proveNode(session,database,scope,currentNode);
            var held=view(session,context.key().miningChainId()).members().get(context.key().consumerId());
            if(replayed(held,context,membership.revision(),true,Set.of(),request.originalClaim()))return Optional.of(held);
            var access=new MongoCachePipelineAccess(client,database,physical,null,requireActive);access.verifyStateStructure();var state=access.state(session);
            if(state.getLong("epoch")!=request.originalStateEpoch())throw MongoCachePipelinePreparations.rebuild(physical);
            var fixed=WorkloadClaimDocuments.live(request.originalClaim());fixed.remove("$expr");
            fixed.append("pipelineScope",MongoPipelinePreparationFacts.scope(physical)).append("pipelinePreparation.phase","PREPARED");
            if(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,fixed,
                    new Document("$inc",new Document("fencedConsumerWrites",1L))).getMatchedCount()!=1)throw fenced();
            access.updateState(session,state,new Document(),false);
            return detachInTransaction(session,context,membership.revision(),request.originalClaim());
        });
    }
    @Override public Optional<SrsConsumerMembership> detachForRetirement(io.tapstate.spi.store.PipelineRetirementPreparation original,
            SrsConsumerMembership membership,WorkloadClaimFence currentNode,Optional<io.tapstate.spi.store.PipelineRetirementRequest.CacheOriginal> isolated) {
        MongoPipelineRetirements.requireOriginalPending(original);Objects.requireNonNull(membership);Objects.requireNonNull(currentNode);Objects.requireNonNull(isolated);
        var physical=original.request().scope();var context=membership.context();var declared=original.request().cache().orElseThrow(MongoPipelineRetirementFacts::fenced);
        var fixed=isolated.orElseThrow(MongoPipelineRetirementFacts::fenced);
        if(!scope.equals(new CacheBusinessScope(physical.clusterId(),physical.bindingVersion(),physical.cacheIncarnation()))
                || !physical.pipelineId().equals(context.key().pipelineId()) || physical.stateGeneration()!=context.stateGeneration()
                || !fixed.claim().key().equals(declared.claim().key()) || fixed.claim().claimGeneration()<declared.claim().claimGeneration()
                || fixed.claim().executionGeneration()<declared.claim().executionGeneration() || fixed.epoch()<declared.epoch())throw fenced();
        return transaction(session -> {
            MongoPipelineRetirements.requireOriginalActive(original);MongoCachePipelinePreparations.proveNode(session,database,scope,currentNode);
            var held=view(session,context.key().miningChainId()).members().get(context.key().consumerId());
            if(replayed(held,context,membership.revision(),true,Set.of(),fixed.claim()))return Optional.of(held);
            var access=new MongoCachePipelineAccess(client,database,physical,null,requireActive);access.verifyStateStructure();var state=access.state(session);
            if(state.getLong("epoch")!=fixed.epoch())throw MongoCachePipelinePreparations.rebuild(physical);
            var expected=WorkloadClaimDocuments.live(fixed.claim());expected.remove("$expr");
            expected.append("pipelineScope",MongoPipelinePreparationFacts.scope(physical)).append("pipelinePreparation.phase","PREPARED");
            if(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,expected,new Document("$inc",new Document("fencedConsumerWrites",1L))).getMatchedCount()!=1)throw fenced();
            access.updateState(session,state,new Document(),false);return detachInTransaction(session,context,membership.revision(),fixed.claim());
        });
    }

    private Optional<SrsConsumerMembership> detachInTransaction(ClientSession session,SrsConsumerContext context,long expectedRevision,WorkloadClaimFence fence) {
        String chain=context.key().miningChainId();
        var view = view(session, chain); var held = view.members().get(context.key().consumerId());
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
    private record Own(SrsConsumerMembership member,MongoSrsConsumerRecords.Cursor cursor) {}

    /** Hot progress never depends on another consumer's private cursor. Closure enumeration stays in control operations. */
    private Own own(ClientSession session,SrsConsumerContext context) {
        String chain = context.key().miningChainId();
        var root = SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",chain)).first();
        if (root == null) throw unavailable(chain);
        var state = chains.state(session,root); var id = new Document("_id",records.id(context.key()));
        var row = SystemCollections.SRS_CONSUMERS.on(database).find(session,id).first();
        if (row == null) {
            // Prove absence through the complete dependency view before allowing a new caller to prepare.
            view(session,chain); return null;
        }
        var member = records.membership(row);
        if (!member.context().equals(context) || state.consumerRevision() < member.revision()) throw unavailable(chain);
        var cursor = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(session,id).first();
        if (member.detached()) { if (cursor != null) throw unavailable(chain); return new Own(member,null); }
        if (state.consumerCount() < 1) throw unavailable(chain);
        return new Own(member,records.cursor(member,cursor));
    }
    private Own requireOwn(ClientSession session,SrsConsumerContext context) {
        var own = own(session,context);
        if (own == null || own.member().detached()) throw unavailable(context.key().miningChainId());
        validateSnapshot(session,context,own.cursor().snapshot());
        return own;
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
    private void prove(ClientSession session,SrsConsumerContext context,WorkloadClaimFence fence) {
        Objects.requireNonNull(fence,"originalPipelineFence");
        if (fence.executionGeneration() < 1 || fence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION || !scope.clusterId().equals(fence.key().clusterId())
                || !context.key().pipelineId().equals(fence.key().resourceId())) throw fenced();
        var physical = new io.tapstate.spi.store.PipelineIntentScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation(),
                context.key().pipelineId(),context.stateGeneration());
        var pipeline = new MongoCachePipelineAccess(client,database,physical,fence,requireActive);
        pipeline.verifyStateStructure(); Document current = pipeline.state(session);
        pipeline.proveConsumerClaim(session);
        pipeline.updateState(session,current,new Document(),false);
    }
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
    private void verifyConsumerStructure() {
        var tables = List.of(SystemCollections.SRS_CHAINS, SystemCollections.SRS_CONSUMERS, SystemCollections.CACHE_SRS_CONSUMER_OFFSETS,
                SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS);
        var names = tables.stream().map(SystemCollections::collectionName).toList();
        var present = MongoCollectionMetadata.matching(database, names);
        if (present.size() != tables.size() || present.stream().anyMatch(row -> !"collection".equals(row.get("type")))) throw unreadableStructure();
        for (var table : tables) {
            var indexes = table.on(database).listIndexes().into(new ArrayList<>());
            if (indexes.stream().anyMatch(index -> index.containsKey("expireAfterSeconds"))) throw unreadableStructure();
            for (var declared : table.indexes()) {
                Document keys = new Document(); declared.keys().forEach(key -> keys.put(key, 1));
                var found = indexes.stream().filter(index -> declared.indexName().equals(index.get("name"))).findFirst().orElse(null);
                if (found == null || !keys.equals(found.get("key")) || Boolean.TRUE.equals(found.get("unique")) != declared.unique()
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
