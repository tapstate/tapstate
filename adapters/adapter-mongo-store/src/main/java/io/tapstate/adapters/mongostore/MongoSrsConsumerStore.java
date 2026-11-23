package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
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
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsChainStore;
import io.tapstate.spi.store.SrsConsumerContext;
import io.tapstate.spi.store.SrsConsumerProgress;
import io.tapstate.spi.store.SrsSnapshotPin;
import io.tapstate.spi.store.SrsConsumerWriterRun;
import io.tapstate.spi.store.SrsConsumerAcknowledgements;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterProgressUpdate;
import io.tapstate.spi.store.WriterProgressOutcome;
import io.tapstate.spi.store.SrsConsumerKey;
import io.tapstate.spi.store.SrsConsumerMembership;
import io.tapstate.spi.store.SrsConsumerStore;
import io.tapstate.spi.store.SrsConsumerTopology;
import io.tapstate.spi.store.SharedSrsRecoverySnapshot;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.CapturePreparation;
import io.tapstate.spi.store.CompletedPipelineFull;
import io.tapstate.spi.store.SrsConsumerRun;
import io.tapstate.spi.store.SrsFreshFull;
import io.tapstate.spi.store.SrsFreshFullPreparation;
import io.tapstate.spi.store.SrsFreshFullReceipt;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.SourceTable;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.bounded;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.boundedRow;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.lookup;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.one;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.match;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.originalRow;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.overflow;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.rows;
import static io.tapstate.adapters.mongostore.MongoPipelineObservationRead.single;

/** Membership, its cursor and minimal chain dependency facts change in one cache transaction. */
public final class MongoSrsConsumerStore implements SrsConsumerStore {
    private static final int CONTROL_SNAPSHOT_RELATIONS = 128;
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final long PIN_BUDGET = TimeUnit.SECONDS.toNanos(15);
    private final MongoClient client;
    private final MongoClaimOperations pinOperations;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    private final Runnable requireStructure;
    private final boolean runtimeStructure;
    private final BooleanSupplier batchControlSnapshot;
    private final BooleanSupplier batchControlReadiness;
    private final MongoSrsChainStore chains;
    private final MongoSrsControlProofs proofs;
    private MongoSrsControlProofs controlProofs(){return proofs;}
    private final MongoSrsConsumerRecords records;
    private final MongoSrsLease readLease;
    private final AtomicReference<ProvenReads> provenReads;
    private final MongoSrsLease cursorLease;
    private final AtomicReference<ProvenCursor> provenCursor;

    public MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope) {
        this(client, database, scope, () -> {});
    }
    private MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive) {
        this(client, database, scope, requireActive, null);
    }
    MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive, Runnable requireStructure) {
        this(client, database, scope, requireActive, requireStructure, MongoSrsLease.perTransaction(), false);
    }

    MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive,
            Runnable requireStructure, MongoSrsLease readLease) {
        this(client, database, scope, requireActive, requireStructure, readLease, false);
    }

    static MongoSrsConsumerStore admittedRuntime(MongoClient client, MongoDatabase database, CacheBusinessScope scope,
            Runnable requireActive, Runnable requireStructure) {
        return new MongoSrsConsumerStore(client, database, scope, requireActive, requireStructure,
                MongoSrsLease.perTransaction(), true);
    }

    MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable active,
            Runnable structure, MongoSrsLease lease, boolean runtime) {
        this(client, database, scope, active, structure, lease, runtime, null);
    }

    MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable active,
            Runnable structure, MongoSrsLease lease, boolean runtime, MongoClaimOperations pinOperations) {
        this(client, database, scope, active, structure, lease, runtime, new MongoSrsControlProofs(), () -> false, () -> false, pinOperations);
    }

    MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable active,
            Runnable structure, MongoSrsLease lease, boolean runtime, MongoSrsControlProofs proofs,
            BooleanSupplier batchControlSnapshot, BooleanSupplier batchControlReadiness) {
        this(client, database, scope, active, structure, lease, runtime, proofs, batchControlSnapshot, batchControlReadiness, null);
    }

    MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable active,
            Runnable structure, MongoSrsLease lease, boolean runtime, MongoSrsControlProofs proofs,
            BooleanSupplier batchControlSnapshot, BooleanSupplier batchControlReadiness, MongoClaimOperations pinOperations) {
        this(client, database, scope, active, structure, lease, runtime, new AtomicReference<>(), lease.fork(),
                new AtomicReference<>(), proofs, batchControlSnapshot, batchControlReadiness, pinOperations);
    }

    private MongoSrsConsumerStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, Runnable requireActive,
            Runnable requireStructure, MongoSrsLease readLease, boolean runtimeStructure, AtomicReference<ProvenReads> provenReads,
            MongoSrsLease cursorLease, AtomicReference<ProvenCursor> provenCursor, MongoSrsControlProofs proofs,
            BooleanSupplier batchControlSnapshot, BooleanSupplier batchControlReadiness, MongoClaimOperations pinOperations) {
        this.pinOperations = pinOperations;
        this.proofs = proofs;
        this.readLease = readLease;
        this.provenReads = provenReads;
        this.cursorLease = cursorLease;
        this.provenCursor = provenCursor;
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.requireStructure = requireStructure == null ? this::verifyConsumerStructure : requireStructure;
        this.runtimeStructure = runtimeStructure;
        this.batchControlSnapshot = batchControlSnapshot;
        this.batchControlReadiness = batchControlReadiness;
        this.chains = new MongoSrsChainStore(client, database, scope, requireActive, this.requireStructure, readLease.fork(), proofs);
        this.records = new MongoSrsConsumerRecords(scope);
    }
    @Override public MongoSrsConsumerStore guardedBy(Runnable additional) {
        Objects.requireNonNull(additional, "requireActive");
        return new MongoSrsConsumerStore(client, database, scope, () -> { requireActive.run(); additional.run(); }, requireStructure, readLease, runtimeStructure, provenReads, cursorLease, provenCursor, controlProofs(), batchControlSnapshot, batchControlReadiness, pinOperations);
    }

    @Override public SharedSrsRecoverySnapshot inspectSharedRecovery(CapturePreparation previous) {
        return transaction(session -> sharedRecoveryView(session,previous).snapshot());
    }
    record SharedView(Document root,SharedSrsRecoverySnapshot snapshot,List<Document> memberships,List<Document> cursors,List<Document> claims) {}
    SharedView sharedRecoveryView(ClientSession session,CapturePreparation previous) {
        return sharedRecoveryView(session,previous,null);
    }
    SharedView cancelledSharedRecoveryView(ClientSession session,CapturePreparation previous,String operation,String planHash,String replacement) {
        var root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",previous.chainId())).first();
        if(root==null)throw unavailable(previous.chainId());
        var control=MongoSrsRecoveryControls.read(root).orElseThrow(() -> unavailable(previous.chainId()));
        if(control.phase()!=MongoSrsRecoveryControls.Phase.CANCELLED || !control.operationId().equals(operation)
                || !control.planHash().equals(planHash) || !control.replacementChain().equals(replacement))throw unavailable(previous.chainId());
        return sharedRecoveryView(session,previous,control);
    }
    private SharedView sharedRecoveryView(ClientSession session,CapturePreparation previous,MongoSrsRecoveryControls.Control cancellation) {
        if(!session.hasActiveTransaction())throw new IllegalStateException("shared controls require their CACHE transaction");
        Objects.requireNonNull(previous,"previous");String chain=previous.chainId();
        if(previous.mode()!=CapturePreparation.Mode.SHARED_SRS || !scope.equals(previous.scope()))throw unavailable(chain);
            try {
                var root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",chain)).first();
                if(root==null || !chain.equals(root.get("_id")) || !previous.captureId().equals(root.get("captureId"))
                        || !MongoSrsRecoveryControls.read(root).equals(Optional.ofNullable(cancellation)))throw unavailable(chain);
                var contexts=new MongoSrsModelContexts(database,scope);
                var state=MongoSrsChainStore.controlState(root,scope,contexts);
                var held=relations(session,chain,root,state);
                requireRecoveryCoordinates(held,contexts);
                var table=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
                var reader=new MongoWorkloadClaimStore(table).inStrictSession(session);
                var captureKey=new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.CAPTURE,previous.captureId());
                var captureRow=table.find(session,new Document("_id",MongoWorkloadClaimStore.id(captureKey))).first();
                if(captureRow==null || !previous.equals(MongoCaptureCachePreparations.requirePreparation(captureRow,captureKey,scope))
                        || !(captureRow.get("fencedChainWrites") instanceof Long writes) || writes<1 || writes>=Long.MAX_VALUE-1)throw unavailable(chain);
                var capture=WorkloadClaimFence.from(reader.decodeStored(captureRow));
                if(!MongoPositionFences.retained(WorkloadClaimDocuments.readCapture(root.get("captureFence",Document.class)),capture))throw unavailable(chain);
                var claims=new ArrayList<Document>();claims.add(captureRow);
                var pipelines=new java.util.TreeMap<String,SharedSrsRecoverySnapshot.Pipeline>();
                var actual=held.members().values().stream().filter(member -> !member.detached())
                        .sorted(Comparator.comparing(member -> member.context().key().consumerId())).toList();
                for(var member:actual) {
                    String pipeline=member.context().key().pipelineId();var retained=pipelines.get(pipeline);
                    if(retained==null) {
                        var key=new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,pipeline);
                        var row=table.find(session,new Document("_id",MongoWorkloadClaimStore.id(key))).first();
                        if(row==null)throw unavailable(chain);
                        var physical=MongoCacheWorkloadClaimStore.preparedScope(row,key,scope);
                        retained=new SharedSrsRecoverySnapshot.Pipeline(physical,WorkloadClaimFence.from(reader.decodeStored(row)));
                        pipelines.put(pipeline,retained);claims.add(row);
                    }
                    if(retained.scope().stateGeneration()!=member.context().stateGeneration()
                            || !MongoPositionFences.retained(member.commandFence(),retained.claim()))throw unavailable(chain);
                }
                var rings=contexts.heads(root).stream().map(head -> head.getString("ringName")).collect(java.util.stream.Collectors.toSet());
                var bounds=MongoSrsLogStore.inspectRetainedBounds(session,database,rings);
                requireRecoveryBounds(held,contexts,bounds.byRing());
                String hash=cancellation==null?MongoSrsRecoveryControls.snapshotHash(root,held.membershipRows(),held.cursorRows(),claims,bounds.rows())
                        :MongoSrsRecoveryControls.cancelledSnapshotHash(root,held.membershipRows(),held.cursorRows(),claims,bounds.rows());
                return new SharedView(root,new SharedSrsRecoverySnapshot(previous,capture,hash,actual,pipelines),
                        held.membershipRows(),held.cursorRows(),List.copyOf(claims));
            }catch(IllegalArgumentException|NullPointerException|ClassCastException invalid){throw unavailable(chain);}
    }

    SharedView cancelSharedRecovery(ClientSession session,SharedView current,io.tapstate.spi.store.StorageRecoveryCommands.Plan plan,String replacement) {
        var cancelled=MongoSrsRecoveryControls.candidate(current.root(),plan.operationId(),plan.planHash(),replacement,
                MongoSrsRecoveryControls.Phase.CANCELLED,current.memberships(),current.cursors());
        chains.replace(session,current.root(),cancelled);
        return cancelledSharedRecoveryView(session,current.snapshot().previous(),plan.operationId(),plan.planHash(),replacement);
    }

    void retireSharedRecovery(ClientSession session,SharedView original,io.tapstate.spi.store.StorageRecoveryCommands.Plan plan,String replacement) {
        String chain=original.snapshot().previous().chainId();
        if(!original.snapshot().equals(plan.manifest().sharedSrs()))throw unavailable(chain);
        var after=new HashMap<String,Document>();
        original.memberships().forEach(row -> after.put(row.get("_id",Document.class).getString("consumer"),row));
        try {
            for(var before:original.snapshot().consumers()) {
                var current=original.snapshot().pipelines().get(before.context().key().pipelineId()).claim();
                var detached=new SrsConsumerMembership(before.context(),Math.addExact(before.revision(),1),true,Set.of(),current);
                writeMember(session,before,detached);after.put(detached.context().key().consumerId(),records.membership(detached));
                var offset=original.cursors().stream().filter(row -> row.get("_id",Document.class).equals(records.id(before.context().key()))).findFirst().orElseThrow(() -> unavailable(chain));
                if(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).deleteOne(session,offset).getDeletedCount()!=1)throw unavailable(chain);
            }
            long revision=Math.addExact(original.root().getLong("consumerRevision"),original.snapshot().consumers().size());
            if(revision>=Long.MAX_VALUE)throw unavailable(chain);
            var stopped=new Document(original.root()).append("consumerCount",0L).append("consumerRevision",revision);
            var retired=MongoSrsRecoveryControls.candidate(stopped,plan.operationId(),plan.planHash(),replacement,
                    MongoSrsRecoveryControls.Phase.RETIRED,new ArrayList<>(after.values()),List.of());
            chains.replace(session,original.root(),retired);
        }catch(ArithmeticException|IllegalArgumentException invalid){throw unavailable(chain);}
    }

    Document requireRetiredSharedRecovery(ClientSession session,io.tapstate.spi.store.StorageRecoveryCommands.Plan plan,String replacement) {
        var original=plan.manifest().sharedSrs();String chain=original.previous().chainId();
        var held=administrativeView(session,chain);var control=MongoSrsRecoveryControls.read(held.root()).orElseThrow(() -> unavailable(chain));
        if(control.phase()!=MongoSrsRecoveryControls.Phase.RETIRED || !control.operationId().equals(plan.operationId())
                || !control.planHash().equals(plan.planHash()) || !control.replacementChain().equals(replacement)
                || !held.state().captureId().equals(original.previous().captureId()))throw unavailable(chain);
        for(var before:original.consumers()) {
            var after=held.members().get(before.context().key().consumerId());
            if(after==null || !after.detached() || !after.context().equals(before.context()) || after.revision()!=before.revision()+1
                    || !after.commandFence().equals(original.pipelines().get(before.context().key().pipelineId()).claim()))throw unavailable(chain);
        }
        return held.root();
    }

    private void requireRecoveryCoordinates(View held,MongoSrsModelContexts contexts) {
        String chain=held.state().miningChainId();long epoch=held.state().epoch();
        var heads=new HashMap<String,Long>();
        for(var head:contexts.heads(held.root())) {
            long captureEpoch=head.get("contextId",Document.class).get("captureContext",Document.class).getLong("epoch");
            if(captureEpoch>epoch)throw unavailable(chain);
            heads.put(head.getString("table"),captureEpoch);
        }
        if(!heads.keySet().containsAll(held.state().servingTables()))throw unavailable(chain);
        var checkpoint=held.state().checkpoint();
        if(checkpoint!=null)for(var entry:checkpoint.tables().entrySet()) {
            var key=entry.getValue().model();
            if(!chain.equals(key.miningChainId()) || !scope.cacheIncarnation().equals(key.cacheIncarnation())
                    || key.captureEpoch()!=checkpoint.position().order().epoch() || !heads.containsKey(entry.getKey())
                    || heads.get(entry.getKey())<key.captureEpoch())throw unavailable(chain);
        }
        for(var cursor:held.cursors().values())if(cursor.snapshot()!=null)for(var key:cursor.snapshot().models().values())
            requireRecoveryModelCoordinate(key,chain,epoch,heads);
        if(held.state().pendingFreshFull()!=null)for(var key:held.state().pendingFreshFull().models().values())
            requireRecoveryModelCoordinate(key,chain,epoch,heads);
    }
    private void requireRecoveryModelCoordinate(SrsModelKey key,String chain,long epoch,Map<String,Long> heads) {
        if(!chain.equals(key.miningChainId()) || !scope.cacheIncarnation().equals(key.cacheIncarnation()) || key.captureEpoch()>epoch
                || !heads.containsKey(key.table()) || heads.get(key.table())<key.captureEpoch())throw unavailable(chain);
    }
    private void requireRecoveryBounds(View held,MongoSrsModelContexts contexts,Map<String,io.tapstate.spi.store.SrsLogBounds> bounds) {
        var rings=new HashMap<String,String>();contexts.heads(held.root()).forEach(head -> rings.put(head.getString("table"),head.getString("ringName")));
        String chain=held.state().miningChainId();
        if(held.state().checkpoint()!=null)held.state().checkpoint().tables().forEach((table,at) -> requireRecoveryBound(chain,table,at.sequence(),rings,bounds));
        for(var cursor:held.cursors().values()) {
            cursor.value().perTableSeq().forEach((table,sequence) -> requireRecoveryBound(chain,table,sequence,rings,bounds));
            cursor.ringDoneThrough().forEach((table,sequence) -> requireRecoveryBound(chain,table,sequence,rings,bounds));
        }
    }
    private void requireRecoveryBound(String chain,String table,long sequence,Map<String,String> rings,Map<String,io.tapstate.spi.store.SrsLogBounds> bounds) {
        String ring=rings.get(table);
        if(ring==null) {if(sequence>=0)throw unavailable(chain);return;}
        var retained=bounds.get(ring);
        if(retained!=null && sequence>retained.largestSequence())throw unavailable(chain);
    }

    @Override public SrsFreshFullPreparation inspectFreshFull(CompletedPipelineFull full, SrsConsumerRun original,
            long expectedProgressRevision, WorkloadClaimFence node, WorkloadClaimFence capture) {
        Objects.requireNonNull(original, "original"); requireRevision(expectedProgressRevision);
        MongoPipelineStateRebuilds.originalFullInputs(full);
        if (!scope.equals(original.cacheScope()) || !original.scope().equals(full.targetScope())) throw fenced();
        var prepared = MongoPipelineStateRebuilds.originalFullCapture(full, original.sourceId(), original.miningChainId());
        var result = checkpointTransaction(original.scope().pipelineId(), session -> {
            proveFreshFull(session, full, original, node, capture, prepared);
            var held = view(session, original.miningChainId());
            requireFreshFull(session, held, original, expectedProgressRevision, capture,
                    MongoPipelineStateRebuilds.originalFullInputs(full));
            return new FreshFull(full, original, expectedProgressRevision, node, capture, prepared, held);
        });
        if (!prepared.equals(MongoPipelineStateRebuilds.originalFullCapture(full, original.sourceId(), original.miningChainId())))
            throw fullUnproven(original.miningChainId());
        return result;
    }

    private void proveFreshFull(ClientSession session, CompletedPipelineFull full, SrsConsumerRun original,
            WorkloadClaimFence node, WorkloadClaimFence capture, CapturePreparation prepared) {
        new MongoCacheBusinessIdentity(database, scope).admitMetadataWrite(session);
        MongoCachePipelinePreparations.proveNode(session, database, scope, node);
        prove(session, original.context(), original.originalClaim()); chains.prove(session, capture);
        var access = new MongoCachePipelineAccess(client, database, original.scope(), original.originalClaim(), requireActive);
        var state = access.state(session);
        var claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,
                new Document("_id", MongoWorkloadClaimStore.id(original.originalClaim().key()))).first();
        MongoCachePipelineRebuilds.requireCompletedTarget(session, database, original.scope(), full, claim, state);
        var captureRow = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,
                new Document("_id", MongoWorkloadClaimStore.id(capture.key()))).first();
        if (captureRow == null || !prepared.equals(MongoCaptureCachePreparations.requirePreparation(captureRow, capture.key(), scope)))
            throw fullUnproven(original.miningChainId());
    }

    private Own requireFullConsumer(View held, SrsConsumerRun original, WorkloadClaimFence capture) {
        String chain = original.miningChainId();
        if (!WorkloadClaimDocuments.readCapture(held.root().get("captureFence", Document.class)).equals(capture))
            throw fullUnproven(chain);
        var member = held.members().get(original.context().key().consumerId());
        if (member == null || member.detached() || !member.context().equals(original.context())
                || member.revision() != original.membershipRevision() || !member.commandFence().equals(original.originalClaim()))
            throw fullUnproven(chain);
        return new Own(held.root(),held.state(),member, held.cursors().get(original.context().key().consumerId()));
    }

    private void requireFreshFull(ClientSession session, View held, SrsConsumerRun original, long expectedRevision,
            WorkloadClaimFence capture, io.tapstate.spi.store.PipelinePreparationInputs inputs) {
        String chain = original.miningChainId(); var own = requireFullConsumer(held, original, capture); var state = held.state();
        var cursor = own.cursor(); var progress = cursor.value(); var selected = inputs.selectedTables().get(original.sourceId());
        if (state.checkpoint() == null || state.epoch() <= state.checkpoint().position().order().epoch()
                || state.resumeOverride() != null || state.consumerCount() != 1
                || held.members().values().stream().anyMatch(member -> !member.detached() && !member.context().equals(original.context()))
                || !own.member().tables().equals(selected) || !selected.containsAll(state.servingTables())
                || !state.requestedTables().equals(selected) || !state.checkpoint().tables().keySet().equals(selected)
                || cursor.revision() != expectedRevision || cursor.revision() >= Long.MAX_VALUE - 1
                || cursor.snapshot() != null || cursor.writerRun() != null || !cursor.ringDoneThrough().isEmpty()
                || progress.sinkAcked() != null || !progress.sinkAckedByTable().isEmpty() || !progress.snapshotCompletedTables().isEmpty()
                || progress.snapshotEpoch() != 0 || progress.cdcStartPosition() != null
                || !progress.perTableSeq().keySet().equals(selected) || progress.perTableSeq().values().stream().anyMatch(value -> value != -1))
            throw fullUnproven(chain);
        var epoch = new Document("_id.chain", chain).append("_id.captureContext.epoch", state.epoch()).append("cacheIncarnation", scope.cacheIncarnation());
        if (SystemCollections.SRS_MODELS.on(database).find(session, epoch).limit(1).first() != null
                || SystemCollections.SRS_MODEL_CONTEXTS.on(database).find(session, epoch).limit(1).first() != null) throw fullUnproven(chain);
    }

    private static TapstateException fullUnproven(String chain) {
        return new TapstateException(IoError.SRS_FULL_PREPARATION_UNPROVEN, Map.of("chain", chain), null);
    }

    private final class FreshFull implements SrsFreshFullPreparation {
        private final CompletedPipelineFull full;
        private final SrsConsumerRun original;
        private final long progressRevision;
        private final WorkloadClaimFence node, capture;
        private final CapturePreparation prepared;
        private final View before;
        private SrsFreshFullReceipt proposed;
        private Document proposedRoot;
        private String fixedPosition;
        private Map<String, SourceTable> fixedTables;
        private java.time.Instant fixedAt;

        FreshFull(CompletedPipelineFull full, SrsConsumerRun original, long progressRevision,
                WorkloadClaimFence node, WorkloadClaimFence capture, CapturePreparation prepared, View before) {
            this.full = full; this.original = original; this.progressRevision = progressRevision;
            this.node = node; this.capture = capture; this.prepared = prepared; this.before = before;
        }

        @Override public SrsChainState originalChain() { return before.state(); }

        @Override public synchronized SrsFreshFullReceipt publish(long snapshotEpoch, String sourcePosition,
                Map<String, SourceTable> actualTables, java.time.Instant preparedAt) {
            String chain = original.miningChainId(); var actual = Map.copyOf(actualTables);
            if (snapshotEpoch != before.state().epoch() || sourcePosition == null || sourcePosition.isBlank()
                    || !actual.keySet().equals(before.state().requestedTables())
                    || actual.entrySet().stream().anyMatch(entry -> !entry.getKey().equals(entry.getValue().name()))) throw fullUnproven(chain);
            var at = Objects.requireNonNull(preparedAt, "preparedAt").truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
            if (fixedPosition == null) { fixedPosition = sourcePosition; fixedTables = actual; fixedAt = at; }
            else if (!fixedPosition.equals(sourcePosition) || !fixedTables.equals(actual)) throw fullUnproven(chain);
            requireCaptureCurrent();
            var result = changingTheConsumer(() -> checkpointTransaction(original.scope().pipelineId(), session -> {
                proveFreshFull(session, full, original, node, capture, prepared);
                var held = view(session, chain);
                if (proposed != null && held.root().equals(proposedRoot)) return retained(session, held, snapshotEpoch, sourcePosition, actual);
                if (!held.root().equals(before.root())) throw fullUnproven(chain);
                requireFreshFull(session, held, original, progressRevision, capture, MongoPipelineStateRebuilds.originalFullInputs(full));
                var own = requireFullConsumer(held, original, capture);
                var models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
                var proposals = new java.util.TreeMap<String, SrsModel>();
                var references = new java.util.TreeMap<String, SrsModelKey>();
                var floors = new java.util.TreeMap<String, Long>();
                for (var entry : new java.util.TreeMap<>(actual).entrySet()) {
                    String table = entry.getKey();
                    var previous = models.requireSnapshotReference(session, held.state().checkpoint().tables().get(table).model());
                    var bounds = SystemCollections.SRS_LOG_BOUNDS.on(database).find(session,
                            new Document("_id", MongoSrsLogStore.boundsId(previous.ringName()))).first();
                    if (bounds == null || bounds.getLong("largestSequence") >= Long.MAX_VALUE - 1) throw fullUnproven(chain);
                    long largest = bounds.getLong("largestSequence");
                    var key = new SrsModelKey(chain, table, scope.cacheIncarnation(), snapshotEpoch, 0);
                    proposals.put(table, new SrsModel(key, previous.ringName(), entry.getValue(), largest + 1));
                    references.put(table, key); floors.put(table, largest);
                }
                var pending = new SrsFreshFull(full.origin(original.sourceId()), snapshotEpoch, references);
                var request = MongoSrsSourceRequests.stored(new SourceResumeOverride(sourcePosition, snapshotEpoch - 1, fixedAt), pending);
                // Each publication reloads the root in this same session, preserving the earlier table's head.
                for (var proposal : proposals.values()) {
                    models.publishInSession(session, proposal, capture); requireActive.run();
                }
                var snapshot = new SrsSnapshotPin(snapshotEpoch, sourcePosition, references);
                var offset = new ConsumerOffset(original.context().key().consumerId(), floors, null, List.of(),
                        sourcePosition, snapshotEpoch, Map.of(), io.tapstate.spi.store.ConsumerProgressKind.SRS);
                var cursor = records.cursor(own.member(), offset, progressRevision + 1, snapshot, null, floors);
                if (SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).replaceOne(session,
                        new Document("_id", records.id(original.context().key())).append("membershipRevision", original.membershipRevision())
                                .append("progressRevision", progressRevision), cursor).getMatchedCount() != 1) throw fullUnproven(chain);
                var latest = chains.requireRoot(session, chain);
                var replacement = new Document(latest).append("resumeOverride", request)
                        .append("servingTables", actual.keySet().stream().sorted().toList());
                chains.replace(session, latest, replacement);
                proveFreshFull(session, full, original, node, capture, prepared);
                var candidate = new SrsFreshFullReceipt(chains.state(session, replacement), new SrsConsumerProgress(original.context(),
                        original.membershipRevision(), progressRevision + 1, offset, snapshot, null, floors));
                // A candidate is never a commit receipt: a retry must prove its exact durable root and cursor.
                proposed = candidate; proposedRoot = new Document(replacement);
                return candidate;
            }));
            requireCaptureCurrent();
            return result;
        }

        private void requireCaptureCurrent() {
            if (!prepared.equals(MongoPipelineStateRebuilds.originalFullCapture(full, original.sourceId(), original.miningChainId())))
                throw fullUnproven(original.miningChainId());
        }

        private SrsFreshFullReceipt retained(ClientSession session, View held, long epoch, String position, Map<String, SourceTable> actual) {
            String chain = original.miningChainId(); var own = requireFullConsumer(held, original, capture);
            var current = new SrsConsumerProgress(original.context(), own.member().revision(), own.cursor().revision(), own.cursor().value(),
                    own.cursor().snapshot(), own.cursor().writerRun(), own.cursor().ringDoneThrough());
            if (!held.root().equals(proposedRoot) || !held.state().equals(proposed.chain()) || !current.equals(proposed.progress())
                    || epoch != proposed.progress().snapshot().snapshotEpoch()
                    || !position.equals(proposed.progress().snapshot().sourcePosition())) throw fullUnproven(chain);
            var models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
            for (var entry : proposed.progress().snapshot().models().entrySet())
                if (!models.requireSnapshotReference(session, entry.getValue()).table().equals(actual.get(entry.getKey()))) throw fullUnproven(chain);
            return proposed;
        }
    }

    SourcePositionEdits positions(WorkloadClaimFence node) {
        Objects.requireNonNull(node, "originalNode");
        return (moves, readers, editedAt) -> {
            var fixed = List.copyOf(moves); var originals = Map.copyOf(readers);
            var at = Objects.requireNonNull(editedAt).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
            if (fixed.isEmpty()) throw new IllegalArgumentException("a position edit requires selected sources");
            checkpointTransaction(originals.keySet().stream().map(PipelineIntentScope::pipelineId).toList(), session -> {
                new MongoCacheBusinessIdentity(database, scope).admitMetadataWrite(session);
                MongoCachePipelinePreparations.proveNode(session, database, scope, node);
                for (var reader : originals.entrySet()) {
                    var physical = reader.getKey();
                    if (!scope.equals(new CacheBusinessScope(physical.clusterId(), physical.bindingVersion(), physical.cacheIncarnation()))) throw fenced();
                    var access = new MongoCachePipelineAccess(client, database, physical, reader.getValue(), requireActive);
                    var state = access.state(session, true);
                    access.proveConsumerClaim(session); access.updateState(session, state, new Document(), false);
                }
                var selected = new java.util.HashSet<String>();
                boolean changed = false;
                for (var move : fixed) {
                    requireActive.run();
                    if (move instanceof SourcePositionEdits.Logged logged) {
                        String chain = logged.original().miningChainId();
                        if (!selected.add(chain)) throw unavailable(chain);
                        var held = view(session, chain);
                        if (!held.state().equals(logged.original()) || held.state().checkpoint() == null) throw unavailable(chain);
                        if (held.state().pendingFreshFull() != null || held.state().checkpoint().positionEditsRestricted()
                                && !held.state().checkpoint().position().token().equals(logged.token()))
                            throw new TapstateException(IoError.SRS_POSITION_MODEL_UNPROVEN, Map.of("chain", chain), null);
                        if (held.state().checkpoint().positionEditsRestricted()) continue;
                        changed = true;
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
                            replaceProgress(session, new Own(held.root(),held.state(),member, cursor), cleared, null, cursor.ringDoneThrough());
                        }
                        if (!attached.equals(logged.consumers().keySet())) throw unavailable(chain);
                        var next = new Document(held.root()).append("resumeOverride", MongoResumeOverrides.stored(new SourceResumeOverride(logged.token(), held.state().epoch(), at)));
                        chains.replace(session, held.root(), next);
                    } else if (move instanceof SourcePositionEdits.Direct direct) {
                        changed = true;
                        String channel = direct.original().context().channelId();
                        if (!selected.add(channel)) throw fenced();
                        var pipeline = originals.get(direct.original().context().scope());
                        if (pipeline == null) throw fenced();
                        new MongoDirectChannelStore(client, database, scope, requireActive)
                                .editPosition(session, direct.original(), direct.token(), at, direct.capture(), pipeline);
                    }
                }
                requireActive.run(); MongoCachePipelinePreparations.proveNode(session, database, scope, node);
                if (!changed) return null;
                // End the original tenures in this same transaction: a delayed writer cannot recreate its old run roster.
                var claimWriter = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database))
                        .inStrictSession(session);
                for (var reader : originals.entrySet()) {
                    var filter = WorkloadClaimDocuments.livePipeline(session, database, reader.getValue()).append("pipelineScope", MongoPipelinePreparationFacts.scope(reader.getKey()))
                            .append("pipelinePreparation.phase", "PREPARED");
                    if (!claimWriter.revokePipelineLease(filter)) throw fenced();
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
            // A missing scoped relation cannot hide a quarantined root's complete control receipt.
            for(var row:SystemCollections.SRS_CHAINS.on(database).find(session,new Document("recoveryControl",new Document("$exists",true)))
                    .projection(new Document("_id",1))) {
                if(!(row.get("_id") instanceof String name) || name.isBlank())throw unreadableStructure();
                names.add(name);
            }
            for(String name:names)for(var member:administrativeView(session,name).members().values())if(member.context().key().pipelineId().equals(original.pipelineId())) {
                if(member.context().stateGeneration()>original.stateGeneration())throw MongoCachePipelinePreparations.rebuild(original);
                if(member.context().stateGeneration()==original.stateGeneration())result.add(member);
            }
            return List.copyOf(result);
        });
    }

    @Override public Optional<PlanningProgress> startLoadProgress(PipelineIntentScope original, String chain, Set<String> sourceIds) {
        Objects.requireNonNull(original, "original");
        requireChainName(chain);
        var sources = Set.copyOf(Objects.requireNonNull(sourceIds, "sourceIds"));
        if (!scope.equals(new CacheBusinessScope(original.clusterId(), original.bindingVersion(), original.cacheIncarnation())))
            throw MongoCacheBusinessIdentity.businessFenced();
        if (sources.isEmpty() || sources.stream().anyMatch(String::isBlank))
            throw new IllegalArgumentException("start planning requires source identities");
        var requested = new HashMap<String, SrsConsumerKey>();
        for (String source : sources) {
            var key = new SrsConsumerKey(chain, original.pipelineId(), source);
            requested.put(key.consumerId(), key);
        }
        return transaction(session -> {
            var filter = records.chain(chain).append("_id.consumer", new Document("$in", List.copyOf(requested.keySet())));
            var captured = requested.size() <= 64 ? startProgressRows(session, chain, filter) : null;
            var root = captured == null ? SystemCollections.SRS_CHAINS.on(database)
                    .find(session, new Document("_id", chain)).first() : captured.get("root", Document.class);
            if (root == null) {
                if (chains.hasRetainedFacts(session, chain))
                    throw new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", chain), null);
                return Optional.empty();
            }
            // Control parsing establishes the original chain without reading its model or log payloads.
            var state = MongoSrsChainStore.decode(root, scope, new MongoSrsModelContexts(database, scope));
            if (!chain.equals(state.miningChainId())) throw unavailable(chain);
            var members = new HashMap<String, SrsConsumerMembership>();
            var membershipRows = captured == null ? SystemCollections.SRS_CONSUMERS.on(database)
                    .find(session, filter).batchSize(64).into(new ArrayList<>()) : captured.getList("members", Document.class);
            for (var row : membershipRows) {
                requireActive.run();
                var member = records.membership(row);
                var context = member.context();
                if (!context.key().equals(requested.get(context.key().consumerId()))
                        || members.put(context.key().consumerId(), member) != null
                        || context.stateGeneration() > original.stateGeneration()
                        || !member.detached() && context.stateGeneration() != original.stateGeneration()) throw unavailable(chain);
            }
            var progress = new HashMap<String, SrsConsumerProgress>();
            var cursorRows = captured == null ? SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database)
                .find(session, filter).batchSize(64).into(new ArrayList<>()) : captured.getList("cursors", Document.class);
            for (var row : cursorRows) {
                requireActive.run();
                if (!(row.get("_id") instanceof Document id) || !(id.get("consumer") instanceof String consumer)) throw unavailable(chain);
                var member = members.get(consumer);
                if (member == null || member.detached()) throw unavailable(chain);
                // Keep the exact embedded-ID semantics of the former point lookup.
                var codec = new org.bson.codecs.DocumentCodec();
                if (!new org.bson.RawBsonDocument(id, codec).getByteBuffer().asNIO().equals(
                        new org.bson.RawBsonDocument(records.id(member.context().key()), codec).getByteBuffer().asNIO()))
                    throw unavailable(chain);
                var cursor = records.cursor(member, row);
                var value = new SrsConsumerProgress(member.context(), member.revision(), cursor.revision(),
                        cursor.value(), cursor.snapshot(), cursor.writerRun(), cursor.ringDoneThrough());
                if (progress.put(consumer, value) != null) throw unavailable(chain);
            }
            if (progress.size() != members.values().stream().filter(member -> !member.detached()).count()) throw unavailable(chain);
            return Optional.of(new PlanningProgress(state, progress.values().stream()
                    .sorted(Comparator.comparing(value -> value.context().key().consumerId())).toList()));
        });
    }

    private Document startProgressRows(ClientSession session, String chain, Document filter) {
        return startProgressRows(session, chain, filter, List.of());
    }

    private Document startProgressRows(ClientSession session, String chain, Document filter, List<Document> additional) {
        var rootProjection = capturedStartRow();
        rootProjection.remove("_id");
        var pipeline = new ArrayList<>(List.of(new Document("$match", new Document("_id", chain)),
                new Document("$project", new Document("root", rootProjection)),
                startProgressLookup(SystemCollections.SRS_CONSUMERS, "members", filter),
                startProgressLookup(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS, "cursors", filter)));
        pipeline.addAll(additional);
        var row = SystemCollections.SRS_CHAINS.on(database).aggregate(session, pipeline).first();
        if (row == null) return null;
        var root = row.get("root", Document.class);
        if (Boolean.TRUE.equals(root.get("large"))) return null;
        var result = new Document(row).append("root", root.get("value", Document.class));
        for (String field : List.of("members", "cursors")) {
            var rows = row.getList(field, Document.class);
            if (rows.size() > 64 || rows.stream().anyMatch(value -> Boolean.TRUE.equals(value.get("large")))) return null;
            result.put(field, rows.stream().map(value -> value.get("value", Document.class)).toList());
        }
        return result;
    }

    private Document startProgressLookup(SystemCollections collection, String field, Document filter) {
        return new Document("$lookup", new Document("from", collection.collectionName())
                .append("pipeline", List.of(new Document("$match", filter), new Document("$limit", 65),
                        new Document("$project", capturedStartRow()))).append("as", field));
    }

    private static Document capturedStartRow() {
        var size = new Document("$bsonSize", "$$ROOT");
        return new Document("_id", 0).append("large", new Document("$gt", List.of(size, 64 * 1024)))
                .append("value", new Document("$cond", List.of(new Document("$lte", List.of(size, 64 * 1024)), "$$ROOT", false)));
    }

    @Override public Optional<PlanningProgress> planningProgress(PipelineIntentScope original, String chain, Set<String> sourceIds) {
        Objects.requireNonNull(original,"original"); requireChainName(chain);
        var sources = Set.copyOf(Objects.requireNonNull(sourceIds,"sourceIds"));
        if (!scope.equals(new CacheBusinessScope(original.clusterId(),original.bindingVersion(),original.cacheIncarnation())))
            throw MongoCacheBusinessIdentity.businessFenced();
        if (sources.isEmpty() || sources.stream().anyMatch(String::isBlank)) throw new IllegalArgumentException("start planning requires source identities");
        return transaction(session -> {
            var root = SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",chain)).first();
            if (root == null) {
                if (chains.hasRetainedFacts(session,chain)) throw new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN,Map.of("chain",chain),null);
                return Optional.empty();
            }
            var facts = new MongoSrsReadFacts(session, database, scope, root, true);
            var state = chains.state(session, root, true, facts);
            if (!chain.equals(state.miningChainId())) throw unavailable(chain);
            var members = new HashMap<String,SrsConsumerMembership>();
            var selected = new HashMap<String,SrsConsumerMembership>();
            long revisions = 0;
            try (var rows = SystemCollections.SRS_CONSUMERS.on(database).find(session,records.chain(chain)).batchSize(64).iterator()) {
                while (rows.hasNext()) {
                    requireActive.run(); var member = records.membership(rows.next()); var context = member.context();
                    if (!chain.equals(context.key().miningChainId()) || members.put(context.key().consumerId(),member) != null) throw unavailable(chain);
                    try { revisions = Math.addExact(revisions,member.revision()); }
                    catch (ArithmeticException invalid) { throw unavailable(chain); }
                    if (!context.key().pipelineId().equals(original.pipelineId()) || !sources.contains(context.key().sourceId())) continue;
                    if (context.stateGeneration() > original.stateGeneration()
                            || !member.detached() && context.stateGeneration() != original.stateGeneration()) throw unavailable(chain);
                    if (!member.detached()) selected.put(context.key().consumerId(),member);
                }
            }
            // Control closure needs cursor identities, not another pipeline's private progress payload.
            var present = new java.util.HashSet<String>();
            try (var rows = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(session,records.chain(chain))
                    .projection(new Document("_id",1).append("formatVersion",1).append("stateGeneration",1)
                            .append("membershipRevision",1).append("progressRevision",1)).batchSize(64).iterator()) {
                while (rows.hasNext()) {
                    requireActive.run(); var row = rows.next();
                    if (!(row.get("_id") instanceof Document id) || !(id.get("consumer") instanceof String consumer)) throw unavailable(chain);
                    var member = members.get(consumer);
                    if (member == null || member.detached() || !id.equals(records.id(member.context().key())) || !present.add(consumer)) throw unavailable(chain);
                    if (!row.keySet().equals(Set.of("_id","formatVersion","stateGeneration","membershipRevision","progressRevision"))) throw unavailable(chain);
                    records.cursorIdentity(member,row);
                }
            }
            long active = members.values().stream().filter(member -> !member.detached()).count();
            if (active != state.consumerCount() || present.size() != active || revisions != state.consumerRevision()) throw unavailable(chain);
            var progress = new HashMap<String,SrsConsumerProgress>();
            if (!selected.isEmpty()) {
                var ids = selected.values().stream().map(member -> records.id(member.context().key())).toList();
                try (var rows = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database)
                        .find(session,new Document("_id",new Document("$in",ids))).batchSize(64).iterator()) {
                    while (rows.hasNext()) {
                        requireActive.run(); var row = rows.next();
                        if (!(row.get("_id") instanceof Document id) || !(id.get("consumer") instanceof String consumer)) throw unavailable(chain);
                        var member = selected.get(consumer);
                        if (member == null) throw unavailable(chain);
                        var cursor = records.cursor(member,row);
                        validateSnapshot(session, member.context(), cursor.snapshot(), facts);
                        var value = new SrsConsumerProgress(member.context(),member.revision(),cursor.revision(),cursor.value(),
                                cursor.snapshot(),cursor.writerRun(),cursor.ringDoneThrough());
                        if (progress.put(consumer,value) != null) throw unavailable(chain);
                    }
                }
            }
            if (progress.size() != selected.size()) throw unavailable(chain);
            return Optional.of(new PlanningProgress(state,progress.values().stream()
                    .sorted(Comparator.comparing(value -> value.context().key().consumerId())).toList()));
        });
    }

    @Override public Optional<SrsConsumerMembership> find(SrsConsumerKey key) {
        Objects.requireNonNull(key, "key");
        return transaction(session -> Optional.ofNullable(view(session, key.miningChainId()).members().get(key.consumerId())));
    }
    @Override public boolean attached(SrsConsumerRun original,String table){
        Objects.requireNonNull(original);Objects.requireNonNull(table);
        if(table.isBlank()||table.length()>512||!scope.equals(new CacheBusinessScope(original.scope().clusterId(),original.scope().bindingVersion(),original.scope().cacheIncarnation())))throw unavailable(original.miningChainId());
        return transaction(session->{
            var row=MongoValidationMetadata.read(session,SystemCollections.SRS_CONSUMERS.on(database),new Document("_id",records.id(original.context().key())),null,256*1024);
            if(row==null)return false;var member=records.membership(row);
            return !member.detached()&&member.context().equals(original.context())&&member.revision()==original.membershipRevision()
                    &&member.commandFence().equals(original.originalClaim())&&member.tables().contains(table);
        });
    }
    @Override public SrsConsumerTopology attached(String chain) {
        return attachedWithChain(chain).topology();
    }

    @Override public AttachedChain attachedWithChain(String chain, SrsChainStore chains) {
        return attachedWithChain(chain);
    }

    @Override public AttachedChain retentionSnapshot(String chain, SrsChainStore chains) {
        return attachedWithChain(chain, IoError.SRS_CHECKPOINT_UNPROVEN);
    }

    @Override public Optional<Map<String, Long>> retentionFloors(String chain, SrsChainStore ignored) {
        requireChainName(chain);
        return transaction(session -> {
            var facts = new MongoSrsReadFacts(session, database, scope, true);
            var root = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_CHAINS, new Document("_id", chain));
            if (root == null) throw new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", chain), null);
            var state = chains.state(session, root, true, facts);
            var view = relations(session, chain, root, state);
            var floors = new java.util.TreeMap<String, Long>();
            for (var member : view.members().values()) {
                if (member.detached() || java.util.Collections.disjoint(member.tables(), state.servingTables())) continue;
                var cursor = view.cursors().get(member.context().key().consumerId());
                validateSnapshot(session, member.context(), cursor.snapshot(), facts);
                for (String table : member.tables()) if (state.servingTables().contains(table))
                    floors.merge(table, cursor.ringDoneThrough().getOrDefault(table, -1L), Math::min);
            }
            return Optional.of(Map.copyOf(floors));
        });
    }

    @Override public AttachedChain attachedWithChain(String chain) {
        return attachedWithChain(chain, IoError.SRS_CONSUMER_UNAVAILABLE);
    }

    private AttachedChain attachedWithChain(String chain, IoError missingRoot) {
        requireChainName(chain);
        return transaction(session -> {
            var view = view(session, chain, missingRoot);
            List<SrsConsumerMembership> active = view.members().values().stream().filter(member -> !member.detached())
                    .sorted(Comparator.comparing(member -> member.context().key().consumerId())).toList();
            var topology = new SrsConsumerTopology(scope, chain, view.state().consumerRevision(), active);
            return new AttachedChain(topology, view.state());
        });
    }

    @Override public boolean supportsControlSnapshot(){return true;}
    @Override public Optional<ControlSnapshot> controlSnapshot(String chain) {
        return readControlSnapshot(chain, view -> {
            var active = activeConsumers(view);
            var cursors = active.stream().map(member -> view.cursors().get(member.context().key().consumerId()).value()).toList();
            return new ControlSnapshot(new SrsConsumerTopology(scope, chain, view.state().consumerRevision(), active),
                    view.state(), cursors);
        });
    }

    @Override public Optional<RetentionSnapshot> retentionSnapshot(String chain) {
        return readControlSnapshot(chain, true, view -> {
            var consumers = activeConsumers(view).stream()
                    .map(member -> new RetentionConsumer(member,
                            view.cursors().get(member.context().key().consumerId()).ringDoneThrough()))
                    .toList();
            return new RetentionSnapshot(view.state(), consumers);
        });
    }

    private static List<SrsConsumerMembership> activeConsumers(View view) {
        return view.members().values().stream().filter(member -> !member.detached())
                .sorted(Comparator.comparing(member -> member.context().key().consumerId())).toList();
    }

    private <T> Optional<T> readControlSnapshot(String chain, Function<View, T> project) {
        return readControlSnapshot(chain, false, project);
    }

    private <T> Optional<T> readControlSnapshot(String chain, boolean retention, Function<View, T> project) {
        requireChainName(chain);
        return readSnapshot(
                session -> Optional.ofNullable(controlView(session, chain))
                        .map(view -> projectControlSnapshot(session, chain, view, retention, project)),
                session -> Optional.ofNullable(readReadyControlView(session, chain))
                        .map(view -> Optional.of(projectControlSnapshot(session, chain, view, retention, project))));
    }

    private <T> T projectControlSnapshot(ClientSession session, String chain, View view, boolean retention, Function<View, T> project) {
        var root = view.root();
        var models=new java.util.HashSet<SrsModelKey>();
        for(var member:view.members().values())if(!member.detached()
                && (!retention || !java.util.Collections.disjoint(member.tables(),view.state().servingTables()))){
            var pin=view.cursors().get(member.context().key().consumerId()).snapshot();
            if(pin!=null)for(var key:pin.models().values()){
                if(!chain.equals(key.miningChainId())||!scope.cacheIncarnation().equals(key.cacheIncarnation()))throw unavailable(chain);
                models.add(key);
            }
        }
        var ordered=models.stream().map(key->MongoSrsModelStore.key(key).toJson()).sorted().toList();
        String fingerprint=io.tapstate.core.model.canonical.CanonicalHash.ofText(MongoSrsChainStore.controlFingerprint(root)+retention+ordered);
        if(!chains.referencesProved(root,fingerprint)){
            var check=chains.beginReferences();chains.state(session,root,true);
            if(!models.isEmpty())new MongoSrsModelStore(client,database,scope,requireActive,requireStructure).requireSnapshotReferences(session,models);
            chains.rememberReferences(session,root,fingerprint,check);
        }
        return project.apply(view);
    }

    private record BatchedRelations(Document root, List<Document> members, List<Document> offsets) {}

    private BatchedRelations readBatchedRelations(ClientSession session, String chain) {
        return decodeBatchedRelations(SystemCollections.SRS_CHAINS.on(database)
                .aggregate(session, batchedRelationsQuery(chain)).first());
    }

    private List<Document> batchedRelationsQuery(String chain) {
        var key = records.chain(chain);
        return List.of(
                match(new Document("_id", chain)), boundedRow(), originalRow(),
                lookup(SystemCollections.SRS_CONSUMERS, "members", bounded(match(key), CONTROL_SNAPSHOT_RELATIONS)),
                lookup(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS, "offsets",
                        List.of(match(key), new Document("$limit", CONTROL_SNAPSHOT_RELATIONS + 1),
                                boundedRow(16 * 1024))));
    }

    private static BatchedRelations decodeBatchedRelations(Document combined) {
        if (combined == null || overflow(combined, "members", CONTROL_SNAPSHOT_RELATIONS)
                || overflow(combined, "offsets", CONTROL_SNAPSHOT_RELATIONS)) return null;
        var root = combined.get("body", Document.class);
        return root.containsKey("_id") ? new BatchedRelations(root, rows(combined, "members"), rows(combined, "offsets")) : null;
    }

    private View readReadyControlView(ClientSession session, String chain) {
        if (!batchControlSnapshot.getAsBoolean() || !batchControlReadiness.getAsBoolean()) return null;
        var combined = SystemCollections.CACHE_IDENTITY.on(database).aggregate(session, List.of(
                match(new Document("_id", "cluster")), boundedRow(), originalRow(),
                one(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business"),
                lookup(SystemCollections.SRS_CHAINS, "chain", batchedRelationsQuery(chain)))).first();
        if (combined == null || overflow(combined, "schema", 1)) return null;
        var identity = combined.get("body", Document.class);
        if (!identity.containsKey("_id")) return null;
        MongoCacheBusinessIdentity.requireObservedReady(identity, single(combined, "schema"), scope);
        return decodeControlView(chain, decodeBatchedRelations(single(combined, "chain")));
    }

    private View decodeControlView(String chain, BatchedRelations batched) {
        if (batched == null) return null;
        var root = batched.root();
        var state = MongoSrsChainStore.decode(root, scope, new MongoSrsModelContexts(database, scope));
        return relations(chain, root, state, batched.members().iterator(), batched.offsets().iterator());
    }

    private View controlView(ClientSession session, String chain) {
        if (batchControlSnapshot.getAsBoolean()) {
            var batched = decodeControlView(chain, readBatchedRelations(session, chain));
            if (batched != null) return batched;
        }
        // The original path handles oversized results and distinguishes a missing chain from retained orphan rows.
        var root = chains.readRoot(session, chain);
        return root == null ? null : relations(session, chain, root,
                MongoSrsChainStore.decode(root, scope, new MongoSrsModelContexts(database, scope)));
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
        var pending=held.state().pendingFreshFull();
        if(pending!=null && pending.models().containsKey(table)) {
            var model=new MongoSrsModelStore(client,database,scope,requireActive,requireStructure)
                    .requireSnapshotReference(session,pending.models().get(table));
            safe=Math.min(safe,model.ddlSequence()-1);
        }
        if (safe >= 0) chains.replace(session,held.root(),new Document(held.root()));
        requireActive.run(); return safe;
    }

    record RetainedModels(Document chain, SrsChainState state, Set<SrsModelKey> pinned) {}

    /** Called only after reclaim has checked business identity and fenced its capture in this transaction. */
    RetainedModels fenceRetainedModels(ClientSession session, String chain, boolean batchRelations, boolean batchReferences) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("model reclamation requires the original transaction");
        var batched = batchRelations ? readBatchedRelations(session, chain) : null;
        var root = batched == null ? chains.requireRoot(session, chain) : batched.root();
        if (batched != null) MongoSrsRecoveryControls.requireOrdinary(root);
        var retained = chains.stateForReclaim(session, root, batchReferences);
        var held = batched == null ? relations(session, chain, root, retained.state())
                : relations(chain, root, retained.state(), batched.members().iterator(), batched.offsets().iterator());
        var result = new java.util.HashSet<SrsModelKey>();
        for (var member : held.members().values()) {
            if (member.detached()) continue;
            var snapshot = held.cursors().get(member.context().key().consumerId()).snapshot();
            validateSnapshot(session, member.context(), snapshot, root, retained.checkpointModels());
            if (snapshot != null) result.addAll(snapshot.models().values());
        }
        var replacement = new Document(root);
        chains.replace(session, root, replacement);
        return new RetainedModels(replacement, held.state(), Set.copyOf(result));
    }
    /** Serializes model reclamation with attach, detach and exact snapshot-pin publication. */
    Set<io.tapstate.spi.store.SrsModelKey> retainedModels(ClientSession session,String chain, MongoSrsReadFacts facts) {
        Document root = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", chain)).first();
        if (root == null) throw new TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE, Map.of("chain", chain), null);
        facts.seed(SystemCollections.SRS_CHAINS, root);
        var state = chains.state(session, root, false, facts);
        var held = relations(session, chain, root, state);
        var result = new java.util.HashSet<io.tapstate.spi.store.SrsModelKey>();
        for(var member:held.members().values()) {
            if(member.detached())continue;
            var snapshot=held.cursors().get(member.context().key().consumerId()).snapshot();
            validateSnapshot(session,member.context(),snapshot, facts);
            if(snapshot!=null)result.addAll(snapshot.models().values());
        }
        var replacement = new Document(held.root());
        chains.replace(session, held.root(), replacement);
        facts.seed(SystemCollections.SRS_CHAINS, replacement);
        return Set.copyOf(result);
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
            requireActive.run(); var retained = administrativeView(session, name);
            for (var member : retained.members().values()) if (member.context().key().pipelineId().equals(original.pipelineId())
                    && (!member.detached() || member.context().stateGeneration() > original.stateGeneration()))
                throw MongoCachePipelinePreparations.rebuild(original);
        }
        requireActive.run();
    }

    @Override public Optional<ConsumerOffset> cursor(SrsConsumerContext context) { return progress(context).map(SrsConsumerProgress::cursor); }
    @Override public Optional<ConsumerOffset> observationCursor(SrsConsumerContext context) {
        return progress(context, true).map(SrsConsumerProgress::cursor);
    }

    @Override public Optional<SrsConsumerProgress> progress(SrsConsumerContext context) {
        return progress(context, false);
    }

    @Override public Optional<SrsConsumerProgress> snapshotResumeProgress(SrsConsumerContext context) {
        Objects.requireNonNull(context, "context");
        return inTransaction(session -> {
            requireStructure.run();
            var captured = capturedStartup(session, context, Map.of(), false, StartupRead.SNAPSHOT_RESUME);
            if (captured != null) return captured.map(value -> value.snapshot().progress());
            validateStore(session, true);
            return progress(session, context, false);
        });
    }

    private Optional<SrsConsumerProgress> progress(SrsConsumerContext context, boolean forObservation) {
        Objects.requireNonNull(context, "context");
        return readSnapshot(session -> progress(session, context, forObservation));
    }

    Optional<ConsumerOffset> admittedObservationCursor(SrsConsumerContext context) {
        Objects.requireNonNull(context, "context");
        return inTransaction(session -> {
            new MongoCacheBusinessIdentity(database, scope).requireAdmissionState(session);
            requireStructure.run();
            requireActive.run();
            return progress(session, context, true).map(SrsConsumerProgress::cursor);
        });
    }

    private Optional<SrsConsumerProgress> progress(ClientSession session, SrsConsumerContext context, boolean forObservation) {
        var facts = new MongoSrsReadFacts(session, database, scope, true);
        var own = own(session, context, true, facts, forObservation);
        if (own == null || own.member().detached()) return Optional.empty();
        var held = own.cursor(); validateSnapshot(session, context, held.snapshot(), facts);
        return Optional.of(new SrsConsumerProgress(context,own.member().revision(),held.revision(),held.value(),held.snapshot(),held.writerRun(),held.ringDoneThrough()));
    }

    @Override public Optional<StartupArrivals> startupArrivals(SrsConsumerContext context, Map<String, String> ringsByTable) {
        var selected = Map.copyOf(ringsByTable);
        return transaction(session -> {
            var facts = new MongoSrsReadFacts(session, database, scope, true);
            var original = own(session, context, true, facts);
            if (original == null || original.member().detached()) return Optional.empty();
            var held = original.cursor();
            validateSnapshot(session, context, held.snapshot(), facts);
            var progress = new SrsConsumerProgress(context, original.member().revision(), held.revision(),
                    held.value(), held.snapshot(), held.writerRun(), held.ringDoneThrough());
            var missing = new java.util.HashSet<String>();
            selected.forEach((table, ring) -> {
                if (!held.ringDoneThrough().containsKey(table)) missing.add(ring);
            });
            var models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
            var log = new MongoSrsLogStore(client, SystemCollections.SRS_EVENTS.on(database),
                    SystemCollections.SRS_LOG_BOUNDS.on(database), SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models);
            var bounds = log.publishedBounds(session, context.key().miningChainId(), missing, original.root());
            return Optional.of(new StartupArrivals(progress, bounds));
        });
    }

    @Override public Optional<StartupSnapshot> startupSnapshot(SrsConsumerContext context, Map<String, String> ringsByTable) {
        return startupSnapshot(context, ringsByTable, false);
    }

    @Override public Optional<StartupSnapshot> startupSnapshot(SrsConsumerContext context, Map<String, String> ringsByTable,
            boolean verifyControlClosure) {
        var selected = Map.copyOf(ringsByTable);
        Function<ClientSession, Optional<StartupSnapshot>> read = session -> {
            requireStructure.run();
            var captured = capturedStartup(session, context, selected, verifyControlClosure, StartupRead.SNAPSHOT);
            if (captured != null) return captured.map(CapturedStartup::snapshot);
            validateStore(session, true);
            return startupSnapshotInSession(session, context, selected, verifyControlClosure);
        };
        return !selected.isEmpty() && MongoCaptureStartupRows.supportsReadSnapshot(client, database)
                ? MongoCaptureStartupRows.readSnapshot(client, requireActive, read) : inTransaction(read);
    }

    @Override public Optional<StartupPreparation> startupPreparation(SrsConsumerContext context, boolean verifyControlClosure) {
        return inTransaction(session -> {
            requireStructure.run();
            var captured = capturedStartup(session, context, Map.of(), verifyControlClosure, StartupRead.PREPARATION);
            if (captured != null) return captured.map(value -> new StartupPreparation(value.snapshot(), value.topology()));
            validateStore(session, true);
            return startupSnapshotInSession(session, context, Map.of(), verifyControlClosure).map(snapshot -> {
                var view = view(session, context.key().miningChainId());
                return new StartupPreparation(snapshot, topology(view));
            });
        });
    }

    private enum StartupRead { PREPARATION, SNAPSHOT, SNAPSHOT_RESUME }

    private record CapturedStartup(StartupSnapshot snapshot, SrsConsumerTopology topology) {}

    private Optional<CapturedStartup> capturedStartup(ClientSession session, SrsConsumerContext context,
            Map<String, String> selected, boolean verifyControlClosure, StartupRead read) {
        boolean withTopology = read == StartupRead.PREPARATION;
        String chain = context.key().miningChainId();
        var filter = records.chain(chain);
        if (!withTopology && !verifyControlClosure) filter.append("_id.consumer", context.key().consumerId());
        var captured = startProgressRows(session, chain, filter, List.of(
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_IDENTITY, "cache", "cluster"),
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business")));
        Document root = captured == null ? null : captured.get("root", Document.class);
        if (root == null || !chain.equals(root.get("_id"))
                || MongoCaptureStartupRows.get(captured, "cache") == null
                || MongoCaptureStartupRows.get(captured, "schema") == null
                || MongoCaptureStartupRows.ambiguous(captured, "cache", "cluster")
                || MongoCaptureStartupRows.ambiguous(captured, "schema", "business")) return null;
        MongoCacheBusinessIdentity.requireObservedReady(MongoCaptureStartupRows.get(captured, "cache"),
                MongoCaptureStartupRows.get(captured, "schema"), scope);
        var members = captured.getList("members", Document.class);
        var cursors = captured.getList("cursors", Document.class);
        if (java.util.stream.Stream.concat(members.stream(), cursors.stream()).anyMatch(row ->
                !(row.get("_id") instanceof Document id) || !chain.equals(id.get("chain"))
                        || !scope.cacheIncarnation().equals(id.get("cacheIncarnation"))
                        || !withTopology && !verifyControlClosure && !context.key().consumerId().equals(id.get("consumer")))) return null;
        var facts = new MongoSrsReadFacts(session, database, scope, root, true);
        var control = MongoSrsChainStore.decode(root, scope, new MongoSrsModelContexts(database, scope));
        facts.prefetchStartupCheckpoint(root, control.checkpoint());
        var state = chains.state(session, root, true, facts);
        var memberRows = members.stream().filter(row -> context.key().consumerId().equals(
                row.get("_id", Document.class).get("consumer"))).toList();
        var ownCursors = cursors.stream().filter(row -> context.key().consumerId().equals(
                row.get("_id", Document.class).get("consumer"))).toList();
        if (memberRows.size() != 1 || ownCursors.size() > 1
                || !exactStartupId(records.id(context.key()), memberRows.getFirst().get("_id"))
                || !ownCursors.isEmpty() && !exactStartupId(records.id(context.key()), ownCursors.getFirst().get("_id"))) return null;
        var member = records.membership(memberRows.getFirst());
        if (!member.context().key().equals(context.key()) || state.consumerRevision() < member.revision()
                || member.context().stateGeneration() != context.stateGeneration()) throw unavailable(chain);
        if (member.detached()) {
            if (!ownCursors.isEmpty()) throw unavailable(chain);
            return Optional.empty();
        }
        if (state.consumerCount() < 1) throw unavailable(chain);
        var cursor = records.cursor(member, ownCursors.isEmpty() ? null : ownCursors.getFirst());
        if (verifyControlClosure) verifyCapturedPlanningControl(state, members, cursors);
        if (read == StartupRead.SNAPSHOT_RESUME || !selected.isEmpty()) facts.prefetchSnapshotReferences(root, cursor.snapshot());
        validateSnapshot(session, context, cursor.snapshot(), facts);
        var view = withTopology ? validateRelations(chain, root, state, members, () -> cursors) : null;
        var progress = new SrsConsumerProgress(context, member.revision(), cursor.revision(), cursor.value(),
                cursor.snapshot(), cursor.writerRun(), cursor.ringDoneThrough());
        return Optional.of(new CapturedStartup(startupSnapshot(session, root, state, progress, selected, facts),
                view == null ? null : topology(view)));
    }

    private static boolean exactStartupId(Document expected, Object actual) {
        if (!(actual instanceof Document id)) return false;
        var codec = new org.bson.codecs.DocumentCodec();
        return new org.bson.RawBsonDocument(expected, codec).getByteBuffer().asNIO().equals(
                new org.bson.RawBsonDocument(id, codec).getByteBuffer().asNIO());
    }

    private SrsConsumerTopology topology(View view) {
        var members = view.members().values().stream().filter(member -> !member.detached())
                .sorted(Comparator.comparing(member -> member.context().key().consumerId())).toList();
        return new SrsConsumerTopology(scope, view.state().miningChainId(), view.state().consumerRevision(), members);
    }

    private StartupSnapshot startupSnapshot(ClientSession session, Document root, SrsChainState chain,
            SrsConsumerProgress progress, Map<String, String> selected, MongoSrsReadFacts facts) {
        if (selected.isEmpty() || chain.epoch() == 0 || !chain.servingTables().containsAll(selected.keySet()))
            return new StartupSnapshot(chain, progress, Optional.empty(), Map.of());
        var models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
        var published = models.startupModels(session, root, selected, chain.checkpoint(), facts);
        var bounds = new HashMap<String, SrsLogBounds>();
        if (chain.pendingFreshFull() == null && published.isPresent()
                && published.orElseThrow().values().stream().allMatch(model -> model.key().captureEpoch() == chain.epoch())) {
            var log = new MongoSrsLogStore(client, SystemCollections.SRS_EVENTS.on(database),
                    SystemCollections.SRS_LOG_BOUNDS.on(database), SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models);
            selected.forEach((table, ring) -> {
                if (!progress.ringDoneThrough().containsKey(table)) bounds.put(ring, log.boundsInSession(session, ring, facts));
            });
        }
        return new StartupSnapshot(chain, progress, published, bounds);
    }

    private Optional<StartupSnapshot> startupSnapshotInSession(ClientSession session, SrsConsumerContext context,
            Map<String, String> selected, boolean verifyControlClosure) {
        var facts = new MongoSrsReadFacts(session, database, scope, true);
        String chainId = context.key().miningChainId();
        if (MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_CHAINS,
                new Document("_id", chainId)) == null) {
            if (chains.hasRetainedFacts(session, chainId))
                throw new TapstateException(IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", chainId), null);
            return Optional.empty();
        }
        var original = own(session, context, true, facts);
        if (original == null || original.member().detached()) return Optional.empty();
        if (verifyControlClosure) verifyPlanningControl(session, original.state());
        var held = original.cursor();
        validateSnapshot(session, context, held.snapshot(), facts);
        var progress = new SrsConsumerProgress(context, original.member().revision(), held.revision(),
                held.value(), held.snapshot(), held.writerRun(), held.ringDoneThrough());
        return Optional.of(startupSnapshot(session, original.root(), original.state(), progress, selected, facts));
    }

    private void verifyPlanningControl(ClientSession session, SrsChainState state) {
        String chain = state.miningChainId();
        var members = new HashMap<String, SrsConsumerMembership>();
        long revisions = 0;
        try (var rows = SystemCollections.SRS_CONSUMERS.on(database).find(session, records.chain(chain)).batchSize(64).iterator()) {
            while (rows.hasNext()) {
                requireActive.run();
                var member = records.membership(rows.next());
                if (!chain.equals(member.context().key().miningChainId())
                        || members.put(member.context().key().consumerId(), member) != null) throw unavailable(chain);
                try { revisions = Math.addExact(revisions, member.revision()); }
                catch (ArithmeticException invalid) { throw unavailable(chain); }
            }
        }
        var present = new java.util.HashSet<String>();
        try (var rows = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(session, records.chain(chain))
                .projection(new Document("_id", 1).append("formatVersion", 1).append("stateGeneration", 1)
                        .append("membershipRevision", 1).append("progressRevision", 1)).batchSize(64).iterator()) {
            while (rows.hasNext()) {
                requireActive.run();
                var row = rows.next();
                if (!(row.get("_id") instanceof Document id) || !(id.get("consumer") instanceof String consumer)) throw unavailable(chain);
                var member = members.get(consumer);
                if (member == null || member.detached() || !id.equals(records.id(member.context().key()))
                        || !present.add(consumer)) throw unavailable(chain);
                if (!row.keySet().equals(Set.of("_id", "formatVersion", "stateGeneration", "membershipRevision", "progressRevision")))
                    throw unavailable(chain);
                records.cursorIdentity(member, row);
            }
        }
        long active = members.values().stream().filter(member -> !member.detached()).count();
        if (active != state.consumerCount() || present.size() != active || revisions != state.consumerRevision()) throw unavailable(chain);
    }

    private void verifyCapturedPlanningControl(SrsChainState state, List<Document> membershipRows, List<Document> cursorRows) {
        String chain = state.miningChainId();
        var members = new HashMap<String, SrsConsumerMembership>();
        long revisions = 0;
        for (var row : membershipRows) {
            requireActive.run();
            var member = records.membership(row);
            if (!chain.equals(member.context().key().miningChainId())
                    || members.put(member.context().key().consumerId(), member) != null) throw unavailable(chain);
            try { revisions = Math.addExact(revisions, member.revision()); }
            catch (ArithmeticException invalid) { throw unavailable(chain); }
        }
        var present = new java.util.HashSet<String>();
        for (var raw : cursorRows) {
            requireActive.run();
            var row = new Document();
            for (String field : List.of("_id", "formatVersion", "stateGeneration", "membershipRevision", "progressRevision"))
                if (raw.containsKey(field)) row.put(field, raw.get(field));
            if (!(row.get("_id") instanceof Document id) || !(id.get("consumer") instanceof String consumer)) throw unavailable(chain);
            var member = members.get(consumer);
            if (member == null || member.detached() || !id.equals(records.id(member.context().key()))
                    || !present.add(consumer)) throw unavailable(chain);
            if (!row.keySet().equals(Set.of("_id", "formatVersion", "stateGeneration", "membershipRevision", "progressRevision")))
                throw unavailable(chain);
            records.cursorIdentity(member, row);
        }
        long active = members.values().stream().filter(member -> !member.detached()).count();
        if (active != state.consumerCount() || present.size() != active || revisions != state.consumerRevision()) throw unavailable(chain);
    }

    @Override public boolean supportsReadEventVerification() { return true; }

    @Override public boolean supportsReadBatchVerification() { return true; }

    @Override public boolean verifyReadEvents(SrsConsumerRun run, String table, String ring, List<ReadEvent> events) {
        if (!scope.equals(run.cacheScope())) return false;
        var batch = List.copyOf(events);
        if (batch.isEmpty()) return true;
        requireActive.run();
        var before = readLease.observe();
        var known = provenReads.get();
        boolean reuse = readLease.stands(before) && known != null && known.lease() == before && known.run().equals(run);
        if (reuse && batch.stream().allMatch(event -> {
                    var model = known.models().get(readModel(table, ring, event));
                    return model != null && model.range().covers(event.sequence());
                })) return true;

        long started = System.nanoTime();
        try {
            var checked = transaction(runtimeStructure, session -> {
                var held = own(session, run.context());
                if (held == null || held.member().detached()
                        || !bound(held.member(), run.membershipRevision(), run.originalClaim())
                        || !held.cursor().value().perTableSeq().containsKey(table)) return null;
                var pin = singleSnapshotModel(session, held);
                var models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
                var references = models.verifyReadBatch(session, held.root(), run.miningChainId(), table, ring, batch, pin);
                if (!readLease.enabled()) return new BatchReadProof(Map.of(), 0);
                var ranges = new HashMap<ReadModel, MongoSrsModelStore.ModelReference>();
                for (var event : batch) {
                    var model = readModel(table, ring, event);
                    ranges.put(model, references.get(model.key(run)));
                }
                long remaining = MongoSrsLease.remainingMillis(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database),
                        session, run.originalClaim());
                return new BatchReadProof(Map.copyOf(ranges), remaining);
            });
            if (checked == null) {
                readLease.drop();
                return false;
            }
            if (reuse && readLease.stands(before)) {
                // Add the proved models without extending the deadline of any previously proved range.
                var ranges = new HashMap<>(known.models());
                ranges.putAll(checked.models());
                provenReads.compareAndSet(known, new ProvenReads(before, run, Map.copyOf(ranges)));
            } else {
                var published = readLease.proved(before, started, checked.remainingMillis());
                if (published != null) provenReads.set(new ProvenReads(published, run, checked.models()));
            }
            return true;
        } catch (RuntimeException failure) {
            MongoStructureProof.invalidate(requireStructure);
            readLease.drop();
            throw failure;
        }
    }

    private static ReadModel readModel(String table, String ring, ReadEvent event) {
        return new ReadModel(table, ring, event.epoch(), event.version(), event.capture());
    }

    private record BatchReadProof(Map<ReadModel, MongoSrsModelStore.ModelReference> models, long remainingMillis) {}

    @Override public Optional<SrsModel> verifyReadEvent(SrsConsumerRun run, String table, String ring, long sequence,
            long epoch, long version, WorkloadClaimFence capture) {
        Objects.requireNonNull(run, "run");
        if (!scope.equals(run.cacheScope())) return Optional.empty();
        requireActive.run();

        var before = readLease.observe();
        var known = provenReads.get();
        boolean reuse = readLease.stands(before) && known != null && known.lease() == before && known.run().equals(run);
        var event = new ReadModel(table, ring, epoch, version, capture);
        boolean coordinates = sequence >= 0 && sequence < Long.MAX_VALUE && epoch > 0 && epoch < Long.MAX_VALUE
                && version >= 0 && version < Long.MAX_VALUE && capture != null;
        if (reuse && coordinates) {
            var proof = known.models().get(event);
            if (proof != null && proof.range().covers(sequence)) return Optional.of(proof.model());
        }

        long started = System.nanoTime();
        try {
            var checked = transaction(session -> {
                var original = own(session, run.context());
                if (original == null || original.member().detached() || !bound(original.member(), run.membershipRevision(), run.originalClaim()))
                    return Optional.<ReadProof>empty();
                var held = original.cursor();
                validateSnapshot(session, run.context(), held.snapshot());
                if (!held.value().perTableSeq().containsKey(table)) return Optional.<ReadProof>empty();

                if (!coordinates)
                    throw new TapstateException(IoError.SRS_EVENT_MODEL_UNAVAILABLE,
                            Map.of("ring", ring, "epoch", String.valueOf(epoch), "version", String.valueOf(version)), null);

                var models = new MongoSrsModelStore(client, database, scope, requireActive, requireStructure);
                var reference = models.requireEventReference(session, event.key(run), ring, capture);
                reference.range().requireAt(sequence, ring, epoch, version);

                long remaining = readLease.enabled() ? MongoSrsLease.remainingMillis(
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), session, run.originalClaim()) : 0;
                var ranges = new HashMap<ReadModel, MongoSrsModelStore.ModelReference>();
                if (reuse) {
                    for (var prior : known.models().keySet()) {
                        if (prior.equals(event) || !held.value().perTableSeq().containsKey(prior.table())) continue;
                        try {
                            ranges.put(prior, models.requireEventReference(session, prior.key(run), prior.ring(), prior.capture()));
                        } catch (TapstateException unavailable) {
                            if (unavailable.code() != IoError.SRS_EVENT_MODEL_UNAVAILABLE) throw unavailable;
                        }
                    }
                }
                ranges.put(event, reference);
                return Optional.of(new ReadProof(reference.model(), Map.copyOf(ranges), remaining));
            });
            if (checked.isEmpty()) {
                readLease.drop();
                return Optional.empty();
            }
            var proof = checked.orElseThrow();
            var published = readLease.proved(before, started, proof.remainingMillis());
            if (published != null) provenReads.set(new ProvenReads(published, run, proof.models()));
            return Optional.of(proof.model());
        } catch (RuntimeException failure) {
            MongoStructureProof.invalidate(requireStructure);
            readLease.drop();
            throw failure;
        }
    }

    private record ReadModel(String table, String ring, long epoch, long version, WorkloadClaimFence capture) {
        SrsModelKey key(SrsConsumerRun run) {
            return new SrsModelKey(run.miningChainId(), table, run.scope().cacheIncarnation(), epoch, version);
        }
    }
    private record ReadProof(SrsModel model, Map<ReadModel, MongoSrsModelStore.ModelReference> models, long remainingMillis) {}
    private record ProvenReads(MongoSrsLease.Lease lease, SrsConsumerRun run,
                               Map<ReadModel, MongoSrsModelStore.ModelReference> models) {}

    @Override public boolean beginWriterRun(SrsConsumerContext context,long membershipRevision,String runId,
            Map<String,List<String>> expected,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision);
        // Freeze caller-owned collections before a transaction callback can be retried.
        var requested = new SrsConsumerWriterRun(membershipRevision,fence,runId,expected,Map.of());
        return checkpointTransaction(context.key().pipelineId(), session -> {
            proveProgress(session, context, fence);
            var own = runtimeStructure ? requireStartupOwn(session, context, true) : requireOwn(session, context);
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

    @Override public boolean advanceWriter(SrsConsumerContext context, long membershipRevision, String runId,
            String table, String writerId, WriterProgress report, WorkloadClaimFence fence) {
        return advanceWriters(context, membershipRevision, runId,
                List.of(new WriterProgressUpdate(table, writerId, report)), fence, true).getFirst().committedOrThrow();
    }

    @Override public List<WriterProgressOutcome> advanceWriters(SrsConsumerContext context, long membershipRevision, String runId,
            List<WriterProgressUpdate> reports, WorkloadClaimFence fence) {
        return advanceWriters(context, membershipRevision, runId, reports, fence, false);
    }

    private List<WriterProgressOutcome> advanceWriters(SrsConsumerContext context, long membershipRevision, String runId,
            List<WriterProgressUpdate> reports, WorkloadClaimFence fence, boolean throwRefusal) {
        Objects.requireNonNull(context, "context");
        requireRevision(membershipRevision);
        List<WriterProgressUpdate> original = List.copyOf(reports);
        if (original.isEmpty()) return List.of();
        if (runId == null || runId.isBlank())
            throw new IllegalArgumentException("an original writer report requires named run, table and writer");
        var malformed = original.stream().map(update -> update.table() == null || update.table().isBlank()
                || update.writerId() == null || update.writerId().isBlank()
                ? new IllegalArgumentException("an original writer report requires named run, table and writer") : null).toList();
        if (throwRefusal && malformed.getFirst() != null) throw malformed.getFirst();
        try {
            return checkpointTransaction(context.key().pipelineId(), runtimeStructure, session -> {
                proveProgress(session, context, fence);
                var first = runtimeStructure ? requireIncrementalOwn(session, context) : requireOwn(session, context);
                if (!bound(first.member(), membershipRevision, fence))
                    return java.util.Collections.nCopies(original.size(), new WriterProgressOutcome(false, null));
                var run = first.cursor().writerRun();
                if (run == null || run.membershipRevision() != membershipRevision
                        || !run.commandFence().equals(fence) || !run.runId().equals(runId))
                    return java.util.Collections.nCopies(original.size(), new WriterProgressOutcome(false, null));
                var outcomes = new ArrayList<WriterProgressOutcome>(original.size());
                var own = first;
                for (int i = 0; i < original.size(); i++) {
                    var update = original.get(i);
                    RuntimeException refusal = malformed.get(i);
                    if (refusal == null && !run.expected().getOrDefault(update.table(), List.of()).contains(update.writerId()))
                        refusal = unavailable(context.key().miningChainId());
                    if (refusal != null) {
                        if (throwRefusal) throw refusal;
                        outcomes.add(new WriterProgressOutcome(false, refusal));
                        continue;
                    }
                    var held = own.cursor();
                    var next = SrsConsumerAcknowledgements.advance(held.writerRun(), held.value(), update.table(), update.writerId(), update.progress());
                    var done = MongoSrsConsumerRecords.reportedFloor(next.cursor(), held.ringDoneThrough(),
                            next.writerRun(), update.table(), update.progress());
                    if (!next.writerRun().equals(held.writerRun()) || !next.cursor().equals(held.value()) || !done.equals(held.ringDoneThrough()))
                        replaceProgress(session, own, next.cursor(), next.writerRun(), done);
                    outcomes.add(new WriterProgressOutcome(true, null));
                    if (i + 1 < original.size()) own = runtimeStructure ? requireIncrementalOwn(session, context) : requireOwn(session, context);
                }
                if (outcomes.stream().noneMatch(WriterProgressOutcome::committed)) throw new UnchangedReports(outcomes);
                return List.copyOf(outcomes);
            });
        } catch (UnchangedReports refused) {
            return refused.outcomes;
        }
    }

    private static final class UnchangedReports extends RuntimeException {
        private final transient List<WriterProgressOutcome> outcomes;
        private UnchangedReports(List<WriterProgressOutcome> outcomes) {
            super(null, null, false, false);
            this.outcomes = List.copyOf(outcomes);
        }
    }


    @Override public boolean startReadingAfter(SrsConsumerContext context,long membershipRevision,long expectedProgressRevision,
            String table,long sequence,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision); requireRevision(expectedProgressRevision);
        if (table == null || table.isBlank() || sequence < -1 || sequence == Long.MAX_VALUE) throw new IllegalArgumentException("invalid original consumer arrival");
        return startReadingAfter(context,membershipRevision,expectedProgressRevision,Map.of(table,sequence),fence);
    }

    @Override public boolean startReadingAfter(SrsConsumerContext context,long membershipRevision,long expectedProgressRevision,
            Map<String,Long> sequences,WorkloadClaimFence fence) {
        return startReadingAfter(context, membershipRevision, expectedProgressRevision, sequences, fence, false, false).isPresent();
    }

    @Override public Optional<SrsConsumerProgress> startReadingAfterAndGet(SrsConsumerContext context, long membershipRevision,
            long expectedProgressRevision, Map<String, Long> sequences, WorkloadClaimFence fence) {
        return startReadingAfter(context, membershipRevision, expectedProgressRevision, sequences, fence, runtimeStructure, true);
    }

    private Optional<SrsConsumerProgress> startReadingAfter(SrsConsumerContext context, long membershipRevision,
            long expectedProgressRevision, Map<String, Long> sequences, WorkloadClaimFence fence, boolean admitted,
            boolean prepareBeforeWrite) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision); requireRevision(expectedProgressRevision);
        Objects.requireNonNull(sequences,"sequences");
        if (sequences.isEmpty() || sequences.entrySet().stream().anyMatch(entry -> entry.getKey() == null || entry.getKey().isBlank()
                || entry.getValue() == null || entry.getValue() < -1 || entry.getValue() == Long.MAX_VALUE))
            throw new IllegalArgumentException("invalid original consumer arrivals");
        var captured = Map.copyOf(sequences);
        return checkpointTransaction(context.key().pipelineId(), admitted, session -> {
            var pipeline = pipelineAccess(context, fence);
            var state = prepareBeforeWrite ? pipeline.stateForWriterProgress(session, true) : pipeline.state(session, true);
            if (!prepareBeforeWrite) proveConsumerWrite(session, pipeline, state);
            var own = prepareBeforeWrite ? requireStartupOwn(session, context, admitted)
                    : requireOwn(session, context, admitted);
            var initialized = prepareArrival(own, membershipRevision, expectedProgressRevision, captured, fence);
            // Keep the exact claim write and the original claim, state, and cursor write order.
            if (prepareBeforeWrite) proveConsumerWrite(session, pipeline, state);
            initialized.filter(progress -> progress.revision() != own.cursor().revision()).ifPresent(progress ->
                    replaceProgress(session, own, progress.cursor(), progress.writerRun(), progress.ringDoneThrough()));
            return initialized;
        });
    }

    private Optional<SrsConsumerProgress> prepareArrival(Own own, long membershipRevision,
            long expectedProgressRevision, Map<String, Long> captured, WorkloadClaimFence fence) {
        var context = own.member().context();
        if (!bound(own.member(),membershipRevision,fence)) return Optional.empty();
        if (!own.member().tables().containsAll(captured.keySet())) throw unavailable(context.key().miningChainId());
        var held = own.cursor(); var old = held.value();
        var missing = new HashMap<String,Long>();
        for (var entry : captured.entrySet()) {
            var confirmed = held.ringDoneThrough().get(entry.getKey());
            if (confirmed == null) missing.put(entry.getKey(),entry.getValue());
            else if (entry.getValue() > confirmed) return Optional.empty();
        }
        if (missing.isEmpty()) return Optional.of(new SrsConsumerProgress(context, membershipRevision, held.revision(),
                old, held.snapshot(), held.writerRun(), held.ringDoneThrough()));
        if (held.revision() != expectedProgressRevision) return Optional.empty();
        // Validate every new arrival before replacing any part of the consumer's original cursor.
        if (held.revision() >= Long.MAX_VALUE - 1 || missing.keySet().stream().anyMatch(table -> old.perTableSeq().get(table) != -1L))
            throw unavailable(context.key().miningChainId());
        var done = new HashMap<>(held.ringDoneThrough()); done.putAll(missing);
        var read = new HashMap<>(old.perTableSeq()); read.putAll(missing);
        var next = new ConsumerOffset(old.pipelineId(),read,old.sinkAcked(),old.snapshotCompletedTables(),old.cdcStartPosition(),
                old.snapshotEpoch(),old.sinkAckedByTable(),old.progressKind());
        var committed = MongoSrsConsumerRecords.confirmedFloors(next, done, held.writerRun());
        return Optional.of(new SrsConsumerProgress(context, membershipRevision, held.revision() + 1,
                next, held.snapshot(), held.writerRun(), committed));
    }

    @Override public boolean advanceReadSequence(SrsConsumerContext context,long membershipRevision,String table,long sequence,WorkloadClaimFence fence) {
        Objects.requireNonNull(context,"context"); requireRevision(membershipRevision);
        if (table == null || table.isBlank() || sequence < -1 || sequence == Long.MAX_VALUE) throw new IllegalArgumentException("invalid original consumer read sequence");
        if (!cursorLease.enabled()) {
            return checkpointTransaction(context.key().pipelineId(), runtimeStructure, session -> {
                proveProgress(session, context, fence);
                var own = runtimeStructure ? requireIncrementalOwn(session, context) : requireOwn(session, context);
                return writeReadCursor(session, own.member(), own.cursor(), membershipRevision, table, sequence, fence);
            });
        }
        var before = cursorLease.observe();
        var known = provenCursor.get();
        var pipeline = pipelineAccess(context, fence);
        long started = System.nanoTime();
        try {
            var result = checkpointInTransaction(context.key().pipelineId(), session -> {
                if (known != null && known.lease() == before && cursorLease.stands(before)
                        && known.member().context().equals(context) && known.member().revision() == membershipRevision
                        && known.member().commandFence().equals(fence)) {
                    var current = pipeline.stateForWriterProgress(session, true);
                    pipeline.updateState(session, current, new Document(), false);
                    var row = SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database)
                            .find(session, new Document("_id", records.id(context.key()))).first();
                    if (row != null && Long.valueOf(membershipRevision).equals(row.get("membershipRevision"))) {
                        var cursor = records.cursor(known.member(), row);
                        return new CursorResult(writeReadCursor(session, known.member(), cursor, membershipRevision, table, sequence, fence), null, 0L);
                    }
                    cursorLease.drop();
                    var own = requireOwn(session, context);
                    return new CursorResult(writeReadCursor(session, own.member(), own.cursor(), membershipRevision, table, sequence, fence), null, 0L);
                }
                validateStore(session, runtimeStructure);
                proveProgress(session, context, fence);
                var own = runtimeStructure ? requireIncrementalOwn(session, context) : requireOwn(session, context);
                if (!writeReadCursor(session, own.member(), own.cursor(), membershipRevision, table, sequence, fence))
                    return new CursorResult(false, null, 0L);
                long remaining = MongoSrsLease.remainingMillis(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), session, fence);
                return new CursorResult(true, own.member(), remaining);
            });
            if (!result.advanced()) {
                cursorLease.drop();
            } else if (result.proved() != null) {
                var published = cursorLease.proved(before, started, result.remainingMillis());
                if (published != null) provenCursor.set(new ProvenCursor(published, result.proved()));
            }
            return result.advanced();
        } catch (RuntimeException failure) {
            MongoStructureProof.invalidate(requireStructure);
            cursorLease.drop();
            throw failure;
        }
    }

    private boolean writeReadCursor(ClientSession session, SrsConsumerMembership member, MongoSrsConsumerRecords.Cursor held,
            long membershipRevision, String table, long sequence, WorkloadClaimFence fence) {
        if (!bound(member, membershipRevision, fence)) return false;
        if (!member.tables().contains(table) || !held.ringDoneThrough().containsKey(table))
            throw unavailable(member.context().key().miningChainId());
        var old = held.value();
        if (sequence <= old.perTableSeq().get(table)) return true;
        var read = new HashMap<>(old.perTableSeq());
        read.put(table, sequence);
        var next = new ConsumerOffset(old.pipelineId(), read, old.sinkAcked(), old.snapshotCompletedTables(), old.cdcStartPosition(),
                old.snapshotEpoch(), old.sinkAckedByTable(), old.progressKind());
        replaceProgress(session, member, held, next, held.writerRun(), held.ringDoneThrough());
        return true;
    }

    private record ProvenCursor(MongoSrsLease.Lease lease, SrsConsumerMembership member) {}
    private record CursorResult(boolean advanced, SrsConsumerMembership proved, long remainingMillis) {}

    private static boolean bound(SrsConsumerMembership member,long revision,WorkloadClaimFence fence) {
        if (member.revision() != revision) return false;
        if (!member.commandFence().equals(fence)) throw fenced();
        return true;
    }
    private void replaceProgress(ClientSession session,Own own,ConsumerOffset value,SrsConsumerWriterRun run) {
        replaceProgress(session,own,value,run,own.cursor().ringDoneThrough());
    }
    private void replaceProgress(ClientSession session,Own own,ConsumerOffset value,SrsConsumerWriterRun run,Map<String,Long> done) {
        replaceProgress(session, own.member(), own.cursor(), value, run, done);
    }

    private void replaceProgress(ClientSession session, SrsConsumerMembership member, MongoSrsConsumerRecords.Cursor held,
            ConsumerOffset value, SrsConsumerWriterRun run, Map<String, Long> done) {
        String chain = member.context().key().miningChainId();
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
        var pipeline = pipelineAccess(context, fence);
        var claimConflict = new AtomicBoolean();
        return changingTheConsumer(() -> pinTransaction(context.key(), fence, claimConflict, (session, schedulingProof) -> {
            // Reserve the chain before reading its model and cursor facts.
            var root = chains.reserveSnapshotPin(session, chain);
            boolean batchReads = root != null && MongoBindingReadFacts.supported(client);
            var state = pipeline.stateForSnapshotPin(session, batchReads, schedulingProof);
            pipeline.updateState(session, state, new Document(), false);
            var facts = root == null ? new MongoSrsReadFacts(session, database, scope, true)
                    : new MongoSrsReadFacts(session, database, scope, root, true);
            var view = root == null ? view(session, chain) : pinRelations(session, chain, root, batchReads, facts, snapshot);
            var member = view.members().get(context.key().consumerId());
            if (member == null || member.detached() || !member.context().equals(context)) throw unavailable(chain);
            if (member.revision() != membershipRevision) throw new UnchangedPin(false);
            if (!member.commandFence().equals(fence)) throw fenced();
            var held = view.cursors().get(context.key().consumerId()); var before = held.snapshot();
            if (!snapshot.equals(before) && held.revision() != expectedProgressRevision) throw new UnchangedPin(false);
            var required = new java.util.HashSet<>(member.tables()); if (before != null) required.addAll(before.models().keySet());
            if (!snapshot.models().keySet().equals(required)) throw unavailable(chain);
            if (before != null && (before.snapshotEpoch() != snapshot.snapshotEpoch() || !before.sourcePosition().equals(snapshot.sourcePosition())
                    || before.models().entrySet().stream().anyMatch(entry -> !entry.getValue().equals(snapshot.models().get(entry.getKey()))))) throw unavailable(chain);
            validateSnapshot(session, context, snapshot, facts);
            if (snapshot.equals(before)) {
                provePinClaim(pipeline, session, claimConflict);
                throw new UnchangedPin(true);
            }
            if (held.revision() >= Long.MAX_VALUE - 1) throw unavailable(chain);
            ConsumerOffset old = held.value();
            var progress = new ConsumerOffset(old.pipelineId(), old.perTableSeq(), old.sinkAcked(), old.snapshotCompletedTables(),
                    snapshot.sourcePosition(), snapshot.snapshotEpoch(), old.sinkAckedByTable(), old.progressKind());
            Document row = records.cursor(member, progress, expectedProgressRevision + 1, snapshot,held.writerRun(),held.ringDoneThrough());
            if (SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).replaceOne(session,
                    new Document("_id", records.id(context.key())).append("membershipRevision", membershipRevision)
                            .append("progressRevision", expectedProgressRevision), row).getMatchedCount() != 1) throw unavailable(chain);
            // An exhausted chain fence can still answer a replay above, but cannot publish a new pin.
            if (root == null) chains.replace(session, view.root(), new Document(view.root()));
            // The exact claim write still fences publication in this transaction.
            provePinClaim(pipeline, session, claimConflict);
            return true;
        }));
    }
    private View pinRelations(ClientSession session, String chain, Document root, boolean batchReads,
            MongoSrsReadFacts facts, SrsSnapshotPin snapshot) {
        if (batchReads) {
            var control = MongoSrsChainStore.decode(root, scope, new MongoSrsModelContexts(database, scope));
            facts.prefetchStartupCheckpoint(root, control.checkpoint(), snapshot);
        }
        var state = chains.state(session, root, true, facts);
        if (batchReads) {
            var captured = startProgressRows(session, chain, records.chain(chain));
            if (captured != null && chain.equals(captured.get("root", Document.class).get("_id"))) {
                var members = captured.getList("members", Document.class);
                var cursors = captured.getList("cursors", Document.class);
                if (java.util.stream.Stream.concat(members.stream(), cursors.stream()).allMatch(row ->
                        row.get("_id") instanceof Document id && chain.equals(id.get("chain"))
                                && scope.cacheIncarnation().equals(id.get("cacheIncarnation"))))
                    return validateRelations(chain, root, state, members, () -> cursors);
            }
        }
        return relations(session, chain, root, state);
    }

    private static void provePinClaim(MongoCachePipelineAccess pipeline, ClientSession session, AtomicBoolean claimConflict) {
        try {
            pipeline.proveConsumerClaim(session, true);
        } catch (MongoException failure) {
            claimConflict.set(failure.getCode() == 112 && failure.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
            throw failure;
        }
    }

    private boolean pinTransaction(SrsConsumerKey key, WorkloadClaimFence fence, AtomicBoolean claimConflict,
            java.util.function.BiFunction<ClientSession, java.util.function.Consumer<MongoPipelineProofFacts.PinProof>, Boolean> action) {
        var budget = MongoReadBudget.until(System.nanoTime() + PIN_BUDGET);
        if (pinOperations == null) return executePinTransaction(fence, claimConflict, action, budget);
        return pinOperations.snapshotPin(key, budget.deadlineNanos(),
                original -> executePinTransaction(fence, claimConflict, action, original));
    }

    private boolean executePinTransaction(WorkloadClaimFence fence, AtomicBoolean claimConflict,
            java.util.function.BiFunction<ClientSession, java.util.function.Consumer<MongoPipelineProofFacts.PinProof>, Boolean> action, MongoReadBudget budget) {
        return MongoStructureFailureScope.inspectOnFailure(client, budget, () -> StoreIo.call(() ->
                MongoCachePipelineStateStore.writeTransaction(client, fence.key().resourceId(), budget, () -> {
            requireActive.run();
            var attempts = new AtomicInteger();
            var retry = new AtomicReference<MongoClaimOperations.PinRetry>();
            var checkedRetry = new AtomicBoolean();
            var checkedInitial = new AtomicBoolean();
            try (var session = client.startSession()) {
                if (pinOperations != null) pinOperations.awaitPipelineRenewal(fence.key(), budget);
                for (int connectionAttempt = 0; ; connectionAttempt++) {
                    var options = TransactionOptions.builder().readConcern(TRANSACTION.getReadConcern())
                            .writeConcern(TRANSACTION.getWriteConcern()).timeout(budget.remainingMillis(), TimeUnit.MILLISECONDS).build();
                    try {
                        return session.withTransaction(() -> {
                            int attempt = attempts.getAndIncrement();
                            // The previous transaction has ended on callback retry. Commit retries
                            // keep the original protection until the commit outcome is known.
                            if (pinOperations != null) {
                                pinOperations.releasePinRetry(retry.getAndSet(null));
                                if (claimConflict.getAndSet(false) && checkedRetry.compareAndSet(false, true)) {
                                    pinOperations.awaitPipelineRenewal(fence.key(), budget);
                                    retry.set(protectPinRetry(fence, budget));
                                }
                            }
                            if (attempt > 0) waitForPinRetry(attempt);
                            requireActive.run();
                            MongoStructureFailureScope.beginCallback();
                            if (!runtimeStructure) requireStructure.run();
                            requireActive.run();
                            budget.remainingMillis();
                            boolean result = action.apply(session, proof -> {
                                if (pinOperations != null && !checkedRetry.get() && checkedInitial.compareAndSet(false, true))
                                    retry.set(protectInitialPin(fence, budget, proof));
                            });
                            requireActive.run();
                            budget.remainingMillis();
                            return result;
                        }, options);
                    } catch (com.mongodb.MongoOperationTimeoutException unavailable) {
                        // A pre-command connection timeout can recover within the original pin window.
                        // Never retry an attempted write or an uncertain commit through this path.
                        if (connectionAttempt != 0 || unavailable.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)
                                || !MongoStructureFailureScope.provedNoCommandStarted(client)) throw unavailable;
                        requireActive.run();
                        budget.remainingMillis();
                    }
                }
            } catch (UnchangedPin result) {
                requireActive.run();
                return result.accepted;
            } catch (RuntimeException failure) {
            MongoStructureProof.invalidate(requireStructure);
                requireActive.run();
                throw failure;
            } finally {
                if (pinOperations != null) pinOperations.releasePinRetry(retry.get());
            }
        })));
    }

    private MongoClaimOperations.PinRetry protectInitialPin(WorkloadClaimFence fence, MongoReadBudget budget,
            MongoPipelineProofFacts.PinProof proof) {
        try {
            if (MongoWorkloadClaimStore.nodeBacked(proof.claim())) return null;
            var observed = MongoWorkloadClaimStore.observedReading(proof.claim());
            if (!observed.leased() || !fence.equals(WorkloadClaimFence.from(observed.claim()))) return null;
            // This old snapshot only bounds local scheduling; the final durable claim CAS remains mandatory.
            long leaseDeadline = proof.sampledAtNanos() + observed.leaseRemaining().toNanos();
            return pinOperations.protectPinRetry(fence, budget.deadlineNanos(), leaseDeadline);
        } catch (TapstateException | ArithmeticException unavailable) {
            budget.remainingMillis();
            return null;
        }
    }

    private MongoClaimOperations.PinRetry protectPinRetry(WorkloadClaimFence fence, MongoReadBudget budget) {
        long sampledAt = System.nanoTime();
        try {
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(budget.database(database)));
            var row = claims.readWithRemainingLease(fence.key());
            if (row == null || MongoWorkloadClaimStore.nodeBacked(row)) return null;
            var observed = MongoWorkloadClaimStore.observedReading(row);
            if (!observed.leased() || !fence.equals(WorkloadClaimFence.from(observed.claim()))) return null;
            long leaseDeadline = sampledAt + observed.leaseRemaining().toNanos();
            return pinOperations.protectPinRetry(fence, budget.deadlineNanos(), leaseDeadline);
        } catch (TapstateException failure) {
            // This sample only admits scheduling protection. The original transaction
            // still makes the authoritative scope and lease decision.
            budget.remainingMillis();
            return null;
        }
    }

    private static void waitForPinRetry(int attempt) {
        long ceiling = Math.min(50, 5L << Math.min(attempt - 1, 4));
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(1, ceiling + 1));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "snapshot pin retry interrupted"), null);
        }
    }

    /** A replay or revision mismatch rolls back the early reservation. */
    private static final class UnchangedPin extends RuntimeException {
        private final boolean accepted;
        private UnchangedPin(boolean accepted) {
            super(null, null, false, false);
            this.accepted = accepted;
        }
    }

    private void validateSnapshot(ClientSession session, SrsConsumerContext context, SrsSnapshotPin snapshot) {
        validateSnapshot(session, context, snapshot, new MongoSrsReadFacts(session, database, scope, false));
    }

    private void validateSnapshot(ClientSession session, SrsConsumerContext context, SrsSnapshotPin snapshot,
            Document verifiedChain, Set<SrsModelKey> checkpointModels) {
        if (snapshot == null) return;
        for (var key : snapshot.models().values()) {
            if (!context.key().miningChainId().equals(key.miningChainId()) || !scope.cacheIncarnation().equals(key.cacheIncarnation())) {
                throw unavailable(context.key().miningChainId());
            }
        }
        var keys = Set.copyOf(snapshot.models().values());
        if (!checkpointModels.containsAll(keys)) {
            new MongoSrsModelStore(client, database, scope, requireActive, requireStructure)
                    .requireSnapshotReferences(session, keys, verifiedChain);
        }
    }
    private void validateSnapshot(ClientSession session, SrsConsumerContext context, SrsSnapshotPin snapshot,
            MongoSrsReadFacts facts) {
        if (snapshot == null) return;
        for (var key : snapshot.models().values()) {
            if (!context.key().miningChainId().equals(key.miningChainId()) || !scope.cacheIncarnation().equals(key.cacheIncarnation())) {
                throw unavailable(context.key().miningChainId());
            }
        }
        new MongoSrsModelStore(client, database, scope, requireActive, requireStructure)
                .requireSnapshotReferences(session, Set.copyOf(snapshot.models().values()), facts);
    }
    @Override public Optional<SrsConsumerMembership> attach(SrsConsumerContext context, long expectedRevision, Set<String> tables, WorkloadClaimFence fence) {
        Objects.requireNonNull(context, "context"); requireRevision(expectedRevision); Set<String> selected = Set.copyOf(Objects.requireNonNull(tables, "tables"));
        if (selected.isEmpty() || selected.stream().anyMatch(String::isBlank)) throw new IllegalArgumentException("an attached consumer requires tables");
        String chain = context.key().miningChainId();
        return changingTheConsumer(() -> checkpointTransaction(context.key().pipelineId(), session -> {
            prove(session,context,fence); var view = view(session, chain); var held = view.members().get(context.key().consumerId());
            if (replayed(held, context, expectedRevision, false, selected, fence)) return Optional.of(held);
            long revision = held == null ? 0 : held.revision();
            if (revision != expectedRevision) return Optional.empty();
            return Optional.of(attachInSnapshot(session, context, selected, fence, view, held, revision).member());
        }));
    }

    @Override public Optional<SrsConsumerMembership> attachForStartup(SrsConsumerContext context, Set<String> tables,
            WorkloadClaimFence fence) {
        return attachForStartup(context, tables, fence, false).map(StartupAttachment::member);
    }

    @Override public Optional<StartupAttachment> attachAndPrepareForStartup(SrsConsumerContext context, Set<String> tables,
            WorkloadClaimFence fence) {
        return attachForStartup(context, tables, fence, true);
    }

    private Optional<StartupAttachment> attachForStartup(SrsConsumerContext context, Set<String> tables,
            WorkloadClaimFence fence, boolean prepare) {
        Objects.requireNonNull(context, "context");
        var selected = Set.copyOf(tables);
        if (selected.isEmpty() || selected.stream().anyMatch(String::isBlank))
            throw new IllegalArgumentException("an attached consumer requires tables");
        String chain = context.key().miningChainId();
        var expected = new java.util.concurrent.atomic.AtomicLong(-1);
        return changingTheConsumer(() -> inTransaction(session -> {
            requireStructure.run();
            var captured = startupAttachView(session, context);
            var view = captured.view();
            var held = view.members().get(context.key().consumerId());
            long revision = held == null ? 0 : held.revision();
            long first = expected.compareAndExchange(-1, revision);
            if (first == -1 && held != null && !held.detached() && held.context().equals(context)
                    && held.tables().equals(selected) && held.commandFence().equals(fence))
                return Optional.of(startupAttachment(session, captured, unchangedAttachment(view, held), prepare));
            // Renew the snapshot on callback retry without adopting a newer membership revision.
            boolean replay = first >= 0 && replayed(held, context, first, false, selected, fence);
            if (first >= 0 && first != revision && !replay) return Optional.empty();
            var pipeline = pipelineAccess(context, fence);
            var state = captured.pipelineState();
            if (state == null) state = pipeline.stateForStartupAttach(session);
            else {
                pipeline.validateState(state);
                pipeline.requirePreparedClaim(captured.preparedClaim());
            }
            pipeline.proveConsumerClaim(session, true);
            pipeline.updateState(session, state, new Document(), false);
            var attached = replay ? unchangedAttachment(view, held) : attachInSnapshot(session, context, selected, fence, view, held, revision);
            return Optional.of(startupAttachment(session, captured, attached, prepare));
        }));
    }

    private record AttachedSnapshot(SrsConsumerMembership member, Document root, MongoSrsConsumerRecords.Cursor cursor) {}

    private static AttachedSnapshot unchangedAttachment(View view, SrsConsumerMembership member) {
        return new AttachedSnapshot(member, view.root(), view.cursors().get(member.context().key().consumerId()));
    }

    private StartupAttachment startupAttachment(ClientSession session, StartupAttachView captured, AttachedSnapshot attached,
            boolean prepare) {
        var member = attached.member();
        if (!prepare) return new StartupAttachment(member, Optional.empty());
        var view = captured.view();
        var context = member.context();
        var cursor = attached.cursor();
        validateSnapshot(session, context, cursor.snapshot(), captured.facts());
        var state = attached.root() == view.root() ? view.state()
                : MongoSrsChainStore.controlState(attached.root(), scope, new MongoSrsModelContexts(database, scope));
        var members = new HashMap<>(view.members());
        members.put(context.key().consumerId(), member);
        var topology = new SrsConsumerTopology(scope, state.miningChainId(), state.consumerRevision(), members.values().stream()
                .filter(candidate -> !candidate.detached())
                .sorted(Comparator.comparing(candidate -> candidate.context().key().consumerId())).toList());
        var progress = new SrsConsumerProgress(context, member.revision(), cursor.revision(), cursor.value(), cursor.snapshot(),
                cursor.writerRun(), cursor.ringDoneThrough());
        var snapshot = new StartupSnapshot(state, progress, Optional.empty(), Map.of());
        return new StartupAttachment(member, Optional.of(new StartupPreparation(snapshot, topology)));
    }

    private AttachedSnapshot attachInSnapshot(ClientSession session, SrsConsumerContext context, Set<String> selected,
            WorkloadClaimFence fence, View view, SrsConsumerMembership held, long revision) {
        String chain = context.key().miningChainId();
        if (revision >= Long.MAX_VALUE - 1 || (held != null && (held.context().stateGeneration() > context.stateGeneration()
                || (!held.detached() && !held.context().equals(context))))) throw unavailable(chain);
        var next = new SrsConsumerMembership(context, revision + 1, false, selected, fence);
        var previousCursor = held == null || held.detached() ? null : view.cursors().get(context.key().consumerId());
        ConsumerOffset cursor = previousCursor == null ? records.initial(context.key(), selected) : records.include(previousCursor.value(), selected);
        Document cursorRow = records.cursor(next, cursor, previousCursor == null ? 0 : previousCursor.revision(),
                previousCursor == null ? null : previousCursor.snapshot(), previousCursor == null ? null : previousCursor.writerRun(),
                previousCursor == null ? Map.of() : previousCursor.ringDoneThrough());
        writeMember(session, held, next);
        if (previousCursor == null) SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).insertOne(session, cursorRow);
        else if (SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).replaceOne(session,
                new Document("_id", records.id(context.key())).append("membershipRevision", held.revision()), cursorRow).getMatchedCount() != 1)
            throw unavailable(chain);
        var root = changeRoot(session, view, held == null || held.detached() ? 1 : 0);
        return new AttachedSnapshot(next, root, records.cursor(next, cursorRow));
    }
    @Override public Optional<SrsConsumerMembership> detach(SrsConsumerContext context, long expectedRevision, WorkloadClaimFence fence) {
        Objects.requireNonNull(context, "context"); requireRevision(expectedRevision);
        return changingTheConsumer(() -> checkpointTransaction(context.key().pipelineId(), session -> {
            prove(session, context, fence);
            var held = view(session, context.key().miningChainId());
            var member = held.members().get(context.key().consumerId());
            if (member != null && !member.detached() && member.context().equals(context)
                    && held.cursors().get(context.key().consumerId()).snapshot() != null) {
                var physical = new io.tapstate.spi.store.PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(),
                        context.key().pipelineId(), context.stateGeneration());
                var row = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find(session,
                        new Document("_id", MongoWorkloadClaimStore.id(fence.key()))).first();
                if (row != null && row.containsKey("pipelineRebuild")) {
                    var access = new MongoCachePipelineAccess(client, database, physical, fence, requireActive);
                    MongoCachePipelineRebuilds.requirePriorMarker(row, physical,
                            row.get("pipelinePreparation", Document.class).getString("preparationId"), access.state(session, runtimeStructure));
                    // Only the original explicit rebuild/retirement paths may discard this full's fixed pin.
                    throw fullUnproven(context.key().miningChainId());
                }
            }
            return detachInTransaction(session, context, expectedRevision, fence);
        }));
    }
    @Override public Optional<SrsConsumerMembership> detachForRebuild(io.tapstate.spi.store.PipelineRebuildPreparation original,
            SrsConsumerMembership membership,WorkloadClaimFence currentNode) {
        MongoPipelineStateRebuilds.requireOriginalPending(original);Objects.requireNonNull(membership,"membership");Objects.requireNonNull(currentNode,"currentNode");
        var request=original.request();var physical=request.originalScope();var context=membership.context();
        if(!scope.equals(new CacheBusinessScope(physical.clusterId(),physical.bindingVersion(),physical.cacheIncarnation()))
                || !physical.pipelineId().equals(context.key().pipelineId()) || physical.stateGeneration()!=context.stateGeneration())throw fenced();
        return changingTheConsumer(() -> checkpointTransaction(context.key().pipelineId(), session -> {
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
        }));
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
        return changingTheConsumer(() -> checkpointTransaction(context.key().pipelineId(), session -> {
            MongoPipelineRetirements.requireOriginalActive(original);MongoCachePipelinePreparations.proveNode(session,database,scope,currentNode);
            var held=view(session,context.key().miningChainId()).members().get(context.key().consumerId());
            if(replayed(held,context,membership.revision(),true,Set.of(),fixed.claim()))return Optional.of(held);
            var access=new MongoCachePipelineAccess(client,database,physical,null,requireActive);access.verifyStateStructure();var state=access.state(session);
            if(state.getLong("epoch")!=fixed.epoch())throw MongoCachePipelinePreparations.rebuild(physical);
            var expected=WorkloadClaimDocuments.live(fixed.claim());expected.remove("$expr");
            expected.append("pipelineScope",MongoPipelinePreparationFacts.scope(physical)).append("pipelinePreparation.phase","PREPARED");
            if(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(session,expected,new Document("$inc",new Document("fencedConsumerWrites",1L))).getMatchedCount()!=1)throw fenced();
            access.updateState(session,state,new Document(),false);return detachInTransaction(session,context,membership.revision(),fixed.claim());
        }));
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
        if (before == null) {
            SystemCollections.SRS_CONSUMERS.on(database).insertOne(session,row);
        }
        else if (SystemCollections.SRS_CONSUMERS.on(database).replaceOne(session,
                new Document("_id", records.id(before.context().key())).append("revision", before.revision()), row).getMatchedCount() != 1) {
            throw unavailable(next.context().key().miningChainId());
        }
    }
    private Document changeRoot(ClientSession session, View view, long countDelta) {
        long count = view.state().consumerCount(); long revision = view.state().consumerRevision(); String chain = view.state().miningChainId();
        if (revision >= Long.MAX_VALUE - 1 || count + countDelta < 0 || count + countDelta >= Long.MAX_VALUE) throw unavailable(chain);
        Document next = new Document(view.root()).append("consumerCount", count + countDelta).append("consumerRevision", revision + 1);
        chains.replace(session, view.root(), next);
        return next;
    }
    private record Own(Document root, SrsChainState state, SrsConsumerMembership member, MongoSrsConsumerRecords.Cursor cursor) {}

    /** Hot progress never depends on another consumer's private cursor. Closure enumeration stays in control operations. */
    private Own own(ClientSession session,SrsConsumerContext context) {
        return own(session, context, false);
    }

    private Own own(ClientSession session, SrsConsumerContext context, boolean businessChecked) {
        return own(session, context, businessChecked, new MongoSrsReadFacts(session, database, scope, businessChecked));
    }

    private Own own(ClientSession session, SrsConsumerContext context, boolean businessChecked, MongoSrsReadFacts facts) {
        return own(session, context, businessChecked, facts, false);
    }

    private Own own(ClientSession session, SrsConsumerContext context, boolean businessChecked, MongoSrsReadFacts facts,
            boolean forObservation) {
        String chain = context.key().miningChainId();
        var root = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_CHAINS, new Document("_id", chain));
        if (root == null) throw unavailable(chain);
        var state = chains.state(session, root, businessChecked, facts); var id = new Document("_id",records.id(context.key()));
        var row = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_CONSUMERS, id);
        if (row == null) {
            // Prove absence through the complete dependency view before allowing a new caller to prepare.
            view(session,chain); return null;
        }
        var member = records.membership(row);
        if (!member.context().key().equals(context.key()) || state.consumerRevision() < member.revision()) throw unavailable(chain);
        if (member.context().stateGeneration() != context.stateGeneration()) {
            if (forObservation && !member.detached()) throw MongoCachePipelineAccess.claimFenced();
            if (!forObservation || member.context().stateGeneration() > context.stateGeneration()) throw unavailable(chain);
        }
        var cursor = MongoSrsReadFacts.find(facts, session, database, SystemCollections.CACHE_SRS_CONSUMER_OFFSETS, id);
        if (member.detached()) { if (cursor != null) throw unavailable(chain); return new Own(root,state,member,null); }
        if (state.consumerCount() < 1) throw unavailable(chain);
        return new Own(root,state,member,records.cursor(member,cursor));
    }
    private Own requireOwn(ClientSession session,SrsConsumerContext context) {
        return requireOwn(session, context, false);
    }

    private Own requireOwn(ClientSession session, SrsConsumerContext context, boolean businessChecked) {
        var own = own(session, context, businessChecked);
        if (own == null || own.member().detached()) throw unavailable(context.key().miningChainId());
        validateSnapshot(session,context,own.cursor().snapshot());
        return own;
    }

    private Own requireStartupOwn(ClientSession session, SrsConsumerContext context, boolean businessChecked) {
        var facts = new MongoSrsReadFacts(session, database, scope, businessChecked);
        if (businessChecked) {
            String chain = context.key().miningChainId();
            facts.prefetchArrival(chain, records.id(context.key()));
            var root = MongoSrsReadFacts.find(facts, session, database, SystemCollections.SRS_CHAINS, new Document("_id", chain));
            if (root != null) {
                var control = MongoSrsChainStore.decode(root, scope, new MongoSrsModelContexts(database, scope));
                facts.prefetchStartupCheckpoint(root, control.checkpoint());
            }
        }
        var own = own(session, context, businessChecked, facts);
        if (own == null || own.member().detached()) throw unavailable(context.key().miningChainId());
        validateSnapshot(session, context, own.cursor().snapshot(), facts);
        return own;
    }

    private SrsModelKey singleSnapshotModel(ClientSession session, Own own) {
        var snapshot = own.cursor().snapshot();
        if (snapshot == null) return null;
        if (snapshot.models().size() != 1) {
            validateSnapshot(session, own.member().context(), snapshot);
            return null;
        }
        var key = snapshot.models().values().iterator().next();
        if (!own.member().context().key().miningChainId().equals(key.miningChainId())
                || !scope.cacheIncarnation().equals(key.cacheIncarnation())) throw unavailable(own.member().context().key().miningChainId());
        return key;
    }

    private Own requireIncrementalOwn(ClientSession session, SrsConsumerContext context) {
        var own = own(session, context);
        if (own == null || own.member().detached()) throw unavailable(context.key().miningChainId());
        var pin = singleSnapshotModel(session, own);
        if (pin != null) {
            new MongoSrsModelStore(client, database, scope, requireActive, requireStructure)
                    .requireIncrementalSnapshot(session, own.root(), pin);
        }
        return own;
    }

    record View(Document root, SrsChainState state, Map<String, SrsConsumerMembership> members,
                        Map<String, MongoSrsConsumerRecords.Cursor> cursors, List<Document> membershipRows, List<Document> cursorRows) {}

    private View readView(ClientSession session,String chain){
        Document root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",chain)).first();
        if(root==null)throw unavailable(chain);
        return relations(session,chain,root,chains.state(session,root,true));
    }

    private View view(ClientSession session, String chain) {
        return view(session, chain, IoError.SRS_CONSUMER_UNAVAILABLE);
    }

    private View view(ClientSession session, String chain, IoError missingRoot) {
        Document root = SystemCollections.SRS_CHAINS.on(database).find(session, new Document("_id", chain)).first();
        if (root == null) throw new TapstateException(missingRoot, Map.of("chain", chain), null);
        SrsChainState state = chains.state(session, root);
        return relations(session,chain,root,state);
    }
    /** Isolated control receipts can prove old detached relationships, never model/event continuation. */
    private View administrativeView(ClientSession session,String chain) {
        var root=SystemCollections.SRS_CHAINS.on(database).find(session,new Document("_id",chain)).first();
        if(root==null)throw unavailable(chain);
        var isolation=MongoSrsRecoveryControls.read(root);
        if(isolation.isEmpty())return relations(session,chain,root,chains.state(session,root));
        var state=MongoSrsChainStore.controlState(root,scope,new MongoSrsModelContexts(database,scope));
        var view=relations(session,chain,root,state);
        MongoSrsRecoveryControls.verify(root,view.membershipRows(),view.cursorRows());
        if(isolation.orElseThrow().phase()==MongoSrsRecoveryControls.Phase.RETIRED && (state.consumerCount()!=0 || !view.cursors().isEmpty()))throw unavailable(chain);
        return view;
    }
    /** Read-only infrastructure inspection; the caller keeps this session's original domain guard. */
    Document isolatedRootForRingInitialization(ClientSession session,String chain) {
        var view=administrativeView(session,chain);
        if(MongoSrsRecoveryControls.read(view.root()).isEmpty())throw unavailable(chain);
        var contexts=new MongoSrsModelContexts(database,scope);
        requireRecoveryCoordinates(view,contexts);
        var rings=contexts.heads(view.root()).stream().map(head -> head.getString("ringName")).collect(java.util.stream.Collectors.toSet());
        requireRecoveryBounds(view,contexts,MongoSrsLogStore.inspectRetainedBounds(session,database,rings).byRing());
        return view.root();
    }
    private record StartupAttachView(View view, Document pipelineState, Document preparedClaim, MongoSrsReadFacts facts) {}

    private StartupAttachView startupAttachView(ClientSession session, SrsConsumerContext context) {
        String chain = context.key().miningChainId();
        String pipeline = context.key().pipelineId();
        var claimId = MongoWorkloadClaimStore.id(new io.tapstate.spi.store.WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, pipeline));
        var captured = startProgressRows(session, chain, records.chain(chain), List.of(
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_IDENTITY, "cache", "cluster"),
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_SCHEMA_MIGRATIONS, "schema", "business"),
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_PIPELINE_STATE, "pipeline", pipeline),
                MongoCaptureStartupRows.lookup(SystemCollections.CACHE_WORKLOAD_CLAIMS, "claim", claimId)));
        Document root = captured == null ? null : captured.get("root", Document.class);
        if (root == null || !chain.equals(root.get("_id"))
                || MongoCaptureStartupRows.get(captured, "cache") == null
                || MongoCaptureStartupRows.get(captured, "schema") == null
                || MongoCaptureStartupRows.ambiguous(captured, "cache", "cluster")
                || MongoCaptureStartupRows.ambiguous(captured, "schema", "business")
                || MongoCaptureStartupRows.ambiguous(captured, "pipeline", pipeline)
                || MongoCaptureStartupRows.ambiguous(captured, "claim", claimId)) return startupAttachFallback(session, chain);
        MongoCacheBusinessIdentity.requireObservedReady(MongoCaptureStartupRows.get(captured, "cache"),
                MongoCaptureStartupRows.get(captured, "schema"), scope);
        var members = captured.getList("members", Document.class);
        var cursors = captured.getList("cursors", Document.class);
        if (java.util.stream.Stream.concat(members.stream(), cursors.stream()).anyMatch(row ->
                !(row.get("_id") instanceof Document id) || !chain.equals(id.get("chain"))
                        || !scope.cacheIncarnation().equals(id.get("cacheIncarnation")))) return startupAttachFallback(session, chain);
        var facts = new MongoSrsReadFacts(session, database, scope, root, true);
        var state = chains.startupState(session, root, facts);
        return new StartupAttachView(validateRelations(chain, root, state, members, () -> cursors),
                MongoCaptureStartupRows.get(captured, "pipeline"), MongoCaptureStartupRows.get(captured, "claim"), facts);
    }

    private StartupAttachView startupAttachFallback(ClientSession session, String chain) {
        validateStore(session, true);
        var view = view(session, chain);
        return new StartupAttachView(view, null, null, new MongoSrsReadFacts(session, database, scope, view.root(), true));
    }

    private View relations(ClientSession session,String chain,Document root,SrsChainState state) {
        var membershipRows = SystemCollections.SRS_CONSUMERS.on(database).find(session, records.chain(chain)).into(new ArrayList<>());
        return validateRelations(chain, root, state, membershipRows, () ->
                SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(database).find(session, records.chain(chain)).into(new ArrayList<>()));
    }

    View relations(String chain, Document root, SrsChainState state, Iterator<Document> memberRows, Iterator<Document> offsetRows) {
        var membershipRows = new ArrayList<Document>();
        memberRows.forEachRemaining(membershipRows::add);
        return validateRelations(chain, root, state, membershipRows, () -> {
            var cursorRows = new ArrayList<Document>();
            offsetRows.forEachRemaining(cursorRows::add);
            return cursorRows;
        });
    }

    private View validateRelations(String chain, Document root, SrsChainState state,
            List<Document> membershipRows, Supplier<List<Document>> readCursors) {
        var members = new HashMap<String, SrsConsumerMembership>();
        long revisions = 0;
        for (var row : membershipRows) {
            SrsConsumerMembership member = records.membership(row);
            if (!chain.equals(member.context().key().miningChainId()) || members.put(member.context().key().consumerId(), member) != null)
                throw unavailable(chain);
            try { revisions = Math.addExact(revisions, member.revision()); }
            catch (ArithmeticException invalid) { throw unavailable(chain); }
        }
        var cursors = new HashMap<String, MongoSrsConsumerRecords.Cursor>();
        var cursorRows = readCursors.get();
        for (var row : cursorRows) {
            if (!(row.get("_id") instanceof Document id) || !(id.get("consumer") instanceof String consumer)) throw unavailable(chain);
            SrsConsumerMembership member = members.get(consumer);
            if (member == null || member.detached() || cursors.put(consumer, records.cursor(member, row)) != null) throw unavailable(chain);
        }
        long active = members.values().stream().filter(member -> !member.detached()).count();
        if (active != state.consumerCount() || cursors.size() != active || revisions != state.consumerRevision()) throw unavailable(chain);
        return new View(root, state, members, cursors, membershipRows, cursorRows);
    }
    private void prove(ClientSession session, SrsConsumerContext context, WorkloadClaimFence fence) {
        var pipeline = pipelineAccess(context, fence);
        Document current = pipeline.state(session, true);
        pipeline.proveConsumerClaim(session, true);
        pipeline.updateState(session, current, new Document(), false);
    }

    private static void proveConsumerWrite(ClientSession session, MongoCachePipelineAccess pipeline, Document state) {
        pipeline.proveConsumerClaim(session, true);
        pipeline.updateState(session, state, new Document(), false);
    }

    private void proveProgress(ClientSession session, SrsConsumerContext context, WorkloadClaimFence fence) {
        var pipeline = pipelineAccess(context, fence);
        Document current = pipeline.stateForWriterProgress(session, true);
        // Claim replacement and execution advancement also write this checkpoint fence.
        pipeline.updateState(session, current, new Document(), false);
    }

    private MongoCachePipelineAccess pipelineAccess(SrsConsumerContext context, WorkloadClaimFence fence) {
        Objects.requireNonNull(fence, "originalPipelineFence");
        if (fence.executionGeneration() < 1 || fence.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !scope.clusterId().equals(fence.key().clusterId())
                || !context.key().pipelineId().equals(fence.key().resourceId())) throw fenced();
        var physical = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(),
                context.key().pipelineId(), context.stateGeneration());
        return new MongoCachePipelineAccess(client, database, physical, fence, requireActive);
    }
    private <T> T changingTheConsumer(Supplier<T> change) {
        readLease.drop();
        cursorLease.drop();
        try { return change.get(); }
        finally {
            readLease.drop();
            cursorLease.drop();
        }
    }

    private <T> T checkpointTransaction(String pipeline, Function<ClientSession, T> action) {
        return checkpointTransaction(pipeline, false, action);
    }

    private <T> T checkpointTransaction(String pipeline, boolean structureChecked, Function<ClientSession, T> action) {
        return MongoCachePipelineStateStore.writeTransaction(client, pipeline, () -> transaction(structureChecked, action));
    }

    private <T> T checkpointTransaction(java.util.Collection<String> pipelines, Function<ClientSession, T> action) {
        return MongoCachePipelineStateStore.writeTransaction(client, pipelines, () -> transaction(action));
    }

    private <T> T checkpointInTransaction(String pipeline, Function<ClientSession, T> action) {
        return MongoCachePipelineStateStore.writeTransaction(client, pipeline, () -> inTransaction(action));
    }

    private <T> T transaction(Function<ClientSession, T> action) {
        return transaction(false, action);
    }

    private <T> T transaction(boolean structureChecked, Function<ClientSession, T> action) {
        return inTransaction(session -> {
            validateStore(session, structureChecked);
            return action.apply(session);
        });
    }

    private void validateStore(ClientSession session, boolean structureChecked) {
        if (!structureChecked) requireStructure.run();
        requireActive.run();
        new MongoCacheBusinessIdentity(database, scope).requireReady(session);
    }

    private <T> T readSnapshot(Function<ClientSession,T> read) {
        return readSnapshot(read, null);
    }

    private <T> T readSnapshot(Function<ClientSession, T> read, Function<ClientSession, Optional<T>> tryCombined) {
        return StoreIo.call(()->{
            try{
                requireActive.run();if(!MongoReadBudget.supportsSnapshotReads(client))return transaction(read);
                try(var session=client.startSession(com.mongodb.ClientSessionOptions.builder().snapshot(true).build())){
                    requireStructure.run();
                    requireActive.run();
                    var combined = tryCombined == null ? Optional.<T>empty() : tryCombined.apply(session);
                    if (combined.isPresent()) {
                        requireActive.run();
                        return combined.orElseThrow();
                    }
                    new MongoCacheBusinessIdentity(database, scope).requireReady(session);
                    T result = read.apply(session);
                    requireActive.run();
                    return result;
                }
            }catch(RuntimeException failure){
                chains.invalidateControlProofs();MongoStructureProof.invalidate(requireStructure);
                try{requireActive.run();}catch(RuntimeException fenced){if(fenced!=failure)failure.addSuppressed(fenced);}
                throw failure;
            }
        });
    }
    private <T> T inTransaction(Function<ClientSession, T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    return session.withTransaction(() -> {
                        MongoStructureFailureScope.beginCallback();
                        requireActive.run();
                        T result = action.apply(session); requireActive.run(); return result;
                    }, TRANSACTION);
                }
            } catch (RuntimeException failure) {
                chains.invalidateControlProofs();MongoStructureProof.invalidate(requireStructure);
                try { requireActive.run(); }
                catch (RuntimeException fence) {
                    if (fence != failure) failure.addSuppressed(fence);
                }
                throw failure;
            }
        }));
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
