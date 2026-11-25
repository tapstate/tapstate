package io.tapstate.spi.store;

import java.util.Optional;
import java.util.Map;
import java.util.List;
import java.util.Set;

/** Logged-capture membership is independent from each consumer's cursor and the source checkpoint. */
public interface SrsConsumerStore {
    SrsConsumerStore guardedBy(Runnable requireActive);
    /** Reads the complete original control closure without loading lost payloads or granting recovery authority. */
    default SharedSrsRecoverySnapshot inspectSharedRecovery(CapturePreparation previous) {
        throw new io.tapstate.core.common.TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain",previous.chainId()),null);
    }
    /** Loads coherent original-generation relations, including detach markers, for lifecycle retirement only. */
    default List<SrsConsumerMembership> forPipeline(PipelineIntentScope original) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);
    }

    Optional<SrsConsumerMembership> find(SrsConsumerKey key);
    /** Bounded exact original relation only; no scan of other consumers, cursors or model bodies. */
    default boolean attached(SrsConsumerRun original,String table){throw new io.tapstate.core.common.TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain",original.miningChainId()),null);}
    SrsConsumerTopology attached(String miningChainId);
    record AttachedChain(SrsConsumerTopology topology, SrsChainState chain) {}

    record PlanningProgress(SrsChainState chain, List<SrsConsumerProgress> consumers) {
        public PlanningProgress { consumers = List.copyOf(consumers); }
    }

    /**
     * Predicts a start using only the requested source consumers at the original pipeline generation.
     * Native stores verify shared control and cursor identities in one snapshot without loading peer
     * progress payloads. Missing retained history still refuses the read. Legacy facets compose reads.
     * This result grants no capture, mutation or cleanup authority.
     */
    default Optional<PlanningProgress> planningProgress(PipelineIntentScope original, String chain, Set<String> sourceIds) {
        var held = attachedWithChain(chain);
        if (!held.topology().scope().equals(new CacheBusinessScope(original.clusterId(),original.bindingVersion(),original.cacheIncarnation()))
                || !chain.equals(held.chain().miningChainId()))
            throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);
        var selected = new java.util.ArrayList<SrsConsumerProgress>();
        for (var member : held.topology().consumers()) {
            if (!member.context().key().pipelineId().equals(original.pipelineId())
                    || !sourceIds.contains(member.context().key().sourceId())) continue;
            if (member.context().stateGeneration() != original.stateGeneration())
                throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
            selected.add(progress(member.context()).orElseThrow(() -> new io.tapstate.core.common.TapstateException(
                    IoError.SRS_CONSUMER_UNAVAILABLE, Map.of("chain", chain), null)));
        }
        return Optional.of(new PlanningProgress(held.chain(), selected));
    }

    /** Predicts only this start's load mode; complete chain integrity is verified during startup. */
    default Optional<PlanningProgress> startLoadProgress(PipelineIntentScope original, String chain, Set<String> sourceIds) {
        return planningProgress(original, chain, sourceIds);
    }

    default AttachedChain attachedWithChain(String chain) {
        throw new UnsupportedOperationException("combined Capture topology is not supported");
    }

    /** Legacy facets compose their reads; native stores override this with one validated snapshot. */
    default AttachedChain attachedWithChain(String chain, SrsChainStore chains) {
        var topology = attached(chain);
        var state = chains.load(chain).orElseThrow(() -> new io.tapstate.core.common.TapstateException(
                IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", chain), null));
        return new AttachedChain(topology, state);
    }

    /** Retention diagnoses a missing checkpoint before inspecting its consumer relations. */
    default AttachedChain retentionSnapshot(String chain, SrsChainStore chains) {
        var state = chains.load(chain).orElseThrow(() -> new io.tapstate.core.common.TapstateException(
                IoError.SRS_CHECKPOINT_UNPROVEN, Map.of("chain", chain), null));
        return new AttachedChain(attached(chain), state);
    }

    /** Native stores return confirmed floors from one snapshot; legacy facets abandon a changed relation. */
    default Optional<Map<String, Long>> retentionFloors(String chain, SrsChainStore chains) {
        var attached = retentionSnapshot(chain, chains);
        var serving = attached.chain().servingTables();
        var floors = new java.util.TreeMap<String, Long>();
        for (var member : attached.topology().consumers()) {
            if (java.util.Collections.disjoint(member.tables(), serving)) continue;
            var held = progress(member.context());
            if (held.isEmpty() || held.orElseThrow().membershipRevision() != member.revision()) return Optional.empty();
            for (String table : member.tables()) if (serving.contains(table))
                floors.merge(table, held.orElseThrow().ringDoneThrough().getOrDefault(table, -1L), Math::min);
        }
        return Optional.of(Map.copyOf(floors));
    }

    record ControlSnapshot(SrsConsumerTopology topology,SrsChainState chain,java.util.List<ConsumerOffset> cursors){
        public ControlSnapshot{cursors=java.util.List.copyOf(cursors);}
    }
    default boolean supportsControlSnapshot(){return false;}
    /** Complete topology, checkpoint and current progress from one original storage snapshot. */
    default Optional<ControlSnapshot> controlSnapshot(String chain){throw new UnsupportedOperationException();}

    record RetentionConsumer(SrsConsumerMembership membership, Map<String, Long> ringDoneThrough) {
        public RetentionConsumer { ringDoneThrough = Map.copyOf(ringDoneThrough); }
    }

    record RetentionSnapshot(SrsChainState chain, List<RetentionConsumer> consumers) {
        public RetentionSnapshot { consumers = List.copyOf(consumers); }
    }

    /**
     * Reads the chain and its consumers' confirmed retention floors together. Empty selects the
     * ordinary reads. The returned floors are proposals for a separately fenced trim.
     */
    default Optional<RetentionSnapshot> retentionSnapshot(String chain) { return Optional.empty(); }

    record StartupArrivals(SrsConsumerProgress progress, Optional<Map<String, SrsLogBounds>> bounds) {}

    /** Reads original progress and the boundaries for its missing rings from one snapshot. */
    default Optional<StartupArrivals> startupArrivals(SrsConsumerContext context, Map<String, String> ringsByTable) {
        throw new UnsupportedOperationException("Capture startup sampling is not supported");
    }

    record StartupSnapshot(SrsChainState chain, SrsConsumerProgress progress,
            Optional<Map<String, SrsModel>> published, Map<String, SrsLogBounds> bounds) {
        public StartupSnapshot {
            published = published.map(Map::copyOf);
            bounds = Map.copyOf(bounds);
        }
    }

    /**
     * Reads the chain and original consumer in one snapshot. A nonempty selection also reads the
     * published models and the boundaries of their missing arrivals. This grants no write authority.
     */
    default Optional<StartupSnapshot> startupSnapshot(SrsConsumerContext context, Map<String, String> ringsByTable) {
        throw new UnsupportedOperationException("Capture startup sampling is not supported");
    }

    /** The same pre-open read supplies this consumer's progress and the shared table demand. */
    record StartupPreparation(StartupSnapshot snapshot, SrsConsumerTopology topology) {}

    default Optional<StartupPreparation> startupPreparation(SrsConsumerContext context, boolean verifyControlClosure) {
        return startupSnapshot(context, Map.of(), verifyControlClosure).map(snapshot ->
                new StartupPreparation(snapshot, attachedWithChain(context.key().miningChainId()).topology()));
    }

    record StartupAttachment(SrsConsumerMembership member, Optional<StartupPreparation> preparation) {}

    /** A store may return the validated topology and retained progress from its committed attach. */
    default Optional<StartupAttachment> attachAndPrepareForStartup(SrsConsumerContext context, Set<String> tables,
            WorkloadClaimFence fence) {
        return attachForStartup(context, tables, fence).map(member -> new StartupAttachment(member, Optional.empty()));
    }

    /** The first source on a shared chain also verifies its complete membership control closure. */
    default Optional<StartupSnapshot> startupSnapshot(SrsConsumerContext context, Map<String, String> ringsByTable,
            boolean verifyControlClosure) {
        return startupSnapshot(context, ringsByTable);
    }

    /** The exact progress committed by an admitted startup's conditional write. */
    default Optional<SrsConsumerProgress> startReadingAfterAndGet(SrsConsumerContext context, long membershipRevision,
            long expectedProgressRevision, Map<String, Long> sequences, WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("Capture arrival receipt is not supported");
    }

    Optional<ConsumerOffset> cursor(SrsConsumerContext context);

    /** Reads only this consumer for observation; a detached predecessor supplies no progress. */
    default Optional<ConsumerOffset> observationCursor(SrsConsumerContext context) { return cursor(context); }

    Optional<SrsConsumerProgress> progress(SrsConsumerContext context);

    /** Reads the current consumer progress and validates any retained snapshot pin for startup. */
    default Optional<SrsConsumerProgress> snapshotResumeProgress(SrsConsumerContext context) {
        return progress(context);
    }

    /** Stages one unused physical chain from an exact earlier consumer ACK under the replacement execution. */
    default Optional<SrsConsumerMembership> prepareConnectorHandoff(SrsConsumerProgress original,
            SrsConsumerContext candidate, WorkloadClaimFence candidateCapture, WorkloadClaimFence currentPipeline,
            java.time.Instant preparedAt) {
        throw new UnsupportedOperationException("connector handoff is not supported");
    }

    /** Reopens a seeded candidate before its first source checkpoint, retaining its original consumer proof. */
    default Optional<SrsChainState> reopenConnectorHandoffEpoch(SrsConsumerProgress original,
            SrsConsumerContext candidate, WorkloadClaimFence currentPipeline, WorkloadClaimFence candidateCapture,
            long expectedEpoch, Set<String> tables) {
        throw new UnsupportedOperationException("connector handoff epoch recovery is not supported");
    }

    /** Pins the candidate models and retained load completion before any candidate Pipeline writer runs. */
    default boolean completeConnectorHandoff(SrsConsumerProgress original, SrsConsumerContext candidate,
            long membershipRevision, long expectedProgressRevision, SrsSnapshotPin models,
            WorkloadClaimFence currentPipeline) {
        throw new UnsupportedOperationException("connector handoff is not supported");
    }

    /** An implementation may validate one event's original consumer and model in one guarded read. */
    default boolean supportsReadEventVerification() { return false; }

    /** Empty means the original consumer is unavailable; model and fence failures retain their errors. */
    default Optional<SrsModel> verifyReadEvent(SrsConsumerRun run, String table, String ring, long sequence,
            long epoch, long version, WorkloadClaimFence capture) {
        throw new UnsupportedOperationException("read event verification is not supported");
    }

    record ReadEvent(long sequence, long epoch, long version, WorkloadClaimFence capture) {}

    default boolean supportsReadBatchVerification() { return false; }

    /** Verifies every event against one view of the original consumer and its models. */
    default boolean verifyReadEvents(SrsConsumerRun run, String table, String ring, java.util.List<ReadEvent> events) {
        throw new UnsupportedOperationException("read batch verification is not supported");
    }

    /** Freezes the original current view and one store-issued completed CONFIG full. */
    default SrsFreshFullPreparation inspectFreshFull(CompletedPipelineFull full, SrsConsumerRun original, long expectedProgressRevision,
            WorkloadClaimFence originalNode, WorkloadClaimFence originalCapture) {
        throw new io.tapstate.core.common.TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,
                Map.of("chain", original.miningChainId()), null);
    }

    /**
     * Pins the original snapshot under one fixed membership, progress revision and Pipeline claim.
     * An identical held pin is success without returning or adopting a newer progress revision.
     * Existing references, row order and replay anchor cannot be replaced; new selected tables may add references.
     */
    boolean pinSnapshot(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision,
                        SrsSnapshotPin snapshot, WorkloadClaimFence originalPipelineFence);


    /** Begins one immutable writer roster per original execution without resetting retained sink progress. */
    boolean beginWriterRun(SrsConsumerContext context, long membershipRevision, String runId,
                           Map<String,List<String>> expectedWritersByTable, WorkloadClaimFence originalPipelineFence);

    /** Monotonically folds one original writer's report and its confirmed cursor in the same cache transaction. */
    boolean advanceWriter(SrsConsumerContext context, long membershipRevision, String runId, String table, String writerId,
                          WriterProgress progress, WorkloadClaimFence originalPipelineFence);

    /** Returns one ordered outcome per original report after its owning store has committed. */
    default List<WriterProgressOutcome> advanceWriters(SrsConsumerContext context, long membershipRevision, String runId,
            List<WriterProgressUpdate> reports, WorkloadClaimFence fence) {
        var outcomes = new java.util.ArrayList<WriterProgressOutcome>(reports.size());
        boolean stopped = false;
        for (var report : reports) {
            boolean committed = !stopped && advanceWriter(context, membershipRevision, runId, report.table(), report.writerId(), report.progress(), fence);
            outcomes.add(new WriterProgressOutcome(committed, null));
            if (!committed) stopped = true;
        }
        return List.copyOf(outcomes);
    }

    /**
     * Saves the original first arrival and its initial read cursor together. A retained confirmed place is never raised by a later arrival.
     * A held place covering the requested floor succeeds without adopting a later progress revision; a new place uses only the original expected revision.
     */
    boolean startReadingAfter(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision, String table, long sequence,
                              WorkloadClaimFence originalPipelineFence);

    /**
     * Records the supplied per-table first arrivals in one original-claim transaction and progress CAS.
     * Every table keeps its separately sampled floor. Existing covering floors are unchanged; adding any
     * missing floor requires the original expected revision. No subset is written on refusal.
     */
    boolean startReadingAfter(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision,
                              Map<String, Long> sequences, WorkloadClaimFence originalPipelineFence);

    /** Advances only this consumer's read cursor; it cannot acknowledge a sink or advance the source checkpoint. */
    boolean advanceReadSequence(SrsConsumerContext context, long membershipRevision, String table, long sequence,
                                WorkloadClaimFence originalPipelineFence);

    /** Uses the original expected relation revision and Pipeline claim; existing progress is preserved. */
    /** Reads or attaches the original retained consumer under this Start's execution fence. */
    default Optional<SrsConsumerMembership> attachForStartup(SrsConsumerContext context, Set<String> tables,
            WorkloadClaimFence fence) {
        var held = find(context.key());
        return held.filter(member -> !member.detached() && member.context().equals(context)
                && member.tables().equals(tables) && member.commandFence().equals(fence))
                .or(() -> attach(context, held.map(SrsConsumerMembership::revision).orElse(0L), tables, fence));
    }

    Optional<SrsConsumerMembership> attach(SrsConsumerContext context, long expectedRevision, Set<String> tables,
                                          WorkloadClaimFence originalPipelineFence);

    /** A confirmed original rebuild may detach its retired consumer under the current node session. */
    default Optional<SrsConsumerMembership> detachForRebuild(PipelineRebuildPreparation original,SrsConsumerMembership membership,
            WorkloadClaimFence currentNode) {
        return detach(membership.context(),membership.revision(),original.request().originalClaim());
    }

    /** A fixed verified deletion step retires only this original physical generation's relation. */
    default Optional<SrsConsumerMembership> detachForRetirement(PipelineRetirementPreparation original,SrsConsumerMembership membership,
            WorkloadClaimFence currentNode,Optional<PipelineRetirementRequest.CacheOriginal> isolated) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);
    }

    /** Removes only this cursor and leaves a revision marker so a late original attach cannot revive it. */
    Optional<SrsConsumerMembership> detach(SrsConsumerContext context, long expectedRevision,
                                          WorkloadClaimFence originalPipelineFence);
}
