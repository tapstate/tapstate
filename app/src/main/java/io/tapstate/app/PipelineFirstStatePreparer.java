package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelinePreparationReceipt;
import io.tapstate.spi.store.PipelineStatePreparations;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** A controlled post-commit preparation of one captured physical identity, never ordinary convergence. */
final class PipelineFirstStatePreparer {
    private final CacheBusinessScope business;
    private final PipelineStatePreparations configuration;
    private final PipelineCachePreparations cache;
    private final Supplier<Optional<WorkloadClaimFence>> currentNode;
    private final Runnable requireActive;
    private final Clock clock;
    private final Duration ttl;

    PipelineFirstStatePreparer(CacheBusinessScope business, PipelineStatePreparations configuration,
            PipelineCachePreparations cache, Supplier<Optional<WorkloadClaimFence>> currentNode,
            Runnable requireActive, Clock clock, Duration ttl) {
        this.business = Objects.requireNonNull(business, "business");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        this.configuration = Objects.requireNonNull(configuration, "configuration").guardedBy(requireActive);
        this.cache = Objects.requireNonNull(cache, "cache");
        this.currentNode = Objects.requireNonNull(currentNode, "currentNode");
        this.clock = Objects.requireNonNull(clock, "clock"); this.ttl = Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero() || ttl.toMillis() < 1) throw new IllegalArgumentException("preparation ttl must be at least one millisecond");
    }

    PipelinePreparation prepare(PipelineIntentScope original) {
        Objects.requireNonNull(original, "original");
        if (!business.equals(new CacheBusinessScope(original.clusterId(), original.bindingVersion(), original.cacheIncarnation()))) throw fenced();
        requireActive.run();
        PipelinePreparation facts = configuration.read(original.pipelineId()).orElseThrow(PipelineFirstStatePreparer::fenced);
        requireOriginal(original, facts);
        requireActive.run();
        if (facts.phase() == PipelinePreparation.Phase.COMPLETE) return facts;
        WorkloadClaimFence node = currentNode.get().orElseThrow(PipelineFirstStatePreparer::claimFenced);
        requireNode(node);
        requireActive.run();
        if (facts.phase() == PipelinePreparation.Phase.UNASSIGNED) {
            var issued = configuration.issueFirstCreate(original, node);
            requireActive.run();
            if (issued instanceof FirstPipelineCreationResult.Issued winner) {
                facts = winner.creation().preparation();
                requireOriginal(original, facts);
                if (facts.phase() != PipelinePreparation.Phase.ISSUED || !node.equals(facts.issuerNode())) throw fenced();
                return finishAttempt(original, facts, () -> cache.reserveFirst(winner.creation(), ttl));
            } else {
                facts = ((FirstPipelineCreationResult.AlreadyIssued) issued).preparation();
                requireOriginal(original, facts);
                if (facts.phase() == PipelinePreparation.Phase.COMPLETE) return facts;
                return continueExisting(original, facts, node);
            }
        } else {
            return continueExisting(original, facts, node);
        }
    }

    private PipelinePreparation continueExisting(PipelineIntentScope original, PipelinePreparation facts, WorkloadClaimFence node) {
        return finishAttempt(original, facts, () -> cache.resume(facts, node, ttl));
    }

    private PipelinePreparation finishAttempt(PipelineIntentScope original, PipelinePreparation facts,
            Supplier<PipelineCachePreparations.Reservation> reserve) {
        try {
            return finish(original, facts, reserve.get());
        } catch (TapstateException failure) {
            if (failure.code() != IoError.PIPELINE_STATE_REBUILD_REQUIRED && failure.code() != IoError.WORKLOAD_CLAIM_FENCED
                    && failure.code() != IoError.PIPELINE_INTENT_FENCED) throw failure;
            requireActive.run();
            // Another original attempt may have completed between our CONFIG read and CACHE query.
            // This is a status query only; it neither repairs CACHE nor grants business authority.
            var completed = configuration.read(original.pipelineId());
            requireActive.run();
            if (completed.isPresent() && completed.get().phase() == PipelinePreparation.Phase.COMPLETE
                    && original.equals(completed.get().scope()) && facts.preparationId().equals(completed.get().preparationId())) return completed.get();
            throw failure;
        }
    }

    private PipelinePreparation finish(PipelineIntentScope original, PipelinePreparation facts, PipelineCachePreparations.Reservation reservation) {
        if (!original.equals(reservation.scope()) || !facts.preparationId().equals(reservation.preparationId())) throw fenced();
        requireActive.run();
        var touchTime = clock.instant();
        requireActive.run();
        PipelinePreparationReceipt receipt = cache.seed(reservation, configuration, touchTime);
        if (!original.equals(receipt.scope()) || !facts.preparationId().equals(receipt.preparationId())) throw fenced();
        requireActive.run();
        PipelinePreparation completed = configuration.complete(receipt);
        requireOriginal(original, completed);
        if (completed.phase() != PipelinePreparation.Phase.COMPLETE || !facts.preparationId().equals(completed.preparationId())) throw fenced();
        requireActive.run();
        return completed;
    }

    private void requireNode(WorkloadClaimFence node) {
        if (node.key().type() != WorkloadClaimType.NODE_SESSION || !business.clusterId().equals(node.key().clusterId())
                || !node.key().resourceId().equals(node.owner().nodeId()) || node.claimGeneration() >= Long.MAX_VALUE - 1
                || node.executionGeneration() == Long.MAX_VALUE || node.topologyRevision() == Long.MAX_VALUE) throw claimFenced();
    }
    private static void requireOriginal(PipelineIntentScope expected, PipelinePreparation facts) {
        if (!expected.pipelineId().equals(facts.pipelineId()) || expected.stateGeneration() != facts.stateGeneration()
                || (facts.phase() != PipelinePreparation.Phase.UNASSIGNED && !expected.equals(facts.scope()))) throw fenced();
    }
    private static TapstateException fenced() { return new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null); }
    private static TapstateException claimFenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
