package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/** CONFIG-only scope publication; callers separately verify old execution isolation and CACHE preparation. */
public interface CacheRecoveryStore {
    record ScopeChange(PipelineIntentScope previousScope, PipelineIntentScope targetScope) {}
    /** The confirmation fixes physical influence, including a mapping change without an authoring edit. */
    static String clusterDependencyHash(CacheAuthScope target, List<ScopeChange> changes) {
        var scopes = changes.stream().sorted(java.util.Comparator.comparing(change -> change.targetScope().pipelineId()))
                .map(change -> java.util.Arrays.asList(change.previousScope() == null ? null : coordinates(change.previousScope()), coordinates(change.targetScope()))).toList();
        return io.tapstate.core.model.canonical.CanonicalHash.ofText(io.tapstate.core.common.JsonWriter.write(List.of(
                "stopped-cache-domain", target.clusterId(), target.bindingVersion(), target.mode().name(), target.cacheIncarnation(), target.authGeneration(), scopes)));
    }
    private static List<Object> coordinates(PipelineIntentScope scope) {
        return List.of(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), scope.pipelineId(), scope.stateGeneration());
    }
    CacheRecoveryStore guardedBy(Runnable requireActive);
    /** Read-only proposal for every current Pipeline; no generation or runtime resource is created. */
    List<ScopeChange> inspectCluster(CacheAuthScope target);
    /** The exact Cluster confirmation publishes RUNNING, all mappings and stopped intents together. */
    List<PipelineCacheRecovery> stageCluster(StorageRecoveryCommands.Plan confirmed);
    Optional<PipelineCacheRecovery> read(String pipelineId);
    /** Read-only inspection of original private contexts; this never grants release or access to their values. */
    default List<RequiredNoteContext> inspectPrivateContexts(String pipelineId) {
        throw new io.tapstate.core.common.TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,java.util.Map.of(),null);
    }
    /** A changed connection must prove its private context is not retained by another shared owner. */
    default void requirePrivateRetirable(String pipelineId,RequiredNoteScope original) {
        throw new io.tapstate.core.common.TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,java.util.Map.of(),null);
    }
    /** Opaque same-domain authority for the one explicitly confirmed Pipeline preparation. */
    FullPreparation authorizeFull(StorageRecoveryCommands.Plan running, String pipelineId);
    interface FullPreparation {
        PipelineCacheRecovery recovery();
        String operationId();
        void requireActive();
    }
}
