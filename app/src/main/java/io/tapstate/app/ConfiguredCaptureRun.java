package io.tapstate.app;

import io.tapstate.adapters.pdk.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.Objects;

/** Fixed necessary-note coordinates; a shared physical drive outlives its opening Pipeline. */
record ConfiguredCaptureRun(ExecutionFence original, PipelineNode node, RequiredNoteScope primary, RequiredNoteScope global,
        String connectorId, String artifactHash, ConnectorRef startupArtifact) {
    ConfiguredCaptureRun(ExecutionFence original, PipelineNode node, RequiredNoteScope primary, RequiredNoteScope global,
            String connectorId, String artifactHash) {
        this(original, node, primary, global, connectorId, artifactHash, null);
    }

    ConfiguredCaptureRun retained() {
        return startupArtifact == null ? this : new ConfiguredCaptureRun(original, node, primary, global, connectorId, artifactHash);
    }

    ConfiguredCaptureRun {
        Objects.requireNonNull(original,"original"); Objects.requireNonNull(node,"node"); Objects.requireNonNull(primary,"primary"); Objects.requireNonNull(global,"global");
        if (original.scope()==null || !original.pipelineId().equals(node.pipelineId())
                || !global.owner().ownerId().equals(original.scope().clusterId())
                || global.owner().bindingVersion()!=original.scope().bindingVersion()
                || primary.owner().kind()==RequiredNoteOwner.Kind.PIPELINE && !ConfiguredSinkRun.privateOwnerMatches(
                        primary.owner(), original, node, PipelineNode.Purpose.CAPTURE)
                || connectorId==null || connectorId.isBlank() || artifactHash==null || !artifactHash.matches("[a-f0-9]{64}")
                || startupArtifact != null && (!artifactHash.equals(startupArtifact.contentHash()) || primary.owner().kind() != RequiredNoteOwner.Kind.CHAIN)
                || global.owner().kind()!=RequiredNoteOwner.Kind.GLOBAL
                || primary.owner().bindingVersion()!=global.owner().bindingVersion()
                || primary.owner().kind()==RequiredNoteOwner.Kind.GLOBAL) throw new IllegalArgumentException("invalid fixed capture notes");
    }

    /** Original start preparation opens only CONFIG-reserved scopes, before any connector code. */
    static ConfiguredCaptureRun prepare(ExecutionFence original, CaptureConfig config, RequiredNoteStore notes,
            ConnectorProvisioner provisioner, Runnable requireOriginalStart) {
        return prepare(original,config,notes,provisioner,requireOriginalStart,false);
    }
    static ConfiguredCaptureRun prepare(ExecutionFence original,CaptureConfig config,RequiredNoteStore notes,
            ConnectorProvisioner provisioner,Runnable requireOriginalStart,boolean recoveryPinned) {
        return prepare(original, config, notes, provisioner, requireOriginalStart, recoveryPinned, false);
    }

    static ConfiguredCaptureRun prepareStartup(ExecutionFence original, CaptureConfig config, RequiredNoteStore notes,
            ConnectorProvisioner provisioner, Runnable requireOriginalStart) {
        return prepare(original, config, notes, provisioner, requireOriginalStart, false, true);
    }

    private static ConfiguredCaptureRun prepare(ExecutionFence original, CaptureConfig config, RequiredNoteStore notes,
            ConnectorProvisioner provisioner, Runnable requireOriginalStart, boolean recoveryPinned, boolean startup) {
        return provisioner.withArtifactRetention(() -> prepareRetainingArtifact(
                original, config, notes, provisioner, requireOriginalStart, recoveryPinned, startup));
    }

    private static ConfiguredCaptureRun prepareRetainingArtifact(ExecutionFence original, CaptureConfig config,
            RequiredNoteStore notes, ConnectorProvisioner provisioner, Runnable requireOriginalStart,
            boolean recoveryPinned, boolean startup) {
        Objects.requireNonNull(requireOriginalStart,"requireOriginalStart"); requireOriginalStart.run();
        if (original.scope()==null || config.node()==null || !original.pipelineId().equals(config.node().pipelineId())) throw unavailable();
        var fixed=notes.guardedBy(requireOriginalStart); var scope=original.scope();
        RequiredNoteOwner owner=config.sharedNotes()==null
                ? RequiredNoteOwner.forNode(scope.bindingVersion(),config.node(),PipelineNode.Purpose.CAPTURE,scope.stateGeneration())
                : new RequiredNoteOwner(scope.bindingVersion(),"pdk.chain."+config.sharedNotes().sharedBy(),
                        RequiredNoteOwner.Kind.CHAIN,config.sharedNotes().sharedBy(),0);
        var globalOwner = new RequiredNoteOwner(scope.bindingVersion(), "pdk.global-state", RequiredNoteOwner.Kind.GLOBAL, scope.clusterId(), 0);
        var captured = startup ? fixed.openCaptureScopes(owner, globalOwner)
                : new RequiredNoteStore.CaptureScopes(fixed.open(owner), fixed.open(globalOwner));
        var primary = captured.primary();
        var global = captured.global();
        var retainedScope = primary;
        var pinned = fixed.originIfPresent(primary).orElse(null);
        if (pinned == null && recoveryPinned) {
            retainedScope = fixed.open(RequiredNoteOwner.forNode(scope.bindingVersion(), config.node(),
                    PipelineNode.Purpose.CAPTURE, scope.stateGeneration()));
            pinned = fixed.origin(retainedScope);
        }
        if (pinned != null && !pinned.equals(
                ConnectorNoteOrigin.capture(config.connectorId(), pinned.artifactHash(), config.settings()))) throw unavailable();
        var artifact = pinned != null ? provisioner.resolveRetained(fixed, retainedScope, pinned)
                : startup ? provisioner.resolveCaptureStartup(config.connectorId()) : provisioner.resolve(config.connectorId());
        requireOriginalStart.run();
        var expected=ConnectorNoteOrigin.capture(config.connectorId(),artifact.contentHash(),config.settings());
        if (pinned == null) provisioner.retain(artifact);
        var prepared = startup ? fixed.prepareSharedOrigin(primary, expected) : fixed.prepareOrigin(primary, expected);
        if (!expected.equals(prepared)) throw unavailable();
        if (pinned == null || !retainedScope.equals(primary)) provisioner.markBound(fixed, primary, expected);
        requireOriginalStart.run();
        return new ConfiguredCaptureRun(original, config.node(), primary, global, config.connectorId(),
                artifact.contentHash(), startup ? artifact : null);
    }

    static ConfiguredCaptureRun prepareBound(ExecutionFence original, CaptureConfig config, RequiredNoteStore notes,
            ConnectorProvisioner provisioner, Runnable guard, ConnectorRunBindings.Binding binding,
            ConnectorRunBindings.Binding previous) {
        return provisioner.withArtifactRetention(() -> prepareBoundRetainingArtifact(
                original, config, notes, provisioner, guard, binding, previous));
    }

    private static ConfiguredCaptureRun prepareBoundRetainingArtifact(ExecutionFence original, CaptureConfig config,
            RequiredNoteStore notes, ConnectorProvisioner provisioner, Runnable guard,
            ConnectorRunBindings.Binding binding, ConnectorRunBindings.Binding previous) {
        var fixed = notes.guardedBy(guard);
        var origin = ConnectorNoteOrigin.capture(config.connectorId(), binding.sha256(), config.settings());
        var owner = binding.notes().owner();
        RequiredNoteScope scope = fixed.open(owner);
        var existing = fixed.originIfPresent(scope);
        if (existing.isEmpty()) {
            var ref = provisioner.resolve(config.connectorId(), binding.sha256());
            provisioner.retain(ref);
            if (previous == null) {
                fixed.prepareOrigin(scope, origin);
                provisioner.markBound(fixed, scope, origin);
            } else {
                scope = fixed.prepareArtifactBinding(owner, origin, previous.notes());
            }
        } else {
            if (!origin.equals(existing.orElseThrow())) throw unavailable();
            provisioner.resolveRetained(fixed, scope, origin);
        }
        var global = fixed.open(new RequiredNoteOwner(original.scope().bindingVersion(), "pdk.global-state",
                RequiredNoteOwner.Kind.GLOBAL, original.scope().clusterId(), 0));
        return new ConfiguredCaptureRun(original, config.node(), scope, global, config.connectorId(), binding.sha256());
    }

    /** The physical CAPTURE or private Pipeline supplies its own original guard, never its opener's live command. */
    PdkCapturePort port(CaptureConfig config, RequiredNoteStore notes, ConnectorProvisioner provisioner, Runnable requireRun) {
        Objects.requireNonNull(requireRun,"requireRun"); requireRun.run();
        if (!node.equals(config.node()) || !connectorId.equals(config.connectorId())
                || (config.sharedNotes()==null
                        ? !primary.owner().hasPurpose(PipelineNode.Purpose.CAPTURE)
                        : primary.owner().kind()!=RequiredNoteOwner.Kind.CHAIN || !primary.owner().ownerId().equals(config.sharedNotes().sharedBy()))) throw unavailable();
        String primaryName = config.sharedNotes() == null
                ? RequiredNoteOwner.namespace(node, PipelineNode.Purpose.CAPTURE) : primary.owner().namespace();
        var captured=Map.of(primaryName,primary,global.owner().namespace(),global);
        java.util.function.Function<String, RequiredNoteScope> opener = namespace -> {
            var token = captured.get(namespace);
            if (token == null) throw unavailable();
            return token;
        };
        var scopes = startupArtifact == null
                ? ConnectorNoteScopes.forBoundRun(notes, primaryName, primary, global, requireRun)
                : ConnectorNoteScopes.forSharedStartup(notes, opener, requireRun);
        var openingArtifact = new java.util.concurrent.atomic.AtomicReference<>(startupArtifact);
        ConnectorProvisioner exact=id -> {
            requireRun.run(); if (!connectorId.equals(id)) throw unavailable();
            var capturedRef = openingArtifact.getAndSet(null);
            var artifact = capturedRef == null ? provisioner.resolve(id, artifactHash)
                    : provisioner.revalidateCaptureStartup(id, capturedRef);
            requireRun.run();
            return artifact;
        };
        return PdkCapturePort.withRequiredNotes(exact,null,scopes);
    }

    /** Current discovery cannot grant a source policy without the original configuration and artifact. */
    SourceModelComparison preparedModelComparison(CaptureConfig config, RequiredNoteStore notes,
            ConnectorProvisioner provisioner, Runnable requireRun) {
        if (startupArtifact == null) return modelComparison(config, notes, provisioner, requireRun);
        requireRun.run();
        return PdkSourceModelComparison.forVerifiedArtifact(connectorId, startupArtifact);
    }

    SourceModelComparison modelComparison(CaptureConfig config, RequiredNoteStore notes,
            ConnectorProvisioner provisioner, Runnable requireRun) {
        requireRun.run();
        if (!node.equals(config.node()) || !connectorId.equals(config.connectorId())
                || (config.sharedNotes()==null
                        ? !primary.owner().hasPurpose(PipelineNode.Purpose.CAPTURE)
                        : primary.owner().kind()!=RequiredNoteOwner.Kind.CHAIN
                                || !primary.owner().ownerId().equals(config.sharedNotes().sharedBy()))) throw unavailable();
        var artifact = provisioner.resolve(connectorId, artifactHash);
        if (!artifactHash.equals(artifact.contentHash())) throw unavailable();
        var expected = ConnectorNoteOrigin.capture(connectorId, artifactHash, config.settings());
        if (!expected.equals(notes.guardedBy(requireRun).origin(primary))) throw unavailable();
        requireRun.run();
        return PdkSourceModelComparison.forVerifiedArtifact(connectorId, artifact);
    }
    private static TapstateException unavailable() { return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null); }
}
