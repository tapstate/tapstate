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
        String connectorId, String artifactHash) {
    ConfiguredCaptureRun {
        Objects.requireNonNull(original,"original"); Objects.requireNonNull(node,"node"); Objects.requireNonNull(primary,"primary"); Objects.requireNonNull(global,"global");
        if (original.scope()==null || !original.pipelineId().equals(node.pipelineId())
                || !global.owner().ownerId().equals(original.scope().clusterId())
                || global.owner().bindingVersion()!=original.scope().bindingVersion()
                || primary.owner().kind()==RequiredNoteOwner.Kind.PIPELINE && !primary.owner().equals(RequiredNoteOwner.forNode(
                        original.scope().bindingVersion(),node,PipelineNode.Purpose.CAPTURE,original.scope().stateGeneration()))
                || connectorId==null || connectorId.isBlank() || artifactHash==null || !artifactHash.matches("[a-f0-9]{64}")
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
        Objects.requireNonNull(requireOriginalStart,"requireOriginalStart"); requireOriginalStart.run();
        if (original.scope()==null || config.node()==null || !original.pipelineId().equals(config.node().pipelineId())) throw unavailable();
        var fixed=notes.guardedBy(requireOriginalStart); var scope=original.scope();
        RequiredNoteOwner owner=config.sharedNotes()==null
                ? RequiredNoteOwner.forNode(scope.bindingVersion(),config.node(),PipelineNode.Purpose.CAPTURE,scope.stateGeneration())
                : new RequiredNoteOwner(scope.bindingVersion(),"pdk.chain."+config.sharedNotes().sharedBy(),
                        RequiredNoteOwner.Kind.CHAIN,config.sharedNotes().sharedBy(),0);
        var primary=fixed.open(owner);
        var global=fixed.open(new RequiredNoteOwner(scope.bindingVersion(),"pdk.global-state",RequiredNoteOwner.Kind.GLOBAL,scope.clusterId(),0));
        var pinned=recoveryPinned?fixed.origin(fixed.open(RequiredNoteOwner.forNode(scope.bindingVersion(),config.node(),PipelineNode.Purpose.CAPTURE,scope.stateGeneration()))):null;
        var artifact=pinned==null?provisioner.resolve(config.connectorId()):provisioner.resolve(config.connectorId(),pinned.artifactHash()); requireOriginalStart.run();
        var expected=ConnectorNoteOrigin.capture(config.connectorId(),artifact.contentHash(),config.settings());
        if(pinned!=null && !pinned.equals(expected))throw unavailable();
        if (!expected.equals(fixed.prepareOrigin(primary,expected))) throw unavailable();
        requireOriginalStart.run(); return new ConfiguredCaptureRun(original,config.node(),primary,global,config.connectorId(),artifact.contentHash());
    }

    /** The physical CAPTURE or private Pipeline supplies its own original guard, never its opener's live command. */
    PdkCapturePort port(CaptureConfig config, RequiredNoteStore notes, ConnectorProvisioner provisioner, Runnable requireRun) {
        Objects.requireNonNull(requireRun,"requireRun"); requireRun.run();
        if (!node.equals(config.node()) || !connectorId.equals(config.connectorId())
                || (config.sharedNotes()==null
                        ? !primary.owner().hasPurpose(PipelineNode.Purpose.CAPTURE)
                        : primary.owner().kind()!=RequiredNoteOwner.Kind.CHAIN || !primary.owner().ownerId().equals(config.sharedNotes().sharedBy()))) throw unavailable();
        var captured=Map.of(primary.owner().namespace(),primary,global.owner().namespace(),global);
        var scopes=ConnectorNoteScopes.of(notes,namespace -> {
            var token=captured.get(namespace); if (token==null) throw unavailable(); return token;
        },requireRun);
        ConnectorProvisioner exact=id -> {
            requireRun.run(); if (!connectorId.equals(id)) throw unavailable();
            var artifact=provisioner.resolve(id,artifactHash); requireRun.run(); return artifact;
        };
        return PdkCapturePort.withRequiredNotes(exact,null,scopes);
    }
    private static TapstateException unavailable() { return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null); }
}
