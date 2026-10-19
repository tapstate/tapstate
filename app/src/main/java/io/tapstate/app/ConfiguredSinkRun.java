package io.tapstate.app;

import io.tapstate.adapters.pdk.ConnectorNoteOrigin;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.store.*;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** Credential-free coordinates captured before submission; member callbacks never resolve newer note tokens. */
record ConfiguredSinkRun(ExecutionFence original,PipelineNode node,PipelineNode.Purpose purpose,
        Note primary,Note global,String artifactHash)implements Serializable {
    record Note(long bindingVersion,String namespace,RequiredNoteOwner.Kind kind,String ownerId,
            long stateGeneration,long generation)implements Serializable {
        Note(RequiredNoteScope original){this(original.owner().bindingVersion(),original.owner().namespace(),original.owner().kind(),original.owner().ownerId(),original.owner().stateGeneration(),original.generation());}
        Note{new RequiredNoteScope(new RequiredNoteOwner(bindingVersion,namespace,kind,ownerId,stateGeneration),generation);}
        RequiredNoteScope token(){return new RequiredNoteScope(new RequiredNoteOwner(bindingVersion,namespace,kind,ownerId,stateGeneration),generation);}
    }
    ConfiguredSinkRun {
        Objects.requireNonNull(original,"original");Objects.requireNonNull(node,"node");Objects.requireNonNull(purpose,"purpose");
        Objects.requireNonNull(primary,"primary");Objects.requireNonNull(global,"global");
        if(original.scope()==null || purpose==PipelineNode.Purpose.CAPTURE || !node.pipelineId().equals(original.pipelineId())
                || !primary.token().owner().equals(RequiredNoteOwner.forNode(original.scope().bindingVersion(),node,purpose,original.scope().stateGeneration()))
                || !global.token().owner().equals(new RequiredNoteOwner(original.scope().bindingVersion(),"pdk.global-state",RequiredNoteOwner.Kind.GLOBAL,original.scope().clusterId(),0))
                || artifactHash==null || !artifactHash.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("configured sink coordinates must retain one original execution and note identity");
    }
    /** Explicit original start preparation, after CONFIG authoring reserved every scope and before plugin code. */
    static ConfiguredSinkRun prepare(ExecutionFence original,SinkConfig config,PipelineNode.Purpose purpose,
            RequiredNoteStore notes,ConnectorProvisioner provisioner,Runnable requireActive) {
        Objects.requireNonNull(requireActive,"requireActive");requireActive.run();
        if(original.scope()==null || config.node()==null || !config.node().pipelineId().equals(original.pipelineId()))throw unavailable();
        var fixed=notes.guardedBy(requireActive);var scope=original.scope();
        var primary=fixed.open(RequiredNoteOwner.forNode(scope.bindingVersion(),config.node(),purpose,scope.stateGeneration()));
        var global=fixed.open(new RequiredNoteOwner(scope.bindingVersion(),"pdk.global-state",RequiredNoteOwner.Kind.GLOBAL,scope.clusterId(),0));
        var ref=provisioner.resolve(config.connectorId());requireActive.run();
        var expected=ConnectorNoteOrigin.capture(config.connectorId(),ref.contentHash(),config.settings());
        if(!expected.equals(fixed.prepareOrigin(primary,expected)))throw unavailable();
        requireActive.run();return new ConfiguredSinkRun(original,config.node(),purpose,new Note(primary),new Note(global),ref.contentHash());
    }
    private static TapstateException unavailable(){return new TapstateException(IoError.NOTE_SCOPE_UNAVAILABLE,Map.of(),null);}
}
