package io.tapstate.app;

import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PipelineInputClosureTest {
    private record Fixture(InMemoryStorePort live, SourceResource input, SourceResource target,
                           ViewResource view, ServeResource serve, PipelineResource authored) {
        PipelinePreparationInputs inputs(ReadOnlyArtifactSnapshot captured) {
            var selected=PipelineInlining.inline(authored,captured);
            return SourceModelSnapshot.capture(authored.id(),List.of(input),live.schemas()).preparationInputs(
                    new PipelineIntentScope("cluster",1,"cache",authored.id(),2),authored,selected,Map.of(input.id(),input));
        }
    }
    private static Fixture fixture() {
        var live=new InMemoryStorePort();
        var input=new SourceResource("input",null,"mongodb",Map.of("uri","mongodb://input.invalid/db"),SourceMode.CDC,List.of(TableRef.literal("orders")),null,null);
        var target=new SourceResource("target",null,"mongodb",Map.of("uri","mongodb://target.invalid/db"),null,null,null,null);
        var view=new ViewResource("reusable_view",null,"id",null,null);
        var serve=new ServeResource("reusable_serve",null,List.of(new SyncElement("sink",target.id(),null,null,null)),null,null,null);
        var pipeline=new PipelineResource("flow",null,List.of(SourceRef.spec(input.id(),true)),null,
                new ViewBlock.Use("materialized",view.id(),FromRef.literal("orders")),
                new ServeBlock.Use(null,serve.id(),FromRef.literal("materialized")),null,null);
        for(Resource resource:List.of(input,target,view,serve,pipeline,
                new SourceResource("unrelated",null,"mongodb",Map.of(),null,null,null,null)))live.artifacts().create(resource);
        return new Fixture(live,input,target,view,serve,pipeline);
    }

    @Test void fullAndStartupUseTheAuthoredPipelineAndExactTransitiveViewServeAndTargetClosure() {
        var f=fixture();var captured=ReadOnlyArtifactSnapshot.capture(f.live.artifacts());var inputs=f.inputs(captured);
        assertThat(inputs.pipelineHash()).isEqualTo(CanonicalHash.of(f.authored));
        assertThat(inputs.pipelineHash()).isNotEqualTo(CanonicalHash.of(PipelineInlining.inline(f.authored,captured)));
        assertThat(inputs.selectedTables()).containsExactlyEntriesOf(Map.of("input",Set.of("orders")));
        assertThat(PipelineInputClosure.dependencyHashes(captured,f.authored,inputs)).containsExactlyInAnyOrderEntriesOf(Map.of(
                f.target.id(),CanonicalHash.of(f.target),f.view.id(),CanonicalHash.of(f.view),f.serve.id(),CanonicalHash.of(f.serve)));
    }

    @Test void laterAuthoringCannotChangeTheCapturedDependencyProofOrAddUnrelatedResources() {
        var f=fixture();var captured=ReadOnlyArtifactSnapshot.capture(f.live.artifacts());var inputs=f.inputs(captured);
        var fixed=PipelineInputClosure.dependencyHashes(captured,f.authored,inputs);
        var changed=new SourceResource(f.target.id(),null,"mongodb",Map.of("uri","mongodb://changed.invalid/db"),null,null,null,null);
        f.live.artifacts().save(changed);
        assertThat(PipelineInputClosure.dependencyHashes(captured,f.authored,inputs)).isEqualTo(fixed);
        var current=PipelineInputClosure.dependencyHashes(ReadOnlyArtifactSnapshot.capture(f.live.artifacts()),f.authored,inputs);
        assertThat(current).containsEntry(f.target.id(),CanonicalHash.of(changed));
        assertThat(current.keySet()).containsExactlyInAnyOrder(f.target.id(),f.view.id(),f.serve.id());
        assertThat(current).isNotEqualTo(fixed);
    }
}
