package io.tapstate.runtime.engine;

import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.PipelineIntentScope;
import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class OperatorRunManifestTest {
    private static OperatorMapBinding binding(String pipeline,long generation,String identity,int execution) {
        return new OperatorMapBinding(new OperatorPlacement(new PipelineIntentScope("cluster",1,"cache",pipeline,generation),
                "cache",OperatorPlacement.Resource.JOIN_FACTS,"join","root","facts"),identity,String.format("%064x",execution));
    }
    @Test void theCapturedPositionsAreImmutableAndEmptyStatelessRunsStillHaveAnOriginalIdentity() {
        var original = binding("orders",1,"run",1); var values = new ArrayList<>(List.of(original));
        var manifest = new OperatorRunManifest("orders","run",values); values.clear();
        assertThat(manifest.bindings()).containsExactly(original);
        assertThatThrownBy(() -> manifest.bindings().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(new OperatorRunManifest("orders","run",List.of()).executionIdentity()).isEqualTo("run");
    }
    @Test void mixedPipelineScopeIdentityExecutionKeysOrDuplicatePhysicalPositionsAreRefused() {
        var original = binding("orders",1,"run",1);
        for (var other : List.of(original,binding("foreign",1,"run",1),binding("orders",2,"run",1),
                binding("orders",1,"foreign",1),binding("orders",1,"run",2))) {
            assertThatThrownBy(() -> new OperatorRunManifest("orders","run",List.of(original,other))).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
