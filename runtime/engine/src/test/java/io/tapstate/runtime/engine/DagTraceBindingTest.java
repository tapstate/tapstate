package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DagTraceBindingTest {

    @Test
    void derivesStableNamesAndRoutesVerticesAndInputs() {
        AtomicReference<String> requested = new AtomicReference<>();
        ProcessorMetaSupplier supplier = null;
        DagTraceBinding binding = new DagTraceBinding(name -> {
            requested.set(name);
            return supplier;
        });

        assertThat(binding.vertexName("root")).isEqualTo("preview.trace.cm9vdA");
        assertThat(binding.inputVertexName("root", "orders")).isEqualTo("preview.trace.input.cm9vdABvcmRlcnM");
        assertThat(binding.inputs().apply("root", "orders")).isSameAs(supplier);
        assertThat(requested).hasValue("root\u0000input\u0000orders");
        assertThat(binding.vertices().apply("root")).isSameAs(supplier);
        assertThat(requested).hasValue("root");
    }

    @Test
    void rejectsMissingVertexFunctions() {
        assertThatThrownBy(() -> new DagTraceBinding(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DagTraceBinding(null, (node, alias) -> null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DagTraceBinding(name -> null, null))
                .isInstanceOf(NullPointerException.class);
    }
}
