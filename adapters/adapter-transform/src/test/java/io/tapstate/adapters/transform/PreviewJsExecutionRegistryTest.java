package io.tapstate.adapters.transform;

import org.graalvm.polyglot.Context;
import org.junit.jupiter.api.Test;
import io.tapstate.core.event.Envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreviewJsExecutionRegistryTest {

    @Test
    void previewScriptsRegisterAndReleaseTheirExecutionContext() {
        String executionId = "preview-" + System.nanoTime();
        var script = StatelessTransforms.previewJs(
                "function process(record, ctx) { record.after.previewed = true; return record; }", executionId);
        try {
            assertThat(script.transform(Envelope.read(1, "orders", java.util.Map.of("id", 1), java.util.Map.of())))
                    .singleElement()
                    .extracting(Envelope::after)
                    .isEqualTo(java.util.Map.of("id", 1, "previewed", true));
        } finally {
            script.close();
            StatelessTransforms.finishPreviewJs(executionId);
        }
    }

    @Test
    void ignoresMissingExecutionIdsAndClosesContextsRegisteredAfterCancellation() {
        try (Context ignored = Context.create()) {
            PreviewJsExecutionRegistry.register(null, ignored);
        }

        String executionId = "cancelled-" + System.nanoTime();
        Context active = Context.create();
        PreviewJsExecutionRegistry.register(executionId, active);
        PreviewJsExecutionRegistry.cancel(executionId);
        Context late = Context.create();
        PreviewJsExecutionRegistry.register(executionId, late);
        assertThatThrownBy(() -> late.eval("js", "1")).isInstanceOf(RuntimeException.class);

        PreviewJsExecutionRegistry.unregister(executionId, active);
        PreviewJsExecutionRegistry.finish(executionId);
        active.close();
        PreviewJsExecutionRegistry.finish(executionId);
    }

    @Test
    void finishClosesRegisteredGuestContextsAndRepeatedFinishIsSafe() {
        String executionId = "finished-" + System.nanoTime();
        Context context = Context.create();
        PreviewJsExecutionRegistry.register(executionId, context);
        PreviewJsExecutionRegistry.finish(executionId);
        assertThatThrownBy(() -> context.eval("js", "1")).isInstanceOf(RuntimeException.class);
        PreviewJsExecutionRegistry.finish(executionId);
    }
}
