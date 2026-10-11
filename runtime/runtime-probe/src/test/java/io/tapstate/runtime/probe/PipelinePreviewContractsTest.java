package io.tapstate.runtime.probe;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelinePreviewContractsTest {

    @Test
    void requestValidatesBoundsAndCopiesCanonicalResources() {
        List<String> resources = new ArrayList<>(List.of("pipeline"));
        PipelinePreviewRequest request = request(resources, 1, null);
        resources.add("later");
        assertThat(request.canonicalResources()).containsExactly("pipeline");

        assertThatThrownBy(() -> request(List.of("pipeline"), 0, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(List.of("pipeline"), 101, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(List.of("pipeline"), 1, " ")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> request(List.of(), 1, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PipelinePreviewRequest(" ", "author", "p", null, 1, null,
                "hash", List.of("pipeline"), Instant.now())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PipelinePreviewRequest("r", "author", "p", " ", 1, null,
                "hash", List.of("pipeline"), Instant.now())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void eventCopiesPayloadAndRejectsInvalidSequence() {
        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        payload.put("rows", 2);
        PipelinePreviewEvent event = new PipelinePreviewEvent("run", "hash", 0, "sample", Instant.now(), payload);
        payload.put("rows", 3);
        assertThat(event.payload()).containsEntry("rows", 2);
        assertThat(new PipelinePreviewEvent("run", "hash", 1, "done", Instant.now(), null).payload()).isEmpty();
        assertThatThrownBy(() -> new PipelinePreviewEvent("run", "hash", -1, "done", Instant.now(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void streamCloseCancelsAndDelegatingProbeForwardsTheSameStream() {
        AtomicInteger cancelled = new AtomicInteger();
        PipelinePreviewStream expected = new PipelinePreviewStream() {
            @Override
            public PipelinePreviewEvent next() {
                return null;
            }

            @Override
            public void cancel() {
                cancelled.incrementAndGet();
            }
        };
        DelegatingPipelinePreviewProbe probe = new DelegatingPipelinePreviewProbe(request -> expected);
        PipelinePreviewRequest request = request(List.of("pipeline"), 2, null);
        assertThat(probe.preview(request)).isSameAs(expected);
        expected.close();
        assertThat(cancelled).hasValue(1);
        assertThat(expected.sampleCacheHit()).isFalse();
    }

    private static PipelinePreviewRequest request(List<String> resources, int rootLimit, String sampleId) {
        return new PipelinePreviewRequest("run", "author", "pipeline", null, rootLimit, sampleId,
                "candidate-hash", resources, Instant.now().plusSeconds(5));
    }
}
