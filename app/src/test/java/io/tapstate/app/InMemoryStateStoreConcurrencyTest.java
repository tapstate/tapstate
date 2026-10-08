package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Concurrent lifecycle workers must not lose checkpoints in the shared wiring fixture. */
class InMemoryStateStoreConcurrencyTest {
    @Test
    void concurrentCreatesKeepEveryPipelineCheckpoint() throws Exception {
        int workerCount = 4;
        int pipelinesPerWorker = 128;
        InMemoryStateStore store = new InMemoryStateStore();
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch begin = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(workerCount)) {
            var results = new ArrayList<Future<?>>();
            for (int worker = 0; worker < workerCount; worker++) {
                int identity = worker;
                results.add(workers.submit(() -> {
                    ready.countDown();
                    assertThat(begin.await(5, TimeUnit.SECONDS)).isTrue();
                    for (int pipeline = 0; pipeline < pipelinesPerWorker; pipeline++) {
                        store.create("pipeline_" + identity + "_" + pipeline,
                                StateJson.of(PipelineState.NEW), Instant.EPOCH);
                    }
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            begin.countDown();
            for (Future<?> result : results) {
                result.get(5, TimeUnit.SECONDS);
            }
            for (int worker = 0; worker < workerCount; worker++) {
                for (int pipeline = 0; pipeline < pipelinesPerWorker; pipeline++) {
                    String id = "pipeline_" + worker + "_" + pipeline;
                    assertThat(store.read(id)).as("the checkpoint created by its actual worker: %s", id)
                            .isPresent().get().satisfies(checkpoint ->
                                    assertThat(checkpoint.pipelineId()).isEqualTo(id));
                }
            }
        } finally {
            begin.countDown();
        }
    }
}
