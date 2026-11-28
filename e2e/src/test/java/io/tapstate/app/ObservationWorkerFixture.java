package io.tapstate.app;

import io.tapstate.e2e.Await;
import java.time.Duration;
import java.util.List;
import org.springframework.context.ConfigurableApplicationContext;

/** Stops the native workers in a test server that will serve a hand-published observation. */
public final class ObservationWorkerFixture {
    private ObservationWorkerFixture() {}

    public static void stopAndAwait(List<ConfigurableApplicationContext> contexts) {
        var lifecycle = onlyWorker(contexts, PipelineConvergences.class);
        var observations = onlyWorker(contexts, PeriodicObservations.class);
        lifecycle.close();
        observations.close();
        // Closing discards queued work, but a worker already in a store call can still finish it.
        Await.until("the observation writers to exit", Duration.ofSeconds(15),
                () -> lifecycle.health().activeWorkers() == 0 && observations.health().activeWorkers() == 0,
                () -> "lifecycle=" + lifecycle.health() + ", observations=" + observations.health());
    }

    public static void awaitIdle(List<ConfigurableApplicationContext> contexts) {
        var observations = onlyWorker(contexts, PeriodicObservations.class);
        var writes = onlyWorker(contexts, ProcessObservationWriter.class);
        Await.until("the observation and history work to drain", Duration.ofSeconds(15), () -> {
            var health = observations.health();
            var usage = writes.usage();
            return health.retainedPipelines() == 0 && health.activeWorkers() == 0
                    && usage.pending() == 0 && usage.inFlight() == 0;
        }, () -> "observations=" + observations.health() + ", writes=" + writes.usage());
    }

    private static <T> T onlyWorker(List<ConfigurableApplicationContext> contexts, Class<T> type) {
        var workers = contexts.stream().flatMap(context -> context.getBeansOfType(type).values().stream()).toList();
        if (workers.size() != 1) throw new AssertionError("expected one " + type.getSimpleName() + ": " + workers.size());
        return workers.getFirst();
    }
}
