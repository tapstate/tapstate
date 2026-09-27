package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Prevents an incomplete or cross-execution Jet collection from becoming a plausible queue total. */
class QueueStatisticsTest {

    @Test
    void pairsOnlyProcessorsOfTheRequestedJobAndOneExecution() {
        JobMetrics collected = JobMetrics.of(Map.of(
                MetricNames.QUEUES_SIZE, List.of(
                        measure(MetricNames.QUEUES_SIZE, 3, "job-a", "exec-1", "proc-0"),
                        measure(MetricNames.QUEUES_SIZE, 8, "job-b", "exec-1", "proc-1")),
                MetricNames.QUEUES_CAPACITY, List.of(
                        measure(MetricNames.QUEUES_CAPACITY, 16, "job-a", "exec-1", "proc-0"),
                        measure(MetricNames.QUEUES_CAPACITY, 16, "job-b", "exec-1", "proc-1"))));

        assertThat(Engine.queueSampleIn(collected, "job-a"))
                .contains(new Engine.QueueSample("exec-1", 3, 16, 1L));
    }

    @Test
    void refusesAMixedExecutionOrAQueueWithoutMatchingCapacity() {
        JobMetrics mixed = JobMetrics.of(Map.of(
                MetricNames.QUEUES_SIZE, List.of(measure(MetricNames.QUEUES_SIZE, 3,
                        "job-a", "exec-1", "proc-0")),
                MetricNames.QUEUES_CAPACITY, List.of(measure(MetricNames.QUEUES_CAPACITY, 16,
                        "job-a", "exec-2", "proc-0"))));
        JobMetrics unpaired = JobMetrics.of(Map.of(
                MetricNames.QUEUES_SIZE, List.of(measure(MetricNames.QUEUES_SIZE, 3,
                        "job-a", "exec-1", "proc-0"))));

        assertThat(Engine.queueSampleIn(mixed, "job-a")).isEmpty();
        assertThat(Engine.queueSampleIn(unpaired, "job-a")).isEmpty();
    }

    private static Measurement measure(String metric, long value, String job, String execution,
            String processor) {
        return Measurement.of(metric, value, 1L, Map.of(
                MetricTags.JOB, job,
                MetricTags.EXECUTION, execution,
                MetricTags.MEMBER, "member-1",
                MetricTags.VERTEX, "serve.out",
                MetricTags.PROCESSOR, processor));
    }
}
