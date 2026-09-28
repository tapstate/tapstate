package io.tapstate.runtime.engine;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.MetricDescriptorImpl;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.Stage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OutputPressureMetricsTest {

    @Test
    void onlyAnOpenedIntervalReadsTheClockAndRepeatedRefusalsDoNotRestartIt() {
        Clock clock = new Clock();
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.SOURCE, clock::read);
        assertThat(metrics.snapshot()).isNull();
        metrics.observed(metrics.scope(), false);
        metrics.startScope(100);
        metrics.observed(metrics.scope(), true);
        assertThat(clock.reads).isZero();
        metrics.observed(metrics.scope(), false);
        clock.now = 500_000;
        metrics.observed(metrics.scope(), false);
        clock.now = 900_000;
        metrics.observed(metrics.scope(), false);
        assertThat(clock.reads).isEqualTo(1);
        clock.now = 1_000_000;
        metrics.observed(metrics.scope(), true);
        metrics.observed(metrics.scope(), true);
        OutputPressureMetrics.Snapshot reading = metrics.snapshot();
        assertThat(clock.reads).isEqualTo(2);
        assertThat(reading.refused()).isEqualTo(3);
        assertThat(reading.completed()).isEqualTo(1);
        assertThat(reading.sumNanos()).isEqualTo(1_000_000);
        assertThat(reading.buckets()[3]).isEqualTo(1);
        assertThat(Arrays.stream(reading.buckets()).sum()).isEqualTo(1);
    }

    @Test
    void cancellationDoesNotFinishTheIntervalOrCarryItsClockIntoANewScope() {
        Clock clock = new Clock();
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.TRANSFORM, clock::read);
        metrics.startScope(100);
        long oldScope = metrics.scope();
        metrics.observed(oldScope, false);
        clock.now = 20_000_000_000L;
        metrics.closeScope();
        assertThat(metrics.snapshot()).isNull();
        metrics.startScope(200);
        metrics.observed(oldScope, true);
        assertThat(metrics.snapshot().sinceMillis()).isEqualTo(200);
        assertThat(metrics.snapshot().completed()).isZero();
        assertThat(metrics.snapshot().refused()).isZero();
        metrics.observed(metrics.scope(), false);
        clock.now += 11_000_000_000L;
        metrics.observed(metrics.scope(), true);
        assertThat(metrics.snapshot().sumNanos()).isEqualTo(11_000_000_000L);
        assertThat(metrics.snapshot().buckets()[HistogramBounds.PROCESS_DURATION.buckets() - 1]).isEqualTo(1);
    }

    @Test
    void aCollectedTupleIsIndependentAndKeepsNativeTagsAndTheUserMetricFlag() {
        Clock clock = new Clock();
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.JOIN, clock::read);
        metrics.startScope(100);
        metrics.observed(metrics.scope(), false);
        clock.now = 5_000_000;
        metrics.observed(metrics.scope(), true);
        MetricDescriptor descriptor = descriptor().withTag(MetricTags.JOB, "job")
                .withTag(MetricTags.EXECUTION, "execution").withTag(MetricTags.MEMBER, "member")
                .withTag(MetricTags.VERTEX, "vertex").withTag(MetricTags.PROCESSOR, "1")
                .withTag(MetricTags.USER, "false");
        RecordingCollection collected = new RecordingCollection();
        metrics.provideDynamicMetrics(descriptor, collected);
        assertThat(collected.values).hasSize(5 + HistogramBounds.PROCESS_DURATION.buckets());
        assertThat(collected.values.stream().map(Value::name).toList()).doesNotHaveDuplicates();
        for (Value value : collected.values) {
            assertThat(tag(value.descriptor(), MetricTags.USER)).isEqualTo("true");
            assertThat(tag(value.descriptor(), MetricTags.JOB)).isEqualTo("job");
            assertThat(tag(value.descriptor(), MetricTags.EXECUTION)).isEqualTo("execution");
            assertThat(tag(value.descriptor(), MetricTags.MEMBER)).isEqualTo("member");
            assertThat(tag(value.descriptor(), MetricTags.VERTEX)).isEqualTo("vertex");
            assertThat(tag(value.descriptor(), MetricTags.PROCESSOR)).isEqualTo("1");
            assertThat(OutputPressureMetricNames.partOf(value.name()).stage()).isEqualTo("join");
        }
        assertThat(tag(descriptor, MetricTags.USER)).isEqualTo("false");
        OutputPressureMetrics.Snapshot before = metrics.snapshot();
        before.buckets()[0] = 100;
        assertThat(Arrays.stream(metrics.snapshot().buckets()).sum()).isEqualTo(1);
    }

    @Test
    void concurrentCollectionNeverObservesAPartiallyCompletedHistogram() throws Exception {
        AtomicLong nanos = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        OutputPressureMetrics metrics = new OutputPressureMetrics(Stage.NEST, nanos::incrementAndGet);
        metrics.startScope(100);
        long scope = metrics.scope();
        Thread writer = new Thread(() -> {
            try {
                for (int index = 0; index < 5_000; index++) {
                    metrics.observed(scope, false);
                    metrics.observed(scope, true);
                }
            } catch (Throwable problem) {
                failure.set(problem);
            }
        }, "output-pressure-test-writer");
        writer.start();
        for (int index = 0; index < 1_000; index++) {
            OutputPressureMetrics.Snapshot reading = metrics.snapshot();
            assertThat(reading.scope()).isEqualTo(scope);
            assertThat(reading.sinceMillis()).isEqualTo(100);
            assertThat(reading.completed()).isLessThanOrEqualTo(reading.refused());
            assertThat(reading.sumNanos()).isEqualTo(reading.completed());
            assertThat(Arrays.stream(reading.buckets()).sum()).isEqualTo(reading.completed());
        }
        writer.join(5_000);
        assertThat(writer.isAlive()).isFalse();
        assertThat(failure.get()).isNull();
        assertThat(metrics.snapshot().completed()).isEqualTo(5_000);
    }

    @Test
    void theComponentVocabularyRejectsUnknownStagesAndBuckets() {
        assertThat(OutputPressureMetricNames.partOf("stage.join.output.refused"))
                .isEqualTo(new OutputPressureMetricNames.Part("join", OutputPressureMetricNames.Kind.REFUSED, -1));
        assertThat(OutputPressureMetricNames.partOf("stage.nest.output.retry.bucket.16"))
                .isEqualTo(new OutputPressureMetricNames.Part("nest", OutputPressureMetricNames.Kind.BUCKET, 16));
        assertThat(OutputPressureMetricNames.partOf("stage.unknown.output.ready")).isNull();
        assertThat(OutputPressureMetricNames.partOf("stage.nest.output.retry.bucket.17")).isNull();
        assertThat(OutputPressureMetricNames.partOf("stage.nest.output.retry.bucket.nope")).isNull();
        assertThat(OutputPressureMetricNames.partOf("stage.nest.active")).isNull();
    }

    private static MetricDescriptorImpl descriptor() {
        return new MetricDescriptorImpl(OutputPressureMetricsTest::descriptor);
    }

    private static String tag(MetricDescriptor descriptor, String name) {
        // Jet materializes descriptor tags into a map, so a later tag replaces an earlier one.
        for (int index = descriptor.tagCount() - 1; index >= 0; index--) {
            if (name.equals(descriptor.tag(index))) {
                return descriptor.tagValue(index);
            }
        }
        return null;
    }

    private static final class Clock {
        private long now;
        private int reads;
        private long read() { reads++; return now; }
    }

    private record Value(MetricDescriptor descriptor, String name, ProbeUnit unit, long value) { }

    private static final class RecordingCollection implements MetricsCollectionContext {
        private final List<Value> values = new ArrayList<>();
        @Override public void collect(MetricDescriptor descriptor, Object source) {
            throw new AssertionError("output tuple collection must use explicit scalar components");
        }
        @Override public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, long value) {
            values.add(new Value(descriptor.copy(), name, unit, value));
        }
        @Override public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, double value) {
            throw new AssertionError("output tuple components must remain integral");
        }
        @Override public void collect(MetricDescriptor descriptor, long value) {
            throw new AssertionError("output tuple collection must name its scalar components");
        }
        @Override public void collect(MetricDescriptor descriptor, double value) {
            throw new AssertionError("output tuple components must remain integral");
        }
    }
}
