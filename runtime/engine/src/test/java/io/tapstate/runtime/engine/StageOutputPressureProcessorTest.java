package io.tapstate.runtime.engine;

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.MetricDescriptorImpl;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StageOutputPressureProcessorTest {

    @Test
    void callbacksKeepTheirArgumentsResultsAndCooperationFlags() throws Exception {
        Delegate delegate = new Delegate(Stage.JOIN);
        StageOutputPressureProcessor measured = new StageOutputPressureProcessor(delegate, Stage.JOIN, () -> 1);
        TestProcessorContext context = new TestProcessorContext();
        TestOutbox outbox = new TestOutbox(10);
        measured.init(outbox, context);
        assertThat(delegate.context).isSameAs(context);
        assertThat(delegate.outbox).isNotSameAs(outbox);
        assertThat(delegate.outbox.bucketCount()).isEqualTo(outbox.bucketCount());
        assertThat(measured.isCooperative()).isFalse();
        assertThat(measured.closeIsCooperative()).isFalse();
        TestInbox input = new TestInbox();
        measured.process(2, input);
        assertThat(delegate.inbox).isSameAs(input);
        assertThat(delegate.ordinal).isEqualTo(2);
        assertThat(measured.tryProcess()).isFalse();
        Watermark bound = new Watermark(1);
        assertThat(measured.tryProcessWatermark(bound)).isFalse();
        assertThat(measured.tryProcessWatermark(3, bound)).isFalse();
        assertThat(delegate.watermark).isSameAs(bound);
        assertThat(delegate.ordinal).isEqualTo(3);
        assertThat(measured.completeEdge(4)).isFalse();
        assertThat(delegate.ordinal).isEqualTo(4);
        assertThat(measured.complete()).isFalse();
        assertThat(measured.saveToSnapshot()).isFalse();
        assertThat(measured.snapshotCommitPrepare()).isFalse();
        assertThat(measured.snapshotCommitFinish(true)).isFalse();
        assertThat(delegate.commitSuccess).isTrue();
        measured.restoreFromSnapshot(input);
        assertThat(delegate.restored).isSameAs(input);
        assertThat(measured.finishSnapshotRestore()).isFalse();
        measured.close();
        assertThat(delegate.calls).containsExactly("init", "process", "tryProcess", "watermark", "edgeWatermark",
                "completeEdge", "complete", "save", "prepare", "finish", "restore", "restoreDone", "close");
    }

    @Test
    void delegateFailuresAreRethrownWithoutBeingReplaced() throws Exception {
        Delegate delegate = new Delegate(Stage.NEST);
        StageOutputPressureProcessor measured = new StageOutputPressureProcessor(delegate, Stage.NEST, () -> 1);
        measured.init(new TestOutbox(10), new TestProcessorContext());
        IllegalStateException failure = new IllegalStateException("test processor failed");
        delegate.failure = failure;
        assertThatThrownBy(measured::complete).isSameAs(failure);
        assertThatThrownBy(measured::close).isSameAs(failure);
    }

    @Test
    void superclassStillCollectsTheOriginalStaticAndDynamicProbeProviders() {
        Delegate delegate = new Delegate(Stage.TRANSFORM);
        StageOutputPressureProcessor measured = new StageOutputPressureProcessor(delegate, Stage.TRANSFORM, () -> 1);
        RecordingCollection collection = new RecordingCollection();
        measured.provideDynamicMetrics(descriptor(), collection);
        assertThat(collection.staticSources).containsExactly(delegate);
        assertThat(collection.names).containsExactly("preserved.dynamic");
        assertThat(collection.values).containsExactly(7L);
    }

    @Test
    void inertUnstagedAndTerminalSinkProcessorsAreNotDecoratedOrDecoratedTwice() {
        Processor inert = new AbstractProcessor() { };
        assertThat(StageOutputPressureProcessor.wrap(inert)).isSameAs(inert);
        Delegate sink = new Delegate(Stage.SINK);
        assertThat(StageOutputPressureProcessor.wrap(sink)).isSameAs(sink);
        Processor wrapped = StageOutputPressureProcessor.wrap(new Delegate(Stage.SOURCE));
        assertThat(wrapped).isInstanceOf(StageOutputPressureProcessor.class);
        assertThat(StageOutputPressureProcessor.wrap(wrapped)).isSameAs(wrapped);
        assertThat(((Staged) wrapped).stage()).isEqualTo(Stage.SOURCE);
    }

    private static MetricDescriptorImpl descriptor() {
        return new MetricDescriptorImpl(StageOutputPressureProcessorTest::descriptor);
    }

    private static final class Delegate implements Processor, Staged, DynamicMetricsProvider {
        private final Stage stage;
        private final List<String> calls = new ArrayList<>();
        private Context context;
        private Outbox outbox;
        private Inbox inbox;
        private Inbox restored;
        private int ordinal;
        private Watermark watermark;
        private boolean commitSuccess;
        private RuntimeException failure;
        private Delegate(Stage stage) { this.stage = stage; }
        @Override public Stage stage() { return stage; }
        @Override public boolean isCooperative() { return false; }
        @Override public boolean closeIsCooperative() { return false; }
        @Override public void init(Outbox outbox, Context context) {
            this.outbox = outbox;
            this.context = context;
            calls.add("init");
        }
        @Override public void process(int ordinal, Inbox inbox) {
            this.ordinal = ordinal;
            this.inbox = inbox;
            calls.add("process");
        }
        @Override public boolean tryProcess() { calls.add("tryProcess"); return false; }
        @Override public boolean tryProcessWatermark(Watermark watermark) {
            this.watermark = watermark;
            calls.add("watermark");
            return false;
        }
        @Override public boolean tryProcessWatermark(int ordinal, Watermark watermark) {
            this.ordinal = ordinal;
            this.watermark = watermark;
            calls.add("edgeWatermark");
            return false;
        }
        @Override public boolean completeEdge(int ordinal) { this.ordinal = ordinal; calls.add("completeEdge"); return false; }
        @Override public boolean complete() {
            calls.add("complete");
            if (failure != null) { throw failure; }
            return false;
        }
        @Override public boolean saveToSnapshot() { calls.add("save"); return false; }
        @Override public boolean snapshotCommitPrepare() { calls.add("prepare"); return false; }
        @Override public boolean snapshotCommitFinish(boolean success) { commitSuccess = success; calls.add("finish"); return false; }
        @Override public void restoreFromSnapshot(Inbox inbox) { restored = inbox; calls.add("restore"); }
        @Override public boolean finishSnapshotRestore() { calls.add("restoreDone"); return false; }
        @Override public void close() {
            calls.add("close");
            if (failure != null) { throw failure; }
        }
        @Override public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
            collection.collect(descriptor.copy(), "preserved.dynamic", ProbeLevel.INFO, ProbeUnit.COUNT, 7);
        }
    }

    private static final class RecordingCollection implements MetricsCollectionContext {
        private final List<Object> staticSources = new ArrayList<>();
        private final List<String> names = new ArrayList<>();
        private final List<Long> values = new ArrayList<>();
        @Override public void collect(MetricDescriptor descriptor, Object source) { staticSources.add(source); }
        @Override public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, long value) {
            names.add(name);
            values.add(value);
        }
        @Override public void collect(MetricDescriptor descriptor, String name, ProbeLevel level, ProbeUnit unit, double value) {
            throw new AssertionError("the preserved test probe is integral");
        }
        @Override public void collect(MetricDescriptor descriptor, long value) {
            throw new AssertionError("the test probe has an explicit name");
        }
        @Override public void collect(MetricDescriptor descriptor, double value) {
            throw new AssertionError("the preserved test probe is integral");
        }
    }
}
