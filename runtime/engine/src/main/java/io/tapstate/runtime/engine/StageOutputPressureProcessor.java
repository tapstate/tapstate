package io.tapstate.runtime.engine;

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Measures ordinary output refusal and successful retry without changing processor callbacks. */
public final class StageOutputPressureProcessor extends ProcessorWrapper implements Staged {

    private final Stage stage;
    private final OutputPressureMetrics pressure;

    StageOutputPressureProcessor(Processor delegate, Stage stage, LongSupplier nanoClock) {
        super(Objects.requireNonNull(delegate, "delegate"));
        this.stage = Objects.requireNonNull(stage, "stage");
        pressure = new OutputPressureMetrics(stage, nanoClock);
    }

    /** Wraps owned source, transform, join and nest processors; native inert placeholders stay unchanged. */
    public static Processor wrap(Processor processor) {
        Objects.requireNonNull(processor, "processor");
        if (processor instanceof StageOutputPressureProcessor || !(processor instanceof Staged staged)
                || staged.stage() == Stage.SINK) {
            return processor;
        }
        return new StageOutputPressureProcessor(processor, staged.stage(), System::nanoTime);
    }

    /** Decorates source instances after their supplier has selected the member that owns the input. */
    public static ProcessorMetaSupplier wrap(ProcessorMetaSupplier supplier) {
        return new WrappingProcessorMetaSupplier(Objects.requireNonNull(supplier, "supplier"),
                StageOutputPressureProcessor::wrap);
    }

    @Override
    public Stage stage() {
        return stage;
    }

    @Override
    protected Outbox wrapOutbox(Outbox outbox) {
        return new OutputPressureOutbox(outbox, pressure);
    }

    @Override
    protected void initWrapper(Outbox outbox, Context context) {
        if (context.hazelcastInstance() != null) {
            pressure.startScope(System.currentTimeMillis());
        }
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext collection) {
        super.provideDynamicMetrics(descriptor.copy(), collection);
        pressure.provideDynamicMetrics(descriptor.copy(), collection);
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            pressure.closeScope();
        }
    }
}
