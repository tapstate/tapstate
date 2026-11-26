package io.tapstate.runtime.engine;

import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceKeyMovePreflight;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.spi.transform.TransformPort;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Evaluates a pure serial prefix once, before source admission can leave a durable recovery interval. */
final class SerialTransformPreflight implements SourceKeyMovePreflight.PreparedRows {
    private static final long serialVersionUID = 1L;
    private final List<SourcePreflightStart.SerialPort> stages;
    private final int lastExpandingStage;
    private final SourceKeyMovePreflight.Check downstream;
    private transient List<TransformPort> ports;
    private record Pending(Envelope input, PreparedTransformRows output) {}
    private transient ArrayDeque<Pending> prepared;
    private transient StageTimer timer;

    SerialTransformPreflight(List<SourcePreflightStart.SerialPort> stages,
            SourceKeyMovePreflight.Check downstream) {
        this.stages = List.copyOf(stages);
        int last = -1;
        for (int i = 0; i < stages.size(); i++) {
            if (stages.get(i).canExpand()) last = i;
        }
        lastExpandingStage = last;
        this.downstream = Objects.requireNonNull(downstream, "downstream");
    }

    @Override
    public void validate(Envelope row) {
        if (ports == null) {
            timer = StageTimer.onProcessor(Stage.TRANSFORM);
            ports = new ArrayList<>(stages.size());
        }
        var output = evaluate(0, row);
        if (prepared == null) prepared = new ArrayDeque<>();
        prepared.addLast(new Pending(row, output));
    }

    private PreparedTransformRows evaluate(int stage, Envelope row) {
        if (ports.size() == stage) ports.add(stages.get(stage).factory().get());
        long started = timer.begin();
        List<Envelope> results;
        try {
            results = ports.get(stage).transform(row);
        } finally {
            timer.end(started);
        }
        // After an actual fork, keep any further expansion on its Jet edge under backpressure.
        // These children are ordinary inputs to the next stage once the source guard has armed.
        boolean deferChildren = results.size() > 1 && stage < lastExpandingStage;
        var transformed = TransformProcessor.outputs(results, row);
        var output = new ArrayList<>(results.size());
        Object item;
        while ((item = transformed.next()) != null) {
            if (!deferChildren && item instanceof Envelope event) {
                if (stage + 1 < stages.size()) item = evaluate(stage + 1, event);
                else downstream.validate(event);
            }
            output.add(item);
        }
        return new PreparedTransformRows(output);
    }

    @Override
    public Object emission(Envelope row) {
        var next = prepared == null ? null : prepared.peekFirst();
        return next != null && next.input() == row ? prepared.removeFirst().output() : row;
    }

    @Override
    public void close() {
        if (prepared != null) prepared.clear();
        Throwable failure = null;
        if (ports != null) {
            for (var port : ports) {
                try {
                    port.close();
                } catch (RuntimeException | Error closing) {
                    if (failure == null) failure = closing;
                    else failure.addSuppressed(closing);
                }
            }
            ports = null;
        }
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }
}
