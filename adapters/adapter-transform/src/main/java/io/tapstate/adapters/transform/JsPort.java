package io.tapstate.adapters.transform;

import io.tapstate.core.event.Envelope;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;

/**
 * The {@code js} port: the full-power escape hatch. It runs an author's GraalVM script for each event
 * through a {@link RowScript}, which declares {@code process(record, ctx)} (required) and optionally
 * {@code filter(record)}. Unlike filter / map, js sees every event including ddl — it is the full-power
 * hatch for logic the declarative transforms cannot express. State / lookup are not part of this
 * stateless tier.
 */
final class JsPort implements TransformPort {

    private final RowScript script;

    JsPort(String source) {
        this(source, null);
    }

    JsPort(String source, String previewExecutionId) {
        this.script = new RowScript(source, previewExecutionId);
    }

    @Override
    public List<Envelope> transform(Envelope event) {
        return script.run(event);
    }

    @Override
    public void close() {
        script.close();
    }
}
