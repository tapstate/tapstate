package io.tapstate.adapters.transform;

import io.tapstate.spi.transform.TransformPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The factory for the stateless row-transform ports. Each entry point takes only the serializable
 * shape of a transform (an expression string, a projection spec) and returns the port that runs it,
 * so the app assembly root can capture that shape in the Jet supplier and build the port member-side.
 */
public final class StatelessTransforms {

    private static final Logger LOG = LoggerFactory.getLogger(UnwindPort.class);

    private StatelessTransforms() {
    }

    /** The {@code filter} port for a CEL predicate over the event envelope. */
    public static TransformPort filter(String expr) {
        return new FilterPort(expr);
    }

    /** The {@code map} port for a field projection captured as a {@link MapSpec}. */
    public static TransformPort map(MapSpec spec) {
        return new MapPort(spec);
    }

    /**
     * The {@code unwind} port for a row expansion captured as an {@link UnwindSpec}.
     *
     * <p>The one thing an expansion has to say and cannot refuse - two elements of a row landing on
     * the same key - goes to the log, which is the whole observation face for this severity. A case
     * builds the port with its own alert instead.
     */
    public static TransformPort unwind(UnwindSpec spec) {
        return new UnwindPort(spec, coded -> LOG.warn(coded.getMessage()));
    }

    /** The {@code js} port for a GraalVM script captured as its source text. */
    public static TransformPort js(String script) {
        return new JsPort(script);
    }
}
