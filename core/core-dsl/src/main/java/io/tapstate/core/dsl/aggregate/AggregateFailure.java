package io.tapstate.core.dsl.aggregate;

import java.util.Map;

/**
 * A function's own refusal, raised before it knows which field or document it is folding for. Whoever
 * applies the function adds those and raises the coded error; the function only knows what went wrong.
 */
public final class AggregateFailure extends RuntimeException {

    private final RollupError code;
    private final Map<String, Object> args;

    AggregateFailure(RollupError code, Map<String, Object> args) {
        super(code.code() + " " + args, null, false, false);
        this.code = code;
        this.args = Map.copyOf(args);
    }

    public RollupError code() {
        return code;
    }

    /** The arguments the function could supply; the caller adds the field and the document. */
    public Map<String, Object> args() {
        return args;
    }
}
