package io.tapstate.core.common;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The carrier for every user-facing / diagnosable first-party error. Unchecked,
 * so it never pollutes signatures; the base class for per-domain typed subclasses (the first is
 * {@code DslException}) that add catch-by-kind and typed accessors.
 *
 * <p>Holds the {@link TapstateErrorCode} (the enum itself — self-describing in-process) plus the
 * named arguments that are the <em>single source</em> of variable data: a throw site supplies
 * {@code arg("field", x)} once, and both this dev string and the (presentation-layer) catalog
 * template consume it. There is never a second, inline message string at the throw site.
 *
 * <p>{@link #getMessage()} is the deterministic developer / log string assembled from the code
 * and the named arguments — zero third-party, never reads the catalog. The user-facing message
 * is rendered by the presentation layer from the per-locale catalog.
 *
 * <p>Programmer errors / invariant violations (NPE, {@code IllegalStateException}) stay bare and
 * are allowed to crash with a stack trace — they must not be laundered into a pretty
 * {@code *.unknown} code that hides the defect.
 */
public class TapstateException extends RuntimeException {

    private final TapstateErrorCode code;
    private final Map<String, Object> args;

    public TapstateException(TapstateErrorCode code, Map<String, Object> args, Throwable cause) {
        super(cause);
        this.code = code;
        this.args = args == null || args.isEmpty()
                ? Map.of()
                : Collections.unmodifiableMap(new LinkedHashMap<>(args));
    }

    /** The error code (enum constant); self-describing inside the process. */
    public TapstateErrorCode code() {
        return code;
    }

    /** The named arguments — the single source of variable data; unmodifiable snapshot. */
    public Map<String, Object> args() {
        return args;
    }

    /**
     * Deterministic developer / log string: the canonical code, then the named arguments sorted
     * by key (so the same error always renders identically), or the bare code when there are none.
     * This is not the user-facing message — that is rendered from the catalog by the presentation
     * layer.
     */
    @Override
    public String getMessage() {
        if (args.isEmpty()) {
            return code.code();
        }
        StringBuilder sb = new StringBuilder(code.code()).append(" {");
        boolean first = true;
        for (Map.Entry<String, Object> e : new TreeMap<>(args).entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(e.getKey()).append('=').append(e.getValue());
            first = false;
        }
        return sb.append('}').toString();
    }
}
