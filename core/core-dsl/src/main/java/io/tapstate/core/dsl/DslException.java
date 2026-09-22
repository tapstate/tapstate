package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A located DSL error (plan poc1 B3): the {@code dsl} domain's typed {@link TapstateException}
 * subclass. It carries a {@link DslError} code plus the named arguments that are
 * the single source of variable data, and adds the typed location accessors the parse layer needs
 * — the field {@code path}, the 1-based {@code line} / {@code column} ({@code 0} when unknown), and
 * the originating {@code source} file name (attached by the workspace loader; {@code null} for
 * in-memory parses).
 *
 * <p>{@code path} is both a typed accessor (for tooling / IDE jump-to) and a message argument: the
 * constructor injects it into the argument map under {@code "path"} for every code that declares a
 * {@code path} placeholder, so that placeholder resolves without the throw site repeating it. A
 * pre-semantic code that declares no {@code path} placeholder keeps the typed accessor but never
 * advertises {@code path} in its rendered arguments.
 */
public final class DslException extends TapstateException {

    private final String path;
    private final String source;
    private final int line;
    private final int column;

    DslException(DslError code, String path, int line, int column, String source, Map<String, Object> args) {
        super(code, withPath(code, args, path), null);
        this.path = path;
        this.source = source;
        this.line = line;
        this.column = column;
    }

    /**
     * Injects {@code path} into the argument map only for codes that declare it as a placeholder. The
     * path stays available as a typed accessor regardless, but a pre-semantic code (one with no
     * {@code path} placeholder) must not advertise an empty {@code path} in its rendered arguments —
     * the named-argument set has to equal the code's declared placeholder contract.
     */
    private static Map<String, Object> withPath(DslError code, Map<String, Object> args, String path) {
        Map<String, Object> merged = args == null ? new LinkedHashMap<>() : new LinkedHashMap<>(args);
        if (code.placeholders().contains("path")) {
            merged.put("path", path);
        }
        return merged;
    }

    /**
     * Returns a copy carrying the originating file name, attached by the workspace loader
     * (B3-6) once it knows which {@code *.tap.yml} a per-file parse error came from. Cross-file
     * closure errors are not located at a single file and keep {@code source == null}.
     */
    public DslException withSource(String source) {
        return new DslException(code(), path, line, column, source, args());
    }

    /** The {@link DslError} code (narrows {@link TapstateException#code()}). */
    @Override
    public DslError code() {
        return (DslError) super.code();
    }

    public String path() {
        return path;
    }

    public String source() {
        return source;
    }

    public int line() {
        return line;
    }

    public int column() {
        return column;
    }
}
