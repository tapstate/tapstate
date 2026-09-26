package io.tapstate.adapters.pdk;

import java.nio.file.Path;

/**
 * Hands the assembly module's tests a synthetic sink connector, which is otherwise reachable only from
 * inside the bridge's own package.
 *
 * <p>It declares the bridge's package for the same reason the real-connector witness next door does: the
 * fixture it needs is package-private, and on the flat test classpath a same-package class can reach it.
 * Nothing here widens the bridge's own surface - the fixture stays package-private, and this is the one
 * place the assembly module's tests touch it, so what they depend on is this method and not the shape of
 * the fixture behind it.
 */
public final class SyntheticSinkJars {

    private SyntheticSinkJars() {
    }

    /**
     * A sink connector that keeps one mark in the map the plugin contract gives it, and reports on every
     * write whether the mark was already there: 0 the first time, 1 once something has filed it. That
     * number is the whole observation - it is the connector itself saying what it found, rather than the
     * host reading its own object back.
     */
    public static Path stateRecordingSink(Path workDir) {
        return Synthetic.stateRecordingSink(workDir);
    }

    /**
     * A sink connector that writes one line to {@code trace} for each thing done to its target - {@code
     * create:<table>}, {@code clear:<table>}, {@code count:<table>}, {@code stop} - and whose tables already
     * exist where {@code exists} says so.
     */
    public static Path preparationSink(Path workDir, Path trace, boolean exists) {
        return Synthetic.preparationSink(workDir, trace, exists);
    }

    /**
     * A sink connector that writes one line to {@code trace} as it starts ({@code init}), as each write call
     * reaches it ({@code write}) and as it stops ({@code stop}).
     */
    public static Path lifecycleSink(Path workDir, Path trace) {
        return Synthetic.lifecycleSink(workDir, trace);
    }
}
