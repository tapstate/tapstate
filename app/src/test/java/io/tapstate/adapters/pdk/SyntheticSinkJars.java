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
}
