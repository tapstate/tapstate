package io.tapstate.core.common;

import java.util.Set;

/** Explicit origin of a diagnostic node's code, independent of its value and definition status. */
public final class DiagnosticCodeSource {
    public static final String FIELD = "codeSource";

    public static final String TAPSTATE_STANDARD = "tapstate-standard";
    public static final String TAPSTATE_LEGACY = "tapstate-legacy";
    public static final String PDK = "pdk";
    public static final String CONNECTOR = "connector";
    public static final String JDBC_SQLSTATE = "jdbc-sqlstate";

    private static final Set<String> VALUES = Set.of(TAPSTATE_STANDARD, TAPSTATE_LEGACY, PDK, CONNECTOR, JDBC_SQLSTATE);

    private DiagnosticCodeSource() { }

    static boolean isKnown(Object value) {
        return value instanceof String source && VALUES.contains(source);
    }
}
