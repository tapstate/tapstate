package io.tapstate.core.common;

/** Request-owned supplementary diagnostics, separate from stable error codes and arguments. */
public final class OperationErrorDiagnostics implements AutoCloseable {
    private static final ThreadLocal<OperationErrorDiagnostics> CURRENT = new ThreadLocal<>();
    private final OperationErrorDiagnostics previous;
    private String detail;

    private OperationErrorDiagnostics() { previous = CURRENT.get(); CURRENT.set(this); }
    public static OperationErrorDiagnostics open() { return new OperationErrorDiagnostics(); }
    public static boolean active() { return CURRENT.get() != null; }

    /** Only the storage adapter supplies confirmed, bounded, non-sensitive structure differences. */
    public static void confirmed(String detail) {
        var scope = CURRENT.get();
        if (scope != null && scope.detail == null && detail != null && !detail.isBlank())
            scope.detail = detail.substring(0, Math.min(detail.length(), 2048));
    }

    public static String appendTo(String message) {
        var scope = CURRENT.get();
        return scope == null || scope.detail == null ? message : message + " Storage structure: " + scope.detail;
    }

    @Override public void close() {
        if (previous == null) CURRENT.remove(); else CURRENT.set(previous);
    }
}
