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

    /** A registered storage-test artifact: connectionTest is observable, while init and discovery refuse. */
    public static Path registeredStorageProbe(Path workDir, Path trace) {
        return registeredStorageProbe(workDir, trace, "mongodb", "mongodb://fixture/accepted");
    }

    /** A synthetic registered adapter used to observe persisted URI and trust handoff, not certify a release artifact. */
    public static Path registeredStorageProbe(Path workDir, Path trace, String connectorId, String acceptedPrefix) {
        String file = trace.toAbsolutePath().toString().replace("\\", "\\\\").replace("\"", "\\\"");
        String code = """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.annotations.TapConnectorClass;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.entity.schema.TapTable;
                import java.util.List;
                import java.util.function.Consumer;
                @TapConnectorClass("mongodb-spec.json")
                public class StorageProbe implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportWriteRecord((c, e, t, r) -> { throw new AssertionError("business write must not run"); });
                    }
                    public void init(TapConnectionContext context) { throw new AssertionError("init must not run"); }
                    public void stop(TapConnectionContext context) {}
                    public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> sink) {
                        throw new AssertionError("discovery must not run");
                    }
                    public int tableCount(TapConnectionContext context) { throw new AssertionError("table count must not run"); }
                    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> sink) throws Throwable {
                        String uri = context.getConnectionConfig().getString("uri");
                        java.nio.file.Files.writeString(java.nio.file.Path.of("%s"), uri + "\\n",
                                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
                        String ca = context.getConnectionConfig().getString("sslCA");
                        if (ca != null) {
                            try (var input = java.nio.file.Files.newInputStream(java.nio.file.Path.of(ca))) {
                                java.security.cert.CertificateFactory.getInstance("X.509").generateCertificates(input);
                            }
                        }
                        sink.accept(new TestItem("fixture", uri.startsWith("%s")
                                ? TestItem.RESULT_SUCCESSFULLY : TestItem.RESULT_FAILED));
                        return ConnectionOptions.create();
                    }
                }
                """.formatted(file, acceptedPrefix);
        return SyntheticJar.compileToJar(workDir, "synthetic.StorageProbe", code,
                java.util.Map.of("mongodb-spec.json", "{\"properties\":{\"id\":\"" + connectorId + "\"}}"), java.util.Map.of());
    }
}
