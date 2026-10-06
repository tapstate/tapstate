package io.tapstate.adapters.pdk;

import io.tapstate.core.logging.LogSink;
import io.tapstate.core.model.PipelineNode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** A connector whose diagnostic messages deliberately carry more detail than its init exception. */
public final class SyntheticCaptureLogJars {

    private SyntheticCaptureLogJars() { }

    public static ConnectorRef refusingSource(Path directory) {
        Path jar = SyntheticJar.compileToJar(directory, "synthetic.LogRefusingSource", """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.entity.logger.TapLogger;
                import io.tapdata.entity.schema.TapTable;
                import java.util.List;
                import java.util.function.Consumer;
                public class LogRefusingSource implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportBatchRead((context, table, offset, size, consumer) -> {
                            throw new AssertionError("init must refuse before snapshot rows");
                        });
                    }
                    public void init(TapConnectionContext context) {
                        context.getLog().error("password authentication failed: no password was given for {}", "source");
                        TapLogger.error("LogRefusingSource", "the connection was refused before any row was read");
                        throw new IllegalStateException("cannot open a connection");
                    }
                    public void stop(TapConnectionContext context) { }
                    public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
                            Consumer<List<TapTable>> consumer) {
                        throw new AssertionError("init must refuse before schema discovery");
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
                        throw new AssertionError("the capture does not probe the connection");
                    }
                    public int tableCount(TapConnectionContext context) { return 1; }
                }
                """);
        return new ConnectorRef(List.of(jar), "synthetic.LogRefusingSource", "2.0.8", null);
    }

    /** A finite real PDK drive with separate context and shared-channel init/tail messages. */
    public static ConnectorRef cdcSource(Path directory) {
        Path jar = SyntheticJar.compileToJar(directory, "synthetic.AdmittedCdcSource", """
                package synthetic;
                import io.tapdata.pdk.apis.TapConnector;
                import io.tapdata.pdk.apis.functions.ConnectorFunctions;
                import io.tapdata.pdk.apis.context.TapConnectionContext;
                import io.tapdata.pdk.apis.entity.ConnectionOptions;
                import io.tapdata.pdk.apis.entity.TestItem;
                import io.tapdata.entity.codec.TapCodecsRegistry;
                import io.tapdata.entity.logger.TapLogger;
                import io.tapdata.entity.schema.TapTable;
                import java.util.List;
                import java.util.function.Consumer;
                public class AdmittedCdcSource implements TapConnector {
                    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {
                        functions.supportTimestampToStreamOffset((context, timestamp) -> "cdc-log-boundary");
                        functions.supportStreamRead((context, tables, offset, size, consumer) -> {
                            context.getLog().warn("native CDC context message");
                            TapLogger.error("AdmittedCdcSource", "native CDC shared message");
                        });
                    }
                    public void init(TapConnectionContext context) {
                        context.getLog().warn("native init context message");
                        TapLogger.error("AdmittedCdcSource", "native init shared message");
                    }
                    public void stop(TapConnectionContext context) { }
                    public void discoverSchema(TapConnectionContext context, List<String> tables, int size,
                            Consumer<List<TapTable>> consumer) {
                        consumer.accept(List.of(new TapTable("orders")));
                    }
                    public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) {
                        throw new AssertionError("the source run does not probe the connection");
                    }
                    public int tableCount(TapConnectionContext context) { return 1; }
                }
                """);
        return new ConnectorRef(List.of(jar), "synthetic.AdmittedCdcSource", "2.0.8", null);
    }

    /** Opens a real isolated PDK handle using the existing explicit-scope entry point. */
    public static RefusingHandle open(ConnectorRef reference, PipelineNode node, LogSink.Scope scope) {
        return new RefusingHandle(PdkConnector.open("log_owner_source", reference, Map.of(), node, null, scope));
    }

    public static final class RefusingHandle implements AutoCloseable {
        private final PdkConnector connector;
        private RefusingHandle(PdkConnector connector) { this.connector = connector; }
        public void drive() throws Throwable {
            connector.underLoader(() -> { connector.connector().init(connector.context()); return null; });
        }
        @Override public void close() {
            connector.stopQuietly();
            connector.close();
        }
    }
}
