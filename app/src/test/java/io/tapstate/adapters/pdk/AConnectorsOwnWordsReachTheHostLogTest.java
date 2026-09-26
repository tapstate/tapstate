package io.tapstate.adapters.pdk;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapdata.entity.logger.TapLogger;
import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.WriteMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a connector says about itself reaches the host's log, filed against the pipeline it was driving.
 *
 * <p>A connector knows things nothing outside it knows: which setting is wrong, which permission is
 * missing, that the server it dialled refused the credentials. Dropped on the way, the reader is left
 * with whatever the host could infer from the outside - a failure code and an exception class - which is
 * the difference between being told the password is wrong and being told the source could not be read.
 *
 * <p>Two halves are asserted separately, because they fail separately and either alone looks like
 * success from the other's vantage point: that the line is written at all, and that it says which
 * pipeline it belongs to. A line written but unattributed reaches the console and never the pipeline's
 * own tail - so somebody watching the run sees nothing, while a test asserting only "it was logged"
 * stays green.
 *
 * <p>It lives in the bridge's own package for the reason its sibling gives: the classes that do this are
 * package-private there, and a same-package test on the flat classpath can reach them. It lives in this
 * module because that is where a logging backend exists to capture with - the bridge itself compiles
 * against the facade alone, by ring rule, so nothing there could observe a line being written.
 */
class AConnectorsOwnWordsReachTheHostLogTest {

    private static final String PIPELINE = "orders_sync";

    @Test
    void aConnectorsWarningIsWrittenToTheHostLog() {
        List<ILoggingEvent> written = captured("io.tapstate.connector.demo",
                () -> new ConnectorLog("demo", PIPELINE).warn("cannot reach {}: {}", "orders", "refused"));

        assertThat(written).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).isEqualTo("cannot reach orders: refused");
        });
    }

    @Test
    void aConnectorsErrorCarriesTheFailureItWasHanded() {
        RuntimeException cause = new IllegalStateException("password authentication failed");

        List<ILoggingEvent> written = captured("io.tapstate.connector.demo",
                () -> new ConnectorLog("demo", PIPELINE).error("cannot open a connection", cause));

        assertThat(written).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.ERROR);
            // The reason is on the throwable, and the read face renders a captured stack with the line:
            // carrying the message alone would file a line saying only that something failed.
            assertThat(event.getThrowableProxy().getMessage()).isEqualTo("password authentication failed");
        });
    }

    @Test
    void routineProgressIsLoweredSoItCannotCrowdOutTheError() {
        List<ILoggingEvent> written = captured("io.tapstate.connector.demo", () -> {
            ConnectorLog log = new ConnectorLog("demo", PIPELINE);
            log.info("read 1000 rows");
            log.debug("polling");
        });

        // The read face holds a bounded window of recent lines; a connector's progress chatter at the
        // level a reader watches would push the one line they came for out of it.
        assertThat(written).extracting(ILoggingEvent::getLevel).containsExactly(Level.DEBUG, Level.DEBUG);
    }

    @Test
    void aLoweredLineCarriesItsPipelineToo() {
        // Lowering rather than dropping is only worth anything if the line can be found by turning the
        // level up. An unattributed line is filtered out of the pipeline's own tail, so a connector author
        // who raises their connector to debug would reach the host log and still see nothing where they
        // are looking -- which is the reading this level mapping exists to provide.
        List<ILoggingEvent> written = captured("io.tapstate.connector.demo", () -> {
            ConnectorLog log = new ConnectorLog("demo", PIPELINE);
            log.info("read 1000 rows");
            log.debug("polling");
            log.trace("offset advanced");
        });

        assertThat(written).hasSize(3).allSatisfy(event -> assertThat(event.getMDCPropertyMap())
                .containsEntry(PipelineAttribution.MDC_KEY, PIPELINE));
    }

    @Test
    void aLineCarriesThePipelineItsConnectorWasOpenedForWhateverThreadWritesIt() throws Exception {
        AtomicReference<String> onItsOwnThread = new AtomicReference<>();

        List<ILoggingEvent> written = captured("io.tapstate.connector.demo", () -> {
            ConnectorLog log = new ConnectorLog("demo", PIPELINE);
            // A connector may retry, poll or reconnect on a thread of its own, long after the call that
            // created it returned; the pipeline it belongs to is a property of the handle, not the thread.
            Thread ownThread = new Thread(() -> {
                log.error("the change stream closed");
                onItsOwnThread.set(MDC.get(PipelineAttribution.MDC_KEY));
            }, "a-connectors-own-thread");
            ownThread.start();
            joinQuietly(ownThread);
        });

        assertThat(written).singleElement()
                .satisfies(event -> assertThat(event.getMDCPropertyMap())
                        .containsEntry(PipelineAttribution.MDC_KEY, PIPELINE));
        assertThat(onItsOwnThread.get())
                .as("the slot is handed back, so the thread is not left claiming a pipeline it is not in")
                .isNull();
    }

    @Test
    void anUnscopedHandleCannotBorrowAnotherExecutionsDiagnosticContext() {
        MDC.put(PipelineAttribution.INCARNATION_MDC_KEY, "new-resource");
        MDC.put(PipelineAttribution.EXECUTION_MDC_KEY, "9");
        try {
            List<ILoggingEvent> written = captured("io.tapstate.connector.demo",
                    () -> new ConnectorLog("demo", PIPELINE).warn("late old-handle warning"));

            assertThat(written).singleElement().satisfies(event -> assertThat(event.getMDCPropertyMap())
                    .containsEntry(PipelineAttribution.MDC_KEY, PIPELINE)
                    .doesNotContainKeys(PipelineAttribution.INCARNATION_MDC_KEY,
                            PipelineAttribution.EXECUTION_MDC_KEY));
            assertThat(MDC.get(PipelineAttribution.INCARNATION_MDC_KEY)).isEqualTo("new-resource");
            assertThat(MDC.get(PipelineAttribution.EXECUTION_MDC_KEY)).isEqualTo("9");
        } finally {
            MDC.remove(PipelineAttribution.INCARNATION_MDC_KEY);
            MDC.remove(PipelineAttribution.EXECUTION_MDC_KEY);
        }
    }

    @Test
    void delayedHandleKeepsItsCapturedOwnerAcrossThreadsAndRestoresTheCaller() throws Exception {
        LogSink.Scope oldRun = new LogSink.Scope("old-resource", 7);
        ConnectorLog log = new ConnectorLog("demo", PIPELINE, oldRun);
        MDC.put(PipelineAttribution.INCARNATION_MDC_KEY, "new-resource");
        MDC.put(PipelineAttribution.EXECUTION_MDC_KEY, "9");
        try {
            List<ILoggingEvent> written = captured("io.tapstate.connector.demo", () -> {
                Thread delayed = new Thread(() -> log.error("old handle finished late"), "old-connector-callback");
                delayed.start();
                joinQuietly(delayed);
            });

            assertThat(written).singleElement().satisfies(event -> assertThat(event.getMDCPropertyMap())
                    .containsEntry(PipelineAttribution.MDC_KEY, PIPELINE)
                    .containsEntry(PipelineAttribution.INCARNATION_MDC_KEY, "old-resource")
                    .containsEntry(PipelineAttribution.EXECUTION_MDC_KEY, "7"));
            assertThat(MDC.get(PipelineAttribution.INCARNATION_MDC_KEY)).isEqualTo("new-resource");
            assertThat(MDC.get(PipelineAttribution.EXECUTION_MDC_KEY)).isEqualTo("9");
        } finally {
            MDC.remove(PipelineAttribution.INCARNATION_MDC_KEY);
            MDC.remove(PipelineAttribution.EXECUTION_MDC_KEY);
        }
    }

    @Test
    void aDriveUsesItsCapturedOwnerForHostFacadeAndSharedPdkLogs(@TempDir Path dir) throws Throwable {
        LogSink.Scope oldRun = new LogSink.Scope("old-resource", 7);
        MDC.put(PipelineAttribution.INCARNATION_MDC_KEY, "new-resource");
        MDC.put(PipelineAttribution.EXECUTION_MDC_KEY, "9");
        try {
            List<ILoggingEvent> written = captured("io.tapstate.connector.shared", () -> {
                try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of(),
                        new PipelineNode(PIPELINE, "src_orders"), null, oldRun)) {
                    connector.underLoader(() -> {
                        TapLogger.warn("Synthetic", "old drive warning");
                        return null;
                    });
                } catch (Throwable failure) {
                    throw new AssertionError(failure);
                }
            });

            assertThat(written).singleElement().satisfies(event -> assertThat(event.getMDCPropertyMap())
                    .containsEntry(PipelineAttribution.MDC_KEY, PIPELINE)
                    .containsEntry(PipelineAttribution.INCARNATION_MDC_KEY, "old-resource")
                    .containsEntry(PipelineAttribution.EXECUTION_MDC_KEY, "7"));
            assertThat(MDC.get(PipelineAttribution.INCARNATION_MDC_KEY)).isEqualTo("new-resource");
            assertThat(MDC.get(PipelineAttribution.EXECUTION_MDC_KEY)).isEqualTo("9");
        } finally {
            MDC.remove(PipelineAttribution.INCARNATION_MDC_KEY);
            MDC.remove(PipelineAttribution.EXECUTION_MDC_KEY);
        }
    }

    @Test
    void sinkPortPassesTheFrozenOwnerToTheMemberSideConnector(@TempDir Path dir) throws Throwable {
        ConnectorRef ref = new ConnectorRef(List.of(Synthetic.countingSink(dir)),
                "synthetic.CountingSink", "2.0.8", null);
        PdkSinkPort port = new PdkSinkPort(connectorId -> ref);
        SinkConfig config = new SinkConfig("demo", Map.of(), WriteMode.UPSERT, DdlPolicy.APPLY,
                null, new PipelineNode(PIPELINE, "sink"));
        LogSink.Scope oldRun = new LogSink.Scope("old-resource", 7);
        MDC.put(PipelineAttribution.INCARNATION_MDC_KEY, "new-resource");
        MDC.put(PipelineAttribution.EXECUTION_MDC_KEY, "9");
        try (PdkSinkWriter writer = (PdkSinkWriter) port.open(config, Map.of(), oldRun)) {
            writer.connector().underLoader(() -> {
                assertThat(MDC.get(PipelineAttribution.MDC_KEY)).isEqualTo(PIPELINE);
                assertThat(MDC.get(PipelineAttribution.INCARNATION_MDC_KEY)).isEqualTo("old-resource");
                assertThat(MDC.get(PipelineAttribution.EXECUTION_MDC_KEY)).isEqualTo("7");
                return null;
            });
            assertThat(MDC.get(PipelineAttribution.INCARNATION_MDC_KEY)).isEqualTo("new-resource");
            assertThat(MDC.get(PipelineAttribution.EXECUTION_MDC_KEY)).isEqualTo("9");
        } finally {
            MDC.remove(PipelineAttribution.INCARNATION_MDC_KEY);
            MDC.remove(PipelineAttribution.EXECUTION_MDC_KEY);
        }
    }

    @Test
    void aDriveNamesItsPipelineForAsLongAsItRunsAndHandsTheSlotBack(@TempDir Path dir) throws Throwable {
        AtomicReference<String> duringTheDrive = new AtomicReference<>();

        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of(),
                new PipelineNode(PIPELINE, "src_orders"), null)) {
            connector.underLoader(() -> {
                duringTheDrive.set(MDC.get(PipelineAttribution.MDC_KEY));
                return null;
            });
        }

        // Everything a connector's own libraries log while the host is driving it is written on this
        // thread, and this is what files those lines against the run they belong to.
        assertThat(duringTheDrive.get()).isEqualTo(PIPELINE);
        assertThat(MDC.get(PipelineAttribution.MDC_KEY))
                .as("a shared thread must not keep a pipeline after the drive that named it returned")
                .isNull();
    }

    @Test
    void aDriveThatBelongsToNoPipelineNamesNone(@TempDir Path dir) throws Throwable {
        AtomicReference<String> duringTheDrive = new AtomicReference<>();

        try (PdkConnector connector = PdkConnector.open("demo", ref(dir), Map.of())) {
            connector.underLoader(() -> {
                duringTheDrive.set(MDC.get(PipelineAttribution.MDC_KEY));
                return null;
            });
        }

        // A schema discovery or a connection test is run on behalf of whoever asked, and any number of
        // pipelines may read the same source: filing its lines against one of them would be inventing
        // an answer rather than leaving the blank a reader can see.
        assertThat(duringTheDrive.get()).isNull();
    }

    @Test
    void theContractsSharedChannelReachesTheHostLogToo() {
        ConnectorLog.installSharedChannel();

        List<ILoggingEvent> written = captured("io.tapstate.connector.shared",
                () -> TapLogger.error("SomeConnector", "cannot open a replication slot"));

        // Until somebody listens, that channel prints straight to standard output: outside the log, the
        // format, the redaction and the read face. It is the channel the shipped connectors actually use.
        assertThat(written).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.ERROR);
            assertThat(event.getFormattedMessage()).contains("cannot open a replication slot");
        });
    }

    /** Collects what reaches {@code loggerName} while {@code body} runs, down to debug. */
    private static List<ILoggingEvent> captured(String loggerName, Runnable body) {
        Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.setContext(logger.getLoggerContext());
        appender.start();
        Level restore = logger.getLevel();
        logger.setLevel(Level.TRACE);
        logger.addAppender(appender);
        try {
            body.run();
        } finally {
            logger.detachAppender(appender);
            appender.stop();
            logger.setLevel(restore);
            MDC.remove(PipelineAttribution.MDC_KEY);
        }
        return List.copyOf(appender.list);
    }

    private static void joinQuietly(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the connector's own thread", interrupted);
        }
    }

    private static ConnectorRef ref(Path dir) {
        return new ConnectorRef(
                List.of(Synthetic.discoverableSource(dir)), "synthetic.Discoverable", "2.0.8", null);
    }
}
