package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.adapters.pdk.SyntheticCaptureLogJars;
import io.tapstate.control.core.PipelineIncarnationService;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SnapshotWorkers;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/** Real PDK handles driven by the actual single-member admission and submission path. */
class AdmittedCdcOnlyLogOwnerTest {

    @Test
    void aDirectCdcOnlyPreparationOpensNoPdkHandleBeforeSubmission(@TempDir Path temporary) throws Exception {
        requireDeferredPdkOpening(temporary, false);
    }

    @Test
    void aSharedCdcOnlyPreparationOpensNoPdkHandleBeforeSubmission(@TempDir Path temporary) throws Exception {
        requireDeferredPdkOpening(temporary, true);
    }

    @Test
    void aDirectCdcOnlyNativeDriveFilesBothChannelsUnderItsActualSubmittedExecution(@TempDir Path temporary)
            throws Exception {
        requireActualScopedLogs(temporary, false);
    }

    @Test
    void aSharedCdcOnlyNativeDriveFilesBothChannelsUnderItsActualSubmittedExecution(@TempDir Path temporary)
            throws Exception {
        requireActualScopedLogs(temporary, true);
    }

    private void requireDeferredPdkOpening(Path temporary, boolean shared) throws Exception {
        try (Fixture fixture = new Fixture(temporary, shared);
             LifecycleActuator.PreparedStart prepared = fixture.actuator.prepareStart(fixture.pipeline)) {
            assertThat(fixture.scopes.current(fixture.pipeline)).isPresent();
            assertThat(fixture.engine.executionJob(fixture.pipeline)).isEmpty();
            assertThat(fixture.handlesOpened).as("preparation reserves capture without opening a native PDK handle")
                    .hasValue(0);
            prepared.submit();
            fixture.requireActualScopedDrive();
        }
    }

    private void requireActualScopedLogs(Path temporary, boolean shared) throws Exception {
        try (Fixture fixture = new Fixture(temporary, shared);
             LifecycleActuator.PreparedStart prepared = fixture.actuator.prepareStart(fixture.pipeline)) {
            prepared.submit();
            fixture.requireActualScopedDrive();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final String pipeline;
        private final InMemoryStorePort store = new InMemoryStorePort();
        private final Map<String, String> incarnationIds = new HashMap<>();
        private final ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        private final AtomicInteger handlesOpened = new AtomicInteger();
        private final CountDownLatch tailMessage = new CountDownLatch(1);
        private final RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        private final Logger logger = (Logger) LoggerFactory.getLogger("io.tapstate.connector");
        private final PipelineLogAppender logAppender = new PipelineLogAppender(sink, new SecretRedactor());
        private final AppenderBase<ILoggingEvent> signal = new AppenderBase<>() {
            @Override protected void append(ILoggingEvent event) {
                if (pipeline.equals(event.getMDCPropertyMap().get(PipelineAttribution.MDC_KEY))
                        && event.getFormattedMessage().contains("native CDC shared message")) {
                    tailMessage.countDown();
                }
            }
        };
        private HazelcastInstance member;
        private SnapshotWorkers workers;
        private CaptureRunUnit unit;
        private StoreBackedPipelineCaptureCoordinator coordinator;
        private Engine engine;
        private EngineLifecycleActuator actuator;

        private Fixture(Path temporary, boolean shared) {
            pipeline = shared ? "admitted_shared_cdc" : "admitted_direct_cdc";
            String source = "admitted_cdc_source";
            String connector = "admitted_cdc_log";
            var reference = SyntheticCaptureLogJars.cdcSource(temporary);
            store.artifacts().save(new SourceResource(source, null, connector, Map.of(), SourceMode.CDC,
                    List.of(TableRef.literal("orders")), null, null));
            store.artifacts().save(new PipelineResource(pipeline, null, List.of(SourceRef.spec(source, shared)),
                    null, new ViewBlock.Inline("orders_view", FromRef.literal("orders"), "id", null), null,
                    new Settings(null, null, null, null, ReadMode.CDC_ONLY, "latest"), null));
            store.schemas().save(new DiscoveredSourceModel(source, connector, 1L, new SourceModel(List.of(
                    new SourceTable("orders", List.of(new SourceField("id", "bigint", TapstateType.INT64)),
                            List.of("id"), List.of())))));
            try {
                Config config = new Config();
                config.setClusterName("admitted-cdc-log-" + System.nanoTime());
                config.setProperty("hazelcast.phone.home.enabled", "false");
                config.setProperty("hazelcast.shutdownhook.enabled", "false");
                config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
                config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
                config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
                config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
                config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
                config.addRingBufferConfig(new RingbufferConfig("srs.*").setCapacity(8)
                        .setInMemoryFormat(InMemoryFormat.OBJECT).setTimeToLiveSeconds(0).setBackupCount(0)
                        .setRingbufferStoreConfig(new RingbufferStoreConfig().setEnabled(true)
                                .setFactoryImplementation(new SrsLogRingbufferStoreFactory(store.srsLog()))));
                member = Hazelcast.newHazelcastInstance(config);
                member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, store.meta());
                member.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, store.srsLog());
                workers = new SnapshotWorkers(1, 1);
                SnapshotBuffer buffer = new SnapshotBuffer(2, 1, 1_024, 512);
                SrsCoordinator srs = new SrsCoordinator(store.meta());
                PdkCapturePort port = new PdkCapturePort(id -> {
                    assertThat(id).isEqualTo(connector);
                    handlesOpened.incrementAndGet();
                    return reference;
                }, store.keyedState());
                unit = new CaptureRunUnit(port, srs, store.meta(), member, buffer, workers);
                ClusterProperties properties = new ClusterProperties();
                properties.setProfile(ClusterProperties.Profile.SINGLE);
                coordinator = (StoreBackedPipelineCaptureCoordinator) new DataPlaneActuationConfiguration().pipelineCaptureCoordinator(
                        store, unit, srs, buffer, CaptureOwnership.single(), properties, LifecycleWorkDispatcher.inline());
                ArtifactStore identities = new ArtifactStore() {
                    @Override public void saveAll(List<Resource> rows) { store.artifacts().saveAll(rows); }
                    @Override public Optional<Resource> get(String id) { return store.artifacts().get(id); }
                    @Override public List<Resource> list() { return store.artifacts().list(); }
                    @Override public synchronized Optional<String> pipelineIncarnationId(String id) {
                        return Optional.ofNullable(incarnationIds.get(id));
                    }
                    @Override public synchronized Optional<String> ensurePipelineIncarnationId(String id, String candidate) {
                        if (get(id).isEmpty()) { return Optional.empty(); }
                        return Optional.of(incarnationIds.computeIfAbsent(id, ignored -> candidate));
                    }
                };
                engine = new Engine(member);
                actuator = new EngineLifecycleActuator(engine, new IdleDagSource(), coordinator,
                        new NestStateTeardown(member, store.operatorStateStores()),
                        PipelineActuationOwnership.single("admitted-cdc-log", new InMemoryWorkloadClaimStore()),
                        new PipelineIncarnationService(identities), scopes);
                logAppender.setContext(logger.getLoggerContext()); logAppender.start(); logger.addAppender(logAppender);
                signal.setContext(logger.getLoggerContext()); signal.start(); logger.addAppender(signal);
            } catch (RuntimeException | Error failure) {
                try { close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }

        private void requireActualScopedDrive() throws InterruptedException {
            Engine.ExecutionJob execution = engine.executionJob(pipeline).orElseThrow();
            assertThat(scopes.current(pipeline)).contains(execution.scope());
            assertThat(incarnationIds.get(pipeline)).isEqualTo(execution.scope().pipelineIncarnationId());
            assertThat(execution.job().bootId()).isEqualTo(engine.submissionBootId());
            assertThat(tailMessage.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(handlesOpened).hasValue(1);
            LogSink.Scope actual = new LogSink.Scope(execution.scope().pipelineIncarnationId(),
                    execution.scope().executionGeneration());
            assertThat(sink.tail(pipeline, actual)).hasSize(4);
            assertThat(sink.tail(pipeline, actual)).extracting(LogLine::message)
                    .anySatisfy(message -> assertThat(message).contains("native init context message"))
                    .anySatisfy(message -> assertThat(message).contains("native init shared message"))
                    .anySatisfy(message -> assertThat(message).contains("native CDC context message"))
                    .anySatisfy(message -> assertThat(message).contains("native CDC shared message"));
            assertThat(sink.tail(pipeline)).isEmpty();
            assertThat(sink.tail("other_pipeline", actual)).isEmpty();
            assertThat(sink.tail(pipeline, new LogSink.Scope(actual.pipelineIncarnationId(),
                    actual.executionGeneration() + 1))).isEmpty();
        }

        @Override public void close() {
            Throwable failure = null;
            for (Runnable cleanup : List.<Runnable>of(
                    () -> { if (engine != null) { engine.cancel(pipeline); } },
                    () -> { if (coordinator != null) { coordinator.close(); } },
                    () -> { if (unit != null) { unit.close(); } },
                    () -> { if (workers != null) { workers.close(); } },
                    () -> { if (member != null) { member.shutdown(); } },
                    () -> { logger.detachAppender(logAppender); logAppender.stop(); },
                    () -> { logger.detachAppender(signal); signal.stop(); })) {
                try { cleanup.run(); }
                catch (RuntimeException | Error ended) {
                    if (failure == null) { failure = ended; }
                    else if (failure != ended) { failure.addSuppressed(ended); }
                }
            }
            if (failure instanceof Error defect) { throw defect; }
            if (failure instanceof RuntimeException refused) { throw refused; }
        }
    }
}
