package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.pdk.ConnectorStateNamespace;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.e2e.Await;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.JobFailureRegistry;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The accepted stop/start pair replaces a real Jet job whose startup fails after the pair lands,
 * clearing its durable Mongo capture position and connector notes through the production actuator.
 * A startup barrier is not available in the declarative vocabulary, so the first job is held here.
 */
class ARerunDuringStartupReplacesTheFailedJobIT {

    private static final String PIPELINE = "startup_restart";
    private static final String SOURCE = "orders_src";
    private static final String GATE_KEY = "restart-startup-gate";
    private static final Duration BUDGET = Duration.ofSeconds(30);

    @Test
    void theAcceptedRerunClearsTheOldPositionAndRunsANewJob() throws Exception {
        DockerGate.require();
        try (MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                    mongo.getReplicaSetUrl("startup_restart"), null, BUDGET))) {
                connection.verify();
                MongoStorePort store = new MongoStorePort(connection, "startup_restart_operators");
                seedArtifacts(store);
                StartupGate gate = new StartupGate();
                HazelcastInstance member = startMember(gate);
                try {
                    SrsCoordinator srs = new SrsCoordinator(store.meta());
                    CaptureStarter starter = (spec, handoff) -> {
                        MiningChainId chain = MiningChainId.resolve(spec.config(), spec.srsKey());
                        srs.provisionSource(spec.sourceId(), chain, spec.config().streams(), spec.retention());
                        srs.attachConsumer(chain, spec.pipelineId());
                        return new CaptureRun(Optional.of(chain), false, 0L, Optional.empty(),
                                Optional.of(() -> { }), new CaptureHealth());
                    };
                    PipelineCaptureCoordinator capture = new StoreBackedPipelineCaptureCoordinator(
                            store, starter, srs, new SnapshotBuffer());
                    Engine engine = new Engine(member);
                    EngineLifecycleActuator actuator = new EngineLifecycleActuator(
                            engine, new StartupDagSource(), capture,
                            new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()));
                    Clock clock = Clock.systemUTC();
                    List<AuditRecord> audit = new ArrayList<>();
                    PipelineLifecycleService lifecycle = new PipelineLifecycleService(
                            new ArtifactQueryService(store.artifacts()), store.desired(),
                            new AuditGate(audit::add, clock),
                            id -> store.state().read(id).map(CheckpointDoc::epoch));
                    ConvergenceDriver driver = new ConvergenceDriver(
                            new PipelineConverger(store.desired(), store.state(), actuator, clock),
                            store.desired(), new ObservationPublisher(store.state(), store.observations()));

                    MiningChainId chain = MiningChainId.resolve(
                            SourceCaptureResolution.of(StoredArtifacts.requireSource(store.artifacts(), SOURCE)).config(), null);
                    ChainPosition oldPosition = new ChainPosition(new SourceOrder(1L, 500L), "old-position");
                    String notes = ConnectorStateNamespace.ofShared(chain.value());
                    store.meta().create(chain.value(), null);
                    store.meta().advanceSourceReadOffset(chain.value(), oldPosition);
                    store.keyedState().save(notes, "slot", new byte[]{1});

                    lifecycle.start("e2e", PIPELINE);
                    driver.reconcile();
                    Job first = member.getJet().getJob(PIPELINE);
                    assertThat(first).isNotNull();
                    assertThat(gate.entered.await(BUDGET.toSeconds(), TimeUnit.SECONDS))
                            .as("the first job reached its held startup before the restart is requested")
                            .isTrue();
                    assertThat(store.observations().read(PIPELINE).orElseThrow().state()).isEqualTo(RUNNING);
                    assertThat(store.meta().read(chain.value()).orElseThrow().sourceRead()).isEqualTo(oldPosition);

                    lifecycle.stop("e2e", PIPELINE, true);
                    lifecycle.start("e2e", PIPELINE);
                    assertThat(audit).extracting(AuditRecord::operationId)
                            .containsExactly("pipeline.start", "pipeline.stop", "pipeline.start");
                    gate.release.countDown();
                    assertThat(engine.awaitTerminal(PIPELINE, BUDGET)).as("the previous job ended").isTrue();
                    assertThat(engine.failureOf(PIPELINE)).as("it ended with the released startup failure").isPresent();

                    driver.reconcile();
                    Job fresh = member.getJet().getJob(PIPELINE);
                    assertThat(fresh).as("the accepted restart submitted a different job").isNotNull();
                    assertThat(fresh.getId()).as("the old failed job was replaced").isNotEqualTo(first.getId());
                    awaitRunning(fresh);
                    for (int tick = 0; tick < 5; tick++) {
                        driver.reconcile();
                    }
                    assertThat(member.getJet().getJob(PIPELINE).getId())
                            .as("later ticks keep the same fresh job").isEqualTo(fresh.getId());
                    assertThat(store.meta().read(chain.value()).orElseThrow().sourceRead())
                            .as("the old capture position was cleared before the fresh capture opened").isNull();
                    assertThat(store.keyedState().load(notes, "slot"))
                            .as("the old connector notes were cleared").isEmpty();
                    assertThat(store.observations().read(PIPELINE).orElseThrow().state()).isEqualTo(RUNNING);
                    assertThat(store.observations().read(PIPELINE).orElseThrow().failure()).isNull();
                } finally {
                    gate.release.countDown();
                    member.shutdown();
                }
            }
        }
    }

    private static void seedArtifacts(MongoStorePort store) {
        DslParser parser = new DslParser();
        store.artifacts().saveAll(List.of(parser.parse("""
                version: tapstate/v1
                kind: source
                id: orders_src
                connector: fake
                config: { host: fixture }
                mode: cdc
                tables: [orders]
                """), parser.parse("""
                version: tapstate/v1
                kind: pipeline
                id: startup_restart
                source: [{ id: orders_src, srs: true }]
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: /.*/
                  sync:
                    - id: sink
                      source: orders_src
                      write_mode: upsert
                      ddl: apply
                """)));
    }

    private static HazelcastInstance startMember(StartupGate gate) {
        Config config = new Config();
        config.setClusterName("startup-restart-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getUserContext().put(GATE_KEY, gate);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void awaitRunning(Job job) {
        Await.until("the fresh job becomes running", BUDGET,
                () -> job.getStatus() == JobStatus.RUNNING, () -> job.getStatus().name());
        assertThat(job.getStatus()).as("the fresh job becomes running").isEqualTo(JobStatus.RUNNING);
    }

    private static final class StartupGate {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
    }

    private static final class StartupDagSource implements DagSource {
        private int builds;

        @Override
        public DAG dagFor(String pipelineId) {
            DAG dag = new DAG();
            ProcessorSupplier supplier = ++builds == 1
                    ? ProcessorSupplier.of(FailingStartup::new) : ProcessorSupplier.of(Running::new);
            dag.newVertex("source", ProcessorMetaSupplier.forceTotalParallelismOne(supplier));
            return dag;
        }

        @Override
        public List<PipelineStateHolding> stateHeldBy(String pipelineId) {
            return List.of();
        }

        @Override
        public NestCapacity capacityOf(String pipelineId) {
            return NestCapacity.none();
        }
    }

    private static final class FailingStartup extends AbstractProcessor {
        @Override
        protected void init(Context context) throws Exception {
            StartupGate gate = (StartupGate) context.hazelcastInstance().getUserContext().get(GATE_KEY);
            gate.entered.countDown();
            if (!gate.release.await(BUDGET.toSeconds(), TimeUnit.SECONDS)) {
                throw new IllegalStateException("startup result was not released within the test budget");
            }
            TapstateException failure = new TapstateException(CaptureError.RECOVERY_PROGRESS_UNPROVEN,
                    Map.of("pipeline", PIPELINE, "source", SOURCE), null);
            JobFailureRegistry.of(context.hazelcastInstance()).record(PIPELINE, failure);
            throw failure;
        }
    }

    private static final class Running extends AbstractProcessor {
        @Override
        public boolean complete() {
            // Jet backs off a cooperative processor that reports no progress.
            return false;
        }
    }
}
