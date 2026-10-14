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
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.e2e.Await;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.JobFailureRegistry;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A retained failed Jet job cannot end a restart waiting for another member's capture. The declarative
 * vocabulary cannot hold a capture claim while delaying ring or table publication, so this case drives
 * the production lifecycle, ownership, capture coordinator and observation publisher over Mongo.
 */
class ADeferredStartOutlivesThePreviousFailedJobIT {

    private static final String PIPELINE = "deferred_start";
    private static final String FAILURE_SWITCH = "fail-submitted-run";
    private static final Duration BUDGET = Duration.ofSeconds(30);
    private static final Duration LEASE = Duration.ofMinutes(5);

    @ParameterizedTest(name = "remote ring already open: {0}")
    @ValueSource(booleans = {false, true})
    void theRestartWaitsThenSubmitsAndStillReportsItsOwnJobFailure(boolean ringAlreadyOpen) {
        DockerGate.require();
        try (MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                    mongo.getReplicaSetUrl("deferred_start"), null, BUDGET))) {
                connection.verify();
                MongoStorePort store = new MongoStorePort(connection, "deferred_start_operators");
                seed(store);
                MiningChainId chain = SourceCaptureResolution.of(
                        StoredArtifacts.requireSource(store.artifacts(), "orders_src")).chainId();
                WorkloadClaimKey captureKey = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE,
                        new StoreBackedPipelineCaptures(store).captureIds(PIPELINE).getFirst());
                assertThat(store.workloadClaims().acquire(captureKey,
                        new WorkloadOwner("node-a", "boot-node-a"), 7, LEASE).acquired()).isTrue();
                if (ringAlreadyOpen) {
                    store.meta().create(chain.value(), null);
                    store.meta().openEpoch(chain.value());
                }

                ClusterProperties properties = new ClusterProperties();
                properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
                ClusterMembershipGate gate = new ClusterMembershipGate(properties);
                gate.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
                assertThat(gate.canCommit(Set.of("node-a", "node-b", "node-c"))).isTrue();
                WorkloadOwner driverOwner = new WorkloadOwner("node-c", "boot-node-c");
                ClusterWorkloadClaims claims = new ClusterWorkloadClaims(store.workloadClaims(), gate);
                CaptureOwnership captureOwnership = new CaptureOwnership(
                        "cluster-a", driverOwner, gate, claims, LEASE);
                PipelineActuationOwnership ownership = new PipelineActuationOwnership(
                        "cluster-a", driverOwner, gate, claims, LEASE, Duration.ofSeconds(30));
                SrsCoordinator srs = new SrsCoordinator(store.meta());
                AtomicInteger attachments = new AtomicInteger();
                CaptureAttacher attacher = (spec, handoff, startTail) -> {
                    assertThat(startTail).as("the remote member keeps the capture claim").isFalse();
                    attachments.incrementAndGet();
                    srs.joinSource(spec.sourceId(), spec.miningChainId(), spec.config().streams());
                    srs.attachConsumer(spec.miningChainId(), spec.consumerId());
                    return new CaptureRun(Optional.of(spec.miningChainId()), true, 0L,
                            Optional.empty(), Optional.of(() -> { }), new CaptureHealth());
                };
                StoreBackedPipelineCaptureCoordinator captures = new StoreBackedPipelineCaptureCoordinator(
                        store, attacher, srs, new SnapshotBuffer(), captureOwnership, Duration.ofHours(1));
                AtomicBoolean failCurrentRun = new AtomicBoolean();
                HazelcastInstance member = member(failCurrentRun);
                Engine engine = new Engine(member);
                EngineLifecycleActuator actuator = new EngineLifecycleActuator(engine, new RunningDagSource(),
                        captures, new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()),
                        ownership);
                try {
                    Clock clock = Clock.systemUTC();
                    PipelineLifecycleService lifecycle = new PipelineLifecycleService(
                            new ArtifactQueryService(store.artifacts()), store.desired(),
                            new AuditGate(record -> { }, clock),
                            id -> store.state().read(id).map(CheckpointDoc::epoch));
                    ConvergenceDriver driver = new ConvergenceDriver(
                            new PipelineConverger(store.desired(), store.state(), actuator, clock), store.desired(),
                            new ObservationPublisher(store.state(), store.observations()), gate::businessEligible,
                            ownership);
                    DAG oldDag = new DAG();
                    oldDag.newVertex("serve.sync_1", ProcessorMetaSupplier.forceTotalParallelismOne(
                            ProcessorSupplier.of(() -> {
                                throw new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                                        Map.of("pipeline", PIPELINE), null);
                            })));
                    engine.submit(PIPELINE, oldDag);
                    Job previous = member.getJet().getJob(PIPELINE);
                    await(previous, JobStatus.FAILED);
                    store.state().create(PIPELINE, StateJson.of(PipelineState.RUNNING),
                            clock.instant());
                    lifecycle.start("e2e", PIPELINE);
                    driver.reconcile();
                    assertThat(store.observations().read(PIPELINE).orElseThrow().state())
                            .isEqualTo(PipelineState.FAILED);

                    lifecycle.stop("e2e", PIPELINE, false);
                    driver.reconcile();
                    lifecycle.start("e2e", PIPELINE);
                    driver.reconcile();
                    for (int pass = 0; pass < 3; pass++) {
                        driver.reconcile();
                        assertThat(store.observations().read(PIPELINE).orElseThrow().state())
                                .as("each pass keeps waiting without failing the unsubmitted run")
                                .isEqualTo(PipelineState.RUNNING);
                        assertThat(store.observations().read(PIPELINE).orElseThrow().failure()).isNull();
                        assertThat(member.getJet().getJob(PIPELINE).getId()).isEqualTo(previous.getId());
                        assertThat(attachments).hasValue(0);
                    }

                    if (!ringAlreadyOpen) {
                        store.meta().create(chain.value(), null);
                        store.meta().openEpoch(chain.value());
                    }
                    store.meta().requestCaptureTables(chain.value(), List.of("orders"));
                    store.meta().publishCaptureTables(chain.value(),
                            store.meta().read(chain.value()).orElseThrow().epoch(), List.of("orders"));
                    driver.reconcile();
                    Job submitted = member.getJet().getJob(PIPELINE);
                    assertThat(submitted.getId()).as("readiness lets the restart submit its own job")
                            .isNotEqualTo(previous.getId());
                    await(submitted, JobStatus.RUNNING);
                    assertThat(attachments).hasValue(1);

                    failCurrentRun.set(true);
                    await(submitted, JobStatus.FAILED);
                    driver.reconcile();
                    assertThat(store.observations().read(PIPELINE).orElseThrow().state())
                            .as("a submitted job's failure is still observed after a capture wait")
                            .isEqualTo(PipelineState.FAILED);
                    assertThat(store.observations().read(PIPELINE).orElseThrow().failure().code())
                            .isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED.code());
                } finally {
                    actuator.stop(PIPELINE, false);
                    captures.close();
                    member.shutdown();
                }
            }
        }
    }

    private static void seed(MongoStorePort store) {
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
                id: deferred_start
                source: [{ id: orders_src, srs: true }]
                settings: { read_mode: cdc_only }
                serve:
                  from: orders_src
                  sync:
                    - id: sync_1
                      source: orders_src
                """)));
    }

    private static HazelcastInstance member(AtomicBoolean failureSwitch) {
        Config config = new Config();
        config.setClusterName("deferred-start-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getUserContext().put(FAILURE_SWITCH, failureSwitch);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void await(Job job, JobStatus status) {
        Await.until("job reaches " + status, BUDGET,
                () -> job.getStatus() == status, () -> job.getStatus().name());
    }

    private static final class RunningDagSource implements DagSource {
        @Override
        public DAG dagFor(String pipelineId) {
            DAG dag = new DAG();
            dag.newVertex("serve.sync_1", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of(Running::new)));
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

    private static final class Running extends AbstractProcessor {
        private HazelcastInstance member;

        @Override
        protected void init(Context context) {
            member = context.hazelcastInstance();
        }

        @Override
        public boolean complete() {
            if (((AtomicBoolean) member.getUserContext().get(FAILURE_SWITCH)).get()) {
                TapstateException failure = new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED,
                        Map.of("pipeline", PIPELINE), null);
                JobFailureRegistry.of(member).record(PIPELINE, failure);
                throw failure;
            }
            return false;
        }
    }
}
