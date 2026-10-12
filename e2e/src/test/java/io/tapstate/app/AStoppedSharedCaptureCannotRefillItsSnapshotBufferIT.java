package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.SourceResource;
import io.tapstate.e2e.Await;
import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SourcePlacement;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.runtime.srs.SrsSourceProcessor;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CapturePort;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.ConnectionReport;
import io.tapstate.spi.capture.DiscoveredSchema;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stopped shared-capture owner retains no late snapshot row while its neighbour delivers through Jet.
 * The declarative vocabulary cannot pause a row between cancellation approval and handoff publication,
 * so this case binds a bounded scheduling barrier to the real capture assembly over a Mongo store.
 */
class AStoppedSharedCaptureCannotRefillItsSnapshotBufferIT {

    private static final Duration BOUND = Duration.ofSeconds(10);
    private static final String TARGET = "shared-stop-target";

    @Test
    void theLateHandoffLeavesNoQueueAndTheNeighbourStillDelivers() throws Exception {
        DockerGate.require();
        try (MongoDBContainer mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            mongo.start();
            try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                    mongo.getReplicaSetUrl("shared_stop"), null, BOUND))) {
                connection.verify();
                MongoStorePort store = new MongoStorePort(connection, "shared_stop_operators");
                SourceResource source = seed(store);
                SnapshotBuffer buffer = new SnapshotBuffer();
                List<Envelope> target = new CopyOnWriteArrayList<>();
                HazelcastInstance member = startMember(buffer, target);
                CountDownLatch rest = new CountDownLatch(1);
                FirstRowGate gate = new FirstRowGate();
                Map<String, CaptureRun> runs = new ConcurrentHashMap<>();
                SrsCoordinator srs = new SrsCoordinator(store.meta());
                CaptureRunUnit unit = new CaptureRunUnit(new ThreeRows(rest), srs, store.meta(), member);
                CaptureAttacher attacher = (spec, handoff, startTail) -> {
                    CaptureRun run = unit.begin(spec,
                            spec.pipelineId().equals("p") ? gate.hold(handoff) : handoff, startTail);
                    runs.put(spec.pipelineId(), run);
                    return run;
                };
                StoreBackedPipelineCaptureCoordinator captures =
                        new StoreBackedPipelineCaptureCoordinator(store, attacher, srs, buffer);
                try {
                    String ring = SourceCaptureResolution.of(source).ringName();
                    captures.startCapture("p");
                    captures.startCapture("q");
                    assertThat(gate.approved.await(BOUND.toSeconds(), TimeUnit.SECONDS)).isTrue();
                    captures.stopCapture("p", false);
                    assertThat(gate.interrupted.await(BOUND.toSeconds(), TimeUnit.SECONDS)).isTrue();
                    assertThat(buffer.snapshotState("p", ring)).isEqualTo(SnapshotBuffer.SnapshotState.UNDECLARED);
                    assertThat(buffer.drain("p", ring)).as("released before the first handoff resumes").isEmpty();
                    rest.countDown();
                    gate.resume.countDown();
                    assertThat(runs.get("p").awaitLoaded(BOUND)).isTrue();
                    assertThat(runs.get("q").awaitLoaded(BOUND)).isTrue();

                    DAG dag = new DAG();
                    dag.edge(Edge.between(dag.newVertex("source", SrsSourceProcessor.snapshotOnlyMetaSupplier(
                                    "q", ring, "orders", 1L, null, SourcePlacement.anyMember())),
                            dag.newVertex("target", ProcessorMetaSupplier.forceTotalParallelismOne(
                                    ProcessorSupplier.of(CollectRows::new)))));
                    Job job = member.getJet().newJob(dag);
                    Await.until("the shared neighbour to deliver its full snapshot", BOUND,
                            () -> target.size() == 3, () -> "rows=" + target + ", job=" + job.getStatus());
                    assertThat(target).extracting(row -> row.after().get("id")).containsExactly(1L, 2L, 3L);
                    assertThat(job.getStatus()).isEqualTo(JobStatus.RUNNING);
                    assertThat(captures.captureFailure("q")).isEmpty();
                    assertThat(buffer.drain("p", ring))
                            .as("the stopped owner's completed load cannot resurrect its released queue").isEmpty();
                } finally {
                    rest.countDown();
                    gate.resume.countDown();
                    captures.stopCapture("p", false);
                    captures.stopCapture("q", false);
                    captures.close();
                    member.shutdown();
                }
            }
        }
    }

    private static SourceResource seed(MongoStorePort store) {
        DslParser parser = new DslParser();
        SourceResource source = (SourceResource) parser.parse("""
                version: tapstate/v1
                kind: source
                id: orders_source
                connector: fake
                config: { host: fixture }
                mode: cdc
                tables: [orders]
                """);
        store.artifacts().save(source);
        for (String pipeline : List.of("p", "q")) {
            store.artifacts().save(parser.parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: [{ id: orders_source, srs: true }]
                    settings: { read_mode: snapshot_only }
                    serve:
                      from: orders_source
                      sync: [{ source: target }]
                    """.formatted(pipeline)));
        }
        return source;
    }

    private static HazelcastInstance startMember(SnapshotBuffer buffer, List<Envelope> target) {
        Config config = new Config();
        config.setClusterName("shared-stop-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, buffer);
        config.getUserContext().put(TARGET, target);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static final class CollectRows extends AbstractProcessor {
        private List<Envelope> target;

        @Override
        @SuppressWarnings("unchecked")
        protected void init(Context context) {
            target = (List<Envelope>) context.hazelcastInstance().getUserContext().get(TARGET);
        }

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            target.add((Envelope) item);
            return true;
        }
    }

    private static final class FirstRowGate {
        final CountDownLatch approved = new CountDownLatch(1);
        final CountDownLatch interrupted = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);

        CaptureHandoff hold(CaptureHandoff delegate) {
            return new CaptureHandoff() {
                @Override
                public void accept(Envelope row) {
                    approved.countDown();
                    long deadline = System.nanoTime() + BOUND.toNanos();
                    boolean restoreInterrupt = false;
                    try {
                        while (true) {
                            try {
                                if (!resume.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                                    throw new AssertionError("the approved row was not released within the bound");
                                }
                                break;
                            } catch (InterruptedException cancelled) {
                                restoreInterrupt = true;
                                interrupted.countDown();
                            }
                        }
                    } finally {
                        if (restoreInterrupt) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    delegate.accept(row);
                }

                @Override
                public void loaded(String table) {
                    delegate.loaded(table);
                }
            };
        }
    }

    private record ThreeRows(CountDownLatch rest) implements CapturePort {
        @Override
        public CaptureBatch snapshot(CaptureConfig config) {
            return new CaptureBatch() {
                private int next = 1;

                @Override
                public boolean hasNext() {
                    if (next == 2) {
                        try {
                            if (!rest.await(BOUND.toSeconds(), TimeUnit.SECONDS)) {
                                throw new AssertionError("the remaining source rows were not released within the bound");
                            }
                        } catch (InterruptedException cancelled) {
                            Thread.currentThread().interrupt();
                            throw new java.util.concurrent.CancellationException("the held source read was interrupted");
                        }
                    }
                    return next <= 3;
                }

                @Override
                public Envelope next() {
                    return Envelope.read(1L, "orders", Map.of("id", (long) next++), Map.of());
                }

                @Override
                public Optional<SourcePosition> seam() {
                    return Optional.of(new SourcePosition("seam-0"));
                }

                @Override
                public void close() { }
            };
        }

        @Override
        public Subscription cdc(CaptureConfig config, CaptureStart start, CaptureListener listener) {
            throw new UnsupportedOperationException("a snapshot-only source opens no tail");
        }

        @Override
        public ConnectionReport testConnection(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DiscoveredSchema discoverSchema(CaptureConfig config) {
            throw new UnsupportedOperationException();
        }
    }
}
