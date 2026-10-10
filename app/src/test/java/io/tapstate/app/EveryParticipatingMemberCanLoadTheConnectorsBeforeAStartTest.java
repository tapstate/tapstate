package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.DAG;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.RegistryConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.NodeParallelism;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Before a run starts anything, every member it would take part on proves it can load the connectors the
 * pipeline's sinks open - through its own provisioner, from the shared registry into its own plugins directory -
 * and that every member loads the same artifact. A member that cannot refuses the start with a code naming it,
 * the connector and why, and nothing has been started: no capture opened, no plan written, no job submitted.
 *
 * <p>Three members in one process, each with a provisioner of its own bound where its sinks look for one. The
 * member that fails is given the real provisioner over a plugins directory that cannot be created, or over a
 * registry that cannot produce the artifact's bytes, so the reason it answers with is the one a member would.
 * The cluster is formed once for every case; each case starts a pipeline of its own and binds its own
 * provisioners.
 */
class EveryParticipatingMemberCanLoadTheConnectorsBeforeAStartTest {

    private static final int FIRST_PORT = 15861;
    private static final AtomicInteger PIPELINES = new AtomicInteger();
    private static final List<HazelcastInstance> MEMBERS = new ArrayList<>();

    private static HazelcastInstance m1;
    private static HazelcastInstance m2;
    private static HazelcastInstance m3;

    private final CountDownLatch released = new CountDownLatch(1);
    private final String pipe = "p" + PIPELINES.incrementAndGet();

    @TempDir
    Path scratch;

    @BeforeAll
    static void formACluster() {
        String cluster = "connector-readiness-" + System.nanoTime();
        m1 = member(cluster, "m1");
        m2 = member(cluster, "m2");
        m3 = member(cluster, "m3");
    }

    @AfterAll
    static void stopTheCluster() {
        MEMBERS.forEach(member -> member.getLifecycleService().terminate());
    }

    @AfterEach
    void letGoOfThisCase() {
        released.countDown();
        MEMBERS.forEach(member -> member.getUserContext().remove(
                PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY));
        Job job = m1.getJet().getJob(pipe);
        if (job != null) {
            job.cancel();
        }
    }

    @Test
    void aMemberThatCannotStageTheConnectorRefusesTheStartBeforeAnythingIsStarted() throws Exception {
        Path aFile = Files.createFile(scratch.resolve("not-a-directory"));
        bind(m1, new Loading("h1"));
        bind(m2, new RegistryConnectorProvisioner(new Registry(true), new ConnectorIntrospector(),
                aFile.resolve("plugins")));
        bind(m3, new Loading("h1"));
        Started started = new Started();

        TapstateException refused = catchThrowableOfType(() -> started.actuator(readiness()).start(pipe),
                TapstateException.class);

        assertThat(refused.code()).isEqualTo(ActuationError.CONNECTOR_UNAVAILABLE_ON_MEMBER);
        assertThat(refused.args()).containsEntry("pipeline", pipe).containsEntry("member", "m2")
                .containsEntry("connector", "pg");
        // The wrapper says what was being done and the cause says why it failed; the reason carries both.
        assertThat((String) refused.args().get("reason"))
                .startsWith("UncheckedIOException: staging connector artifact " + aFile.resolve("plugins"))
                .matches("(?s).* \\(\\w+Exception: .+\\)");
        started.nothingWasStarted();
    }

    @Test
    void aMemberThatCannotFetchTheArtifactsBytesRefusesTheStartWithItsOwnCodedReason() {
        bind(m1, new Loading("h1"));
        bind(m2, new Loading("h1"));
        bind(m3, new RegistryConnectorProvisioner(new Registry(false), new ConnectorIntrospector(),
                scratch.resolve("plugins")));
        Started started = new Started();

        TapstateException refused = catchThrowableOfType(() -> started.actuator(readiness()).start(pipe),
                TapstateException.class);

        assertThat(refused.code()).isEqualTo(ActuationError.CONNECTOR_UNAVAILABLE_ON_MEMBER);
        assertThat(refused.args()).containsEntry("member", "m3")
                .containsEntry("reason", "connector.load-failed {connector=pg}");
        started.nothingWasStarted();
    }

    @Test
    void aMemberWithNoConnectorRegistryRefusesTheStart() {
        // The submitting member itself: the member a start runs on is asked like any other.
        bind(m2, new Loading("h1"));
        bind(m3, new Loading("h1"));
        Started started = new Started();

        TapstateException refused = catchThrowableOfType(() -> started.actuator(readiness()).start(pipe),
                TapstateException.class);

        assertThat(refused.code()).isEqualTo(ActuationError.CONNECTOR_UNAVAILABLE_ON_MEMBER);
        assertThat(refused.args()).containsEntry("member", "m1")
                .containsEntry("reason", "the member has no connector registry to load connectors from");
        started.nothingWasStarted();
    }

    @Test
    void membersLoadingDifferentArtifactsForOneConnectorRefuseTheStart() {
        bind(m1, new Loading("h1"));
        bind(m2, new Loading("h1"));
        bind(m3, new Loading("h2"));
        Started started = new Started();

        TapstateException refused = catchThrowableOfType(() -> started.actuator(readiness()).start(pipe),
                TapstateException.class);

        assertThat(refused.code()).isEqualTo(ActuationError.CONNECTOR_DIFFERS_ACROSS_MEMBERS);
        assertThat(refused.args()).containsEntry("connector", "pg")
                .containsEntry("artifacts", "m1=h1, m2=h1, m3=h2");
        started.nothingWasStarted();
    }

    @Test
    void aMemberThatDoesNotAnswerInTimeRefusesTheStart() {
        bind(m1, new Loading("h1"));
        bind(m2, connectorId -> {
            try {
                released.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return new ConnectorRef(List.of(), "x", "1.0", null, null, "h1", false);
        });
        bind(m3, new Loading("h1"));
        Started started = new Started();

        TapstateException refused = catchThrowableOfType(
                () -> started.actuator(new HazelcastConnectorReadiness(m1, Duration.ofMillis(300))).start(pipe),
                TapstateException.class);

        assertThat(refused.code()).isEqualTo(ActuationError.CONNECTOR_UNAVAILABLE_ON_MEMBER);
        assertThat(refused.args()).containsEntry("member", "m2").containsEntry("connector", "pg")
                .containsEntry("reason", "no answer within 300ms");
        started.nothingWasStarted();
    }

    @Test
    void whenEveryMemberLoadsEveryConnectorTheRunStartsHavingAskedEachOfThem() {
        Loading one = new Loading("h1");
        Loading two = new Loading("h1");
        Loading three = new Loading("h1");
        bind(m1, one);
        bind(m2, two);
        bind(m3, three);
        Started started = new Started(Map.of("serve.orders", "pg", "view.order_state", "mongodb"));

        started.actuator(readiness()).start(pipe);

        assertThat(started.captures).containsExactly(pipe);
        assertThat(started.plans).hasSize(1);
        assertThat(m1.getJet().getJob(pipe)).isNotNull();
        assertThat(one.asked).containsExactly("mongodb", "pg");
        assertThat(two.asked).containsExactly("mongodb", "pg");
        assertThat(three.asked).containsExactly("mongodb", "pg");
    }

    @Test
    void aSinksWritersShareAConnectorOnlyWhereEveryMemberLoadedItAsCertifiedToBeShared() {
        bind(m1, new Loading("h1", true));
        bind(m2, new Loading("h1", true));
        bind(m3, new Loading("h1", false));

        assertThat(readiness().requireEveryMemberCanLoad(pipe, Set.of("pg"))).isEmpty();

        bind(m3, new Loading("h1", true));

        assertThat(readiness().requireEveryMemberCanLoad(pipe, Set.of("pg"))).containsExactly("pg");
    }

    @Test
    void theRunsPlanSaysItsSinksWritersShareTheConnectorEveryMemberLoadedAsCertified() {
        bind(m1, new Loading("h1", true));
        bind(m2, new Loading("h1", true));
        bind(m3, new Loading("h1", true));
        Started started = new Started();

        started.actuator(readiness()).start(pipe);

        assertThat(started.plans).singleElement().satisfies(plan -> assertThat(plan.nodes())
                .filteredOn(node -> node.node().equals("serve.orders")).singleElement()
                .extracting(node -> node.resources().connectorMode(), node -> node.resources().connectorInstances())
                .containsExactly("shared", 3));
    }

    @Test
    void aPipelineWhoseSinksOpenNoConnectorAsksNoMember() {
        Loading one = new Loading("h1");
        bind(m1, one);
        Started started = new Started(Map.of());

        started.actuator(readiness()).start(pipe);

        assertThat(started.captures).containsExactly(pipe);
        assertThat(one.asked).isEmpty();
    }

    private HazelcastConnectorReadiness readiness() {
        return new HazelcastConnectorReadiness(m1, Duration.ofSeconds(20));
    }

    private static void bind(HazelcastInstance member, ConnectorProvisioner provisioner) {
        member.getUserContext().put(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, provisioner);
    }

    /** A member of {@code cluster} named {@code nodeId}, as the app names each member it starts. */
    private static HazelcastInstance member(String cluster, String nodeId) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE, nodeId);
        // A range of its own: two clusters sharing one find each other's members while a build runs them.
        config.getNetworkConfig().setPort(FIRST_PORT).setPortAutoIncrement(true).setPortCount(3);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(
                List.of("127.0.0.1:" + FIRST_PORT, "127.0.0.1:" + (FIRST_PORT + 1), "127.0.0.1:" + (FIRST_PORT + 2)));
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        MEMBERS.add(member);
        return member;
    }

    /** What a start started: the captures it opened and the plans it wrote, over the idle stand-in topology. */
    private final class Started {

        private final Map<String, String> sinkConnectors;
        private final List<String> captures = Collections.synchronizedList(new ArrayList<>());
        private final List<ExecutionPlan> plans = Collections.synchronizedList(new ArrayList<>());

        Started() {
            this(Map.of("serve.orders", "pg"));
        }

        Started(Map<String, String> sinkConnectors) {
            this.sinkConnectors = sinkConnectors;
        }

        EngineLifecycleActuator actuator(ConnectorReadiness readiness) {
            IdleDagSource idle = new IdleDagSource();
            DagSource source = new DagSource() {
                @Override
                public NestCapacity capacityOf(String pipelineId) {
                    return NestCapacity.none();
                }

                @Override
                public DAG dagFor(String pipelineId) {
                    return idle.dagFor(pipelineId);
                }

                @Override
                public List<PipelineStateHolding> stateHeldBy(String pipelineId) {
                    return idle.stateHeldBy(pipelineId);
                }

                @Override
                public Map<String, String> sinkConnectors(String pipelineId) {
                    return sinkConnectors;
                }

                @Override
                public PlannedDag plannedDagFor(String pipelineId, ExecutionFence fence) {
                    // The idle stand-in topology, planned as sinks two per member on the three members: six
                    // writers, so a connector per member reads differently from a connector per writer.
                    Map<String, NodeParallelism> nodes = new LinkedHashMap<>();
                    sinkConnectors.keySet().forEach(sink -> nodes.put(sink, new NodeParallelism(sink, 6,
                            NodeParallelism.Origin.EXPLICIT, NodeParallelism.Scope.NATIVE, 3, 2, 6, List.of())));
                    return new PlannedDag(dagFor(pipelineId), new ExecutionShape(3, nodes, Map.of()),
                            List.of("m1", "m2", "m3"), Map.of(), Map.of());
                }
            };
            PipelineCaptureCoordinator capture = new PipelineCaptureCoordinator() {
                @Override
                public void startCapture(String pipelineId) {
                    captures.add(pipelineId);
                }

                @Override
                public void stopCapture(String pipelineId, boolean purgeState) {
                }
            };
            ExecutionPlanRecorder recorder = new ExecutionPlanRecorder() {
                @Override
                public void record(ExecutionPlan plan) {
                    plans.add(plan);
                }

                @Override
                public void forget(String pipelineId) {
                }
            };
            return new EngineLifecycleActuator(new Engine(m1), source, capture,
                    new NestStateTeardown(m1, new InMemoryKeyedStateStore(), new InMemoryNestDeadLetterStore()),
                    PipelineActuationOwnership.single(), recorder,
                    Clock.fixed(Instant.parse("2026-09-26T10:00:00Z"), ZoneOffset.UTC), readiness);
        }

        void nothingWasStarted() {
            assertThat(captures).as("no capture was opened").isEmpty();
            assertThat(plans).as("no plan was written").isEmpty();
            assertThat(m1.getJet().getJob(pipe)).as("no job was submitted").isNull();
        }
    }

    /**
     * Loads every connector as the artifact {@code contentHash}, certified to be shared or not, remembering which
     * it was asked for, in order.
     */
    private static final class Loading implements ConnectorProvisioner {

        private final String contentHash;
        private final boolean shareSafe;
        private final List<String> asked = Collections.synchronizedList(new ArrayList<>());

        Loading(String contentHash) {
            this(contentHash, false);
        }

        Loading(String contentHash, boolean shareSafe) {
            this.contentHash = contentHash;
            this.shareSafe = shareSafe;
        }

        @Override
        public ConnectorRef resolve(String connectorId) {
            asked.add(connectorId);
            return new ConnectorRef(List.of(), "x", "1.0", null, null, contentHash, shareSafe);
        }
    }

    /** One connector, {@code pg}, registered as the artifact {@code h1}, whose bytes it can or cannot produce. */
    private static final class Registry implements ConnectorRegistry {

        private final boolean hasBytes;

        Registry(boolean hasBytes) {
            this.hasBytes = hasBytes;
        }

        @Override
        public RegistrationOutcome register(String connectorId, String pdkApiVersion, RegistrationSource source,
                byte[] artifact) {
            throw new UnsupportedOperationException("nothing registers during a start");
        }

        @Override
        public List<ConnectorRegistration> list() {
            return List.of(new ConnectorRegistration("pg", "h1", "1.0", RegistrationSource.SEED));
        }

        @Override
        public Optional<byte[]> artifact(String contentHash) {
            return hasBytes && "h1".equals(contentHash) ? Optional.of(new byte[] {1, 2, 3}) : Optional.empty();
        }

        @Override
        public boolean hasArtifact(String contentHash) {
            return hasBytes && "h1".equals(contentHash);
        }
    }
}
