package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Workspace;
import io.tapstate.core.model.Resource;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a pipeline taken down for good lets go of the state its joins kept, in both places it is kept.
 *
 * <p>Left behind, that state is inherited by whatever is applied under the same id next: the mirrors hold
 * dimension rows as they were when the last run ended, so the rebuilt run widens fresh fact rows with values
 * the source no longer holds. Nothing reports it - the pipeline runs, the row count is right, every row is
 * present, and each column reads as plausible. Only comparing the target against the source by hand finds it.
 *
 * <p>The state is seeded directly rather than by running a job. What is under test is the letting go, and a
 * run would only be a slower way of putting entries where these put them.
 */
class AStoppedPipelineLetsGoOfItsJoinStateTest {

    private static final String PIPELINE = "wide";
    private static final String STEP = "widen";

    /** The mirror of the driving rows, which one join step keeps whatever its sources are. */
    private static final String FACT_NAMESPACE = "join." + PIPELINE + "." + STEP + ".fact";

    /** The dimension mirror and the reverse index of the one dimension this join reads. */
    private static final String DIM_NAMESPACE = "join." + PIPELINE + "." + STEP + ".dim.c";
    private static final String INDEX_NAMESPACE = "join." + PIPELINE + "." + STEP + ".index.c";

    /**
     * The same pair under the alias the join is driven from. No run writes to these - the driving source is
     * mirrored as the fact, not as a dimension - and they are named anyway, because which alias the SQL
     * drives from is the query's answer rather than the wiring's, and a drop that finds nothing costs a
     * round trip while a name left unsaid strands its entries for good.
     */
    private static final String FACT_ALIAS_DIM_NAMESPACE = "join." + PIPELINE + "." + STEP + ".dim.o";
    private static final String FACT_ALIAS_INDEX_NAMESPACE = "join." + PIPELINE + "." + STEP + ".index.o";

    /** A namespace belonging to some other pipeline, which no stop of this one may touch. */
    private static final String OTHER_PIPELINE_NAMESPACE = "join.other_pipe.some_step.dim.c";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("join-teardown-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("the namespaces a stop drops include the mirrors and the index every join step keeps")
    void namesEveryNamespaceTheJoinKeepsStateIn() {
        InMemoryStorePort store = seeded(JOIN_PIPELINE);

        Set<String> namespaces = new StoreBackedDagSource(store).stateNamespacesOf(PIPELINE);

        assertThat(namespaces).containsExactlyInAnyOrder(FACT_NAMESPACE, DIM_NAMESPACE, INDEX_NAMESPACE,
                FACT_ALIAS_DIM_NAMESPACE, FACT_ALIAS_INDEX_NAMESPACE);
    }

    /**
     * <b>The case the defect is.</b> A pipeline whose only step is a join used to be answered with no
     * namespaces at all, because the answer was reached only for a pipeline that nests - so its mirrors were
     * never named, never noted, and never dropped. The store cannot list what it holds, deliberately, so
     * what a stop does not name by then is state nothing will ever name again.
     */
    @Test
    @DisplayName("a stop drops the join state in both places it is kept, and drops nobody else's")
    void stoppingLetsGoOfWhatTheJoinKeptInMemoryAndOnDisk() {
        InMemoryStorePort store = seeded(JOIN_PIPELINE);
        seedState(store, FACT_NAMESPACE, DIM_NAMESPACE, INDEX_NAMESPACE, OTHER_PIPELINE_NAMESPACE);

        actuator(store).stop(PIPELINE);

        assertThat(store.keyedState().load(FACT_NAMESPACE, "k")).isEmpty();
        assertThat(store.keyedState().load(DIM_NAMESPACE, "k")).isEmpty();
        assertThat(store.keyedState().load(INDEX_NAMESPACE, "k")).isEmpty();
        // The map too, not only the store behind it. Dropping the documents alone leaves a live map holding
        // the entries and writing them straight back through the store as it is asked for them, so the
        // entries reappear a moment after they were deleted with nothing saying they had gone.
        assertThat(member.getMap(FACT_NAMESPACE).size()).isZero();
        assertThat(member.getMap(DIM_NAMESPACE).size()).isZero();
        assertThat(member.getMap(INDEX_NAMESPACE).size()).isZero();
        assertThat(store.keyedState().load(OTHER_PIPELINE_NAMESPACE, "k"))
                .describedAs("a stop drops what this pipeline named, never what merely looks like it")
                .isPresent();
    }

    /**
     * A run says where it keeps state as it starts, so state written by a run whose join step has since been
     * edited away is still dropped: worked out from the pipeline as it now reads, the names would be none.
     */
    @Test
    @DisplayName("a stop lets go of where the run wrote, not where the pipeline edited under it would")
    void stateTheRunKeptIsDroppedThoughTheJoinStepWasTakenOutWhileItRan() {
        InMemoryStorePort store = seeded(JOIN_PIPELINE);
        seedState(store, FACT_NAMESPACE, DIM_NAMESPACE, INDEX_NAMESPACE);
        EngineLifecycleActuator actuator = actuator(store);
        actuator.start(PIPELINE);
        // Taking the join out while the run is up is an ordinary edit: an apply never asks what state the
        // pipeline is in, and the run already up goes on keeping state where it was built to keep it.
        seeded(store, PIPELINE_WITHOUT_JOIN);

        actuator.stop(PIPELINE);

        assertThat(store.keyedState().load(FACT_NAMESPACE, "k"))
                .describedAs("what the run kept is dropped by the names it ran under, not the ones it ends under")
                .isEmpty();
        assertThat(store.keyedState().load(DIM_NAMESPACE, "k")).isEmpty();
        assertThat(store.keyedState().load(INDEX_NAMESPACE, "k")).isEmpty();
    }

    @Test
    void aPipelineThatJoinsNothingHasNoJoinNamespaceToBeLetGoOf() {
        InMemoryStorePort store = seeded(PIPELINE_WITHOUT_JOIN);

        assertThat(new StoreBackedDagSource(store).stateNamespacesOf(PIPELINE))
                .describedAs("a stop of an ordinary pipeline drops nothing, and notes nothing to drop later")
                .isEmpty();
    }

    // ---- fixtures ----------------------------------------------------------------------

    private static final String ORDERS_SRC = """
            version: tapstate/v1
            kind: source
            id: orders_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ orders ]
            """;

    private static final String CUSTOMERS_SRC = """
            version: tapstate/v1
            kind: source
            id: customers_src
            connector: mysql
            config: { host: h }
            mode: cdc
            tables: [ customers ]
            """;

    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: orders_dest
            connector: mongodb
            config: { uri: u }
            """;

    private static final String JOIN_PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: wide
            source: [ orders_src, customers_src ]
            transforms:
              - id: widen
                type: join
                from: { o: orders, c: customers }
                engine: builtin
                sql: |
                  SELECT o.id AS order_id, c.name AS customer_name
                  FROM o LEFT JOIN c ON o.customer_ref = c.cust_ref
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    /**
     * The same pipeline with the join replaced by an ordinary step, which is what an edit that takes one
     * out leaves. Under the same step id on purpose: what makes a namespace this pipeline's is the step
     * being a join, not the name it happens to carry.
     */
    private static final String PIPELINE_WITHOUT_JOIN = """
            version: tapstate/v1
            kind: pipeline
            id: wide
            source: [ orders_src, customers_src ]
            transforms:
              - { id: widen, from: [ orders ], type: filter, expr: "after.id != 0" }
            serve:
              from: widen
              sync: [ { id: sync_1, source: orders_dest } ]
            """;

    /** The actuator as production composes it, over a capture coordinator that does nothing. */
    private EngineLifecycleActuator actuator(InMemoryStorePort store) {
        return new EngineLifecycleActuator(new Engine(member), new StoreBackedDagSource(store),
                new NoOpCaptureCoordinator(),
                new NestStateTeardown(member, store.keyedState(), store.nestDeadLetters()));
    }

    /** An entry in each namespace, in both places state is kept, as a run would have left them. */
    private void seedState(InMemoryStorePort store, String... namespaces) {
        for (String namespace : namespaces) {
            store.keyedState().save(namespace, "k", "held".getBytes(StandardCharsets.UTF_8));
            member.getMap(namespace).put("k", "held");
        }
    }

    /** The two sources, the sink connection and {@code pipeline}, parsed and validated as a workspace. */
    private static InMemoryStorePort seeded(String pipeline) {
        InMemoryStorePort store = new InMemoryStorePort();
        seeded(store, pipeline);
        OpenRingGenerations.forSources(store, "orders_src", "customers_src");
        // A start refuses a pipeline whose sources have not been discovered, so the models are here for the
        // cases that run one. They say nothing about the namespaces, which come from the wiring alone.
        discovered(store, "orders_src", "orders", List.of("id"),
                new SourceField("id", "bigint"), new SourceField("customer_ref", "bigint"));
        discovered(store, "customers_src", "customers", List.of("id"),
                new SourceField("id", "bigint"), new SourceField("cust_ref", "bigint"),
                new SourceField("name", "varchar"));
        return store;
    }

    private static void discovered(InMemoryStorePort store, String connectionId, String table,
            List<String> key, SourceField... fields) {
        store.schemas().save(new DiscoveredSourceModel(connectionId, "mysql", 0L,
                new SourceModel(List.of(new SourceTable(table, List.of(fields), key, null)))));
    }

    /** The same, into a store that already holds a workspace - which is what an apply over one is. */
    private static void seeded(InMemoryStorePort store, String pipeline) {
        DslParser parser = new DslParser();
        List<Resource> resources = new ArrayList<>();
        for (String document : List.of(ORDERS_SRC, CUSTOMERS_SRC, TARGET, pipeline)) {
            resources.add(parser.parse(document));
        }
        Workspace.of(resources);
        resources.forEach(store.artifacts()::save);
    }
}
