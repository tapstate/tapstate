package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a pipeline taken down for good lets go of the state its nests kept, and that a takedown interrupted
 * halfway is finished rather than left as the state of the world.
 *
 * <p>Without this a stop leaves every entry where it is, and the next start reads it back through the cold
 * layer as its own - so a pipeline restarted after a stop assembles onto documents the previous run built,
 * with no symptom anywhere. Nothing else in the system would ever name those entries again either: the
 * state store has no way to list a namespace, deliberately, so what is not dropped by name is not droppable.
 *
 * <p>The state is seeded directly rather than by running a job. What is under test is the letting go, and a
 * run would only be a slower way of putting entries where these put them.
 */
class AStoppedPipelineLetsGoOfItsNestStateTest {

    private static final String PIPELINE = "nested_orders";
    private static final String PARENT_SOURCE = "src_orders";
    private static final String CHILD_SOURCE = "src_items";
    private static final String GRANDCHILD_SOURCE = "src_parts";
    private static final String DEST_ID = "tgt_mongo";
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String GRANDCHILD_TABLE = "item_parts";
    private static final String STEP = "order_doc";

    /** The assembler's namespace and the one resolver's, as the compiled tree names them. */
    private static final String ROOT_NAMESPACE = "nest." + PIPELINE + "." + STEP + ".$root";
    private static final String ITEMS_NAMESPACE = "nest." + PIPELINE + "." + STEP + ".items";

    /** Where the shape the tree compiled to is written down. */
    private static final String SHAPE_NAMESPACE = "nest.shape." + PIPELINE;

    /** Where this pipeline's outstanding drop, and the record of where its runs keep state, are written. */
    private static final String TEARDOWN_NAMESPACE = "nest.teardown." + PIPELINE;

    /** A namespace belonging to some other pipeline, which no stop of this one may touch. */
    private static final String OTHER_PIPELINE_NAMESPACE = "nest.other_pipe.some_step.$root";

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("nest-teardown-test-" + System.nanoTime());
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
    @DisplayName("the namespaces a stop drops are every vertex's, plus where the shape was written down")
    void namesEveryNamespaceTheTreeKeepsStateIn() {
        InMemoryStorePort store = seedStore();

        Set<String> namespaces = new StoreBackedDagSource(store).stateNamespacesOf(PIPELINE);

        // One per compiled vertex - the resolver serving the embed that has children, and the assembler -
        // and the record of the shape, which is state about the state and is let go of with it.
        assertThat(namespaces).containsExactlyInAnyOrder(ROOT_NAMESPACE, ITEMS_NAMESPACE, SHAPE_NAMESPACE);
    }

    @Test
    void aPipelineThatNestsNothingHasNoNamespaceToBeLetGoOf() {
        InMemoryStorePort store = seedStore();
        store.artifacts().save(pipelineWithoutNest());

        assertThat(new StoreBackedDagSource(store).stateNamespacesOf(PIPELINE))
                .describedAs("a stop of an ordinary pipeline drops nothing, and notes nothing to drop later")
                .isEmpty();
    }

    @Test
    @DisplayName("a stop drops the state in both places it is kept, and drops nobody else's")
    void stoppingLetsGoOfWhatTheRunKeptInMemoryAndOnDisk() {
        InMemoryStorePort store = seedStore();
        seedState(store, ROOT_NAMESPACE, ITEMS_NAMESPACE, SHAPE_NAMESPACE, OTHER_PIPELINE_NAMESPACE);

        actuator(store).stop(PIPELINE);

        assertThat(store.keyedState().load(ROOT_NAMESPACE, "k")).isEmpty();
        assertThat(store.keyedState().load(ITEMS_NAMESPACE, "k")).isEmpty();
        assertThat(store.keyedState().load(SHAPE_NAMESPACE, "k")).isEmpty();
        // The map too, not only the store behind it: a live map answers from memory whether or not the
        // store still has the entry, so dropping one alone leaves the state readable.
        assertThat(member.getMap(ROOT_NAMESPACE).size()).isZero();
        assertThat(member.getMap(ITEMS_NAMESPACE).size()).isZero();
        assertThat(store.keyedState().load(OTHER_PIPELINE_NAMESPACE, "k"))
                .describedAs("a stop drops what this pipeline named, never what merely looks like it")
                .isPresent();
    }

    @Test
    @DisplayName("a stop lets go of where the run wrote, not where the pipeline edited under it would")
    void stateTheRunKeptIsDroppedThoughTheNestStepWasTakenOutWhileItRan() {
        InMemoryStorePort store = seedStore();
        seedState(store, ROOT_NAMESPACE, ITEMS_NAMESPACE, SHAPE_NAMESPACE, OTHER_PIPELINE_NAMESPACE);
        EngineLifecycleActuator actuator = actuator(store);
        actuator.start(PIPELINE);
        // Taking the nest step out while the run is up is an ordinary edit: an apply never asks what state
        // the pipeline is in, and the run already up goes on keeping state where it was built to keep it.
        store.artifacts().save(pipelineWithoutNest());

        actuator.stop(PIPELINE);

        // Worked out from the pipeline as it now reads, the names are none at all - and a stop that drops
        // none leaves every entry where it is with nothing left that can name it: the namespaces are gone
        // from the definition, and the store has no way to list what it holds.
        assertThat(store.keyedState().load(ROOT_NAMESPACE, "k"))
                .describedAs("what the run kept is dropped by the names it ran under, not the ones it ends under")
                .isEmpty();
        assertThat(store.keyedState().load(ITEMS_NAMESPACE, "k")).isEmpty();
        assertThat(store.keyedState().load(SHAPE_NAMESPACE, "k")).isEmpty();
        assertThat(member.getMap(ROOT_NAMESPACE).size()).isZero();
        assertThat(member.getMap(ITEMS_NAMESPACE).size()).isZero();
        assertThat(store.keyedState().load(OTHER_PIPELINE_NAMESPACE, "k"))
                .describedAs("naming what the run wrote is still not licence to name anybody else's")
                .isPresent();
    }

    @Test
    @DisplayName("every run's namespaces are still there to be dropped, not only the last one's")
    void whatEachRunSaidItWouldKeepAddsUpUntilAStopLetsGoOfIt() {
        InMemoryStorePort store = seedStore();
        seedState(store, ROOT_NAMESPACE, ITEMS_NAMESPACE);
        NestStateTeardown teardown = new NestStateTeardown(member, store.keyedState());
        teardown.willKeepStateIn(PIPELINE, Set.of(ROOT_NAMESPACE));
        // A later run of the same pipeline keeps state somewhere else - an embed renamed, or taken out and
        // put back. The earlier run's entries are still there, and a record that only remembered the run
        // that wrote it last would be the second way to strand them.
        teardown.willKeepStateIn(PIPELINE, Set.of(ITEMS_NAMESPACE));

        teardown.note(PIPELINE, Set.of());
        teardown.finishPending(PIPELINE);

        assertThat(store.keyedState().load(ROOT_NAMESPACE, "k")).isEmpty();
        assertThat(store.keyedState().load(ITEMS_NAMESPACE, "k")).isEmpty();
    }

    @Test
    @DisplayName("a pipeline that nests nothing writes no record of keeping state, and notes no drop")
    void anOrdinaryPipelineIsUntouchedByAnyOfThis() {
        InMemoryStorePort store = seedStore();
        store.artifacts().save(pipelineWithoutNest());
        EngineLifecycleActuator actuator = actuator(store);

        actuator.start(PIPELINE);
        actuator.stop(PIPELINE);

        // Recording "this run keeps state in nowhere" would put a record where a pipeline with no state has
        // none to describe, and every later reader would have to tell that from a run that kept something.
        assertThat(store.keyedState().load(TEARDOWN_NAMESPACE, "kept"))
                .describedAs("a pipeline with no nest in it leaves nothing written down")
                .isEmpty();
        assertThat(store.keyedState().load(TEARDOWN_NAMESPACE, "namespaces"))
                .describedAs("and so notes no drop for a later start to finish")
                .isEmpty();
    }

    @Test
    @DisplayName("a start finishes the drop an interrupted stop noted but did not carry out")
    void aStopThatDiedBeforeItFinishedIsFinishedByTheNextStart() {
        InMemoryStorePort store = seedStore();
        seedState(store, ROOT_NAMESPACE, ITEMS_NAMESPACE, SHAPE_NAMESPACE);
        // What a stop that died between noting the drop and finishing it leaves behind: the note, and the
        // state it names. A stop is driven once, on the transition, so nothing will drive it again.
        new NestStateTeardown(member, store.keyedState())
                .note(PIPELINE, Set.of(ROOT_NAMESPACE, ITEMS_NAMESPACE, SHAPE_NAMESPACE));

        actuator(store).start(PIPELINE);

        assertThat(store.keyedState().load(ROOT_NAMESPACE, "k"))
                .describedAs("the run starting here must not read entries the last one was told to let go of")
                .isEmpty();
        assertThat(store.keyedState().load(ITEMS_NAMESPACE, "k")).isEmpty();
        // The note goes only once what it names is gone, so a start interrupted in its turn still has one.
        assertThat(store.keyedState().load(TEARDOWN_NAMESPACE, "namespaces")).isEmpty();
    }

    @Test
    @DisplayName("a start with no drop outstanding leaves the state where it is")
    void aStartAfterACrashRatherThanAStopKeepsTheStateItStillAnswersFor() {
        InMemoryStorePort store = seedStore();
        seedState(store, ROOT_NAMESPACE, ITEMS_NAMESPACE, SHAPE_NAMESPACE);

        actuator(store).start(PIPELINE);

        // A run that died without a stop was never told to let go of anything. Dropping here would take the
        // written-down shape with it, and with it the one thing that tells a renamed path from a cold start.
        assertThat(store.keyedState().load(ROOT_NAMESPACE, "k")).isPresent();
        assertThat(store.keyedState().load(SHAPE_NAMESPACE, "k")).isPresent();
    }

    @Test
    @DisplayName("the drop waits for the job to be over rather than racing what it is still writing")
    void theStateAProcessorWritesAsItClosesIsDroppedRatherThanLeftBehind() {
        InMemoryStorePort store = seedStore();
        String namespace = "nest." + PIPELINE + ".late_writer.$root";
        EngineLifecycleActuator actuator = new EngineLifecycleActuator(new Engine(member),
                new WritesStateAsItCloses(namespace), new NoOpCaptureCoordinator(),
                new NestStateTeardown(member, store.keyedState()));

        actuator.start(PIPELINE);
        awaitRunning();
        actuator.stop(PIPELINE);
        // Read only once the job is genuinely finished, which is the moment the two orderings first differ.
        // Read at the moment the stop returns, a drop that had raced ahead would look identical to one that
        // had waited: the write it is racing has not landed yet either, so the map is empty under both.
        awaitJobOver();

        // A cancel only asks, and a processor goes on writing while it winds down. A drop that took the
        // cancel for the ending would run first and be undone by the write that followed it - leaving an
        // entry nothing will ever name again, with the note that named it already gone.
        assertThat(member.getMap(namespace).size())
                .describedAs("what the job wrote on its way out is inside the drop, not after it")
                .isZero();
    }

    @Test
    void droppingTheSameNamespacesAgainIsNotAnError() {
        InMemoryStorePort store = seedStore();
        seedState(store, ROOT_NAMESPACE);
        NestStateTeardown teardown = new NestStateTeardown(member, store.keyedState());
        teardown.note(PIPELINE, Set.of(ROOT_NAMESPACE));

        teardown.finishPending(PIPELINE);
        teardown.finishPending(PIPELINE);

        // The second pass has no note to act on, which is what makes a resumed drop safe to resume twice.
        assertThat(store.keyedState().load(ROOT_NAMESPACE, "k")).isEmpty();
    }

    // ---- fixtures ---------------------------------------------------------------------

    /** Waits for the pipeline's job to be running, so a stop of it is a stop of something. */
    private void awaitRunning() {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            Job job = member.getJet().getJob(PIPELINE);
            if (job != null && job.getStatus() == JobStatus.RUNNING) {
                return;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("the job never reached RUNNING within budget");
    }

    /**
     * Waits for the pipeline's job to be over - over meaning every processor has closed, which is what the
     * job's own future settling stands for. Polling the status instead would answer before the closing is
     * done, and the write being watched for happens inside that window.
     */
    private void awaitJobOver() {
        Job job = member.getJet().getJob(PIPELINE);
        if (job == null) {
            return;
        }
        try {
            job.getFuture().toCompletableFuture().get(15, TimeUnit.SECONDS);
        } catch (CancellationException | ExecutionException over) {
            // However it ended, it ended - which is all this waits for.
        } catch (TimeoutException notOver) {
            throw new AssertionError("the job was still running 15s after the stop");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * A topology of one processor that writes an entry into {@code namespace} as it closes, and takes long
     * enough over it that a drop which did not wait would already have run. Which is the failure being
     * ruled out: nothing about it is observable afterwards except the entry that should not be there.
     */
    private record WritesStateAsItCloses(String namespace) implements DagSource {

        /** Not what this is about: the entry it writes is the subject, not what a map may hold. */
        @Override
        public NestCapacity capacityOf(String pipelineId) {
            return NestCapacity.none();
        }

        @Override
        public DAG dagFor(String pipelineId) {
            DAG dag = new DAG();
            String target = namespace;
            dag.newVertex("late_writer", ProcessorMetaSupplier.forceTotalParallelismOne(
                    ProcessorSupplier.of((SupplierEx<Processor>) () -> new LateWriter(target))));
            return dag;
        }

        @Override
        public Set<String> stateNamespacesOf(String pipelineId) {
            return Set.of(namespace);
        }
    }

    /** Runs until stopped, then writes as it goes - the shape of a processor flushing state on close. */
    private static final class LateWriter extends AbstractProcessor {

        private final String namespace;
        private HazelcastInstance instance;

        private LateWriter(String namespace) {
            this.namespace = namespace;
        }

        @Override
        protected void init(Context context) {
            this.instance = context.hazelcastInstance();
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            sleep(20);
            return false;
        }

        @Override
        public void close() {
            sleep(300);
            instance.getMap(namespace).put("written-on-the-way-out", "held");
        }

        private static void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** The actuator as production composes it, over a capture coordinator that does nothing. */
    private EngineLifecycleActuator actuator(InMemoryStorePort store) {
        return new EngineLifecycleActuator(new Engine(member), new StoreBackedDagSource(store),
                new NoOpCaptureCoordinator(), new NestStateTeardown(member, store.keyedState()));
    }

    /** An entry in each namespace, in both places state is kept, as a run would have left them. */
    private void seedState(InMemoryStorePort store, String... namespaces) {
        for (String namespace : namespaces) {
            store.keyedState().save(namespace, "k", "held".getBytes(StandardCharsets.UTF_8));
            member.getMap(namespace).put("k", "held");
        }
    }

    /** The three sources, the sink connection, a discovered model each, and the two-level nest pipeline. */
    private static InMemoryStorePort seedStore() {
        InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
        artifacts.save(source(PARENT_SOURCE, PARENT_TABLE));
        artifacts.save(source(CHILD_SOURCE, CHILD_TABLE));
        artifacts.save(source(GRANDCHILD_SOURCE, GRANDCHILD_TABLE));
        artifacts.save(new SourceResource(DEST_ID, null, "fake", Map.of("host", "d"), null, null, null, null, null));
        artifacts.save(pipeline());

        InMemoryStorePort store = new InMemoryStorePort(artifacts);
        store.schemas().save(discovered(PARENT_SOURCE,
                new SourceTable(PARENT_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("name", "string")),
                        List.of("id"), List.of())));
        store.schemas().save(discovered(CHILD_SOURCE,
                new SourceTable(CHILD_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("order_id", "int"),
                                new SourceField("sku", "string")),
                        List.of("id"), List.of())));
        store.schemas().save(discovered(GRANDCHILD_SOURCE,
                new SourceTable(GRANDCHILD_TABLE,
                        List.of(new SourceField("id", "int"), new SourceField("item_id", "int")),
                        List.of("id"), List.of())));
        OpenRingGenerations.forSources(store, PARENT_SOURCE, CHILD_SOURCE, GRANDCHILD_SOURCE);
        return store;
    }

    /** A root with one embed that itself has one, so the tree compiles to a resolver and an assembler. */
    private static PipelineResource pipeline() {
        Embed part = new Embed("p", Map.of("item_id", "id"), EmbedAs.ARRAY, "parts", List.of("id"),
                null, null, null);
        Embed item = new Embed("i", Map.of("order_id", "id"), EmbedAs.ARRAY, "items", List.of("id"),
                null, null, List.of(part));
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("o", List.of("id"), null, null, List.of(item)));
        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("o", FromRef.literal(PARENT_TABLE));
        aliases.put("i", FromRef.literal(CHILD_TABLE));
        aliases.put("p", FromRef.literal(GRANDCHILD_TABLE));
        return new PipelineResource(PIPELINE, null, List.of(PARENT_SOURCE, CHILD_SOURCE, GRANDCHILD_SOURCE),
                List.of(Step.inline(STEP, FromClause.aliases(aliases), body, null, null)), null,
                new ServeBlock.Inline(null, FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null);
    }

    /** The same pipeline with its nest step taken out, so nothing keeps state. */
    private static PipelineResource pipelineWithoutNest() {
        return new PipelineResource(PIPELINE, null, List.of(PARENT_SOURCE), List.of(), null,
                new ServeBlock.Inline(null, FromRef.literal(PARENT_SOURCE),
                        List.of(new SyncElement("sync_1", DEST_ID, null, null, null, null)), null, null),
                new Settings(null, null, null, null, ReadMode.SNAPSHOT_AND_CDC, "earliest"), null);
    }

    private static SourceResource source(String id, String table) {
        return new SourceResource(id, null, "fake", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal(table)), null, null, null);
    }

    private static DiscoveredSourceModel discovered(String connectionId, SourceTable table) {
        return new DiscoveredSourceModel(connectionId, "fake", 0L, new SourceModel(List.of(table)));
    }
}
