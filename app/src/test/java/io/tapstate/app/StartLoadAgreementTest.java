package io.tapstate.app;

import com.hazelcast.function.SupplierEx;
import io.tapstate.control.core.PipelineWriteTargets;
import io.tapstate.control.core.StartIntent;
import io.tapstate.control.core.StartPlan;
import io.tapstate.control.core.StartPlanner;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StartLoad;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The start verb's prediction and the run's own decision, compared cell by cell: what the verb tells a
 * person about a start has to be what the start then does. Each cell seeds the durable records a real
 * history leaves behind, asks the verb, then lets the run decide over the records as they will be when
 * it gets there -- after a clearing stop has run, where the cell's start comes after one.
 */
class StartLoadAgreementTest {

    private static final String PIPE = "pipe";

    @Test
    void aFirstStartIsANewFullLoad() {
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
        assertThat(cell.agree(Optional.empty(), StartIntent.START, false)).isEqualTo(StartLoad.FULL_LOAD);
    }

    @Test
    void aSeamRecordedBeforeTheTargetWasPreparedIsAlreadyAResume() {
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
        cell.seed(store -> store.meta().setCdcStart(cell.chain, PIPE, "seam-0", 1L));
        assertThat(cell.agree(Optional.empty(), StartIntent.START, false)).isEqualTo(StartLoad.RESUME);
    }

    @Test
    void eachKindOfProgressMakesTheStartAResume() {
        List<Consumer<Cell>> progress = List.of(
                c -> c.store.meta().advanceConsumerReadSeq(c.chain, PIPE, "orders", 4L),
                c -> c.store.meta().advanceSinkAcked(c.chain, PIPE, new ChainPosition(new SourceOrder(1, 9), "pos-9")),
                c -> c.store.meta().markSnapshotComplete(c.chain, PIPE, "orders"));
        for (Consumer<Cell> seed : progress) {
            Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
            seed.accept(cell);
            assertThat(cell.agree(Optional.empty(), StartIntent.START, false)).isEqualTo(StartLoad.RESUME);
        }
    }

    @Test
    void aChangesOnlyReadIsNeverLoaded() {
        Cell cell = cell(ReadMode.CDC_ONLY, false);
        assertThat(cell.agree(Optional.empty(), StartIntent.START, false)).isEqualTo(StartLoad.CDC_ONLY);
    }

    @Test
    void aStartAfterAClearingStopThatFinishedIsANewFullLoad() {
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
        cell.loaded();
        cell.purge();
        assertThat(cell.agree(Optional.of(stopped(true)), StartIntent.START, false))
                .isEqualTo(StartLoad.FULL_LOAD);
    }

    @Test
    void aStartOverAClearingStopNotYetCarriedOutIsANewFullLoad() {
        // The records are all still there when the verb asks; the clearing stop runs between the verb and
        // the run, because the start is written as "stop, clearing, then start". Reading the records as they
        // stand would call this a resume and the person would be told the target is left alone.
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
        cell.loaded();
        assertThat(cell.agree(Optional.of(stopped(true)), StartIntent.START, true))
                .isEqualTo(StartLoad.FULL_LOAD);
    }

    @Test
    void aStartAfterAStopThatKeptTheStateIsAResume() {
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
        cell.loaded();
        assertThat(cell.agree(Optional.of(stopped(false)), StartIntent.START, false))
                .isEqualTo(StartLoad.RESUME);
    }

    @Test
    void aRerunOnAChainThisPipelineHasAloneIsANewFullLoad() {
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
        cell.loaded();
        assertThat(cell.agree(Optional.of(running()), StartIntent.RERUN, true)).isEqualTo(StartLoad.FULL_LOAD);
    }

    @Test
    void aRerunOnAChainAnotherPipelineStillReadsIsANewFullLoadToo() {
        // What this pipeline loaded is recorded on its own record, which the clearing stop takes away while
        // the other pipeline's stays: the rerun loads every table again, shared chain or not.
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, true);
        cell.loaded();
        cell.seed(store -> store.meta().markSnapshotComplete(cell.chain, "other", "orders"));
        assertThat(cell.agree(Optional.of(running()), StartIntent.RERUN, true)).isEqualTo(StartLoad.FULL_LOAD);
        assertThat(cell.store.meta().read(cell.chain).orElseThrow().snapshotCompletedTables("other"))
                .containsExactly("orders");
    }

    @Test
    void thePlanNamesTheTableTheSinkWrites() {
        Cell cell = cell(ReadMode.SNAPSHOT_AND_CDC, false);
        StartPlan plan = cell.planner().plan(cell.pipeline(), Optional.empty(), StartIntent.START);
        assertThat(plan.entries()).singleElement().satisfies(entry -> {
            assertThat(entry.target().element()).isEqualTo("sink");
            assertThat(entry.target().kind()).isEqualTo(PipelineWriteTargets.WriteTarget.Kind.SYNC);
            assertThat(entry.coordinate()).isEqualTo("dest/orders");
            assertThat(entry.onFullLoad()).isEqualTo(io.tapstate.core.model.OnFullLoad.APPEND);
            assertThat(entry.target().definedIn()).isNull();
        });
        assertThat(cell.bindTargets()).containsOnlyKeys("orders");
        assertThat(cell.bindTargets().get("orders").name()).isEqualTo(plan.entries().getFirst().target().table());
    }

    private static DesiredState stopped(boolean purge) {
        return new DesiredState(PIPE, PipelineState.STOPPED, "rev", purge, null, false, null);
    }

    private static DesiredState running() {
        return new DesiredState(PIPE, PipelineState.RUNNING, "rev", false, null, false, null);
    }

    private static Cell cell(ReadMode mode, boolean shared) {
        InMemoryStorePort store = new InMemoryStorePort();
        store.artifacts().save(new SourceResource("src", null, "mysql", Map.of("host", "h"), SourceMode.CDC,
                List.of(TableRef.literal("orders")), null, null));
        store.artifacts().save(new SourceResource("dest", null, "mysql", Map.of("host", "other"), null, null, null, null));
        store.schemas().save(new DiscoveredSourceModel("src", "mysql", 0L, new SourceModel(List.of(
                new SourceTable("orders", List.of(new SourceField("id", "bigint", TapstateType.INT64, null)),
                        List.of("id"), List.of())))));
        store.artifacts().save(pipeline(PIPE, mode));
        if (shared) {
            store.artifacts().save(pipeline("other", mode));
        }
        OpenRingGenerations.forSources(store, "src");
        SourceResource source = StoredArtifacts.requireSource(store.artifacts(), "src");
        String chain = SourceCaptureResolution.of(source, SourceDiscovery.model(store, source)).chainId().value();
        if (shared) {
            store.meta().advanceConsumerReadSeq(chain, "other", "orders", 2L);
        }
        return new Cell(store, chain);
    }

    private static PipelineResource pipeline(String id, ReadMode mode) {
        return new PipelineResource(
                id, null, List.of(SourceRef.spec("src", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("src"), List.of(
                        new SyncElement("sink", "dest", null, null, null, null)), null, null),
                new Settings(null, null, null, null, mode, null), null);
    }

    private static final class Cell {
        final InMemoryStorePort store;
        final String chain;

        Cell(InMemoryStorePort store, String chain) {
            this.store = store;
            this.chain = chain;
        }

        void seed(Consumer<InMemoryStorePort> seed) {
            seed.accept(store);
        }

        /** The records an earlier run that finished its load leaves behind. */
        void loaded() {
            store.meta().setCdcStart(chain, PIPE, "seam-0", 1L);
            store.meta().advanceConsumerReadSeq(chain, PIPE, "orders", 7L);
            store.meta().markSnapshotComplete(chain, PIPE, "orders");
        }

        /** A clearing stop, carried out by the capture side's own teardown. */
        void purge() {
            new StoreBackedPipelineCaptureCoordinator(store, (spec, passthrough) -> {
                throw new AssertionError("a stop starts no capture");
            }, new SrsCoordinator(store.meta()), new SnapshotBuffer()).stopCapture(PIPE, true);
        }

        PipelineResource pipeline() {
            return StoredArtifacts.requirePipeline(store.artifacts(), PIPE);
        }

        StartPlanner planner() {
            return new StartPlanner(new StoreBackedPipelineChains(store), store.meta(),
                    new StoreBackedPipelineWriteTargets(store));
        }

        /**
         * Asks the verb, then lets the run decide -- after the clearing stop, when {@code clearsBetween}
         * says one runs in between -- and requires the two to agree.
         */
        StartLoad agree(Optional<DesiredState> prior, StartIntent intent, boolean clearsBetween) {
            StartLoad predicted = planner().plan(pipeline(), prior, intent).load();
            if (clearsBetween) {
                purge();
            }
            CapturingBinder binder = new CapturingBinder();
            new StoreBackedDagSource(store, binder).prepareStart(PIPE, "tapstate").build(null);
            assertThat(binder.fullLoad)
                    .as("the run's own decision against the verb's prediction %s", predicted)
                    .isEqualTo(predicted == StartLoad.FULL_LOAD);
            return predicted;
        }

        Map<String, TargetTable> bindTargets() {
            CapturingBinder binder = new CapturingBinder();
            new StoreBackedDagSource(store, binder).dagFor(PIPE);
            return binder.targets;
        }
    }

    private static final class CapturingBinder implements StoreBackedDagSource.SinkWriterBinder {
        boolean fullLoad;
        Map<String, TargetTable> targets;

        @Override
        public SupplierEx<? extends SinkWriter> bind(String connector, Map<String, Object> settings,
                WriteMode mode, DdlPolicy ddl, TargetTable target, PipelineNode node) {
            throw new AssertionError("the complete sink policy must reach the binder");
        }

        @Override
        public SupplierEx<? extends SinkWriter> bind(String connector, Map<String, Object> settings,
                WriteMode mode, DdlPolicy ddl, Map<String, TargetTable> targets, PipelineNode node,
                OnFullLoad policy, boolean fullLoad) {
            this.fullLoad = fullLoad;
            this.targets = targets;
            return new PdkSinkWriterFactory(connector, settings, mode, ddl, targets, node, policy, fullLoad);
        }
    }
}
