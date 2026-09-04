package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * What a mining chain accumulated belongs to the chain, so a stop only takes it when the pipeline
 * stopping was the last one reading it.
 *
 * <p>Two consumers throughout, and that is the whole design of these cases. With one pipeline on a
 * chain, "clear only what is this pipeline's" and "clear the chain as well" produce the same store,
 * word for word -- so a single-consumer case cannot tell a correct implementation from one that takes
 * a shared record away from whoever is still reading it. What that costs the survivor is not an error
 * either: it reads its whole source again, successfully and quietly.
 *
 * <p>All three of the chain's own fields are asserted rather than one standing in for the rest. They are
 * written by three different paths, and the one that decides whether a re-read happens at all -- the seam
 * the tail resumes from -- is the one a partial clearing is most likely to leave behind.
 *
 * <p>Which tables have finished their initial load is asserted separately and in both directions, because
 * it is not the chain's: it answers "are this pipeline's rows in this pipeline's target", and the
 * pipelines sharing a chain each write somewhere of their own. A stop therefore takes exactly one
 * pipeline's marks -- keeping the survivor's is what stops it re-reading its whole source, and dropping
 * the leaving one's is what makes its next run redo the load it asked to redo.
 */
class StoppingOnePipelineDoesNotClearTheChainTest {

    @Test
    void aStopAskedToKeepLeavesTheStoppingPipelinesOwnFinishedTables() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p");
        fixture.coordinator.startCapture("q");
        fixture.leaveACursorFor("p");
        fixture.leaveACursorFor("q");
        fixture.leaveWhatTheChainAccumulated();
        fixture.leaveAFinishedTableFor("p");
        fixture.leaveAFinishedTableFor("q");

        fixture.coordinator.stopCapture("p", false);

        SrsMeta chain = fixture.chainRecord().orElseThrow(
                () -> new AssertionError("the chain was removed by a stop that was asked to keep"));
        assertThat(chain.snapshotCompletedTables("p"))
                .as("the tables the stopping pipeline had already loaded: it asked for its state to be "
                        + "kept, and a table it no longer remembers finishing is one its next run reads "
                        + "in full -- which is the whole of what keeping was asked to prevent")
                .containsExactly("orders");
        assertThat(chain.snapshotCompletedTables("q"))
                .as("and the survivor's, which no stop of somebody else may touch")
                .containsExactly("orders");
    }

    @Test
    void stoppingOneOfTwoLeavesEverythingTheChainItselfAccumulated() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p");
        fixture.coordinator.startCapture("q");
        fixture.leaveACursorFor("p");
        fixture.leaveACursorFor("q");
        fixture.leaveWhatTheChainAccumulated();
        fixture.leaveAFinishedTableFor("p");
        fixture.leaveAFinishedTableFor("q");

        fixture.coordinator.stopCapture("p", true);

        SrsMeta chain = fixture.chainRecord().orElseThrow(
                () -> new AssertionError("the chain a survivor still reads was removed outright"));
        assertThat(chain.sourceRead())
                .as("how far the chain had read")
                .isEqualTo(new ChainPosition(new SourceOrder(1L, 500L), "token-500"));
        assertThat(chain.cdcStartPosition())
                .as("the seam its tail resumes from -- losing this is what makes a survivor re-read")
                .isEqualTo("seam-1");
        assertThat(chain.schemaHistory()).as("the schema the chain saw").hasSize(1);
        assertThat(fixture.consumersOnTheChain())
                .as("and only the leaving pipeline's own cursor was given back")
                .containsExactly("q");
        // Which tables finished their initial load is each pipeline's own answer, so a stop takes exactly
        // one pipeline's marks. Both halves are asserted because each fails on its own: keeping the
        // survivor's is what stops it re-reading its whole source for no reason, and dropping the leaving
        // one's is what makes that pipeline's next run actually redo the load it asked to redo.
        assertThat(chain.snapshotCompletedTables("q"))
                .as("the survivor keeps what it finished -- the other half of a silent re-read")
                .containsExactly("orders");
        assertThat(chain.snapshotCompletedTables("p"))
                .as("and the pipeline that asked for its state to be cleared no longer looks finished")
                .isEmpty();
    }

    @Test
    void theLastPipelineOffTheChainIsTheOneThatClearsIt() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p");
        fixture.coordinator.startCapture("q");
        fixture.leaveACursorFor("p");
        fixture.leaveACursorFor("q");
        fixture.leaveWhatTheChainAccumulated();

        fixture.coordinator.stopCapture("p", true);
        fixture.coordinator.stopCapture("q", true);

        // The pair of this and the case above is what makes either one an assertion. Read alone, the
        // first is satisfied by an implementation that never clears the chain at all, which leaks every
        // chain the product ever opens; this one is satisfied by one that always clears it.
        assertThat(fixture.chainRecord())
                .as("nobody is left reading it, so what it accumulated is this stop's to clear")
                .isEmpty();
    }

    @Test
    void theLastPipelineOffTheChainStillLeavesItWhenItWasAskedToKeep() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p");
        fixture.leaveACursorFor("p");
        fixture.leaveWhatTheChainAccumulated();

        fixture.coordinator.stopCapture("p", false);

        // Being the last one off decides whether the record is this stop's to take; what the stop was
        // asked decides whether it takes anything at all. Both have to hold, and only a case for each
        // says which of the two an implementation is actually reading.
        assertThat(fixture.chainRecord()).isPresent();
        assertThat(fixture.consumersOnTheChain()).containsExactly("p");
    }

    /** Two pipelines over one source, so both resolve to the very same mining chain. */
    private static final class Fixture {

        private final InMemoryStorePort store;
        private final SrsCoordinator srsCoordinator;
        private final StoreBackedPipelineCaptureCoordinator coordinator;
        private final SourceResource source = new SourceResource("orders_src", null, "mysql",
                Map.of("host", "h"), SourceMode.CDC, List.of(TableRef.literal("orders")), null, null, null);

        Fixture() {
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            artifacts.save(source);
            artifacts.save(pipeline("p"));
            artifacts.save(pipeline("q"));
            store = new InMemoryStorePort(artifacts);
            srsCoordinator = new SrsCoordinator(store.meta());
            coordinator = new StoreBackedPipelineCaptureCoordinator(
                    store, this::start, srsCoordinator, new SnapshotBuffer());
        }

        private CaptureRun start(CaptureRunSpec spec, java.util.function.Consumer<Envelope> passthrough) {
            MiningChainId chainId = MiningChainId.resolve(spec.config(), spec.srsKey());
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.pipelineId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(), Optional.of(() -> {
            }), new CaptureHealth());
        }

        /** One consumer's own cursor, which is that pipeline's to give back when it stops. */
        void leaveACursorFor(String pipelineId) {
            store.meta().upsertConsumerOffset(chainId(), new ConsumerOffset(pipelineId, Map.of(), null));
        }

        /** The chain's own three fields, each written by the path that really writes it. */
        void leaveWhatTheChainAccumulated() {
            store.meta().advanceSourceReadOffset(
                    chainId(), new ChainPosition(new SourceOrder(1L, 500L), "token-500"));
            store.meta().setCdcStart(chainId(), "seam-1", 1L);
            store.meta().appendSchemaVersion(chainId(), new SchemaVersion(0, Map.of("id", "int"), 0));
        }

        /**
         * One pipeline's finished table, which is that pipeline's and not the chain's. Seeded only for
         * pipelines that are actually on the chain: the mark creates the consumer entry when there is
         * none, so marking for an absent pipeline would put it on the chain.
         */
        void leaveAFinishedTableFor(String pipelineId) {
            store.meta().markSnapshotComplete(chainId(), pipelineId, "orders");
        }

        Optional<SrsMeta> chainRecord() {
            return store.meta().read(chainId());
        }

        List<String> consumersOnTheChain() {
            return chainRecord().orElseThrow().consumerOffsets().stream()
                    .map(ConsumerOffset::pipelineId)
                    .toList();
        }

        private String chainId() {
            return MiningChainId.resolve(SourceCaptureResolution.of(source).config(), null).value();
        }

        private static PipelineResource pipeline(String id) {
            return new PipelineResource(id, null, List.of(SourceRef.spec("orders_src", true)), null, null,
                    new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                            List.of(new SyncElement("sync_1", "orders_src", null, null, null, null)), null, null),
                    new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
        }
    }
}
