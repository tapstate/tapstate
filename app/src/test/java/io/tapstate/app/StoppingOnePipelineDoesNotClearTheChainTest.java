package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.FromRef;
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
 * <p>All four of the chain's own fields are asserted rather than one standing in for the rest. They are
 * written by four different paths, and the two that decide whether a re-read happens at all -- the seam
 * the tail resumes from, and which tables have finished their initial load -- are the two a partial
 * clearing is most likely to leave behind.
 */
class StoppingOnePipelineDoesNotClearTheChainTest {

    @Test
    void stoppingOneOfTwoLeavesEverythingTheChainItselfAccumulated() {
        Fixture fixture = new Fixture();
        fixture.coordinator.startCapture("p");
        fixture.coordinator.startCapture("q");
        fixture.leaveACursorFor("p");
        fixture.leaveACursorFor("q");
        fixture.leaveWhatTheChainAccumulated();

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
        assertThat(chain.snapshotCompletedTables())
                .as("which tables finished their initial load -- the other half of a silent re-read")
                .containsExactly("orders");
        assertThat(fixture.consumersOnTheChain())
                .as("and only the leaving pipeline's own cursor was given back")
                .containsExactly("q");
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

        /** All four of the chain's own fields, each written by the path that really writes it. */
        void leaveWhatTheChainAccumulated() {
            store.meta().advanceSourceReadOffset(
                    chainId(), new ChainPosition(new SourceOrder(1L, 500L), "token-500"));
            store.meta().setCdcStart(chainId(), "seam-1", 1L);
            store.meta().appendSchemaVersion(chainId(), new SchemaVersion(0, Map.of("id", "int"), 0));
            store.meta().markSnapshotComplete(chainId(), "orders");
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
            return new PipelineResource(id, null, List.of("orders_src"), null, null,
                    new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                            List.of(new SyncElement("sync_1", "orders_src", null, null, null, null)), null, null),
                    new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
        }
    }
}
