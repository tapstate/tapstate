package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.ConnectorNotes;
import io.tapstate.adapters.pdk.ConnectorStateNamespace;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.runtime.srs.CaptureHandoff;
import io.tapstate.runtime.srs.CaptureHealth;
import io.tapstate.runtime.srs.CaptureRun;
import io.tapstate.runtime.srs.CaptureRunSpec;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsCoordinator;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SrsConsumerId;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * What a source connector set up on its source to read changes -- a replication slot -- goes when the state
 * it belongs to is cleared, and not before: with the chain, once the last pipeline reading it clears its
 * state, which for a pipeline reading its source directly is whenever that pipeline does, its chain being its
 * own.
 *
 * <p>Each case that expects nothing let go of has a partner in which the same stop does let go, so neither
 * is satisfied by an implementation that never releases, or by one that releases on every stop.
 */
class ClearingLetsGoOfWhatTheSourceSetUpTest {

    private final ListAppender<ILoggingEvent> written = new ListAppender<>();
    private Logger logger;

    @BeforeEach
    void listen() {
        logger = (Logger) LoggerFactory.getLogger(StoreBackedPipelineCaptureCoordinator.class);
        written.start();
        logger.addAppender(written);
    }

    @AfterEach
    void stopListening() {
        logger.detachAppender(written);
    }

    /**
     * A chain two pipelines read keeps its slot while either still reads it, and lets go of it -- once --
     * when the last one clears its state: after the chain's record is gone, and while the notes that name the
     * slot are still there to be read.
     */
    @Test
    void theLastPipelineToClearItsStateOffASharedChainLetsGoOfTheChainsSlot() {
        Fixture fixture = new Fixture(true, true);
        fixture.coordinator.startCapture("p");
        fixture.coordinator.startCapture("q");
        fixture.noteASlotOnTheSharedChain();

        fixture.coordinator.stopCapture("p", true);
        assertThat(fixture.released).as("q still reads through the slot").isEmpty();

        fixture.coordinator.stopCapture("q", true);
        assertThat(fixture.released).singleElement().satisfies(released -> {
            assertThat(released.pipelineId()).isEqualTo("q");
            assertThat(released.srsEnabled()).isTrue();
            assertThat(released.chainRecordGone()).as("let go of only after the chain's record is gone").isTrue();
            assertThat(released.notesStillThere())
                    .as("while the notes naming the slot are still there to be read").isTrue();
        });
        assertThat(fixture.sharedNotes()).as("and the notes go after it").isZero();
    }

    /**
     * The same where this process owns the captures it tails, which is how a server always runs: the release
     * goes through the attacher that starts its runs.
     */
    @Test
    void theLastPipelineToClearItsStateLetsGoOfTheSlotWhereCapturesAreOwned() {
        Fixture fixture = new Fixture(true, true, true);
        fixture.coordinator.startCapture("p");
        fixture.coordinator.startCapture("q");

        fixture.coordinator.stopCapture("p", true);
        assertThat(fixture.released).as("q still reads through the slot").isEmpty();

        fixture.coordinator.stopCapture("q", true);
        assertThat(fixture.released).singleElement().satisfies(released -> {
            assertThat(released.pipelineId()).isEqualTo("q");
            assertThat(released.chainRecordGone()).isTrue();
        });
    }

    /**
     * A stop asked to keep the state lets go of nothing, last one off or not -- and the same pipeline's
     * clearing stop afterwards does, so this cannot pass for an implementation that never releases.
     */
    @Test
    void aStopAskedToKeepTheStateLetsGoOfNothing() {
        Fixture fixture = new Fixture(true, true);
        fixture.coordinator.startCapture("p");
        fixture.noteASlotOnTheSharedChain();

        fixture.coordinator.stopCapture("p", false);
        assertThat(fixture.released).isEmpty();
        assertThat(fixture.sharedNotes()).as("the notes are kept with the state").isOne();

        fixture.coordinator.startCapture("p");
        fixture.coordinator.stopCapture("p", true);
        assertThat(fixture.released).extracting(Released::pipelineId).containsExactly("p");
    }

    /**
     * A pipeline reading its source directly reads it through a chain of its own, so clearing its state lets
     * go of its own slot even while another pipeline reads the same source through the shared chain -- whose
     * slot stays.
     */
    @Test
    void aDirectReaderLetsGoOfItsOwnSlotWhileTheSharedChainKeepsItsOwn() {
        Fixture fixture = new Fixture(false, true);
        fixture.coordinator.startCapture("p");
        fixture.coordinator.startCapture("q");

        fixture.coordinator.stopCapture("p", true);

        assertThat(fixture.released).singleElement().satisfies(released -> {
            assertThat(released.pipelineId()).isEqualTo("p");
            assertThat(released.srsEnabled()).as("its own, read directly").isFalse();
        });
        assertThat(fixture.chainRecordGone()).as("the shared chain q reads is untouched").isFalse();
    }

    /**
     * A pipeline this process holds no run for -- after a start that threw part way, or a process that came
     * up over an earlier one's work -- is let go of on its source through what it is defined to read.
     */
    @Test
    void aPipelineWithNoRunHereIsLetGoOfThroughItsDefinition() {
        Fixture fixture = new Fixture(true, true);
        fixture.seedAChainNobodyIsRunning();
        fixture.leaveACursorFor("p");

        fixture.coordinator.stopCapture("p", true);

        assertThat(fixture.released).singleElement().satisfies(released -> {
            assertThat(released.pipelineId()).isEqualTo("p");
            assertThat(released.chainRecordGone()).isTrue();
        });
    }

    /**
     * A chain the pipeline left a cursor on but that no source in its definition reads any more -- the source
     * was pointed elsewhere since -- is cleared all the same, and what was set up there to read it is said to
     * be left on the source rather than released through a source that reads something else.
     */
    @Test
    void aChainNoDefinedSourceReadsAnyMoreIsClearedAndSaysWhatItLeaves() {
        Fixture fixture = new Fixture(true, true);
        String elsewhere = MiningChainId.resolve(
                new CaptureConfig("mysql", Map.of("host", "elsewhere"), List.of("orders")), null).value();
        fixture.store.meta().create(elsewhere, null);
        fixture.store.meta().upsertConsumerOffset(elsewhere, new ConsumerOffset(
                SrsConsumerId.of("p", "orders_src").value(), Map.of(), null));
        fixture.store.keyedState().save(ConnectorStateNamespace.ofShared(elsewhere), "tapdata_pg_slot",
                ConnectorNotes.encode("slot-of-the-old-settings"));

        fixture.coordinator.stopCapture("p", true);

        assertThat(fixture.store.meta().read(elsewhere)).as("the chain was cleared all the same").isEmpty();
        assertThat(fixture.released).as("nothing defined reads it, so nothing is released through it").isEmpty();
        assertThat(written.list).filteredOn(event -> event.getLevel() == Level.WARN)
                .singleElement()
                .satisfies(event -> assertThat(event.getFormattedMessage())
                        .contains(elsewhere).contains("p").contains("slot-of-the-old-settings"));
        assertThat(fixture.store.keyedState().count(ConnectorStateNamespace.ofShared(elsewhere)))
                .as("the notes went with the chain once their slot was named").isZero();
    }

    /**
     * A source that keeps what it set up does not stop the clearing: the state goes, and the coded refusal
     * naming what is left there is logged, for somebody to remove on the source by hand.
     */
    @Test
    void aSourceThatKeepsItsSlotIsSaidAndTheClearingGoesOn() {
        Fixture fixture = new Fixture(true, true);
        fixture.refuseWith(new TapstateException(ConnectorError.RELEASE_FAILED,
                Map.of("connector", "mysql", "detail", "connection refused", "resources", "slot-7"), null));
        fixture.coordinator.startCapture("p");
        fixture.noteASlotOnTheSharedChain();

        fixture.coordinator.stopCapture("p", true);

        assertThat(fixture.chainRecordGone()).as("the state was cleared all the same").isTrue();
        assertThat(fixture.sharedNotes()).as("notes included").isZero();
        assertThat(written.list).filteredOn(event -> event.getLevel() == Level.WARN)
                .singleElement()
                .satisfies(event -> assertThat(event.getFormattedMessage())
                        .contains("p").contains("orders_src")
                        .contains(ConnectorError.RELEASE_FAILED.code()).contains("slot-7"));
    }

    private record Released(String pipelineId, boolean srsEnabled, boolean chainRecordGone,
            boolean notesStillThere) {
    }

    /** Two pipelines over one source, each reading it through the shared chain or directly as it is told. */
    private static final class Fixture {

        private final InMemoryStorePort store;
        private final SrsCoordinator srsCoordinator;
        private final StoreBackedPipelineCaptureCoordinator coordinator;
        private final List<Released> released = new CopyOnWriteArrayList<>();
        private final SourceResource source = new SourceResource("orders_src", null, "mysql",
                Map.of("host", "h"), SourceMode.CDC, List.of(TableRef.literal("orders")), null, null);
        private Optional<TapstateException> refusal = Optional.empty();

        Fixture(boolean pThroughTheChain, boolean qThroughTheChain) {
            this(pThroughTheChain, qThroughTheChain, false);
        }

        /** With {@code owned}, the coordinator owns the captures it tails, as a server's does. */
        Fixture(boolean pThroughTheChain, boolean qThroughTheChain, boolean owned) {
            InMemoryArtifactStore artifacts = new InMemoryArtifactStore();
            artifacts.save(source);
            artifacts.save(pipeline("p", pThroughTheChain));
            artifacts.save(pipeline("q", qThroughTheChain));
            store = new InMemoryStorePort(artifacts);
            srsCoordinator = new SrsCoordinator(store.meta());
            coordinator = owned
                    ? new StoreBackedPipelineCaptureCoordinator(store, new CaptureAttacher() {
                        @Override
                        public CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff, boolean startTail) {
                            return Fixture.this.start(spec);
                        }

                        @Override
                        public Optional<TapstateException> release(CaptureRunSpec spec) {
                            return Fixture.this.release(spec);
                        }
                    }, srsCoordinator, new SnapshotBuffer())
                    : new StoreBackedPipelineCaptureCoordinator(store, new CaptureStarter() {
                        @Override
                        public CaptureRun start(CaptureRunSpec spec, CaptureHandoff handoff) {
                            return Fixture.this.start(spec);
                        }

                        @Override
                        public Optional<TapstateException> release(CaptureRunSpec spec) {
                            return Fixture.this.release(spec);
                        }
                    }, srsCoordinator, new SnapshotBuffer());
        }

        private Optional<TapstateException> release(CaptureRunSpec spec) {
            released.add(new Released(spec.pipelineId(), spec.srsEnabled(), chainRecordGone(), sharedNotes() > 0));
            return refusal;
        }

        void refuseWith(TapstateException refused) {
            refusal = Optional.of(refused);
        }

        private CaptureRun start(CaptureRunSpec spec) {
            MiningChainId chainId = spec.miningChainId();
            srsCoordinator.provisionSource(spec.sourceId(), chainId, spec.config().streams(), spec.retention());
            srsCoordinator.attachConsumer(chainId, spec.consumerId());
            return new CaptureRun(Optional.of(chainId), false, 0L, Optional.empty(), Optional.of(() -> {
            }), new CaptureHealth());
        }

        /** What a connector reading the shared chain records there: the name of the slot it created. */
        void noteASlotOnTheSharedChain() {
            store.keyedState().save(ConnectorStateNamespace.ofShared(chainId()), "tapdata_pg_slot",
                    "slot-7".getBytes(StandardCharsets.UTF_8));
        }

        long sharedNotes() {
            return store.keyedState().count(ConnectorStateNamespace.ofShared(chainId()));
        }

        void seedAChainNobodyIsRunning() {
            store.meta().create(chainId(), null);
        }

        void leaveACursorFor(String pipelineId) {
            store.meta().upsertConsumerOffset(chainId(), new ConsumerOffset(
                    SrsConsumerId.of(pipelineId, source.id()).value(), Map.of(), null));
        }

        boolean chainRecordGone() {
            return store.meta().read(chainId()).isEmpty();
        }

        private String chainId() {
            return MiningChainId.resolve(SourceCaptureResolution.of(source).config(), null).value();
        }

        private static PipelineResource pipeline(String id, boolean throughTheChain) {
            return new PipelineResource(id, null, List.of(SourceRef.spec("orders_src", throughTheChain)), null,
                    null, new ServeBlock.Inline(null, FromRef.literal("orders_src"),
                            List.of(new SyncElement("sync_1", "orders_src", null, null, null)), null, null),
                    new Settings(null, null, null, null, ReadMode.CDC_ONLY, "earliest"), null);
        }
    }
}
