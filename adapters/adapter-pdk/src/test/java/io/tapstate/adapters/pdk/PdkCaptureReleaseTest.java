package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SharedNotes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Letting go of what a connector set up on its source to read changes -- above all the replication slot it
 * created -- through the notes a read over the same config keeps: the physical capture's for a shared one, the
 * node's own for a source read directly.
 *
 * <p>Each case that expects a slot let go of compares it with the slot an earlier drive created, so a release
 * opened over notes other than the ones that drive kept reads no slot, or a slot of its own, and fails the
 * comparison rather than passing it.
 */
class PdkCaptureReleaseTest {

    private static final String CHANNEL = "tapstate.test.slot-keeping-source";

    private static final PipelineNode P1 = new PipelineNode("p1", "src");

    private final HeapKeyedState store = new HeapKeyedState();
    private final Map<String, Object> channel = new ConcurrentHashMap<>();
    private final List<Object> released = new CopyOnWriteArrayList<>();
    private ConnectorRef ref;
    private PdkCapturePort port;

    @BeforeEach
    void openThePort(@TempDir Path dir) {
        channel.put("released", released);
        System.getProperties().put(CHANNEL, channel);
        Path jar = Synthetic.slotKeepingSource(dir, CHANNEL);
        ref = new ConnectorRef(List.of(jar), "synthetic.SlotKeepingSource", "2.0.8", null);
        port = new PdkCapturePort(connectorId -> ref, store);
    }

    @AfterEach
    void closeTheChannel() {
        System.getProperties().remove(CHANNEL);
    }

    /**
     * Releasing a shared capture has its connector let go of the slot the capture's notes name. The notes stay
     * where they are: they are the only record of what was set up, and dropping them is the caller's.
     */
    @Test
    void releasingASharedCaptureLetsGoOfTheSlotItsNotesName() {
        String created = slotReadBy(sharedOn("chain-a"));

        assertThat(port.release(sharedOn("chain-a"))).isEmpty();

        assertThat(released).as("the connector was asked to let go of the slot the capture's notes name")
                .containsExactly(created);
        assertThat(store.count(ConnectorStateNamespace.ofShared("chain-a")))
                .as("the notes are left for the caller").isPositive();
    }

    /**
     * A slot recorded in the node's own notes, before the capture kept notes of its own, is still the one let
     * go of: the release reads the capture's notes the way its stream does, carrying over from the earlier node.
     */
    @Test
    void aSlotOnlyTheEarlierNodeRecordedIsLetGoOfThroughTheCapturesNotes() {
        String kept = slotReadBy(config().at(P1));

        assertThat(port.release(sharedOn("chain-b"))).isEmpty();

        assertThat(released).containsExactly(kept);
    }

    /** A source read directly keeps its notes as its node's own, and releasing that node lets go of its slot. */
    @Test
    void releasingANodeLetsGoOfTheSlotItsOwnNotesName() {
        String created = slotReadBy(config().at(P1));

        assertThat(port.release(config().at(P1))).isEmpty();

        assertThat(released).containsExactly(created);
    }

    /**
     * A source that cannot be reached keeps its slot; the release answers with a coded refusal naming it, for
     * somebody to drop by hand, instead of throwing -- a clearing that failed over it would leave a pipeline
     * that can neither keep its state nor let go of it.
     */
    @Test
    void aSourceThatCannotBeReachedIsAnsweredForWithTheSlotItKeeps() {
        String created = slotReadBy(sharedOn("chain-c"));
        channel.put("unreachable", true);

        Optional<TapstateException> refused = port.release(sharedOn("chain-c"));

        assertThat(refused).hasValueSatisfying(refusal -> {
            assertThat(refusal.code()).isEqualTo(ConnectorError.RELEASE_FAILED);
            assertThat(refusal.args()).containsEntry("connector", "postgres").containsEntry("resources", created);
            assertThat(refusal.args().get("detail").toString()).contains("connection refused");
        });
    }

    /**
     * A source that never answers is answered for once the release's bound runs out, with the slot it keeps,
     * and the connector's call is interrupted: the clearing that asked is not held for as long as the source
     * stays silent.
     */
    @Test
    void aReleaseThatDoesNotAnswerIsAnsweredForOnceItsBoundRunsOut() throws InterruptedException {
        port = new PdkCapturePort(connectorId -> ref, store, PdkCapturePort.DEFAULT_PREFLIGHT_TIMEOUT,
                Duration.ofSeconds(5), System::nanoTime, Duration.ofMillis(300));
        String created = slotReadBy(config().at(P1));
        channel.put("hang", true);

        long began = System.nanoTime();
        Optional<TapstateException> refused = port.release(config().at(P1));

        assertThat(Duration.ofNanos(System.nanoTime() - began)).isLessThan(Duration.ofSeconds(10));
        assertThat(refused).hasValueSatisfying(refusal -> {
            assertThat(refusal.code()).isEqualTo(ConnectorError.RELEASE_FAILED);
            assertThat(refusal.args()).containsEntry("resources", created);
            assertThat(refusal.args().get("detail").toString()).contains("did not answer");
        });
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!channel.containsKey("interrupted") && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(channel).as("the connector's call was interrupted").containsKey("interrupted");
    }

    /**
     * A release given up on can no longer touch its notes: what it would still write there would come back after
     * the caller dropped them, and what it would still read could be what a run started since keeps there.
     */
    @Test
    void aReleaseGivenUpOnCanNoLongerTouchItsNotes() throws InterruptedException {
        port = new PdkCapturePort(connectorId -> ref, store, PdkCapturePort.DEFAULT_PREFLIGHT_TIMEOUT,
                Duration.ofSeconds(5), System::nanoTime, Duration.ofMillis(300));
        slotReadBy(config().at(P1));
        channel.put("hang", true);
        channel.put("writeAfterInterrupt", true);

        assertThat(port.release(config().at(P1))).isPresent();

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!channel.containsKey("lateWrite") && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(channel).as("the late write was refused").containsEntry("lateWrite", "refused");
        assertThat(store.load(ConnectorStateNamespace.of(P1), "late")).isEmpty();
    }

    /** A connector built on the postgres connector keeps its slot under the same note, and is answered for alike. */
    @Test
    void aSlotIsNamedWhateverTheConnectorIsCalled() {
        CaptureConfig highgo = new CaptureConfig("highgo", Map.of(), List.of("t1")).at(P1);
        String created = slotReadBy(highgo);
        channel.put("unreachable", true);

        assertThat(port.release(highgo)).hasValueSatisfying(refusal ->
                assertThat(refusal.args()).containsEntry("connector", "highgo").containsEntry("resources", created));
    }

    /**
     * What notes name on the source can be read without a connector -- for a clearing whose capture nothing defined
     * reads any more -- each name once, and a note that cannot be read is passed over.
     */
    @Test
    void whatNotesNameIsReadWithoutAConnector() {
        store.save("pdk.chain.c1", "tapdata_pg_slot", ConnectorStateCodec.encode("slot-a"));
        store.save(ConnectorStateNamespace.of(P1), "tapdata_pg_slot", ConnectorStateCodec.encode("slot-a"));
        store.save(ConnectorStateNamespace.of(new PipelineNode("p2", "src")), "tapdata_pg_slot",
                new byte[]{9, 9});

        assertThat(PdkCapturePort.namedIn(store, List.of("pdk.chain.c1", ConnectorStateNamespace.of(P1),
                ConnectorStateNamespace.of(new PipelineNode("p2", "src")), "pdk.chain.empty")))
                .containsExactly("slot-a");
    }

    /**
     * A drive that kept no notes anywhere a later one could read set nothing up through them, so nothing is
     * asked of the connector: neither a config naming no node nor a port with no store to keep notes in.
     */
    @Test
    void aDriveThatKeptNoNotesHasNothingToLetGoOf() {
        assertThat(port.release(config())).isEmpty();
        assertThat(new PdkCapturePort(connectorId -> ref).release(config().at(P1))).isEmpty();

        assertThat(released).as("the connector was never asked").isEmpty();
    }

    /** One snapshot drive over {@code config}, answering which slot it read through. */
    private String slotReadBy(CaptureConfig config) {
        List<Envelope> rows = new ArrayList<>();
        try (CaptureBatch batch = port.snapshot(config)) {
            while (batch.hasNext()) {
                rows.add(batch.next());
            }
        }
        assertThat(rows).as("the drive reached the connector's read").hasSize(1);
        return String.valueOf(rows.get(0).after().get("slot"));
    }

    /** A config whose notes are chain {@code chainId}'s, opened for {@link #P1} and carried over from its own. */
    private static CaptureConfig sharedOn(String chainId) {
        return config().at(P1).sharing(new SharedNotes(chainId, List.of(P1)));
    }

    /** Named after the connector whose notes name a slot, so a refusal can say which slot is left. */
    private static CaptureConfig config() {
        return new CaptureConfig("postgres", Map.of(), List.of("t1"));
    }
}
