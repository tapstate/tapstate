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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a connector records for a change stream several pipelines share -- above all the replication slot it
 * created on the source -- is the stream's, whichever pipeline opens it; and letting go of the stream lets go
 * of the slot.
 *
 * <p>Filed under the pipeline that opened the stream, the notes are empty the next time another pipeline
 * does: the connector reads that as a first run and creates a fresh slot where the source is now, every
 * change in between is gone, and the old slot holds the source's log for good. Each case below that reads a
 * slot back compares it with the slot an earlier drive created, so a drive handed empty notes reads a slot
 * of its own and fails the comparison rather than passing it.
 */
class AChainsStreamKeepsOneSetOfNotesTest {

    private static final String CHANNEL = "tapstate.test.slot-keeping-source";

    private static final PipelineNode P1 = new PipelineNode("p1", "src");
    private static final PipelineNode P2 = new PipelineNode("p2", "src");

    private final HeapKeyedState store = new HeapKeyedState();
    private final Map<String, Object> channel = new ConcurrentHashMap<>();
    private final List<Object> released = new CopyOnWriteArrayList<>();
    private PdkCapturePort port;

    @BeforeEach
    void openThePort(@TempDir Path dir) {
        channel.put("released", released);
        System.getProperties().put(CHANNEL, channel);
        Path jar = Synthetic.slotKeepingSource(dir, CHANNEL);
        ConnectorRef ref = new ConnectorRef(List.of(jar), "synthetic.SlotKeepingSource", "2.0.8", null);
        port = new PdkCapturePort(connectorId -> ref, store);
    }

    @AfterEach
    void closeTheChannel() {
        System.getProperties().remove(CHANNEL);
    }

    /**
     * The stream is the same stream whoever opens it: the pipeline that opens it next, and the one still on
     * it after the pipeline that created the slot was cleared, both read through that slot. The last reading
     * is what the notes used to do -- the node's own notes are empty for any node but the one that wrote
     * them -- and is here so the first two cannot be passing for a reason that has nothing to do with the
     * chain.
     */
    @Test
    void everyPipelineOnAChainReadsThroughTheSlotTheFirstOneCreated() {
        String created = slotReadBy(sharedOn("chain-a", P1));

        assertThat(slotReadBy(sharedOn("chain-a", P2)))
                .as("another pipeline on the chain opening the stream next").isEqualTo(created);
        store.dropNamespace(ConnectorStateNamespace.of(P1));
        assertThat(slotReadBy(sharedOn("chain-a", P2)))
                .as("after the pipeline that created it was cleared").isEqualTo(created);
        assertThat(slotReadBy(config().at(P2)))
                .as("the node's own notes, which this pipeline never wrote to").isNotEqualTo(created);
    }

    /**
     * A slot recorded while each pipeline still kept its own notes is carried over the first time the chain's
     * notes are asked for it -- from the pipeline opening the stream first, then from the others on the chain
     * -- and once carried it no longer depends on where it came from.
     */
    @Test
    void aSlotRecordedBeforeTheChainKeptNotesIsCarriedOverOnce() {
        String p1Kept = slotReadBy(config().at(P1));

        assertThat(slotReadBy(sharedOn("chain-b", P2, P1)))
                .as("opened by a pipeline that kept none, carried from one on the chain that did")
                .isEqualTo(p1Kept);
        store.dropNamespace(ConnectorStateNamespace.of(P1));
        assertThat(slotReadBy(sharedOn("chain-b", P2)))
                .as("read from the chain's own notes once carried").isEqualTo(p1Kept);
    }

    /** Where two pipelines each kept a slot, the one opening the stream is asked first. */
    @Test
    void theNotesOfThePipelineOpeningTheStreamAreAskedFirst() {
        slotReadBy(config().at(P1));
        String p2Kept = slotReadBy(config().at(P2));

        assertThat(slotReadBy(sharedOn("chain-c", P2, P1))).isEqualTo(p2Kept);
    }

    /**
     * Releasing a chain's notes has its connector let go of the slot they name, then drops them: whatever runs
     * over the chain next begins from notes that name nothing, rather than from a slot that is gone.
     */
    @Test
    void releasingAChainLetsGoOfItsSlotAndThenOfItsNotes() {
        String created = slotReadBy(sharedOn("chain-d", P1));

        assertThat(port.release(sharedOn("chain-d", P1))).isEmpty();

        assertThat(released).as("the connector was asked to let go of the slot its notes name")
                .containsExactly(created);
        assertThat(store.count(ConnectorStateNamespace.ofShared("chain-d"))).as("and the notes went").isZero();
        assertThat(slotReadBy(sharedOn("chain-d", P1))).isNotEqualTo(created);
    }

    /** A source read directly keeps its notes as its node's own, and releasing that node lets go of its slot. */
    @Test
    void releasingANodeLetsGoOfTheSlotItsOwnNotesName() {
        String created = slotReadBy(config().at(P1));

        assertThat(port.release(config().at(P1))).isEmpty();

        assertThat(released).containsExactly(created);
        assertThat(store.count(ConnectorStateNamespace.of(P1))).isZero();
    }

    /**
     * A source that cannot be reached keeps its slot; the release answers with a coded refusal naming it, for
     * somebody to drop by hand, instead of throwing -- a clearing that failed over it would leave a pipeline
     * that can neither keep its state nor let go of it. The notes go all the same.
     */
    @Test
    void aSourceThatCannotBeReachedIsAnsweredForWithTheSlotItKeeps() {
        String created = slotReadBy(sharedOn("chain-e", P1));
        channel.put("unreachable", true);

        Optional<TapstateException> refused = port.release(sharedOn("chain-e", P1));

        assertThat(refused).hasValueSatisfying(refusal -> {
            assertThat(refusal.code()).isEqualTo(ConnectorError.RELEASE_FAILED);
            assertThat(refusal.args()).containsEntry("connector", "postgres").containsEntry("resources", created);
            assertThat(refusal.args().get("detail").toString()).contains("connection refused");
        });
        assertThat(store.count(ConnectorStateNamespace.ofShared("chain-e"))).isZero();
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

    /**
     * A config whose notes are chain {@code chainId}'s, opened for {@code opener} and carried over from its
     * own notes and then from each of {@code others}'.
     */
    private static CaptureConfig sharedOn(String chainId, PipelineNode opener, PipelineNode... others) {
        List<PipelineNode> carriedFrom = new ArrayList<>(List.of(opener));
        carriedFrom.addAll(List.of(others));
        return config().at(opener).sharing(new SharedNotes(chainId, carriedFrom));
    }

    /** Named after the connector whose notes name a slot, so a refusal can say which slot is left. */
    private static CaptureConfig config() {
        return new CaptureConfig("postgres", Map.of(), List.of("t1"));
    }
}
