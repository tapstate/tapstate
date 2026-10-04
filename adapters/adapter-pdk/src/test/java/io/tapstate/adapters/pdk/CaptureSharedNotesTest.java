package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SharedNotes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.MDC;
import org.slf4j.helpers.BasicMDCAdapter;
import org.slf4j.spi.MDCAdapter;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Native capture identity survives an owner change without mixing independent source notes. */
class CaptureSharedNotesTest {

    private static final PipelineNode FIRST = new PipelineNode("support_pipeline", "case_source");
    private static final PipelineNode NEXT = new PipelineNode("mail_pipeline", "mail_source");
    private static final String SHARED = ConnectorStateNamespace.ofShared("mc-crm");
    private final HeapKeyedState store = new HeapKeyedState();

    private DurableStateMap shared(PipelineNode... earlier) {
        return new DurableStateMap(store, SHARED,
                List.of(earlier).stream().map(ConnectorStateNamespace::of).toList(), true);
    }

    private DurableStateMap privateNotes(PipelineNode node) {
        return new DurableStateMap(store, ConnectorStateNamespace.of(node));
    }

    @Test
    void aNewOwnerReadsTheNativeSlotCarriedFromItsKnownEarlierOwner() {
        privateNotes(FIRST).put("slot", "slot-before-restart");

        assertThat(shared(FIRST).get("slot")).isEqualTo("slot-before-restart");
        assertThat(shared(NEXT).get("slot")).isEqualTo("slot-before-restart");
        assertThat(privateNotes(NEXT).get("slot")).isNull();
        assertThat(privateNotes(FIRST).get("slot")).isEqualTo("slot-before-restart");
    }

    @Test
    void contradictoryOldNotesAreRefusedInsteadOfChoosingOneNativeSlot() {
        privateNotes(FIRST).put("slot", "slot-one");
        privateNotes(NEXT).put("slot", "slot-two");

        assertThatThrownBy(() -> shared(FIRST, NEXT).get("slot"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> {
                    TapstateException failure = (TapstateException) thrown;
                    assertThat(failure.code()).isEqualTo(ConnectorError.STATE_UNREADABLE);
                    assertThat(failure.args().get("detail").toString()).contains("restore verified notes");
                });
        assertThat(store.count(SHARED)).isZero();
        assertThat(privateNotes(FIRST).get("slot")).isEqualTo("slot-one");
        assertThat(privateNotes(NEXT).get("slot")).isEqualTo("slot-two");
    }

    @Test
    void consistentOldNotesCanBeCarriedAndCurrentSharedNotesRemainAuthoritative() {
        privateNotes(FIRST).put("slot", "same-slot");
        privateNotes(NEXT).put("slot", "same-slot");
        assertThat(shared(FIRST, NEXT).get("slot")).isEqualTo("same-slot");

        shared().put("slot", "current-slot");
        privateNotes(NEXT).put("slot", "an-unrelated-new-private-slot");
        assertThat(shared(FIRST, NEXT).get("slot")).isEqualTo("current-slot");
    }

    @Test
    void expiredSharedNotesDoNotReturnFromTheEarlierNodesAfterRestart() {
        privateNotes(FIRST).put("slot", "old-slot");
        DurableStateMap current = shared(FIRST);
        assertThat(current.remove("slot")).isEqualTo("old-slot");

        assertThat(shared(FIRST).get("slot")).isNull();
        assertThat(privateNotes(FIRST).get("slot")).isEqualTo("old-slot");
        shared(FIRST).put("slot", "replacement-slot");
        assertThat(shared(FIRST).get("slot")).isEqualTo("replacement-slot");
        shared(FIRST).put("slot", null);
        assertThat(shared(FIRST).get("slot")).isNull();
    }

    @Test
    void clearingSharedNotesPreservesOldNodesAndBlocksTheirUnreadKeys() {
        privateNotes(FIRST).put("slot", "old-slot");
        privateNotes(FIRST).put("unread-checkpoint", 7L);
        shared(FIRST).get("slot");

        shared(FIRST).clear();

        assertThat(shared(FIRST).get("slot")).isNull();
        assertThat(shared(FIRST).get("unread-checkpoint")).isNull();
        assertThat(privateNotes(FIRST).get("slot")).isEqualTo("old-slot");
        assertThat(privateNotes(FIRST).get("unread-checkpoint")).isEqualTo(7L);
        shared(FIRST).put("slot", "new-slot");
        assertThat(shared(FIRST).get("slot")).isEqualTo("new-slot");
        shared(FIRST).reset();
        assertThat(shared(FIRST).get("slot")).isNull();
    }

    @Test
    void anUnverifiedStoredValueIsNotCopiedIntoThePhysicalCaptureNamespace() {
        store.save(ConnectorStateNamespace.of(FIRST), "slot", new byte[]{99});

        assertThatThrownBy(() -> shared(FIRST).get("slot"))
                .isInstanceOf(TapstateException.class)
                .satisfies(thrown -> assertThat(((TapstateException) thrown).code())
                        .isEqualTo(ConnectorError.STATE_UNREADABLE));
        assertThat(store.count(SHARED)).isZero();
        assertThat(store.load(ConnectorStateNamespace.of(FIRST), "slot"))
                .hasValueSatisfying(bytes -> assertThat(bytes).containsExactly((byte) 99));
    }

    @Test
    void claimingANativeIdentityKeepsTheCarriedIdentityAndOneSharedWinner() {
        privateNotes(FIRST).put("slot", "kept-slot");
        assertThat(shared(FIRST).putIfAbsent("slot", "unwanted-slot")).isEqualTo("kept-slot");
        assertThat(shared(NEXT).putIfAbsent("slot", "another-unwanted-slot")).isEqualTo("kept-slot");
        assertThat(shared(FIRST).putIfAbsent("new-key", "first")).isNull();
        assertThat(shared(NEXT).putIfAbsent("new-key", "second")).isEqualTo("first");
    }

    @Test
    void sharedPortRoutingKeepsActualOwnerAttributionAndOrdinaryRouting(@TempDir Path temporary) throws Throwable {
        Path jar = Synthetic.emittingSource(temporary);
        ConnectorRef reference = new ConnectorRef(List.of(jar), "synthetic.EmittingSource", "2.0.8", null);
        PdkCapturePort port = new PdkCapturePort(ignored -> reference, store);
        SharedNotes notes = new SharedNotes("mc-crm", List.of(FIRST));
        CaptureConfig capture = new CaptureConfig("demo", Map.of(), List.of("t1"), NEXT).sharing(notes);

        // This adapter module carries the logging facade only. Supply the facade's real in-memory MDC
        // for this assertion, then restore its original provider without adding a production backend.
        MDCAdapter original = MDC.getMDCAdapter();
        Field adapterField = MDC.class.getDeclaredField("MDC_ADAPTER");
        adapterField.setAccessible(true);
        adapterField.set(null, new BasicMDCAdapter());
        try {
            MDC.put(PipelineAttribution.MDC_KEY, "caller_pipeline");
            try (CaptureBatch batch = port.snapshot(capture)) {
                PdkConnector opened = ((PdkCaptureBatch) batch).connector();
                assertThat(opened.stateNamespace()).isEqualTo(SHARED);
                assertThat(opened.underLoader(() -> MDC.get(PipelineAttribution.MDC_KEY)))
                        .isEqualTo(NEXT.pipelineId());
                assertThat(MDC.get(PipelineAttribution.MDC_KEY))
                        .as("the actual owner's claim restores the caller's prior attribution")
                        .isEqualTo("caller_pipeline");
                opened.context().getStateMap().put("slot", "shared-slot");
            }
            assertThat(MDC.get(PipelineAttribution.MDC_KEY)).isEqualTo("caller_pipeline");
        } finally {
            adapterField.set(null, original);
        }
        try (CaptureBatch batch = port.snapshot(capture.at(FIRST))) {
            PdkConnector opened = ((PdkCaptureBatch) batch).connector();
            assertThat(opened.stateNamespace()).isEqualTo(SHARED);
            assertThat(opened.context().getStateMap().get("slot")).isEqualTo("shared-slot");
        }
        try (CaptureBatch batch = port.snapshot(new CaptureConfig("demo", Map.of(), List.of("t1"), NEXT))) {
            PdkConnector opened = ((PdkCaptureBatch) batch).connector();
            assertThat(opened.stateNamespace()).isEqualTo(ConnectorStateNamespace.of(NEXT));
            assertThat(opened.context().getStateMap().get("slot")).isNull();
        }
    }
}
