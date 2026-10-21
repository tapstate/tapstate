package io.tapstate.runtime.srs;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStartedListener;
import io.tapstate.spi.capture.SourcePosition;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The capture-health holder: it reports no failure while the tail is alive, keeps the first failure its cdc
 * stream reports, and its recording wrapper routes a listener's stream error onto it while passing events
 * through. Keeping the first matters because the cause that stopped the tail is the one worth surfacing; a
 * later straggler must not overwrite it. What became of an acknowledged position is read off as well, as
 * readings that never fail the run.
 */
class CaptureHealthTest {

    /**
     * Stands in for the code a failed acknowledgement carries. The adapter that raises the real one is out of
     * this module's reach, and all the health keeps of it is the code string.
     */
    private static final TapstateErrorCode ACKNOWLEDGE_FAILED = new TapstateErrorCode() {
        @Override
        public String code() {
            return "connector.acknowledge-failed";
        }

        @Override
        public Severity severity() {
            return Severity.ERROR;
        }

        @Override
        public Set<String> placeholders() {
            return Set.of();
        }
    };

    @Test
    void isEmptyUntilAFailureIsRecorded() {
        assertThat(new CaptureHealth().failure()).isEmpty();
    }

    @Test
    void keepsTheFirstFailureWhenSeveralAreReported() {
        CaptureHealth health = new CaptureHealth();
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");

        health.fail(first);
        health.fail(second);

        // A stream reports one failure, but if several ever arrive the cause that stopped the tail is the one
        // kept -- a later straggler must not overwrite it.
        assertThat(health.failure()).contains(first);
    }

    @Test
    void theRecordingListenerPassesEventsThroughAndRecordsAnError() {
        CaptureHealth health = new CaptureHealth();
        Envelope[] delivered = new Envelope[1];
        CaptureListener listener = health.recording((events, pos) -> delivered[0] = events.get(0));
        assertThat(listener).isInstanceOf(CaptureStartedListener.class);
        Envelope event = Envelope.insert(1L, "orders", Map.of("id", 1), Map.of());

        listener.onBatch(List.of(event), Optional.empty());
        assertThat(delivered[0]).as("events pass through to the wrapped handler").isSameAs(event);
        assertThat(health.failure()).as("no failure while only events flow").isEmpty();

        RuntimeException boom = new RuntimeException("stream boom");
        listener.onError(boom);
        assertThat(health.failure()).as("an error on the listener is recorded on the health").contains(boom);
    }

    @Test
    void onlyAnOptedInDelegateReceivesItsStartingAnchorWithoutCountingItAsAChange() {
        CaptureHealth health = new CaptureHealth();
        SourcePosition[] observed = new SourcePosition[1];
        Envelope[] delivered = new Envelope[1];
        CaptureListener recorded = health.recording(new CaptureStartedListener() {
            @Override
            public void onStart(SourcePosition position) {
                observed[0] = position;
            }

            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                delivered[0] = events.get(0);
            }
        });
        assertThat(recorded).isInstanceOf(CaptureStartedListener.class);
        SourcePosition anchor = new SourcePosition("before-first-change");
        ((CaptureStartedListener) recorded).onStart(anchor);

        assertThat(observed[0]).isSameAs(anchor);
        assertThat(health.receivedRows()).isEmpty();
        Envelope change = Envelope.insert(1L, "orders", Map.of("id", 1), Map.of());
        recorded.onBatch(List.of(change), Optional.empty());
        assertThat(delivered[0]).isSameAs(change);
        assertThat(health.receivedRows()).isEqualTo(Map.of("orders", Map.of("i", 1L)));
        RuntimeException failure = new RuntimeException("capture failed");
        recorded.onError(failure);
        assertThat(health.failure()).contains(failure);
    }

    /**
     * A failed acknowledgement is counted and its code kept, and the run is not failed.
     *
     * <p>The run's failure is shared by every pipeline reading the capture, and fails all of them. What a
     * position the source was not told costs is some log kept a while longer, so failing those pipelines over
     * it would make an outage of a delay; a count that climbs for as long as it keeps happening is what it gets
     * instead.
     */
    @Test
    void aFailedAcknowledgementIsCountedAndItsCodeKeptWithoutFailingTheRun() {
        CaptureHealth health = new CaptureHealth();
        List<Throwable> handedOn = new ArrayList<>();
        CaptureListener listener = health.recording(new CaptureListener() {
            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
            }

            @Override
            public void onAcknowledgeFailed(Throwable failure) {
                handedOn.add(failure);
            }
        });
        assertThat(health.consecutiveAcknowledgeFailures()).as("nothing has failed yet").isZero();
        assertThat(health.lastAcknowledgeFailureCode()).isEmpty();
        assertThat(health.lastAcknowledgedAt()).isEmpty();

        TapstateException refused = new TapstateException(ACKNOWLEDGE_FAILED, Map.of(), null);
        listener.onAcknowledgeFailed(refused);
        listener.onAcknowledgeFailed(refused);

        assertThat(health.consecutiveAcknowledgeFailures()).isEqualTo(2);
        assertThat(health.lastAcknowledgeFailureCode()).contains("connector.acknowledge-failed");
        assertThat(health.failure()).as("a release that is only late fails nothing").isEmpty();
        assertThat(handedOn).as("handed on to the wrapped listener").containsExactly(refused, refused);

        listener.onAcknowledgeFailed(new IllegalStateException("carries no code"));

        assertThat(health.consecutiveAcknowledgeFailures()).isEqualTo(3);
        assertThat(health.lastAcknowledgeFailureCode()).as("the last failure carried no code").isEmpty();
        assertThat(health.failure()).isEmpty();
    }

    @Test
    void anAcknowledgementThatGoesThroughResetsTheCountAndSaysWhen() {
        CaptureHealth health = new CaptureHealth();
        List<SourcePosition> handedOn = new ArrayList<>();
        CaptureListener listener = health.recording(new CaptureListener() {
            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
            }

            @Override
            public void onAcknowledged(SourcePosition position) {
                handedOn.add(position);
            }
        });
        listener.onAcknowledgeFailed(new TapstateException(ACKNOWLEDGE_FAILED, Map.of(), null));
        listener.onAcknowledgeFailed(new TapstateException(ACKNOWLEDGE_FAILED, Map.of(), null));
        SourcePosition released = new SourcePosition("released-to");

        Instant before = Instant.now();
        listener.onAcknowledged(released);
        Instant after = Instant.now();

        assertThat(health.consecutiveAcknowledgeFailures()).as("the run of failures is over").isZero();
        assertThat(health.lastAcknowledgedAt())
                .hasValueSatisfying(at -> assertThat(at).isBetween(before, after));
        assertThat(health.lastAcknowledgeFailureCode())
                .as("what the last failure was is still there to read")
                .contains("connector.acknowledge-failed");
        assertThat(health.failure()).isEmpty();
        assertThat(handedOn).as("handed on to the wrapped listener").containsExactly(released);
    }

    /**
     * A listener that also hears where its stream begins is wrapped as one, and what became of its
     * acknowledgements is read off through that wrapper the same way: a direct channel is started with one.
     */
    @Test
    void aListenerThatHearsTheStartHasItsAcknowledgementsReadOffToo() {
        CaptureHealth health = new CaptureHealth();
        List<String> heard = new ArrayList<>();
        CaptureListener listener = health.recording(new CaptureStartedListener() {
            @Override
            public void onStart(SourcePosition position) {
                heard.add("start " + position.token());
            }

            @Override
            public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
            }

            @Override
            public void onAcknowledged(SourcePosition position) {
                heard.add("acknowledged " + position.token());
            }

            @Override
            public void onAcknowledgeFailed(Throwable failure) {
                heard.add("failed");
            }
        });
        assertThat(listener).isInstanceOf(CaptureStartedListener.class);

        listener.onAcknowledgeFailed(new TapstateException(ACKNOWLEDGE_FAILED, Map.of(), null));
        assertThat(health.consecutiveAcknowledgeFailures()).isEqualTo(1);
        listener.onAcknowledged(new SourcePosition("p1"));

        assertThat(health.consecutiveAcknowledgeFailures()).isZero();
        assertThat(health.lastAcknowledgedAt()).isPresent();
        assertThat(health.failure()).isEmpty();
        assertThat(heard).containsExactly("failed", "acknowledged p1");
    }
}
