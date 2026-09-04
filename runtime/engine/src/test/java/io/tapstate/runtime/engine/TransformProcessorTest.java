package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestSupport;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.transform.TransformPort;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The generic adapter that wraps a stateless {@link TransformPort} into a Jet processor: for each
 * inbound event it emits every event the port returns, drops the event when the port returns none,
 * and fans out when the port returns several. The bounded-outbox scenarios TestSupport runs also
 * pin that the adapter, not the port, carries the emit-side backpressure. It stamps the inbound
 * source position onto every output (the port itself is pure and never carries one), and its vertex
 * runs at total parallelism one so the ordered position stream a sink acks is never re-laned.
 */
class TransformProcessorTest {

    private static Envelope event(int id) {
        return Envelope.insert(id, "orders", Map.of("id", id), null);
    }

    /** What a stateless port produces: a fresh envelope of its own, carrying no source position. */
    private static Envelope rebuiltWithoutPosition(Envelope e) {
        return new Envelope(e.op(), e.ts(), e.src(), e.before(), e.after(), e.schema());
    }

    @Test
    void map_keeps_the_one_event() {
        TransformPort identity = e -> List.of(e);
        TestSupport.verifyProcessor(() -> new TransformProcessor(identity))
                .input(List.of(event(1), event(2)))
                .expectOutput(List.of(event(1), event(2)));
    }

    @Test
    void filter_drops_by_emitting_none() {
        TransformPort dropAll = e -> List.of();
        TestSupport.verifyProcessor(() -> new TransformProcessor(dropAll))
                .input(List.of(event(1), event(2)))
                .expectOutput(List.of());
    }

    /**
     * Word that a chain got past changes with nothing to deliver for them is not a change, and a port is a
     * pure function over rows - so it is passed on rather than handed to one. A nest may be followed by a
     * transform, which is where such a word meets this adapter; handing it over casts it to an envelope and
     * kills the job, and dropping it leaves the frontier of whatever sent it standing still for the whole
     * run. The port here drops everything, so what comes out is the word and only because it was passed on.
     */
    @Test
    void word_about_a_chain_is_passed_on_rather_than_transformed() {
        TransformPort dropAll = e -> List.of();
        SettledPositions word = new SettledPositions(
                Map.of("orders", new ChainPosition(new SourceOrder(1, 7), "p7")));
        TestSupport.verifyProcessor(() -> new TransformProcessor(dropAll))
                .input(List.of(event(1), word, event(2)))
                .expectOutput(List.of(word));
    }

    @Test
    void fan_out_emits_each_returned_event() {
        TransformPort duplicate = e -> List.of(e, e);
        TestSupport.verifyProcessor(() -> new TransformProcessor(duplicate))
                .input(List.of(event(1)))
                .expectOutput(List.of(event(1), event(1)));
    }

    @Test
    void stamps_the_inbound_source_position_onto_the_kept_event() {
        // the port rebuilds a positionless envelope; the adapter must carry the inbound position over.
        TransformPort rebuildingMap = e -> List.of(rebuiltWithoutPosition(e));
        TestSupport.verifyProcessor(() -> new TransformProcessor(rebuildingMap))
                .input(List.of(event(1).withSrcPos("p1")))
                .expectOutput(List.of(event(1).withSrcPos("p1")));
    }

    @Test
    void stamps_the_inbound_source_position_onto_every_fan_out_output() {
        TransformPort duplicate = e -> List.of(rebuiltWithoutPosition(e), rebuiltWithoutPosition(e));
        TestSupport.verifyProcessor(() -> new TransformProcessor(duplicate))
                .input(List.of(event(1).withSrcPos("p1")))
                .expectOutput(List.of(event(1).withSrcPos("p1"), event(1).withSrcPos("p1")));
    }

    @Test
    void leaves_the_position_null_when_the_inbound_event_has_none() {
        TransformPort rebuildingMap = e -> List.of(rebuiltWithoutPosition(e));
        TestSupport.verifyProcessor(() -> new TransformProcessor(rebuildingMap))
                .input(List.of(event(1)))
                .expectOutput(List.of(event(1)));
    }

    @Test
    void pins_the_transform_vertex_to_total_parallelism_one() throws Exception {
        TransformPort identity = e -> List.of(e);
        // The whole-cluster pin, not the preferredLocalParallelism proxy: a per-member supplier would
        // also report 1 but re-lane the ordered position stream across members.
        assertThat(TotalParallelismOne.pins(TransformProcessor.metaSupplier(() -> identity), 3)).isTrue();
    }
}
