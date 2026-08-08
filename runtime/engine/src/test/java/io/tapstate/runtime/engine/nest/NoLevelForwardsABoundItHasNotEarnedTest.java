package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.SinkProcessor;
import io.tapstate.runtime.engine.TransformProcessor;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

/**
 * Pins that no level passes on a bound that arrived at it. A bound handed to a level without an edge
 * attached to it has already been combined across every edge feeding that level, and passing it on says
 * "everything at or below this has left me" — which is a claim about this level's own work, not about its
 * upstream's. A level holding a change beneath that value, or one that has simply not emitted yet, would
 * be claiming the change is safely downstream when it is still sitting in its own state, and the frontier
 * would advance past it. The engine's default for that callback is to pass it on, so every level has to
 * say otherwise explicitly; the last test here is the control that shows the default really does forward
 * and that these overrides are therefore load-bearing rather than decorative.
 *
 * <p>What a level does with the bounds arriving <em>per edge</em> — combining them with what it holds and
 * emitting a bound of its own — is a separate matter from not passing this one on, and is not pinned here.
 */
class NoLevelForwardsABoundItHasNotEarnedTest {

    private static final Watermark BOUND = new Watermark(100, (byte) 1);

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    private final TestOutbox outbox = new TestOutbox(128);

    @Test
    void aResolverDoesNotPassOnABoundThatArrivedWithoutAnEdge() throws Exception {
        Processor resolver = new ResolverProcessor(TOPOLOGY.vertexAt(List.of("policies")),
                new HeapNestStore<>(), element -> {
                });

        assertThat(accepts(resolver)).isTrue();
        assertThat(whatWentOut())
                .describedAs("a resolver holds children waiting for a parent, so passing a bound on claims "
                        + "they have gone downstream when they are still in its own state")
                .isEmpty();
    }

    @Test
    void theAssemblerDoesNotPassOnABoundThatArrivedWithoutAnEdge() throws Exception {
        Processor assembler = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(),
                new HeapNestStore<>(), "doc");

        assertThat(accepts(assembler)).isTrue();
        assertThat(whatWentOut())
                .describedAs("the assembler holds orphans and undelivered documents, and a bound passed on "
                        + "would cover both")
                .isEmpty();
    }

    @Test
    void aStatelessTransformDoesNotPassOnABoundThatArrivedWithoutAnEdge() throws Exception {
        Processor transform = new TransformProcessor(event -> List.of(event));

        assertThat(accepts(transform)).isTrue();
        assertThat(whatWentOut())
                .describedAs("a transform holds nothing, but a bound of its own is still something it works "
                        + "out per edge rather than repeats")
                .isEmpty();
    }

    @Test
    void theSinkDoesNotPassOnABoundThatArrivedWithoutAnEdge() throws Exception {
        Processor sink = new SinkProcessor(neverCalled(), 1, 16);

        assertThat(accepts(sink)).isTrue();
        assertThat(whatWentOut())
                .describedAs("the sink is where a bound is consumed; anything it emits would be a record for "
                        + "the target")
                .isEmpty();
    }

    @Test
    void theEngineDefaultWouldHavePassedItOn() throws Exception {
        // The control: without an override this is what every level above would be doing, and it would do
        // it without an exception, a log line or a compile error to say so.
        Processor bare = new AbstractProcessor() {
        };

        assertThat(accepts(bare)).isTrue();
        assertThat(whatWentOut())
                .describedAs("if the default did not forward, the overrides above would be pinning nothing")
                .containsExactly(BOUND);
    }

    private boolean accepts(Processor processor) throws Exception {
        processor.init(outbox, new TestProcessorContext());
        return processor.tryProcessWatermark(BOUND);
    }

    private List<Object> whatWentOut() {
        List<Object> drained = new ArrayList<>();
        outbox.drainQueueAndReset(0, drained, false);
        return drained;
    }

    private static SinkWriter neverCalled() {
        return new SinkWriter() {

            @Override
            public CompletionStage<WriteResult> write(List<Envelope> records) {
                throw new AssertionError("a bound must never reach the writer");
            }

            @Override
            public void close() {
            }
        };
    }
}
