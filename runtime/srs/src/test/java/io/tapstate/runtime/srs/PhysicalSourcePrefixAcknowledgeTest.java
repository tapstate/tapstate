package io.tapstate.runtime.srs;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What a source is told it may release, on the chain where a quiet table's confirmation used to carry a
 * shared position past a busier table's change. The source is told only positions every table has landed,
 * in order, including one a run that carried no change named -- and never one past what a reader that lost
 * its generation could not write down.
 */
class PhysicalSourcePrefixAcknowledgeTest {

    private static final String CHAIN = "chain-told";

    private final CaptureRunUnitTest.InMemoryMeta meta = new CaptureRunUnitTest.InMemoryMeta();
    private final CaptureHealth health = new CaptureHealth();
    private final List<String> told = new CopyOnWriteArrayList<>();
    private PhysicalSourcePrefix prefix;
    private Subscription followed;

    @AfterEach
    void close() {
        if (prefix != null) {
            prefix.close();
        }
        if (followed != null) {
            followed.close();
        }
    }

    @Test
    void theSourceIsToldOnlyWhatEveryTableHasLandedInTheOrderItWasRead() {
        meta.create(CHAIN, null);
        long epoch = meta.openEpoch(CHAIN);
        meta.selectConsumerTables(CHAIN, "pipe", List.of("orders", "customers"), epoch);
        prefix = PhysicalSourcePrefix.shared(meta, CHAIN, epoch, List.of("customers", "orders"), health,
                (table, seq) -> { });
        prefix.start(Optional.of(new SourcePosition("t0")));
        followed = SourceAcknowledgements.follow(meta, CHAIN, recording(), health);

        prefix.admitted(Map.of("orders", 0L), "t1");
        prefix.admitted(Map.of("customers", 0L), "t2");
        prefix.admitted(Map.of(), "h3");
        ack(epoch, "customers");
        tellOnce();
        assertThat(told).as("the orders change has not landed, so nothing past where the stream began")
                .containsExactly("t0");

        ack(epoch, "orders");
        tellOnce();
        // The runs are released one after another, so a scheduled read landing between two of them tells a
        // position in between: still one every table landed, and still in order.
        List<String> released = List.of("t0", "t1", "t2", "h3");
        assertThat(told).as("released behind everything before it, the heartbeat's position is told last")
                .startsWith("t0").endsWith("h3")
                .isSubsetOf(released)
                .isSortedAccordingTo(java.util.Comparator.comparingInt(released::indexOf));
        List<String> beforeTheLoss = List.copyOf(told);

        prefix.admitted(Map.of("orders", 1L), "t4");
        meta.openEpoch(CHAIN);
        meta.advanceTableSinkAcked(CHAIN, "pipe", "orders", new ChainPosition(new SourceOrder(epoch, 1), null));
        catchThrowable(prefix::tick);
        tellOnce();
        assertThat(told).as("a reader that lost its generation wrote nothing, so nothing new is told")
                .isEqualTo(beforeTheLoss);
    }

    private void ack(long epoch, String table) {
        meta.advanceTableSinkAcked(CHAIN, "pipe", table, new ChainPosition(new SourceOrder(epoch, 0), null));
    }

    private void tellOnce() {
        prefix.tick();
        ((SourceAcknowledgements.Followed) followed).handOverQuietly();
    }

    private Subscription recording() {
        return new Subscription() {
            @Override
            public void acknowledge(SourcePosition durable) {
                told.add(durable.token());
            }

            @Override
            public void close() {
            }
        };
    }
}
