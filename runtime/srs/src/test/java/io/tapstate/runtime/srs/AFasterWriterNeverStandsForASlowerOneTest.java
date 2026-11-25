package io.tapstate.runtime.srs;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.srs.SrsWriterFrontier.Landed;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterRun;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * How far a pipeline has landed a table is as far as the slowest writer expected to receive it, and nothing
 * at all while one of them has said nothing - a faster writer's progress proves nothing about the changes a
 * slower one still holds, and a resume from the faster one's position skips exactly those.
 */
class AFasterWriterNeverStandsForASlowerOneTest {

    private static SourceOrder at(long seq) {
        return new SourceOrder(3, seq);
    }

    private static ChainPosition tokened(long seq) {
        return new ChainPosition(at(seq), "w" + seq);
    }

    private static WriterRun run(Map<String, WriterProgress> progress) {
        return new WriterRun("g7", Map.of("orders", List.of("serve.a#0", "serve.b#0", "view.v#0")),
                Map.of("orders", progress), 3L);
    }

    @Test
    void theSlowestExpectedWriterDecidesHowFarTheTableHasLanded() {
        Optional<Landed> landed = SrsWriterFrontier.landed(run(Map.of(
                "serve.a#0", new WriterProgress(at(100), tokened(100)),
                "serve.b#0", new WriterProgress(at(50), tokened(50)),
                "view.v#0", new WriterProgress(at(90), tokened(90)))), "orders");

        assertThat(landed).hasValueSatisfying(value -> {
            assertThat(value.durableThrough()).isEqualTo(at(50));
            assertThat(value.resumableAt()).isEqualTo(tokened(50));
        });
    }

    @Test
    void aWriterThatHasSaidNothingHoldsTheTableBackEntirely() {
        assertThat(SrsWriterFrontier.landed(run(Map.of(
                "serve.a#0", new WriterProgress(at(100), tokened(100)),
                "serve.b#0", new WriterProgress(at(50), tokened(50)))), "orders")).isEmpty();
    }

    @Test
    void aWriterToldNothingMoreIsComingNeitherHoldsTheTableBackNorLendsItAPlaceToResume() {
        // view.v#0 was given none of the rows: a bound moved it to 120 and it has settled nothing tokened.
        // The lowest progress is serve.b#0's 50, and the place to resume from is the highest tokened position
        // at or below it - serve.a#0's 100 lies above it and would skip what serve.b#0 has not written.
        Optional<Landed> landed = SrsWriterFrontier.landed(run(Map.of(
                "serve.a#0", new WriterProgress(at(100), tokened(100)),
                "serve.b#0", new WriterProgress(at(60), tokened(50)),
                "view.v#0", new WriterProgress(at(120), null))), "orders");

        assertThat(landed).hasValueSatisfying(value -> {
            assertThat(value.durableThrough()).isEqualTo(at(60));
            assertThat(value.resumableAt()).isEqualTo(tokened(50));
        });
    }

    @Test
    void theLoadIsPassedOnlyOnceEveryWriterHasPassedItsReservedPosition() {
        Landed atTheLoad = new Landed(SourceOrder.snapshotRow(3), null);
        Landed beforeTheLoad = new Landed(new SourceOrder(2, 900), null);

        assertThat(SrsWriterFrontier.passedLoad(atTheLoad, 3L)).isTrue();
        assertThat(SrsWriterFrontier.passedLoad(new Landed(at(0), null), 3L)).isTrue();
        assertThat(SrsWriterFrontier.passedLoad(beforeTheLoad, 3L)).isFalse();
        assertThat(SrsWriterFrontier.passedLoad(atTheLoad, null))
                .as("a pipeline that recorded no load has none to pass").isFalse();
    }
}
