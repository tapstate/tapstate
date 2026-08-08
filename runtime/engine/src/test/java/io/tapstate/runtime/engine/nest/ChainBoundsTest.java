package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * What one vertex instance is still holding, chain by chain, gathered from the state entries that hold
 * it. Its whole job is to answer "the lowest change on this chain that has not left here", which is what
 * a level's bound is tightened by: promise anything at or above that and a restart replays from above a
 * change that was never delivered, so it is neither sent nor replayable and no count anywhere is wrong.
 *
 * <p>Each key says what it holds in full rather than by increment, so a key that has let go is impossible
 * to forget: forgetting a release pins a chain forever, and forgetting a hold loses data — both come from
 * the same bookkeeping and only one of them is loud.
 *
 * <p>This answer is free to fall. It states what is being held right now, not what has been promised;
 * keeping a promise from being taken back belongs to the level that makes it, not here.
 */
class ChainBoundsTest {

    private static final String CLAIMS = "claim";
    private static final String POLICIES = "policy";

    private static final Object C1 = java.util.List.of("C1");
    private static final Object C2 = java.util.List.of("C2");

    private static Map<String, ChainPosition> on(String chain, long seq) {
        return Map.of(chain, new ChainPosition(at(seq), "t" + seq));
    }

    private static Map<String, ChainPosition> on(String chain, long seq, String other, long otherSeq) {
        Map<String, ChainPosition> positions = new LinkedHashMap<>();
        positions.put(chain, new ChainPosition(at(seq), "t" + seq));
        positions.put(other, new ChainPosition(at(otherSeq), "t" + otherSeq));
        return positions;
    }

    @Test
    void anInstanceHoldingNothingReportsNoBoundOnAnyChain() {
        assertThat(new ChainBounds().lowest(CLAIMS)).isNull();
    }

    @Test
    void theOneKeyHoldingSomethingIsWhatTheChainReports() {
        ChainBounds bounds = new ChainBounds();

        bounds.holding(C1, on(CLAIMS, 100));

        assertThat(bounds.lowest(CLAIMS)).isEqualTo(at(100));
    }

    @Test
    void theLowestOfSeveralKeysHoldingTheSameChainIsWhatIsReported() {
        ChainBounds bounds = new ChainBounds();

        bounds.holding(C1, on(CLAIMS, 400));
        bounds.holding(C2, on(CLAIMS, 100));

        assertThat(bounds.lowest(CLAIMS)).isEqualTo(at(100));
    }

    @Test
    void whenTheLowestKeyLetsGoTheNextOneUpIsReported() {
        ChainBounds bounds = new ChainBounds();
        bounds.holding(C1, on(CLAIMS, 400));
        bounds.holding(C2, on(CLAIMS, 100));

        bounds.holding(C2, Map.of());

        assertThat(bounds.lowest(CLAIMS))
                .describedAs("one key letting go says nothing about what the others still hold")
                .isEqualTo(at(400));
    }

    @Test
    void aChainEveryKeyHasLetGoOfIsNotHeldAtAll() {
        ChainBounds bounds = new ChainBounds();
        bounds.holding(C1, on(CLAIMS, 400));

        bounds.holding(C1, Map.of());

        assertThat(bounds.lowest(CLAIMS)).isNull();
    }

    @Test
    void whatOneChainHoldsSaysNothingAboutAnother() {
        ChainBounds bounds = new ChainBounds();

        bounds.holding(C1, on(CLAIMS, 400, POLICIES, 5));

        assertThat(bounds.lowest(CLAIMS)).isEqualTo(at(400));
        assertThat(bounds.lowest(POLICIES)).isEqualTo(at(5));
    }

    @Test
    void aKeyThatMovesToAnotherChainLetsGoOfTheFirst() {
        ChainBounds bounds = new ChainBounds();
        bounds.holding(C1, on(CLAIMS, 100));

        bounds.holding(C1, on(POLICIES, 5));

        assertThat(bounds.lowest(CLAIMS)).isNull();
        assertThat(bounds.lowest(POLICIES)).isEqualTo(at(5));
    }

    @Test
    void twoKeysHoldingTheSameOrderAreBothAccountedFor() {
        ChainBounds bounds = new ChainBounds();
        bounds.holding(C1, on(CLAIMS, 100));
        bounds.holding(C2, on(CLAIMS, 100));

        bounds.holding(C1, Map.of());

        assertThat(bounds.lowest(CLAIMS))
                .describedAs("every row of one snapshot shares a single order, so two keys can hold the "
                        + "same one and the first to let go must not release the other's")
                .isEqualTo(at(100));
    }

    @Test
    void sayingTheSameThingTwiceChangesNothing() {
        ChainBounds bounds = new ChainBounds();
        bounds.holding(C1, on(CLAIMS, 100));

        // A key is re-read and reported every time its drain settles, whether or not what it holds moved.
        bounds.holding(C1, on(CLAIMS, 100));
        bounds.holding(C1, Map.of());

        assertThat(bounds.lowest(CLAIMS))
                .describedAs("counted twice, one release would leave the chain pinned for good")
                .isNull();
    }

    @Test
    void aKeyRaisingWhatItHoldsRaisesTheChain() {
        ChainBounds bounds = new ChainBounds();
        bounds.holding(C1, on(CLAIMS, 100));

        bounds.holding(C1, on(CLAIMS, 400));

        assertThat(bounds.lowest(CLAIMS)).isEqualTo(at(400));
    }

    @Test
    void whatIsHeldFallsWhenAKeyTakesInSomethingLower() {
        ChainBounds bounds = new ChainBounds();
        bounds.holding(C1, on(CLAIMS, 400));

        bounds.holding(C2, on(CLAIMS, 100));

        assertThat(bounds.lowest(CLAIMS))
                .describedAs("this is what is held right now, not a promise: only the level that promises "
                        + "has to keep its answer from going backwards")
                .isEqualTo(at(100));
    }
}
