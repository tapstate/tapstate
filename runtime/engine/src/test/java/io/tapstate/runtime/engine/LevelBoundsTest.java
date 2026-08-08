package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.Watermark;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * What one level promises downstream, worked out from what each of its edges promised and what it is
 * still holding. Every case here is a way of promising too much: the two that matter most are answering
 * before an edge has spoken at all, and answering with the edge that spoke last rather than the lowest
 * of them. Both look right on a single-edge chain and both lose data the moment a second edge exists.
 */
class LevelBoundsTest {

    private static final String ORDERS = "orders";
    private static final String ITEMS = "order_items";
    private static final ChainAxes AXES = ChainAxes.assign(List.of(ORDERS, ITEMS));

    private final Map<String, SourceOrder> held = new HashMap<>();

    private LevelBounds bounds(Map<Integer, List<String>> chainsByOrdinal) {
        return new LevelBounds(chainsByOrdinal, AXES, held::get);
    }

    private static long packed(long epoch, long seq) {
        return FrontierOrders.pack(ORDERS, new SourceOrder(epoch, seq));
    }

    @Test
    void saysNothingUntilEveryEdgeExpectedToCarryTheChainHasPromised() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS), 1, List.of(ORDERS)));
        byte axis = AXES.axisOf(ORDERS);

        assertThat(bounds.observe(0, axis, packed(1, 100)))
                .describedAs("the edge that has not spoken may still be carrying changes beneath this")
                .isEmpty();
        assertThat(bounds.observe(1, axis, packed(1, 60)))
                .describedAs("both have promised now, so the lower of the two is safe to pass on")
                .hasValue(packed(1, 60));
    }

    @Test
    void promisesTheLowestOfItsEdgesRatherThanTheOneThatSpokeLast() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS), 1, List.of(ORDERS)));
        byte axis = AXES.axisOf(ORDERS);
        bounds.observe(0, axis, packed(1, 100));
        bounds.observe(1, axis, packed(1, 60));

        assertThat(bounds.observe(1, axis, packed(1, 120)))
                .describedAs("edge 0 is the lowest now, and its 100 is all that has really gone")
                .hasValue(packed(1, 100));
    }

    @Test
    void settlesAChainOnTheOneEdgeThatCarriesItRatherThanWaitingForTheOthers() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS), 1, List.of(ITEMS)));

        assertThat(bounds.observe(0, AXES.axisOf(ORDERS), packed(1, 40)))
                .describedAs("orders travels one edge, so that edge alone settles it")
                .hasValue(packed(1, 40));
        assertThat(bounds.observe(1, AXES.axisOf(ITEMS), 70L))
                .describedAs("and the chain on the other edge is settled by that one alone")
                .hasValue(70L);
    }

    @Test
    void keepsItsPromiseJustBeneathTheLowestChangeItStillHolds() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS)));
        SourceOrder pending = new SourceOrder(3, 12);
        held.put(ORDERS, pending);

        assertThat(bounds.observe(0, AXES.axisOf(ORDERS), packed(9, 400)))
                .describedAs("everything under the held change has gone; the held one itself has not")
                .hasValue(FrontierOrders.pack(ORDERS, pending) - 1);
    }

    @Test
    void neverPromisesPastWhatItsUpstreamPromised() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS)));
        held.put(ORDERS, new SourceOrder(9, 400));

        assertThat(bounds.observe(0, AXES.axisOf(ORDERS), packed(1, 30)))
                .describedAs("holding nothing beneath the upstream bound does not license going past it")
                .hasValue(packed(1, 30));
    }

    @Test
    void staysSilentRatherThanRepeatingABoundItHasAlreadySent() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS)));
        byte axis = AXES.axisOf(ORDERS);
        assertThat(bounds.observe(0, axis, packed(1, 50))).hasValue(packed(1, 50));

        assertThat(bounds.observe(0, axis, packed(1, 50)))
                .describedAs("the engine tears the job down on a bound that does not strictly climb")
                .isEmpty();
    }

    @Test
    void crashesRatherThanQuietlySwallowingABoundThatWentBackwards() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS)));
        byte axis = AXES.axisOf(ORDERS);
        bounds.observe(0, axis, packed(1, 100));
        held.put(ORDERS, new SourceOrder(1, 40));

        assertThatThrownBy(() -> bounds.observe(0, axis, packed(1, 120)))
                .describedAs("a bound going backwards means an upstream broke its own promise, and the "
                        + "one thing that must not happen is for it to pass unremarked")
                .isInstanceOf(IllegalStateException.class)
                .isNotInstanceOf(TapstateException.class)
                .hasMessageContaining(ORDERS)
                .hasMessageContaining(String.valueOf(packed(1, 100)))
                .hasMessageContaining(String.valueOf(packed(1, 40) - 1));
    }

    @Test
    void saysNothingWhenTheChangeItHoldsIsTheLowestTheDomainHas() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS)));
        held.put(ORDERS, SourceOrder.snapshotRow(0));

        assertThat(bounds.observe(0, AXES.axisOf(ORDERS), packed(4, 9)))
                .describedAs("there is no value beneath the bottom of the domain to promise")
                .isEmpty();
    }

    @Test
    void keepsTheBoundItWorkedOutWhenTheOutboxWouldNotTakeIt() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS)));
        List<Watermark> taken = new ArrayList<>();
        Predicate<Watermark> outboxFull = onward -> false;
        Watermark arrived = new Watermark(packed(1, 50), AXES.axisOf(ORDERS));

        assertThat(bounds.advance(0, arrived, outboxFull))
                .describedAs("answering false is what makes the engine bring the same bound back")
                .isFalse();
        assertThat(taken).isEmpty();

        assertThat(bounds.advance(0, arrived, taken::add)).isTrue();
        assertThat(taken)
                .describedAs("working the same bound out a second time finds it no advance on what was "
                        + "already recorded as sent, so holding it is the only way it goes anywhere")
                .extracting(Watermark::timestamp)
                .containsExactly(packed(1, 50));
    }

    @Test
    void crashesWhenAChainArrivesOnAnEdgeTheCompiledTopologyDoesNotRouteItOver() {
        LevelBounds bounds = bounds(Map.of(0, List.of(ORDERS), 1, List.of(ITEMS)));

        assertThatThrownBy(() -> bounds.observe(1, AXES.axisOf(ORDERS), packed(1, 10)))
                .describedAs("the expected set is what makes waiting safe; a chain outside it means the "
                        + "set is too small, which is the direction that loses data")
                .isInstanceOf(IllegalStateException.class)
                .isNotInstanceOf(TapstateException.class)
                .hasMessageContaining(ORDERS);
    }
}
