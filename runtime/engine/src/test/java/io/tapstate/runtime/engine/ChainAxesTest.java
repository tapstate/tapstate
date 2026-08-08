package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Locks how a pipeline's chains are numbered onto the axes a bound can travel on. Every bound rides an
 * axis identified by one byte, so a chain has to be given a number before the job starts and every
 * instance has to arrive at the same one — two instances numbering the same chain differently would have
 * their bounds combined as though they were about different chains, which is the lowest of two unrelated
 * promises being taken as the promise for both.
 *
 * <p>The numbering is therefore derived from the chain names alone, in a fixed order, rather than from
 * the order an instance happens to observe them in. That also caps how many chains a pipeline can draw
 * from, and the cap is reachable in practice: a tree whose every leaf reads a table of its own spends an
 * axis per leaf while the vertex count that is checked today stays at zero.
 */
class ChainAxesTest {

    @Test
    void numbersChainsFromOneInNameOrderSoTwoInstancesAgreeWithoutComparingNotes() {
        ChainAxes fromOneOrder = ChainAxes.assign(List.of("orders", "customers", "items"));
        ChainAxes fromAnother = ChainAxes.assign(List.of("items", "orders", "customers"));

        assertThat(fromOneOrder.axisOf("customers")).isEqualTo((byte) 1);
        assertThat(fromOneOrder.axisOf("items")).isEqualTo((byte) 2);
        assertThat(fromOneOrder.axisOf("orders")).isEqualTo((byte) 3);
        for (String chain : List.of("orders", "customers", "items")) {
            assertThat(fromAnother.axisOf(chain))
                    .describedAs("chain %s numbered by what was observed first rather than by name", chain)
                    .isEqualTo(fromOneOrder.axisOf(chain));
        }
    }

    @Test
    void keepsTheFirstAxisForTheEnginesOwnMessages() {
        // The engine's own markers, including the one that says a queue has gone idle, always carry axis
        // zero. A real chain sharing that axis would have its bound overwritten by one of them and its
        // queue would stop constraining anything, so the axis is spent rather than shared.
        ChainAxes axes = ChainAxes.assign(namedChains(ChainAxes.MAX_CHAINS));

        for (String chain : namedChains(ChainAxes.MAX_CHAINS)) {
            assertThat(axes.axisOf(chain))
                    .describedAs("chain %s was put on the axis the engine speaks on", chain)
                    .isNotEqualTo((byte) 0);
        }
    }

    @Test
    void holdsAsManyChainsAsThereAreAxesAndRefusesOneMore() {
        ChainAxes axes = ChainAxes.assign(namedChains(ChainAxes.MAX_CHAINS));

        Set<Byte> distinct = new LinkedHashSet<>();
        for (String chain : namedChains(ChainAxes.MAX_CHAINS)) {
            distinct.add(axes.axisOf(chain));
        }
        assertThat(distinct)
                .describedAs("two chains sharing an axis have their bounds mistaken for one another's")
                .hasSize(ChainAxes.MAX_CHAINS);

        assertThatThrownBy(() -> ChainAxes.assign(namedChains(ChainAxes.MAX_CHAINS + 1)))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("nest.chain-axis-limit-exceeded")
                .hasMessageContaining("chains=256")
                .hasMessageContaining("limit=255");
    }

    @Test
    void readsAnAxisBackToTheChainItStandsFor() {
        // A bound arrives as an axis and has to be attributed to a chain again. Past the middle of the
        // range the byte is negative, which is a difference in how it prints and not in what it names.
        ChainAxes axes = ChainAxes.assign(namedChains(ChainAxes.MAX_CHAINS));

        for (String chain : List.of("t000", "t126", "t199", "t254")) {
            assertThat(axes.chainOn(axes.axisOf(chain)))
                    .describedAs("axis %s", axes.axisOf(chain))
                    .isEqualTo(chain);
        }
        assertThat(axes.axisOf("t199"))
                .describedAs("the axes above the middle of the range are the negative bytes")
                .isNegative();
    }

    @Test
    void theSameChainNamedTwiceTakesOneAxis() {
        ChainAxes axes = ChainAxes.assign(List.of("orders", "customers", "orders"));

        assertThat(axes.axisOf("orders")).isEqualTo((byte) 2);
        assertThatThrownBy(() -> axes.chainOn((byte) 3))
                .describedAs("the repeated name was given an axis of its own")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void askingAboutSomethingItWasNotBuiltWithTearsDownRatherThanAnsweringAnAxis() {
        // A chain or axis the job was not built with is a defect in the job's own wiring, not a condition
        // a user can act on: answering with a default would put a bound on the wrong chain silently.
        ChainAxes axes = ChainAxes.assign(List.of("orders"));

        assertThatThrownBy(() -> axes.axisOf("nowhere"))
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> axes.chainOn((byte) 7))
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(TapstateException.class);
    }

    @Test
    void travelsToAMemberWithTheGraphRatherThanBeingWorkedOutAgainThere() throws Exception {
        // Every member has to put a chain on the axis every other member puts it on. Sending the numbering
        // rather than the recipe is what guarantees that: a member deriving it from what it can see would
        // be numbering a different set of names.
        ChainAxes axes = ChainAxes.assign(List.of("orders", "customers", "items"));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(axes);
        }
        ChainAxes arrived;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            arrived = (ChainAxes) in.readObject();
        }

        for (String chain : List.of("orders", "customers", "items")) {
            assertThat(arrived.axisOf(chain)).isEqualTo(axes.axisOf(chain));
            assertThat(arrived.chainOn(arrived.axisOf(chain))).isEqualTo(chain);
        }
    }

    private static List<String> namedChains(int count) {
        List<String> chains = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            chains.add(String.format("t%03d", i));
        }
        return chains;
    }
}
