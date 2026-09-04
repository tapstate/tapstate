package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.Test;

/**
 * The one place a nest is told what it may hold. Every knob here shares two properties, and both of them
 * are why this could not stay a static method handing back a map configuration.
 *
 * <p><b>Per namespace, not per job.</b> One tree's documents differ in width by orders of magnitude - a
 * customer document holds as many elements as that customer has policies and claims against them, where a
 * document of a narrow tree beside it holds a handful - so a single number covering both is either too
 * small for the wide one or too large to catch the narrow one growing without bound. A limit that applies
 * to everything is a limit that catches nothing.
 *
 * <p><b>It travels.</b> The limit is decided where the job is assembled and enforced on whichever member
 * runs the vertex, so it has to survive the trip between them. A limit that stayed behind would leave
 * every vertex running unguarded while the configuration that was meant to guard them reads as set.
 */
class ACapacityLimitIsSetPerNamespaceAndTravelsWithTheJobTest {

    private static final String CUSTOMERS = "nest.p.doc.$root";
    private static final String ORDERS = "nest.p.other.$root";

    @Test
    void aNamespaceNobodyConfiguredTakesTheDefault() {
        NestSettings settings = NestSettings.defaults();

        assertThat(settings.elementsAllowedIn(CUSTOMERS)).isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
    }

    @Test
    void aNamespaceGivenALimitOfItsOwnTakesThatOne() {
        NestSettings settings = NestSettings.defaults().withElementLimit(CUSTOMERS, 12L);

        assertThat(settings.elementsAllowedIn(CUSTOMERS)).isEqualTo(12L);
    }

    @Test
    void aLimitSetOnOneNestDoesNotReachAnother() {
        NestSettings settings = NestSettings.defaults().withElementLimit(CUSTOMERS, 12L);

        assertThat(settings.elementsAllowedIn(ORDERS))
                .describedAs("another nest's documents are a different width and keep their own limit")
                .isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
    }

    @Test
    void aNamespaceNobodyConfiguredHoldsTheDefaultPending() {
        NestSettings settings = NestSettings.defaults();

        assertThat(settings.pendingAllowedIn(CUSTOMERS)).isEqualTo(NestSettings.DEFAULT_PENDING_LIMIT);
    }

    @Test
    void aNamespaceGivenAPendingLimitOfItsOwnTakesThatOne() {
        NestSettings settings = NestSettings.defaults().withPendingLimit(CUSTOMERS, 56L);

        assertThat(settings.pendingAllowedIn(CUSTOMERS)).isEqualTo(56L);
    }

    /**
     * The two limits are separate numbers about separate quantities - how wide a document has grown, and
     * how much is stuck waiting for something that has not arrived - so setting either has to leave the
     * other where it was.
     */
    @Test
    void howMuchMayWaitAndHowWideADocumentMayGrowAreSetApart() {
        NestSettings settings = NestSettings.defaults().withElementLimit(CUSTOMERS, 12L);

        assertThat(settings.pendingAllowedIn(CUSTOMERS)).isEqualTo(NestSettings.DEFAULT_PENDING_LIMIT);
        assertThat(NestSettings.defaults().withPendingLimit(CUSTOMERS, 56L).elementsAllowedIn(CUSTOMERS))
                .isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
    }

    @Test
    void aNamespaceNobodyConfiguredTakesTheDefaultReferenceFanout() {
        assertThat(NestSettings.defaults().referrersAllowedIn(CUSTOMERS))
                .isEqualTo(NestSettings.DEFAULT_REFERENCE_FANOUT_LIMIT);
    }

    /**
     * The one number here that is not about what fits. How wide a document may grow and how much may wait
     * are both quantities something has to hold; how many rows point at one row is refused although what
     * records them divides across buckets and stores perfectly well - what it bounds is how many documents
     * one edit rewrites. Two limits about different things have to be settable apart, or a deployment
     * widening a table it knows is wide would be loosening a guard on something else.
     */
    @Test
    void howManyMayPointAtARowIsSetApartFromHowWideADocumentMayGrow() {
        NestSettings settings = NestSettings.defaults().withReferenceFanoutLimit(CUSTOMERS, 7L);

        assertThat(settings.referrersAllowedIn(CUSTOMERS)).isEqualTo(7L);
        assertThat(settings.referrersAllowedIn(ORDERS))
                .describedAs("filed under the namespace that set it, like every other number here")
                .isEqualTo(NestSettings.DEFAULT_REFERENCE_FANOUT_LIMIT);
        assertThat(settings.elementsAllowedIn(CUSTOMERS))
                .isEqualTo(NestSettings.DEFAULT_ELEMENT_LIMIT);
        assertThat(NestSettings.defaults().withElementLimit(CUSTOMERS, 12L).referrersAllowedIn(CUSTOMERS))
                .isEqualTo(NestSettings.DEFAULT_REFERENCE_FANOUT_LIMIT);
    }

    @Test
    void aNamespaceNobodyConfiguredTakesTheDefaultSendWindow() {
        assertThat(NestSettings.defaults().sendWindowIn(CUSTOMERS))
                .isEqualTo(NestSettings.DEFAULT_SEND_WINDOW_MILLIS);
    }

    @Test
    void aNamespaceGivenASendWindowOfItsOwnTakesThatOne() {
        NestSettings settings = NestSettings.defaults().withSendWindow(CUSTOMERS, 200L);

        assertThat(settings.sendWindowIn(CUSTOMERS)).isEqualTo(200L);
        assertThat(settings.sendWindowIn(ORDERS))
                .describedAs("filed under the namespace that set it, like every other number here")
                .isEqualTo(NestSettings.DEFAULT_SEND_WINDOW_MILLIS);
    }

    /**
     * Zero is a real setting - send every version as it is assembled - so it has to survive being written
     * down. A guard that refused it, or a lookup that fell back to the default for it, would leave a
     * deployment reading its own configuration back as 50 while running unthrottled or the other way round.
     */
    @Test
    void aWindowOfZeroIsASettingRatherThanAnAbsentOne() {
        assertThat(NestSettings.defaults().withSendWindow(CUSTOMERS, 0L).sendWindowIn(CUSTOMERS))
                .isZero();
    }

    @Test
    void aWindowShorterThanNoWindowIsRefused() {
        assertThatThrownBy(() -> NestSettings.defaults().withSendWindow(CUSTOMERS, -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CUSTOMERS);
    }

    /**
     * The two ways of asking for something that cannot be built. A negative window is nonsense outright; a
     * window with folding turned off is worse than nonsense, because it reads as a setting - it would delay
     * every change by the window and merge none of them, which is the cost of throttling with none of the
     * point.
     */
    @Test
    void aSendPolicyThatCouldNotDoWhatItSaysIsRefused() {
        assertThatThrownBy(() -> new NestSendPolicy(-1L, true))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NestSendPolicy(50L, false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void everyLimitReachesTheMemberThatWillEnforceIt() throws Exception {
        NestSettings settings = NestSettings.defaults()
                .withElementLimit(CUSTOMERS, 12L).withElementLimit(ORDERS, 34L)
                .withPendingLimit(CUSTOMERS, 56L).withPendingLimit(ORDERS, 78L)
                .withSendWindow(CUSTOMERS, 200L).withSendWindow(ORDERS, 10L)
                .withReferenceFanoutLimit(CUSTOMERS, 90L).withReferenceFanoutLimit(ORDERS, 91L);

        NestSettings arrived = roundTripped(settings);

        assertThat(arrived.elementsAllowedIn(CUSTOMERS)).isEqualTo(12L);
        assertThat(arrived.elementsAllowedIn(ORDERS)).isEqualTo(34L);
        assertThat(arrived.pendingAllowedIn(CUSTOMERS)).isEqualTo(56L);
        assertThat(arrived.pendingAllowedIn(ORDERS)).isEqualTo(78L);
        assertThat(arrived.sendWindowIn(CUSTOMERS)).isEqualTo(200L);
        assertThat(arrived.sendWindowIn(ORDERS)).isEqualTo(10L);
        assertThat(arrived.referrersAllowedIn(CUSTOMERS)).isEqualTo(90L);
        assertThat(arrived.referrersAllowedIn(ORDERS)).isEqualTo(91L);
    }

    /** What the job submission does to anything the vertices are configured with, and nothing more. */
    private static NestSettings roundTripped(NestSettings settings) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(settings);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (NestSettings) in.readObject();
        }
    }
}
