package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Rows on their way between two keys are the one thing this vertex holds that is in no document at all, so
 * they are the one thing nothing else already bounds. A document is bounded by its own width and by what
 * stays in memory; a hand-over is bounded by neither, because it belongs to neither side.
 *
 * <p>Two bounds, because there are two ways for one to go wrong and they need different answers. Too much
 * at once is a structural change bigger than this deployment is set up for, and it fails the job while
 * everything is still consistent. Never collected is a half of a move that is not being worked, and time is
 * the only thing that can tell it apart from one that has not been worked <em>yet</em> - so it is given a
 * protection and then given up on.
 *
 * <p><b>Given up on means handed to the dead-letter channel, not dropped.</b> Those rows were read out of a
 * document and nothing will ever send them again, so dropping them loses data that no assertion about a
 * document could see. And the hold on the frontier goes with them: an entry given up on is no longer
 * something a replay would finish, so keeping the frontier beneath it would be waiting for what has already
 * been decided against - which is exactly how a time bound becomes unreachable.
 */
class WhatIsParkedIsBoundedInSizeAndInTimeTest {

    private static final TransformBody.Nest TREE = nest(new NestRoot("customer", List.of("customer_id"),
            null, true,
            List.of(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final String NAMESPACE = TOPOLOGY.assembler().mapName();

    private static final int OWN_ROWS = 0;
    private static final int POLICIES = 1;
    private static final int DEPARTURES = 2;

    private final NestBinding.NestStores stores = HeapNestStores.onHeap();
    private final HeapNestStore<RootAssembly> documents = new HeapNestStore<>();
    private final TestOutbox out = new TestOutbox(512);
    private final Ticking clock = new Ticking();
    private final List<ReleasedChild> givenUp = new ArrayList<>();

    @Test
    void aHandOverPastTheLimitFailsTheJobRatherThanParkingIt() throws Exception {
        AssemblerProcessor losing = assembler(NestSettings.defaults().withParkingLimit(NAMESPACE, 2));
        customer(losing, "C1");
        policies(losing, "C1", 3);

        assertThatThrownBy(() -> feed(losing, DEPARTURES, renamed("C1", "C2")))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("TS0490023");
    }

    /** The positive control: the same move under a limit that allows it goes through untouched. */
    @Test
    void aHandOverWithinTheLimitIsParked() throws Exception {
        AssemblerProcessor losing = assembler(NestSettings.defaults().withParkingLimit(NAMESPACE, 3));
        customer(losing, "C1");
        policies(losing, "C1", 3);

        feed(losing, DEPARTURES, renamed("C1", "C2"));

        assertThat(stores.forParking(TOPOLOGY.assembler()).count()).isPositive();
    }

    @Test
    void aHandOverNobodyCollectsIsGivenUpOnRatherThanHeldForever() throws Exception {
        AssemblerProcessor losing = assembler(
                NestSettings.defaults().withMigrationProtection(NAMESPACE, 1_000L));
        customer(losing, "C1");
        policies(losing, "C1", 2);
        feed(losing, DEPARTURES, renamed("C1", "C2"));

        clock.advance(1_000L);
        losing.tryProcess();

        assertThat(givenUp)
                .describedAs("those rows are in no document and nothing will send them again, so they are "
                        + "handed on rather than dropped")
                .hasSize(2);
        assertThat(stores.forParking(TOPOLOGY.assembler()).count())
                .describedAs("and the parking area does not go on holding what has been given up on")
                .isZero();
    }

    /**
     * The discrimination that stops the bound above from being satisfied by giving up immediately: inside
     * its protection, a hand-over is left exactly where it is. Nothing about a move that has not been
     * collected yet says it is not about to be.
     */
    @Test
    void aHandOverInsideItsProtectionIsLeftAlone() throws Exception {
        AssemblerProcessor losing = assembler(
                NestSettings.defaults().withMigrationProtection(NAMESPACE, 1_000L));
        customer(losing, "C1");
        policies(losing, "C1", 2);
        feed(losing, DEPARTURES, renamed("C1", "C2"));

        clock.advance(999L);
        losing.tryProcess();

        assertThat(givenUp).isEmpty();
        assertThat(stores.forParking(TOPOLOGY.assembler()).count()).isPositive();
    }

    /**
     * The half that costs more than the rows. While a hand-over is outstanding the frontier is held below
     * the change that started it, so a half that never comes pins the chain for the life of the job - and a
     * time bound that let go of the rows but not the hold would not have bounded anything.
     */
    @Test
    void givingUpAlsoLetsGoOfWhatTheMoveWasHoldingBack() throws Exception {
        AssemblerProcessor losing = assembler(
                NestSettings.defaults().withMigrationProtection(NAMESPACE, 1_000L));
        customer(losing, "C1");
        policies(losing, "C1", 2);
        feed(losing, DEPARTURES, renamed("C1", "C2"));

        clock.advance(1_000L);
        losing.tryProcess();
        // A second turn: the first is what gives up, and what it changed is only visible afterwards.
        losing.tryProcess();

        assertThat(stores.forParking(TOPOLOGY.assembler()).count()).isZero();
        assertThat(givenUp).hasSize(2);
    }

    // ---- harness ------------------------------------------------------------------------

    private AssemblerProcessor assembler(NestSettings settings) throws Exception {
        AssemblerProcessor processor = new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(),
                documents, "doc", null, null, ReplayFloor.NONE, settings, clock,
                NestSendPolicy.within(0), stores.forParking(TOPOLOGY.assembler()),
                (from, released) -> givenUp.add(released));
        processor.init(out, new TestProcessorContext());
        return processor;
    }

    private void feed(AssemblerProcessor processor, int ordinal, Envelope event) {
        TestInbox inbox = new TestInbox();
        inbox.queue().add(event);
        processor.process(ordinal, inbox);
        out.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    private void customer(AssemblerProcessor processor, String customerId) {
        feed(processor, OWN_ROWS, Envelope.insert(1, "customer",
                row("customer_id", customerId, "name", "n"), null).withOrder(at(1)));
    }

    private void policies(AssemblerProcessor processor, String customerId, int count) {
        for (int i = 0; i < count; i++) {
            feed(processor, POLICIES, Envelope.insert(2, "policy",
                    row("policy_id", "P" + i, "customer_id", customerId, "policy_no", "PN-" + i), null)
                    .withOrder(at(2 + i)));
        }
    }

    private static Envelope renamed(String was, String is) {
        return Envelope.update(9, "customer",
                row("customer_id", was, "name", "n"),
                row("customer_id", is, "name", "n"), null).withOrder(at(90));
    }

    /** A clock that only moves when the test moves it, so a protection is measured rather than waited for. */
    private static final class Ticking implements NestClock {

        private long now = 1_000_000L;

        void advance(long millis) {
            now += millis;
        }

        @Override
        public long millis() {
            return now;
        }
    }
}
