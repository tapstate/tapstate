package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tracking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What a drain leaves behind when the durable half of a hand-over fails.
 *
 * <p>A drain persists every entry it touched as it ends, and does so whether it ended by running out of
 * items or by throwing - which is what makes the order inside a hand-over load-bearing. Read the rows out
 * of an entry first and the entry is empty in memory; if publishing them then fails, that empty entry is
 * what gets stored. The rows are in no entry and in no parking area, and a replay cannot bring them back:
 * the order was already applied, so the same change arriving again is rejected as one already seen. Nothing
 * counts it and no document is missing anything a test could assert on - the rows simply never existed.
 *
 * <p>So the publish comes first and the entry is emptied only once it landed. A failure then costs a retry
 * that parks the same rows twice, which the element identities make harmless, rather than the rows.
 */
class AFailedHandoffLeavesWhatItWasCarryingTest {

    private static final TransformBody.Nest TRACKED = nest("customer", List.of("customer_id"),
            tracking(embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies",
                    List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id")))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TRACKED, tables());
    private static final NestVertex POLICIES = TOPOLOGY.vertexAt(List.of("policies"));

    private static final int OWN_ROWS = 0;
    private static final int CLAIMS = 1;
    private static final int TWIN = twinOf(POLICIES.pathId());
    private static final long RENAMED_AT = 200L;

    private final HeapNestStore<ResolverState> mappings = new HeapNestStore<>();
    private final TestOutbox outbox = new TestOutbox(256);

    @Test
    void aFailedParkingWriteLeavesTheChildrenInTheEntryTheyWaitedIn() throws Exception {
        ResolverProcessor vacating = resolver(new RefusesToSave());
        feed(vacating, CLAIMS, claim(1, "K1", "P1"));

        assertThatThrownBy(() -> feed(vacating, TWIN, policyRenamed(RENAMED_AT, "P1", "P2", "C1")))
                .describedAs("the write itself is what fails; the drain's own persistence still runs")
                .isInstanceOf(IllegalStateException.class);

        ResolverState left = mappings.load(List.of("P1"));
        assertThat(left).describedAs("the entry the children waited in was stored by the drain").isNotNull();
        assertThat(left.waiting())
                .describedAs("nothing was published, so this entry is the only copy of those rows there is. "
                        + "Emptied here they are in no parking area either, and the replay that would "
                        + "rebuild them is rejected as a change already seen")
                .hasSize(1);
    }

    /**
     * The discrimination that stops the case above from passing on an implementation that simply never
     * hands anything over: with a parking area that accepts the write, the children do leave the entry.
     */
    @Test
    void aParkingWriteThatLandsDoesEmptyTheEntry() throws Exception {
        NestBinding.NestStores stores = HeapNestStores.onHeap();
        ResolverProcessor vacating = resolver(stores.forParking(POLICIES));
        feed(vacating, CLAIMS, claim(1, "K1", "P1"));

        feed(vacating, TWIN, policyRenamed(RENAMED_AT, "P1", "P2", "C1"));

        assertThat(mappings.load(List.of("P1")).waiting())
                .describedAs("they are in the parking area now, so a second copy here would be a duplicate")
                .isEmpty();
        assertThat(stores.forParking(POLICIES).count()).isPositive();
    }

    @Test
    void aParentDeletionDoesNotConsultAnUnrelatedFailingDeadLetterChannel() throws Exception {
        NestBinding.NestStores stores=HeapNestStores.onHeap();
        ResolverProcessor deleting=resolver(stores.forParking(POLICIES),(from,released) -> {throw new AssertionError("a missing parent does not abandon its children");});
        feed(deleting,CLAIMS,claim(1,"K1","P1"));feed(deleting,OWN_ROWS,policyDeleted(RENAMED_AT,"P1","C1"));
        assertThat(mappings.load(List.of("P1")).waiting()).hasSize(1);assertThat(mappings.load(List.of("P1")).deleted()).isTrue();
    }

    @Test
    void aParentRecreationReleasesTheRetainedWaitingWithoutWritingDeadLetter() throws Exception {
        NestBinding.NestStores stores=HeapNestStores.onHeap();List<ReleasedChild> written=new ArrayList<>();
        ResolverProcessor deleting=resolver(stores.forParking(POLICIES),(from,released) -> written.add(released));
        feed(deleting,CLAIMS,claim(1,"K1","P1"));feed(deleting,OWN_ROWS,policyDeleted(RENAMED_AT,"P1","C1"));
        assertThat(written).isEmpty();assertThat(mappings.load(List.of("P1")).waiting()).hasSize(1);
        feed(deleting,OWN_ROWS,Envelope.insert(RENAMED_AT+1,"policy",row("policy_id","P1","customer_id","C1","policy_no","PN-P1"),null).withOrder(at(RENAMED_AT+1)));
        assertThat(mappings.load(List.of("P1")).waiting()).isEmpty();assertThat(written).isEmpty();
    }

    // ---- harness ------------------------------------------------------------------------

    private ResolverProcessor resolver(NestStore<ParkedSubtree> parking) throws Exception {
        return resolver(parking, (from, released) -> { });
    }

    private ResolverProcessor resolver(NestStore<ParkedSubtree> parking, NestDeadLetter deadLetter)
            throws Exception {
        ResolverProcessor processor = new ResolverProcessor(POLICIES, mappings, deadLetter,
                null, null, ReplayFloor.NONE, NestClock.SYSTEM, NestSettings.defaults(), parking);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    private static int twinOf(List<String> pathId) {
        for (NestInbound edge : POLICIES.inbound()) {
            if (edge.carriesDepartures() && edge.pathId().equals(pathId)) {
                return edge.ordinal();
            }
        }
        throw new AssertionError("no departure edge carries " + pathId);
    }

    private void feed(ResolverProcessor processor, int ordinal, Envelope event) {
        TestInbox inbox = new TestInbox();
        inbox.queue().add(event);
        processor.process(ordinal, inbox);
        outbox.drainQueueAndReset(0, new ArrayList<>(), false);
    }

    private static Envelope claim(long seq, String claimId, String policyId) {
        return Envelope.insert(seq, "claim", row("claim_id", claimId, "policy_id", policyId), null)
                .withOrder(at(seq));
    }

    private static Envelope policyDeleted(long seq, String policyId, String customerId) {
        return Envelope.delete(seq, "policy",
                row("policy_id", policyId, "customer_id", customerId, "policy_no", "PN-1"), null)
                .withOrder(at(seq));
    }

    private static Envelope policyRenamed(long seq, String was, String is, String customerId) {
        return Envelope.update(seq, "policy",
                row("policy_id", was, "customer_id", customerId, "policy_no", "PN-1"),
                row("policy_id", is, "customer_id", customerId, "policy_no", "PN-1"), null)
                .withOrder(at(seq));
    }

    /** A parking area that reads but refuses every write, as one whose member is unreachable would. */
    private static final class RefusesToSave implements NestStore<ParkedSubtree> {

        private final HeapNestStore<ParkedSubtree> entries = new HeapNestStore<>();

        @Override
        public ParkedSubtree load(Object key) {
            return entries.load(key);
        }

        @Override
        public void save(Object key, ParkedSubtree state) {
            throw new IllegalStateException("parking write refused");
        }

        @Override
        public void remove(Object key) {
            entries.remove(key);
        }

        @Override
        public long count() {
            return entries.count();
        }
    }
}
