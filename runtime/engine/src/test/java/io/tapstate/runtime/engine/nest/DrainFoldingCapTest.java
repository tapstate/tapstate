package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Both stateful vertices hold the keys a drain touches locally and store each of them once when the batch
 * is done, so a key hit twenty times in one batch costs one write. That is the whole point of folding,
 * and it is also unbounded: a drain that touches ten thousand distinct keys holds ten thousand documents
 * on the heap before a single one is written back.
 *
 * <p>So there is a ceiling, and reaching it stores what is held and starts again. What these tests pin is
 * that the ceiling is real rather than nominal: a drain wider than it must write partway through, not at
 * the end. The observation is the order of calls the store sees - without a ceiling every load in a batch
 * happens before the first save, and the run of consecutive loads is as wide as the batch.
 *
 * <p>Correctness may not pay for it: every key the drain touched is still stored exactly as if it had all
 * been held to the end, which is asserted alongside.
 */
class DrainFoldingCapTest {

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());

    /** Wide enough that a ceiling has to be hit more than once, so one early store cannot fake it. */
    private static final int KEYS = DrainFolding.MAX_KEYS_HELD * 2 + 3;

    @Test
    void anAssemblerWiderThanTheCeilingStoresPartwayThroughTheDrain() throws Exception {
        RecordingStore<RootAssembly> store = new RecordingStore<>();
        AssemblerProcessor processor =
                new AssemblerProcessor(TOPOLOGY.assembler(), TOPOLOGY.slots(), store, "doc");
        processor.init(new TestOutbox(KEYS * 2), new TestProcessorContext());

        TestInbox inbox = new TestInbox();
        for (int i = 0; i < KEYS; i++) {
            inbox.add(Envelope.insert(i, "customer", row("customer_id", "c" + i, "name", "n" + i), null)
                    .withOrder(new SourceOrder(1L, i)));
        }
        processor.process(0, inbox);

        assertThat(store.longestRunOfLoads())
                .describedAs("a drain may not hold more keys than the ceiling before writing any of them back")
                .isLessThanOrEqualTo(DrainFolding.MAX_KEYS_HELD);
        assertThat(store.saved())
                .describedAs("every key the drain touched is still stored, ceiling or no ceiling")
                .hasSize(KEYS);
    }

    @Test
    void aResolverWiderThanTheCeilingStoresPartwayThroughTheDrain() throws Exception {
        RecordingStore<ResolverState> store = new RecordingStore<>();
        ResolverProcessor processor = new ResolverProcessor(
                TOPOLOGY.vertexAt(List.of("policies")), store, element -> { });
        processor.init(new TestOutbox(KEYS * 2), new TestProcessorContext());

        TestInbox inbox = new TestInbox();
        for (int i = 0; i < KEYS; i++) {
            // policy_id is what this vertex partitions by - it is the column the claims beneath name.
            inbox.add(Envelope.insert(i, "policy",
                            row("policy_id", "pid" + i, "customer_id", "c" + i, "policy_no", "p" + i), null)
                    .withOrder(new SourceOrder(1L, i)));
        }
        processor.process(0, inbox);

        assertThat(store.longestRunOfLoads())
                .describedAs("a drain may not hold more keys than the ceiling before writing any of them back")
                .isLessThanOrEqualTo(DrainFolding.MAX_KEYS_HELD);
        assertThat(store.saved())
                .describedAs("every key the drain touched is still stored, ceiling or no ceiling")
                .hasSize(KEYS);
    }

    /**
     * A store that remembers the order it was called in. The interleaving is the measurement: folding to
     * the end of the batch reads every key before it writes any, and a ceiling breaks that run.
     */
    private static final class RecordingStore<S> implements NestStore<S> {

        private final Map<Object, S> entries = new LinkedHashMap<>();
        private final List<Boolean> calls = new ArrayList<>();

        @Override
        public S load(Object key) {
            calls.add(Boolean.TRUE);
            return entries.get(key);
        }

        @Override
        public void save(Object key, S state) {
            calls.add(Boolean.FALSE);
            entries.put(key, state);
        }

        @Override
        public void remove(Object key) {
            entries.remove(key);
        }

        @Override
        public long count() {
            return entries.size();
        }

        /** The most keys read in a row without one being written back. */
        private int longestRunOfLoads() {
            int longest = 0;
            int run = 0;
            for (Boolean load : calls) {
                run = load ? run + 1 : 0;
                longest = Math.max(longest, run);
            }
            return longest;
        }

        private Map<Object, S> saved() {
            return entries;
        }
    }
}
