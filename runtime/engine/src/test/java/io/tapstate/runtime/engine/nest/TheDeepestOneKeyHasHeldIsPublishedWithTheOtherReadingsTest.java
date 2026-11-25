package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * How deep one key's wait has ever been, published alongside what a namespace holds and what its reading
 * costs.
 *
 * <p>It is the one quantity bounded by a limit that nothing else can see coming. What waits lives inside a
 * single entry, so a namespace of one key holding ten thousand changes is one entry: the count of entries
 * does not move, what the layer behind the memory holds does not move, and the share of reads served from
 * memory does not move either - the key is hot, so it is in memory. Every published number stays flat and
 * then the job stops on the limit. A run that fills a queue over weeks and one that fills it in seconds are
 * indistinguishable until the moment they are not.
 *
 * <p><b>A mark rather than a level, and it does not fall.</b> What it answers is whether any single key has
 * ever been near the wall, which is a question a reading of the moment cannot answer and a reader cannot
 * reconstruct from two of them: a maximum is not a difference. It is reported and nothing is alarmed on it -
 * the number a threshold would be set against is the deployment's, and there is no knob to turn in answer
 * to one today.
 */
class TheDeepestOneKeyHasHeldIsPublishedWithTheOtherReadingsTest {

    @Test
    void keyMetricsKeepTheirOwnUnitsAndRequireTheCompletePair() {
        Map<String, Long> kinds = new java.util.HashMap<>(Map.of(
                NestStateMetricNames.ACCESSES, 100L, NestStateMetricNames.BACKFILLS, 400L,
                NestStateMetricNames.KEY_ACCESSES, 800L, NestStateMetricNames.COLD_KEYS, 400L));
        NestStateReading reading = NestStateMetricNames.readingFrom(kinds, OptionalLong.empty());
        assertThat(reading.accesses()).isEqualTo(100);
        assertThat(reading.backfills()).isEqualTo(400);
        assertThat(reading.keyAccesses()).hasValue(800);
        assertThat(reading.coldKeys()).hasValue(400);
        NestStateReading placed = reading.withOrigin(new io.tapstate.core.lifecycle.NestOrigin("doc", "[]", "state"));
        assertThat(placed.keyAccesses()).hasValue(800);
        assertThat(placed.coldKeys()).hasValue(400);
        kinds.remove(NestStateMetricNames.COLD_KEYS);
        NestStateReading incomplete = NestStateMetricNames.readingFrom(kinds, OptionalLong.empty());
        assertThat(incomplete.keyAccesses()).isEmpty();
        assertThat(incomplete.coldKeys()).isEmpty();
    }

    private static final String NAMESPACE = "nest.p.step.$root";
    private static final String ANOTHER = "nest.p.step.policies";

    private static final TransformBody.Nest TREE = nest("customer", List.of("customer_id"),
            embed("policy", "customer_id", "customer_id", EmbedAs.ARRAY, "policies", List.of("policy_no"),
                    embed("claim", "policy_id", "policy_id", EmbedAs.ARRAY, "claims", List.of("claim_id"))));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestVertex ASSEMBLER = TOPOLOGY.assembler();
    private static final NestVertex POLICIES = TOPOLOGY.vertexAt(List.of("policies"));

    private static final int OWN_ROWS = 0;
    private static final int FROM_BENEATH = 1;

    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aKeyHoldingMoreThanAnyBeforeItRaisesTheMark() {
        NestStateStats stats = statsOfAMember();

        stats.holding(NAMESPACE, 3L);
        stats.holding(NAMESPACE, 7L);

        assertThat(stats.counted(NAMESPACE).pendingHighWater()).isEqualTo(7L);
    }

    /**
     * The case that tells a mark from a reading of the moment. A queue that filled and drained is the whole
     * reason this is kept: an implementation that stores what it was last told reports the drained key and
     * says nothing ever came close.
     */
    @Test
    void aKeyHoldingLessThanTheMarkLeavesItWhereItIs() {
        NestStateStats stats = statsOfAMember();

        stats.holding(NAMESPACE, 7L);
        stats.holding(NAMESPACE, 2L);

        assertThat(stats.counted(NAMESPACE).pendingHighWater())
                .describedAs("a mark is what was reached, not what is held now")
                .isEqualTo(7L);
    }

    @Test
    void eachNamespaceKeepsItsOwnMark() {
        NestStateStats stats = statsOfAMember();

        stats.holding(NAMESPACE, 9L);
        stats.holding(ANOTHER, 4L);

        assertThat(stats.counted(NAMESPACE).pendingHighWater()).isEqualTo(9L);
        assertThat(stats.counted(ANOTHER).pendingHighWater())
                .describedAs("a level nothing is stuck on is not marked by one that is")
                .isEqualTo(4L);
    }

    @Test
    void aNamespaceNothingHasBeenReportedForIsAtZero() {
        NestStateStats stats = statsOfAMember();

        assertThat(stats.counted(NAMESPACE).pendingHighWater()).isZero();
    }

    @Test
    void aNamespaceThatIsForgottenTakesItsMarkWithIt() {
        NestStateStats stats = statsOfAMember();
        stats.holding(NAMESPACE, 9L);

        stats.forget(NAMESPACE);

        assertThat(stats.counted(NAMESPACE).pendingHighWater())
                .describedAs("a pipeline whose state was let go of leaves no mark for the next one to read")
                .isZero();
    }

    /**
     * That the number reaches the run's statistics at all. Without this the mark is kept perfectly and
     * published nowhere, which reads exactly like a namespace where nothing ever waited.
     */
    @Test
    void theMarkGoesOutWithTheOtherReadings() {
        member = memberWith();
        NestStateStats stats = NestStateStats.of(member);
        List<long[]> published = new ArrayList<>();
        NestStateGauge gauge = (namespace, entries, accesses, backfills, backfillMillis, pendingHighWater) ->
                published.add(new long[] {entries, accesses, backfills, backfillMillis, pendingHighWater});
        MapNestStore<String> store = new MapNestStore<>(member.getMap(NAMESPACE), stats, gauge);

        store.holding(6L);
        store.save("k1", "a-state");

        assertThat(published).isNotEmpty();
        assertThat(published.get(published.size() - 1)[4])
                .describedAs("the mark rides out with the readings the store already publishes")
                .isEqualTo(6L);
    }

    /**
     * That the resolver reports the depth it already worked out for the limit. The number is per key: a
     * level holding one change under each of two keys has a deepest of one, and an implementation reporting
     * what the level holds altogether says two.
     */
    @Test
    void aResolverKeyReportsHowDeepItsOwnWaitGot() throws Exception {
        RecordingStore<ResolverState> store = new RecordingStore<>();
        ResolverProcessor processor = new ResolverProcessor(POLICIES, store, UNUSED, NestSettings.defaults());
        processor.init(new TestOutbox(256), new TestProcessorContext());

        feed(processor, FROM_BENEATH, claim(1, "k1", "p1"), claim(2, "k2", "p1"), claim(3, "k3", "p2"));

        assertThat(store.deepest)
                .describedAs("two changes waited under one key and one under another")
                .isEqualTo(2L);
    }

    /**
     * The assembler's own bucket, which the resolver's never sees: rows absorbed under a root that has not
     * arrived are held by the document rather than by the level below it.
     */
    @Test
    void anAssemblerDocumentReportsHowDeepItsOwnWaitGot() throws Exception {
        RecordingStore<RootAssembly> store = new RecordingStore<>();
        AssemblerProcessor processor = new AssemblerProcessor(
                ASSEMBLER, TOPOLOGY.slots(), store, "doc", NestSettings.defaults());
        processor.init(new TestOutbox(256), new TestProcessorContext());

        feed(processor, FROM_BENEATH, policyOf("c1", "p1", 1), policyOf("c1", "p2", 2));

        assertThat(store.deepest)
                .describedAs("two policies waited for a customer row that has not come")
                .isEqualTo(2L);
    }

    /**
     * That the kind is read back out of a run's statistics into a reading. The gauge naming a kind nobody
     * reads publishes a number that reaches no reader, and nothing about that looks like a failure: the
     * metric is present, at zero, for the same reason an untouched namespace would be.
     */
    @Test
    void everyKindThatIsPublishedIsReadBackIntoTheReading() {
        Map<String, Long> kinds = Map.of(
                NestStateMetricNames.ENTRIES, 11L,
                NestStateMetricNames.ACCESSES, 22L,
                NestStateMetricNames.BACKFILLS, 33L,
                NestStateMetricNames.BACKFILL_MILLIS, 44L,
                NestStateMetricNames.PENDING_HIGH_WATER, 55L);

        NestStateReading reading = NestStateMetricNames.readingFrom(kinds, OptionalLong.of(66L));

        assertThat(reading).isEqualTo(new NestStateReading(11L, 22L, 33L, 44L, 55L, OptionalLong.of(66L)));
    }

    @Test
    void aKindMissingFromTheStatisticsReadsAsZeroRatherThanFailing() {
        NestStateReading reading = NestStateMetricNames.readingFrom(
                Map.of(NestStateMetricNames.ENTRIES, 11L), OptionalLong.empty());

        assertThat(reading).isEqualTo(new NestStateReading(11L, 0L, 0L, 0L, 0L, OptionalLong.empty()));
    }

    private NestStateStats statsOfAMember() {
        member = memberWith();
        return NestStateStats.of(member);
    }

    private static HazelcastInstance memberWith() {
        Config config = new Config();
        config.setClusterName("pending-high-water-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.addMapConfig(NestSettings.defaults().stateMaps());
        return Hazelcast.newHazelcastInstance(config);
    }

    /** Nothing is let go of here: what this is about is the count, not the release. */
    private static final NestDeadLetter UNUSED = (from, released) -> {
        throw new AssertionError("counting how deep a wait got does not release what is waiting");
    };

    /** A store that keeps what it was told about depth, over a heap of entries. */
    private static final class RecordingStore<S> implements NestStore<S> {

        private static final long serialVersionUID = 1L;

        private final HeapNestStore<S> entries = new HeapNestStore<>();
        private long deepest;

        @Override
        public S load(Object key) {
            return entries.load(key);
        }

        @Override
        public void save(Object key, S state) {
            entries.save(key, state);
        }

        @Override
        public void remove(Object key) {
            entries.remove(key);
        }

        @Override
        public long count() {
            return entries.count();
        }

        @Override
        public void holding(long pending) {
            deepest = Math.max(deepest, pending);
        }
    }

    private static void feed(ResolverProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static void feed(AssemblerProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static Envelope claim(long seq, String claimId, String policyId) {
        return Envelope.insert(seq, "claim", row("claim_id", claimId, "policy_id", policyId), null)
                .withOrder(at(seq));
    }

    private static KeyedElement policyOf(String customerId, String policyId, long seq) {
        return new KeyedElement(List.of(customerId), new NestElement(
                new ElementRef(List.of("policies"), null, List.of("PN-" + policyId), List.of(policyId)),
                row("policy_id", policyId, "policy_no", "PN-" + policyId), at(seq),
                Map.of("policy", new ChainPosition(at(seq), null))), seq);
    }
}
