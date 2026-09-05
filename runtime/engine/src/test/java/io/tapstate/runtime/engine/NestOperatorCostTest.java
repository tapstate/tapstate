package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.nest.NestBinding;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestStateStats;
import io.tapstate.runtime.engine.nest.NestTable;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.WriteResult;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.transform.TransformPort;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Measures what one nest event costs the operator, in the two directions a tree can point: the rows
 * gathered <em>into</em> a document, and the row a document <em>points at</em> and fetches. Everything
 * here is a count of what the operator did per event, never a rate: how many events a second this machine
 * pushes is a property of the machine, where how many times a state is reached for and how many trips
 * behind it that costs are properties of the design and travel to any machine.
 *
 * <p><b>Both directions are measured, not just the new one.</b> The carrier under the direction that
 * already existed was changed to make room for the other, so it is not old code that nobody touched - a
 * number for it that predates the change would be measuring something that is no longer running.
 *
 * <p>The one measurement that needs saying out loud is the last pair: how many batch reads a document's
 * references cost, and how many trips behind the map those batches turned into. They are counted apart
 * because a batch that has quietly become a trip per key looks identical from every other angle - the
 * documents still render, the values are still right, and the only trace is that the second number grows
 * with the keys asked for while the first stays at one. To see it at all the entries have to be out of
 * memory when the batch is made, so the reference map is evicted between the two phases; evicting rather
 * than sizing a budget down is what makes the cold path exact instead of approximately reached.
 *
 * <p>Phases, and why the counters are read between them rather than at the end: filing a row reaches for
 * the state exactly as reading one does, so a single total cannot say which half it came from. Each shape
 * therefore lands the side that has to be there first, waits for the operator to go quiet, and only then
 * feeds the side whose per-event cost is the subject.
 *
 * <p>The readings are taken from the member's own counters rather than from the job's published metrics.
 * Those are collected on a cadence, so a difference taken across a phase boundary would be a difference
 * between two collections rather than between two phases - and the error is invisible, because both
 * numbers are plausible.
 *
 * <p><b>This is a gate as well as an instrument, and it runs in the default build.</b> Every table it
 * prints is a trend record and nothing more; what fails the build is the assertions, and every one of
 * them is over a count. No assertion here reads a clock. The same tree and the same code measured 6.9 s
 * on a build machine and over 60 s on a developer's, and loading that developer's machine moved it by a
 * further factor of nearly four - so a wall-clock gate would be flaky, and a flaky gate is switched off.
 * A count is a property of the design and carries to any machine.
 *
 * <p><b>Which numbers are pinned exactly, and which are pinned as a property.</b> The distinction is not
 * a softening; it is the difference between a number the design fixes and a number the run's drain
 * boundary decides, and pinning the second exactly would produce a gate that reddens on a rerun:
 *
 * <ul>
 *   <li><b>Exact.</b> What one event costs where the operator does not fold: the two touches of a state
 *       that is read out and written back, the one write behind it, the one document out. Also the trips
 *       a batch of references turns into, which is fixed by how many distinct rows were asked for and by
 *       nothing else - not by how many documents asked.</li>
 *   <li><b>A ratio, because the absolute number floats.</b> Where rows fold into one drain, how many
 *       drains a phase happens to take moves every absolute count with it (measured across reruns of one
 *       unchanged tree: 404/202/201 one round, 400/200/200 the next). What does not move is the cost of
 *       one drain, so that is what is asserted - two touches per write, whatever the drain count.</li>
 *   <li><b>Bounded, and only where the failure it guards is an order of magnitude.</b> That folding
 *       happened at all is asserted as a bound rather than a value: folding gone means one write per row
 *       instead of one per drain, which is a factor of five here, and no drain-boundary jitter reaches
 *       half that.</li>
 * </ul>
 *
 * <p><b>The rule for changing a number in here, which is the whole value of an exact gate.</b> A gate
 * that says "exactly" will be reddened by legitimate changes, and the reflex is to update the number and
 * move on. Doing that turns this into a threshold gate with extra steps - and a threshold gate at least
 * does not claim to be precise. <b>So: the change that moves a number here says, in the same change set,
 * what the extra touch or trip or write is and why it has to be there.</b> Not "the baseline moved" - the
 * mechanism. A number that nobody can account for is a regression that has been written down.
 *
 * <p>To run this one alone, with its tables:
 *
 * <pre>{@code mvn -o test -pl runtime/engine -am -Dtest=NestOperatorCostTest -Dsurefire.failIfNoSpecifiedTests=false}</pre>
 */
class NestOperatorCostTest {

    private static final String STEP = "order_doc";

    /** How many roots each shape is measured over. Enough that a per-event number is not one sample. */
    private static final int ROOTS = 200;

    /** How many child rows each root gathers, for the shape that gathers an array of them. */
    private static final int CHILDREN_PER_ROOT = 5;

    /** How many distinct rows the documents point at, for the shape that fetches one. */
    private static final int REFERENCED_ROWS = 40;

    /** How many documents point at one row, for the shape measuring what an edit to it costs. */
    private static final int REFERRERS_OF_ONE_ROW = 50;

    /** How long the counters must stand still before a phase counts as over. */
    private static final Duration STILL_FOR = Duration.ofMillis(1_500);

    /** How long a phase may take before the run is abandoned rather than reported on. */
    private static final Duration PHASE_BUDGET = Duration.ofSeconds(60);

    /** The cold layer, and what it was asked to do. Static because the stores are built on the member. */
    private static final Map<String, byte[]> COLD = new ConcurrentHashMap<>();
    private static final Map<String, Trips> TRIPS = new ConcurrentHashMap<>();

    /** What each source has left to emit. Static for the same reason. */
    private static final Map<String, Queue<Map<String, Object>>> FEED = new ConcurrentHashMap<>();

    /** What the sink was handed: how many documents, and how many bytes they came to. */
    private static final AtomicLong EMITTED = new AtomicLong();
    private static final AtomicLong DOCUMENT_BYTES = new AtomicLong();

    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        COLD.clear();
        TRIPS.clear();
        FEED.clear();
        EMITTED.set(0);
        DOCUMENT_BYTES.set(0);
        Config config = new Config();
        config.setClusterName("nest-operator-cost-gate-" + System.nanoTime());
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(2);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        member = Hazelcast.newHazelcastInstance(config);
        NestStateMapStoreFactory.bindTo(member, new CountingColdLayer());
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    /**
     * The direction that already existed, gathering many rows into an array under one root. Two phases
     * rather than one: a root arriving at a level that has never held it costs something different from a
     * child arriving at a root that is already there, and a single total over both would be neither
     * number. The second is the one every row after the first pays.
     */
    @Test
    void whatOneRowGatheredIntoAnArrayCosts() {
        String pipeline = "bench-array";
        String root = namespace(pipeline, "$root");
        Job job = member.getJet().newJob(gatheringDag(pipeline, EmbedAs.ARRAY));

        Snapshot start = snapshot(root);
        feed("orders", roots());
        quiesce();
        Snapshot afterRoots = snapshot(root);

        feed("order_items", children());
        quiesce();
        Report children = report(afterRoots, snapshot(root), ROOTS * CHILDREN_PER_ROOT);

        Report arrivals = report(start, afterRoots, ROOTS);
        job.cancel();
        print("1:N phase 1 - a root row arriving where nothing was held for it", arrivals);
        print("1:N phase 2 - a row gathered into an array under a root already there", children);
        assertMeasured(children);
        assertThat(arrivals.emitted())
                .describedAs("a root arriving is a document; %d arrived and %d came out",
                        ROOTS, arrivals.emitted())
                .isEqualTo(ROOTS);
        assertOneRowPerEvent(arrivals, root, 2);
        assertFoldedIntoDrains(children, root);
    }

    /**
     * The same direction with one row rather than many, which is the other shape the existing carrier
     * serves. Measured apart from the array because they are two different embeds over one carrier, and a
     * threshold taken on the array alone would exempt the one nobody looked at.
     */
    @Test
    void whatOneRowGatheredIntoAnObjectCosts() {
        String pipeline = "bench-object";
        String root = namespace(pipeline, "$root");
        Job job = member.getJet().newJob(gatheringDag(pipeline, EmbedAs.OBJECT));

        Snapshot start = snapshot(root);
        feed("orders", roots());
        quiesce();
        Snapshot afterRoots = snapshot(root);

        feed("order_items", oneChildPerRoot());
        quiesce();
        Report children = report(afterRoots, snapshot(root), ROOTS);

        Report arrivals = report(start, afterRoots, ROOTS);
        job.cancel();
        print("1:1 phase 1 - a root row arriving where nothing was held for it", arrivals);
        print("1:1 phase 2 - a single row gathered into an object under a root already there", children);
        assertMeasured(children);
        assertOneRowPerEvent(arrivals, root, 2);
        // Nothing folds here - one row per root - so this phase is asserted as an unfolded cost, and the
        // root it lands under is resident from the phase before, which is what makes the cold reads zero.
        assertOneRowPerEvent(children, root, 0);
        assertThat(children.emitted())
                .describedAs("one row under one root is one document; %d rows produced %d",
                        ROOTS, children.emitted())
                .isEqualTo(ROOTS);
    }

    /**
     * The new direction: the document fetches the row it points at. The rows are landed first and then
     * evicted, so every one of them is fetched from behind the map - which is the only state in which the
     * batch and the trips it turns into are different numbers.
     */
    @Test
    void whatOneDocumentPointingAtARowCosts() {
        String pipeline = "bench-reference";
        String root = namespace(pipeline, "$root");
        String referenced = namespace(pipeline, "customer");
        String index = referenced + ".refs";
        Job job = member.getJet().newJob(referencingDag(pipeline));

        feed("customers", referencedRows());
        quiesce();
        // Out of memory but not out of the store, so the reads that follow are the cold path exactly.
        member.getMap(referenced).evictAll();
        Snapshot before = snapshot(root, referenced, index);

        feed("orders", rootsPointingAtRows());
        quiesce();
        Report report = report(before, snapshot(root, referenced, index), ROOTS);

        job.cancel();
        print("N:1 - a document fetching the row it points at", report);
        assertMeasured(report);
        Counters batches = report.after().of(referenced).minus(report.before().of(referenced));
        assertThat(batches.accesses())
                .describedAs("the reference namespace was never read, so there is nothing here to report "
                        + "on - a bench that measures the wrong namespace prints zeroes rather than failing")
                .isPositive();
        assertThat(batches.backfills())
                .describedAs("every referenced row was evicted before this phase, so a read of one has to "
                        + "have gone behind the map; zero here means the eviction did not take and the "
                        + "cold numbers below are about a path nothing walked")
                .isPositive();

        assertOneRowPerEvent(report, root, 2);
        // The reading this whole shape exists to pin. A pointed-at row is fetched once because it is one
        // row, not once per document that points at it: REFERENCED_ROWS rows were asked for by ROOTS
        // documents, and it is the first number that decides the trips. Were it the second, every reading
        // above would be unchanged and every document would still render - the count is the only trace.
        assertThat(batches.coldLoads())
                .describedAs("%d documents pointing at %d rows cost %d reads behind the map. It has to be "
                        + "the row count: a fetch driven by the documents asking would grow with them and "
                        + "leave every other number, and every document, exactly as they are",
                        ROOTS, REFERENCED_ROWS, batches.coldLoads())
                .isEqualTo(REFERENCED_ROWS);
        assertThat(batches.coldBatchedKeys())
                .describedAs("the batches asked for %d keys to fetch %d distinct rows, so a key was asked "
                        + "for more than once - the batch is being formed per document rather than per "
                        + "drain", batches.coldBatchedKeys(), REFERENCED_ROWS)
                .isEqualTo(REFERENCED_ROWS);
        assertThat(batches.coldBatches())
                .describedAs("%d keys were fetched in %d calls, which is more calls than keys - a batch "
                        + "that asks for less than one key is not a batch. This is a ceiling and not a "
                        + "value on purpose: the base splits a batch across partitions today, so the two "
                        + "are equal, and the work to stop it splitting can only push the calls down",
                        batches.coldBatchedKeys(), batches.coldBatches())
                .isLessThanOrEqualTo(batches.coldBatchedKeys());
        assertThat(batches.coldSaves())
                .describedAs("fetching a row a document points at wrote %d times. Reading is not writing; "
                        + "a write here is the pointed-at row being copied somewhere per reader, which is "
                        + "the one thing this direction exists not to do", batches.coldSaves())
                .isZero();

        Counters indexEntries = report.namespaces().get(index);
        assertThat(indexEntries.accesses())
                .describedAs("%d rows registered themselves against the row they point at with %d touches "
                        + "of the index. One row, one registration - more means a referring row is being "
                        + "written into the index more than once", ROOTS, indexEntries.accesses())
                .isEqualTo(ROOTS);
        assertThat(indexEntries.coldSaves())
                .describedAs("%d registrations wrote behind the map %d times. One write each; the entry "
                        + "being rewritten twice per row is how this grows with fanout without anything "
                        + "else changing", ROOTS, indexEntries.coldSaves())
                .isEqualTo(ROOTS);
        assertThat(indexEntries.coldLoads())
                .describedAs("the index read behind the map %d times over %d touches, which is more reads "
                        + "than touches. How many of the touches find their entry resident is the run's "
                        + "to decide, so this is a ceiling; reading more often than it is asked is not",
                        indexEntries.coldLoads(), indexEntries.accesses())
                .isLessThanOrEqualTo(indexEntries.accesses());
    }

    /**
     * What one edit to a pointed-at row costs, over the documents it reaches. This is the amplification
     * the fanout limit exists to bound: nothing about those documents changed, and every one of them is
     * assembled and sent again, so the cost of the edit is the cost of a document multiplied by however
     * many point at the row.
     *
     * <p>Reported per document reached rather than per event, because per event it is one number that says
     * nothing - the whole point is that the one event is not one document's worth of work.
     */
    @Test
    void whatAnEditToAPointedAtRowCosts() {
        String pipeline = "bench-edit";
        String root = namespace(pipeline, "$root");
        String referenced = namespace(pipeline, "customer");
        String index = referenced + ".refs";
        Job job = member.getJet().newJob(referencingDag(pipeline));

        feed("customers", List.of(row("customer_id", 0, "name", "before")));
        quiesce();
        feed("orders", ordersAllPointingAtOneRow());
        quiesce();
        Snapshot before = snapshot(root, referenced, index);

        feed("customers", List.of(row("customer_id", 0, "name", "after")));
        quiesce();
        Report report = report(before, snapshot(root, referenced, index), 1);

        job.cancel();
        print("N:1 - one edit to a row " + REFERRERS_OF_ONE_ROW + " documents point at", report);
        assertThat(report.emitted())
                .describedAs("the edit reached no document, so what is reported below is the cost of an "
                        + "edit that did nothing rather than the amplification it is meant to show")
                .isEqualTo(REFERRERS_OF_ONE_ROW);
        printPerDocument(report);

        // What one edit costs is one document's worth of work per document it reaches - no more, because
        // redoing a document is redoing a document, and no less, because a cheaper number here would mean
        // some of the reached documents were not rebuilt. Asserted per document rather than per event:
        // the event is one, and the whole point is that one event is not one document's worth of work.
        Counters roots = report.namespaces().get(root);
        assertThat(roots.accesses())
                .describedAs("%d documents were rebuilt with %d touches of their state, which is %.3f "
                        + "each where rebuilding one is two - read it out, write it back. An edit that "
                        + "reaches a document cannot cost less than rebuilding it, and anything above is "
                        + "a touch this amplification does not account for",
                        report.emitted(), roots.accesses(), (double) roots.accesses() / report.emitted())
                .isEqualTo(2 * REFERRERS_OF_ONE_ROW);
        assertThat(roots.coldSaves())
                .describedAs("%d rebuilt documents wrote behind the map %d times, where it is one each",
                        report.emitted(), roots.coldSaves())
                .isEqualTo(REFERRERS_OF_ONE_ROW);
        assertThat(roots.coldLoads())
                .describedAs("rebuilding the reached documents read %d times behind the map. They were "
                        + "assembled in the phase before and are resident, so a read here means the edit "
                        + "path is fetching what it was already holding", roots.coldLoads())
                .isZero();
    }

    /**
     * What the parking area costs a round that parks nothing. A row of a level's own embed asks, every time
     * it arrives, what may have been left for the identity it now has - and in a round with no structural
     * key change nothing ever was, so every one of those asks is for a key that is not in the map and is
     * not behind it either.
     *
     * <p>Measured as its own namespace rather than folded into the level's, because a total over both
     * would hide it. The level's own reading is mostly served from memory; this one cannot be served from
     * memory at all, and for a reason no budget changes: <b>an absent key never becomes an entry</b>, so
     * nothing the round does makes the next ask a hit. The ratio the other phases report as a hit rate is
     * fixed at one here.
     *
     * <p><b>The cold writes are what say whether anything could have been lost rather than merely
     * re-read.</b> A namespace nothing ever wrote to has nothing an eviction could have dropped, which is
     * the difference between a round-trip that costs time and one that costs data - and they are the same
     * count from every other angle.
     */
    @Test
    void whatTheParkingAreaCostsARoundThatParksNothing() {
        String pipeline = "bench-parking";
        String level = namespace(pipeline, "items");
        String parking = level + ".parking";
        Job job = member.getJet().newJob(parkingDag(pipeline));

        feed("orders", roots());
        quiesce();
        Snapshot before = snapshot(level, parking);

        int rows = ROOTS * CHILDREN_PER_ROOT;
        feed("order_items", children());
        quiesce();
        Report report = report(before, snapshot(level, parking), rows);

        job.cancel();
        print("1:N - what the parking area is asked for in a round that parks nothing", report);
        assertMeasured(report);
        // The level's own cost, held to the same price as any other level holding one row per event. The
        // parking asks below are counted apart from it, and a total over the two would let either drift.
        assertOneRowPerEvent(report, level, 2);

        Counters parked = report.namespaces().get(parking);
        assertThat(parked.accesses())
                .describedAs("one ask per row of the level's own embed, whether or not anything is parked")
                .isEqualTo(rows);
        assertThat(parked.backfills())
                .describedAs("every ask went behind the map: an absent key is never resident, so nothing "
                        + "the round did could make the next one a hit")
                .isEqualTo(parked.accesses());
        assertThat(parked.coldSaves())
                .describedAs("nothing was ever written here, so no eviction could have dropped anything - "
                        + "what these trips cost is time, not data")
                .isZero();
    }

    // ---- what a phase is worth --------------------------------------------------------

    /**
     * Waits until the operator has stopped doing anything. Quiet is read off the counters themselves
     * rather than off a queue depth: what a phase costs is exactly what moved them, so they are also what
     * says the phase is over.
     */
    private void quiesce() {
        long deadline = System.nanoTime() + PHASE_BUDGET.toNanos();
        long still = 0;
        long last = -1;
        while (System.nanoTime() < deadline) {
            long now = totalWork();
            still = now == last ? still + 1 : 0;
            last = now;
            if (still * 100L >= STILL_FOR.toMillis()) {
                return;
            }
            sleep();
        }
        throw new AssertionError("the operator was still working after " + PHASE_BUDGET
                + "; the numbers below would be of half a phase");
    }

    /** Everything that moves while the operator works, in one number, for telling motion from quiet. */
    private long totalWork() {
        long trips = 0;
        for (Trips namespace : TRIPS.values()) {
            trips += namespace.loads.get() + namespace.saves.get() + namespace.deletes.get();
        }
        return trips + EMITTED.get();
    }

    private static void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private Snapshot snapshot(String... namespaces) {
        Map<String, Counters> counters = new LinkedHashMap<>();
        for (String namespace : namespaces) {
            NestStateStats.Counted counted = NestStateStats.of(member).counted(namespace);
            Trips trips = TRIPS.getOrDefault(namespace, new Trips());
            counters.put(namespace, new Counters(counted.accesses(), counted.backfills(),
                    counted.backfillNanos(), trips.loads.get(), trips.saves.get(), trips.deletes.get(),
                    trips.bytes.get(), trips.batches.get(), trips.batchedKeys.get()));
        }
        return new Snapshot(counters, EMITTED.get(), DOCUMENT_BYTES.get());
    }

    private static Report report(Snapshot before, Snapshot after, int events) {
        return new Report(before, after, events);
    }

    /**
     * That the phase measured anything at all. A harness wired to the wrong namespace, or one whose feed
     * never reached a vertex, reports a table of zeroes - which reads as an operator that costs nothing
     * rather than as a bench that measured nothing.
     */
    private static void assertMeasured(Report report) {
        assertThat(report.events()).describedAs("no events were fed, so there is nothing per event")
                .isPositive();
        assertThat(report.emitted())
                .describedAs("nothing came out of the job, so whatever the counters below say, it is not "
                        + "the cost of assembling a document")
                .isPositive();
        assertThat(report.documentBytes())
                .describedAs("documents came out with no content, which no threshold should be read off")
                .isPositive();
    }

    // ---- what the build refuses to let change ------------------------------------------

    /**
     * What one event costs a level that folds nothing into one drain: two touches of the state - read it
     * out, change it, write it back - and one write behind the map carrying the result. Every shape a
     * tree can take pays exactly this at a level holding one row per event, which is why one check stands
     * over all of them: a shape that costs more is a shape whose carrier grew a touch nobody asked for.
     *
     * @param coldReadsPerEvent what the same event costs behind the map, which is a different question
     *     with a different answer per phase. A key reached for the first time is asked for and then
     *     written to, and both go behind a map that has nothing for it; a key already resident costs
     *     none. So this is a parameter rather than a constant, and passing the wrong one is caught by
     *     the run rather than by review.
     */
    private static void assertOneRowPerEvent(Report report, String namespace, long coldReadsPerEvent) {
        long events = report.events();
        Counters delta = report.namespaces().get(namespace);
        assertThat(delta.accesses())
                .describedAs("[%s] one event reached for the state %.3f times. Two is the entire carrier - "
                        + "read it out, write it back - so a third touch is one the design does not "
                        + "account for. If it is deliberate, say in this change what the extra touch is "
                        + "and why it has to be there; updating the number alone turns this gate into a "
                        + "threshold that only claims to be exact", namespace,
                        (double) delta.accesses() / events)
                .isEqualTo(2 * events);
        assertThat(delta.coldSaves())
                .describedAs("[%s] one event wrote behind the map %.3f times. One state, one write out; "
                        + "two means the same state was written twice in one event", namespace,
                        (double) delta.coldSaves() / events)
                .isEqualTo(events);
        assertThat(delta.coldLoads())
                .describedAs("[%s] one event read behind the map %.3f times, where this phase costs %d. "
                        + "Reading more than the phase costs means a key that was resident was fetched "
                        + "anyway; reading less means the phase is not walking the path it names",
                        namespace, (double) delta.coldLoads() / events, coldReadsPerEvent)
                .isEqualTo(coldReadsPerEvent * events);
    }

    /**
     * That the rows of one drain were folded, and that the drain cost the same whatever folded into it.
     * Asserted as a ratio and a bound rather than as values, because the absolute counts move with how
     * many drains the run happened to take - measured over reruns of one unchanged tree, 404/202/201 one
     * round and 400/200/200 the next. The cost of a drain does not move, and neither does the factor
     * folding is worth, so those are the two things worth refusing to let change.
     */
    private static void assertFoldedIntoDrains(Report report, String namespace) {
        Counters delta = report.namespaces().get(namespace);
        assertThat(delta.coldSaves())
                .describedAs("[%s] nothing was written, so there were no drains to reason about and the "
                        + "ratio below would be over zero", namespace)
                .isPositive();
        assertThat(delta.accesses())
                .describedAs("[%s] a drain cost %.3f touches of the state, and a drain is the same read "
                        + "out and write back as any other event - the rows folding into it are not "
                        + "supposed to add touches, which is the whole of what folding is for",
                        namespace, (double) delta.accesses() / delta.coldSaves())
                .isEqualTo(2 * delta.coldSaves());
        assertThat(delta.coldLoads())
                .describedAs("[%s] a fold read behind the map, which it cannot legitimately do: the root "
                        + "these rows fold into was landed and left resident by the phase before",
                        namespace)
                .isZero();
        assertThat(delta.coldSaves() * 2)
                .describedAs("[%s] %d rows drained %d times, so folding bought less than the factor of "
                        + "two this bound allows. Folding gone entirely is one write per row - a factor "
                        + "of %d here - and no drain-boundary jitter reaches half of that, so this is a "
                        + "bound on a collapse, not on a number", namespace, report.events(),
                        delta.coldSaves(), CHILDREN_PER_ROOT)
                .isLessThanOrEqualTo(report.events());
    }

    // ---- printing ---------------------------------------------------------------------

    private static void print(String title, Report report) {
        StringBuilder out = new StringBuilder("\n").append(title).append('\n');
        out.append("  events fed in this phase           ").append(report.events()).append('\n');
        out.append("  documents emitted                  ").append(report.emitted()).append('\n');
        out.append(per("emissions per event", report.emitted(), report.events()));
        out.append(per("document bytes per document", report.documentBytes(), report.emitted()));
        report.namespaces().forEach((namespace, delta) -> {
            out.append("  [").append(namespace).append("]\n");
            out.append(per("state accesses per event", delta.accesses(), report.events()));
            out.append(per("trips behind the map per event", delta.backfills(), report.events()));
            out.append(per("cold reads per event", delta.coldLoads(), report.events()));
            out.append(per("cold writes per event", delta.coldSaves(), report.events()));
            out.append(per("cold deletes per event", delta.coldDeletes(), report.events()));
            out.append(per("bytes written cold per event", delta.coldBytes(), report.events()));
            out.append(per("trips behind the map per read", delta.backfills(), Math.max(delta.accesses(), 1)));
            out.append(per("of those trips, batch calls", delta.coldBatches(), report.events()));
            out.append(per("keys per batch call", delta.coldBatchedKeys(),
                    Math.max(delta.coldBatches(), 1)));
            out.append("    millis behind the map (this machine) "
                    + delta.backfillNanos() / 1_000_000L + '\n');
        });
        System.out.println(out);
    }

    /** The same phase again, divided by the documents it reached rather than by the events fed into it. */
    private static void printPerDocument(Report report) {
        long documents = Math.max(report.emitted(), 1);
        StringBuilder out = new StringBuilder("  the same, per document reached\n");
        out.append(per("document bytes", report.documentBytes(), documents));
        report.namespaces().forEach((namespace, delta) -> {
            out.append("  [").append(namespace).append("]\n");
            out.append(per("state accesses per document", delta.accesses(), documents));
            out.append(per("trips behind the map per document", delta.backfills(), documents));
            out.append(per("cold writes per document", delta.coldSaves(), documents));
            out.append(per("bytes written cold per document", delta.coldBytes(), documents));
        });
        System.out.println(out);
    }

    private static String per(String what, long total, long over) {
        double each = over == 0 ? 0 : (double) total / over;
        return String.format("    %-38s %8d   %.3f each%n", what, total, each);
    }

    // ---- the shapes under measurement -------------------------------------------------

    /** orders as the root with order_items gathered beneath it, as an array or as a single object. */
    private static DAG gatheringDag(String pipeline, EmbedAs as) {
        Embed item = new Embed("item", Map.of("order_id", "order_id"), as,
                as == EmbedAs.ARRAY ? "items" : "item",
                as == EmbedAs.ARRAY ? List.of("item_id") : null, null, null, null);
        return dag(pipeline, item, levels("item", new NestTable("order_items", List.of("item_id"))));
    }

    /**
     * The same gathering with one more level beneath it, which is the shallowest tree that gives a level a
     * vertex of its own. A level is compiled into one only when something hangs below it: a two-level tree
     * is assembled in a single place, so it has no parking area to ask of at all and measuring one there
     * reports zeroes rather than a cost.
     */
    private static DAG parkingDag(String pipeline) {
        Embed tax = new Embed("tax", Map.of("item_id", "item_id"), EmbedAs.ARRAY, "taxes",
                List.of("tax_id"), null, null, null);
        Embed item = new Embed("item", Map.of("order_id", "order_id"), EmbedAs.ARRAY, "items",
                List.of("item_id"), null, null, List.of(tax));
        return dag(pipeline, item, levels("item", new NestTable("order_items", List.of("item_id")),
                "tax", new NestTable("item_taxes", List.of("tax_id"))));
    }

    /** The levels beneath the root, by the alias each is written under. Ordered, because edges are. */
    private static Map<String, NestTable> levels(Object... aliasesAndTables) {
        Map<String, NestTable> levels = new LinkedHashMap<>();
        for (int level = 0; level < aliasesAndTables.length; level += 2) {
            levels.put((String) aliasesAndTables[level], (NestTable) aliasesAndTables[level + 1]);
        }
        return levels;
    }

    /**
     * orders as the root with the customer each one points at placed beneath it. Nothing declares a
     * direction: customers.customer_id is what identifies a customer and orders.cust_ref is not what
     * identifies an order, which is the whole of what says which way this embed points.
     */
    private static DAG referencingDag(String pipeline) {
        Embed customer = new Embed("customer", Map.of("customer_id", "cust_ref"), EmbedAs.OBJECT,
                "customer", null, null, null, null);
        return dag(pipeline, customer, levels("customer", new NestTable("customers",
                List.of("customer_id"))));
    }

    private static DAG dag(String pipeline, Embed embed, Map<String, NestTable> others) {
        TransformBody.Nest body = new TransformBody.Nest(null, null,
                new NestRoot("order", List.of("order_id"), null, null, List.of(embed)));

        Map<String, FromRef> aliases = new LinkedHashMap<>();
        aliases.put("order", FromRef.literal("orders"));
        others.forEach((alias, table) -> aliases.put(alias, FromRef.literal(table.name())));
        Step step = Step.inline(STEP, FromClause.aliases(aliases), body, null, null);

        List<String> sourceNames = new ArrayList<>();
        sourceNames.add("orders");
        others.values().forEach(table -> sourceNames.add(table.name()));
        PipelineResource resource = new PipelineResource(pipeline, null, sourceNames, List.of(step), null,
                new ServeBlock.Inline("serve", FromRef.literal(STEP),
                        List.of(new SyncElement("sync_1", "dest", null, null, null, null)), null, null),
                null, null);

        Map<String, ProcessorMetaSupplier> sources = new LinkedHashMap<>();
        sources.put("orders", fedSource("orders"));
        others.values().forEach(table -> sources.put(table.name(), fedSource(table.name())));

        Map<String, NestTable> tables = new LinkedHashMap<>();
        tables.put("order", new NestTable("orders", List.of("order_id")));
        tables.putAll(others);

        DagBindings bindings = new DagBindings(
                sources::get,
                s -> (SupplierEx<TransformPort>) () -> event -> List.of(event),
                syncElement -> (SupplierEx<SinkWriter>) MeasuringSinkWriter::new,
                ref -> List.of(((FromRef.Literal) ref).ref()),
                new NestBinding(tables::get, NestBinding.onMap(), (from, released) -> { }));

        return PipelineDagBuilder.build(resource, bindings);
    }

    private static String namespace(String pipeline, String level) {
        return "nest." + pipeline + "." + STEP + "." + level;
    }

    // ---- the rows ---------------------------------------------------------------------

    private static List<Map<String, Object>> roots() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int order = 1; order <= ROOTS; order++) {
            rows.add(row("order_id", order, "code", "code-" + order));
        }
        return rows;
    }

    private static List<Map<String, Object>> rootsPointingAtRows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int order = 1; order <= ROOTS; order++) {
            rows.add(row("order_id", order, "cust_ref", order % REFERENCED_ROWS));
        }
        return rows;
    }

    private static List<Map<String, Object>> children() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int order = 1; order <= ROOTS; order++) {
            for (int child = 0; child < CHILDREN_PER_ROOT; child++) {
                rows.add(row("item_id", order * 100 + child, "order_id", order, "sku", "sku-" + child));
            }
        }
        return rows;
    }

    private static List<Map<String, Object>> oneChildPerRoot() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int order = 1; order <= ROOTS; order++) {
            rows.add(row("item_id", order * 100, "order_id", order, "sku", "sku-" + order));
        }
        return rows;
    }

    private static List<Map<String, Object>> ordersAllPointingAtOneRow() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int order = 1; order <= REFERRERS_OF_ONE_ROW; order++) {
            rows.add(row("order_id", order, "cust_ref", 0));
        }
        return rows;
    }

    private static List<Map<String, Object>> referencedRows() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int customer = 0; customer < REFERENCED_ROWS; customer++) {
            rows.add(row("customer_id", customer, "name", "customer-" + customer));
        }
        return rows;
    }

    private static Map<String, Object> row(Object... fields) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int field = 0; field < fields.length; field += 2) {
            row.put((String) fields[field], fields[field + 1]);
        }
        return row;
    }

    private static void feed(String stream, List<Map<String, Object>> rows) {
        FEED.computeIfAbsent(stream, name -> new ConcurrentLinkedQueue<>()).addAll(rows);
    }

    // ---- doubles ----------------------------------------------------------------------

    /** A source that emits whatever it has been handed and then stays alive, so the job keeps running. */
    private static ProcessorMetaSupplier fedSource(String stream) {
        return ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of((SupplierEx<Processor>) () -> new FedSource(stream)));
    }

    private static final class FedSource extends AbstractProcessor {

        private final String stream;
        private long emitted;

        private FedSource(String stream) {
            this.stream = stream;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            Queue<Map<String, Object>> queue = FEED.computeIfAbsent(stream,
                    name -> new ConcurrentLinkedQueue<>());
            Map<String, Object> row = queue.poll();
            if (row == null) {
                sleep();
                return false;
            }
            Envelope event = Envelope.insert(emitted + 1L, stream, row, null)
                    .withOrder(new SourceOrder(1, emitted));
            if (!tryEmit(event)) {
                // Put it back rather than dropping it: a row this source swallowed would show up as an
                // operator that did less work, which is the direction a bench must never be wrong in.
                queue.add(row);
                return false;
            }
            emitted++;
            return false;
        }
    }

    /** A sink that counts what it is handed and how large it was, and keeps none of it. */
    private static final class MeasuringSinkWriter implements SinkWriter {

        @Override
        public CompletionStage<WriteResult> write(List<Envelope> batch) {
            for (Envelope written : batch) {
                Map<String, Object> document = written.after();
                if (document != null) {
                    EMITTED.incrementAndGet();
                    DOCUMENT_BYTES.addAndGet(sizeOf(document));
                }
            }
            return CompletableFuture.completedFuture(new WriteResult(batch.size()));
        }

        @Override
        public void close() {
        }
    }

    /**
     * How large a document is, as this JVM writes one out. It is a stand-in for whatever a sink would
     * encode it as and is only ever compared with itself, which is what makes it a number that travels:
     * the encoder is the same everywhere this runs, so the same document weighs the same on any machine.
     */
    private static long sizeOf(Map<String, Object> document) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(new LinkedHashMap<>(document));
        } catch (IOException cannotHappenInMemory) {
            throw new UncheckedIOException(cannotHappenInMemory);
        }
        return bytes.size();
    }

    /** A cold layer in a map that counts every trip made to it, per namespace. */
    private static final class CountingColdLayer implements KeyedStateStore, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            trips(namespace).loads.incrementAndGet();
            return Optional.ofNullable(COLD.get(namespace + "\0" + key));
        }

        /**
         * One trip for the whole batch, which is what a store with a batch read of its own costs and so
         * what the threshold on this is written against. Counting a key each here would report the number
         * the per-key form costs no matter which form is running, and the two would be indistinguishable -
         * which is the exact confusion the threshold exists to catch.
         */
        @Override
        public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            if (keys.isEmpty()) {
                return Map.of();
            }
            Trips trips = trips(namespace);
            trips.loads.incrementAndGet();
            trips.batches.incrementAndGet();
            trips.batchedKeys.addAndGet(keys.size());
            Map<String, byte[]> found = new LinkedHashMap<>();
            for (String key : keys) {
                byte[] state = COLD.get(namespace + "\0" + key);
                if (state != null) {
                    found.put(key, state);
                }
            }
            return found;
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            Trips trips = trips(namespace);
            trips.saves.incrementAndGet();
            trips.bytes.addAndGet(state.length);
            COLD.put(namespace + "\0" + key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            trips(namespace).deletes.incrementAndGet();
            COLD.remove(namespace + "\0" + key);
        }

        @Override
        public void dropNamespace(String namespace) {
            COLD.keySet().removeIf(entry -> entry.startsWith(namespace + "\0"));
        }

        @Override
        public long count(String namespace) {
            return COLD.keySet().stream().filter(entry -> entry.startsWith(namespace + "\0")).count();
        }

        private static Trips trips(String namespace) {
            return TRIPS.computeIfAbsent(namespace, name -> new Trips());
        }
    }

    /** What one namespace's cold layer was asked to do. */
    private static final class Trips {

        private final AtomicLong loads = new AtomicLong();
        private final AtomicLong saves = new AtomicLong();
        private final AtomicLong deletes = new AtomicLong();
        private final AtomicLong bytes = new AtomicLong();

        /**
         * How many of the loads arrived as a batch, and how many keys those batches carried between them.
         * Counted apart because "the batch verb is never called" and "it is called once per key" produce
         * the same trip count, and only the second is something the store can do anything about.
         */
        private final AtomicLong batches = new AtomicLong();
        private final AtomicLong batchedKeys = new AtomicLong();
    }

    /** What one namespace had cost by some moment. */
    private record Counters(long accesses, long backfills, long backfillNanos, long coldLoads,
            long coldSaves, long coldDeletes, long coldBytes, long coldBatches, long coldBatchedKeys) {

        static final Counters NONE = new Counters(0, 0, 0, 0, 0, 0, 0, 0, 0);

        Counters minus(Counters earlier) {
            return new Counters(accesses - earlier.accesses, backfills - earlier.backfills,
                    backfillNanos - earlier.backfillNanos, coldLoads - earlier.coldLoads,
                    coldSaves - earlier.coldSaves, coldDeletes - earlier.coldDeletes,
                    coldBytes - earlier.coldBytes, coldBatches - earlier.coldBatches,
                    coldBatchedKeys - earlier.coldBatchedKeys);
        }
    }

    /** What every namespace, and the sink, had cost by some moment. */
    private record Snapshot(Map<String, Counters> namespaces, long emitted, long documentBytes) {

        Counters of(String namespace) {
            return namespaces.getOrDefault(namespace, Counters.NONE);
        }
    }

    /** What one phase cost, over the events fed into it. */
    private record Report(Snapshot before, Snapshot after, int events) {

        long emitted() {
            return after.emitted() - before.emitted();
        }

        long documentBytes() {
            return after.documentBytes() - before.documentBytes();
        }

        Map<String, Counters> namespaces() {
            Map<String, Counters> deltas = new LinkedHashMap<>();
            after.namespaces().forEach((namespace, counters) ->
                    deltas.put(namespace, counters.minus(before.of(namespace))));
            return deltas;
        }
    }
}
