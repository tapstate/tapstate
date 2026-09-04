package io.tapstate.runtime.engine.join;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.fail;

/**
 * The join operator's regression gate: it still reads the way it is recorded to, and still costs what
 * it is recorded to cost.
 *
 * <p><b>This one runs in the ordinary build</b>, unlike {@link JoinBenchRun} beside it, whose name
 * matches no surefire pattern. That is the point of it: this repository has shipped cases that existed,
 * were documented, and were executed by nothing. A gate nobody runs and a gate that finds nothing
 * produce the same green.
 *
 * <p><b>Two kinds of assertion, and they are not interchangeable.</b>
 *
 * <ul>
 *   <li><b>How the operator reaches its state</b> - how many times it asked the mirror for a page of
 *       keys, how many times it asked for one key on its own, how many keys those asks carried, and how
 *       many calls changed something. These are exact integers and they are a property of the operator
 *       alone: the same numbers come back from either arm, on any tier, on any machine. The failure they
 *       exist for is the one that leaves everything else green - a store grows a batch method, the
 *       operator keeps asking one key at a time, every correctness case still passes and the round trips
 *       are still there.
 *   <li><b>What the full phase costs</b>, as a ratio against the same executor over plain heap state,
 *       measured in the same JVM in the same run. Never as a wall clock: this repository has measured
 *       the same tree at 6.9 seconds in CI and over 60 seconds on a developer machine, so an absolute
 *       threshold is either slack enough to catch nothing or tight enough to redden on a busy runner.
 *       The ratio divides the machine out.
 * </ul>
 *
 * <p><b>The counts are the sharp half and the ratio is the blunt one, and it is worth knowing which is
 * doing the work.</b> Two changes were made to the operator to find out. Going back to one key at a
 * time, and reading one index page per trip instead of gathering several, both moved a count and were
 * caught exactly; neither moved the ratio outside its margin. Writing the fact mirror twice per row made
 * the full phase 59 percent slower and the ratio let it through - a ratio wide enough not to redden on a
 * busy runner cannot be narrower than that. That third one is why writes are counted here at all: as a
 * count it is caught the moment it lands. What is left to the ratio is the shape of regression that
 * changes no call count, and there the margin means it has to be close to a doubling.
 *
 * <p><b>Why the cold layer's own trip count is not asserted here, though it is the number the operator
 * was optimised against.</b> It cannot be made to mean one thing on any tier:
 *
 * <ul>
 *   <li>on the resident tier nothing is evicted, so a rebuild reads its rows out of memory and reaches
 *       the layer <em>zero</em> times - measured, for a rebuild of ten thousand rows. An assertion there
 *       is satisfied by an operator that reads every row one at a time just as happily;
 *   <li>on the mixed tier it is non-zero but not reproducible, because what is resident is decided by
 *       an eviction that samples rather than orders. Three identical runs of that same rebuild reached
 *       the layer 344, 272 and 348 times.
 * </ul>
 *
 * <p>So each tier is blind in one of the two ways an assertion can be worthless, and neither blindness
 * announces itself. The counts above have neither problem, and they hold the same claim one level
 * higher up: the layer is reached in pages because the operator asks in pages. The layer's own numbers
 * are still measured and still printed, as the trend the nightly lane keeps.
 *
 * <p><b>Which rows run is a property, so one class serves every tier of the gate.</b> The default is
 * the three the ordinary build can afford; the nightly lane passes the whole matrix. Both hold their
 * rows against the same recorded numbers, so the two cannot disagree about what the operator does.
 *
 * <pre>
 *   mvn -pl runtime/engine -am test -Dtest=JoinPerformanceGateTest \
 *       -Djoinperf.scenarios=F1,F2,I1,I2,I3,I4,R1,R2,R3,R4,R5 -Djoinperf.tiers=hot,mixed,cold
 * </pre>
 *
 * <p>Adding {@code -Djoinperf.record=true} rewrites the rows it ran into the golden and asserts
 * nothing. Re-recording is how a deliberate change is accepted; it is not how a red is cleared.
 */
class JoinPerformanceGateTest {

    private static final Path GOLDEN = Path.of("src", "test", "resources", "join-performance.golden");

    /**
     * The corpus every row of this gate runs, so one recorded number serves the ordinary build and the
     * nightly lane alike. A rebuild at one key in ten thousand is the frailest shape this operator has
     * and still finishes in well under a second, which is what lets the ordinary build carry it.
     */
    private static final JoinBenchRun.Sizes SIZES = new JoinBenchRun.Sizes(2_000, 10, 10_000, 5, 500);

    /**
     * The scenarios whose ratio is held to a threshold: the full phase, which is what the ratio is a
     * statement about. The incremental and rebuild rows are held by their read counts, which are exact
     * and need no repetition to be trusted - so those rows take one sample and cost one run each.
     */
    private static final Set<String> TIMED = Set.of("F1", "F2");

    /**
     * How much above the recorded ratio still passes.
     *
     * <p><b>It is bounded from both sides by measurement, and the window between them is narrow enough
     * to be worth writing down.</b> Five runs of this gate on an idle machine produced ratios of 100,
     * 102, 115, 121 and 123. The spread is the carrier arm's - its smallest of three samples moved
     * between 434 and 557 milliseconds, while the heap arm's stayed within a tenth of a millisecond of
     * itself - and the recorded number is one of those five, because recording is one run.
     *
     * <ul>
     *   <li><b>Above 1.42, or it reddens on nothing.</b> The worst case is a low recording against a
     *       high reading: 142 measured against 100 recorded, both of which have been seen.
     *   <li><b>Below 1.82, or it stops catching a doubling.</b> The worst case is the mirror: a full
     *       phase that got twice as slow reads about 224 against a recorded 123.
     * </ul>
     *
     * <p>1.6 sits between them with room on each side. Anything outside that window is not a judgement
     * call - it is a gate that either cries wolf or sees nothing, and which one depends on the run it
     * was recorded from.
     *
     * <p><b>What it is not asked to catch.</b> A regression that costs less than half again is left to
     * the counts above, which are exact - and to the recorded number moving the next time someone
     * re-records, in a reviewed file rather than in a threshold nobody looks at.
     */
    private static final double MARGIN = 1.6;

    /**
     * How many samples each arm takes, and why the two numbers differ by so much.
     *
     * <p><b>They are set from the spread each arm was measured to have, not from a guess.</b> Over eight
     * samples in each of three separate JVMs, the carrier's smallest was 413, 391 and 391 milliseconds -
     * within three percent of each other by the third sample, because the work is large enough to
     * dominate anything else on the machine. The heap arm's smallest over the first three was 5.5, 3.5
     * and 2.8, and only settled to 2.1, 1.8 and 1.9 once it had eight: it does the same work in about
     * two milliseconds, so what it mostly measures early on is the compiler catching up.
     *
     * <p><b>The asymmetry costs nothing, which is why it is worth having.</b> A carrier sample builds a
     * member and takes it down again - measured at 6.3 seconds each, against 0.4 seconds of actual
     * work - and a heap sample builds nothing at all. So samples are spent where they are free and
     * saved where they are not.
     *
     * <p>Both are constants rather than properties. The recorded ratio is only a number about this
     * pair, and a flag that changed either would leave the golden describing a measurement nobody is
     * making any more, while still comparing against it.
     */
    private static final int CARRIER_SAMPLES = 3;

    private static final int HEAP_SAMPLES = 8;

    @Test
    void theOperatorStillReadsAndCostsWhatItIsRecordedTo() throws IOException {
        List<String> scenarios = List.of(System.getProperty("joinperf.scenarios", "F1,I1,R2").split(","));
        List<String> tiers = List.of(System.getProperty("joinperf.tiers", "mixed").split(","));
        boolean record = Boolean.getBoolean("joinperf.record");

        Map<String, String> golden = readGolden();
        List<String> complaints = new ArrayList<>();

        System.out.println("# joinperf carrierSamples=" + CARRIER_SAMPLES + " heapSamples="
                + HEAP_SAMPLES + " " + SIZES);
        System.out.println(String.join("\t", "joinperf", "scenario", "tier", "batchReads",
                "singleReads", "keysRead", "writes", "ratio", "carrierMs", "heapMs", "coldTrips",
                "coldKeys"));

        for (String tier : tiers) {
            for (String scenario : scenarios) {
                Measured measured = measure(scenario, tier, complaints);
                System.out.println("joinperf\t" + measured.row(scenario, tier));
                System.out.flush();
                if (record) {
                    golden.put(key(scenario, tier), measured.goldenRow(scenario, tier));
                    continue;
                }
                check(scenario, tier, measured, golden.get(key(scenario, tier)), complaints);
            }
        }

        if (record) {
            writeGolden(golden);
            System.out.println("recorded " + golden.size() + " row(s) into " + GOLDEN);
            return;
        }
        if (!complaints.isEmpty()) {
            fail(String.join("\n", complaints));
        }
    }

    // ---------------------------------------------------------------- measuring

    /**
     * Runs one row.
     *
     * <p>The smallest of the samples is what a ratio is taken from rather than the mean. A sample can
     * only be made slower by what else the machine was doing, never faster, so the least disturbed of
     * them is the one closest to what the operator costs; averaging mixes in whatever else the runner
     * had queued. The first sample is the interpreter's and is discarded by the same rule without
     * having to be named.
     */
    private static Measured measure(String scenario, String tier, List<String> complaints) {
        boolean timed = TIMED.contains(scenario);
        long heapNanos = Long.MAX_VALUE;
        long carrierNanos = Long.MAX_VALUE;
        JoinBenchRun.Result carrier = null;
        JoinBenchRun.Result heap = null;

        for (int i = 0; i < (timed ? HEAP_SAMPLES : 1); i++) {
            heap = JoinBenchRun.run(scenario, tier, SIZES, JoinBenchRun.Arm.HEAP);
            heapNanos = Math.min(heapNanos, heap.nanos());
        }
        for (int i = 0; i < (timed ? CARRIER_SAMPLES : 1); i++) {
            JoinBenchRun.Result each = JoinBenchRun.run(scenario, tier, SIZES, JoinBenchRun.Arm.CARRIER);
            carrierNanos = Math.min(carrierNanos, each.nanos());
            if (carrier != null && !shape(carrier).equals(shape(each))) {
                complaints.add(scenario + "/" + tier + ": two runs of the same scenario reached the "
                        + "state differently - " + shape(carrier) + " then " + shape(each) + ". A "
                        + "number that moves between runs cannot be recorded, and this gate records it");
            }
            carrier = each;
        }

        // The counts are meant to be the operator's own, so the two arms must agree on them. If they
        // ever do not, the number is a property of the store and nothing recorded about it means what
        // this gate says it means - which is worth a red of its own rather than a quietly weaker
        // assertion. It is also the only thing here that would notice the heap arm having stopped
        // running the same scenario as the carrier one.
        if (!shape(heap).equals(shape(carrier))) {
            complaints.add(scenario + "/" + tier + ": the two arms reach the state differently - heap "
                    + shape(heap) + ", carrier " + shape(carrier) + ". The counts this gate holds are "
                    + "supposed to be the operator's, and a store cannot be allowed to move them");
        }
        return new Measured(carrier, carrierNanos, heapNanos);
    }

    /** batchReads/singleReads/keysRead/writes - the whole of how one run reached its state. */
    private static String shape(JoinBenchRun.Result result) {
        return result.batchReads() + "/" + result.singleReads() + "/" + result.keysRead() + "/"
                + result.writes();
    }

    // ---------------------------------------------------------------- judging

    private static void check(String scenario, String tier, Measured measured, String recorded,
            List<String> complaints) {
        if (recorded == null) {
            complaints.add(scenario + "/" + tier + ": no recorded row. Either the row is new - record "
                    + "it with -Djoinperf.record=true in the same change set - or the golden lost it");
            return;
        }
        String[] want = recorded.split("\t");
        String wantShape = want[2] + "/" + want[3] + "/" + want[4] + "/" + want[5];
        if (!wantShape.equals(shape(measured.carrier))) {
            complaints.add(scenario + "/" + tier + ": the operator reaches its state differently now - "
                    + "recorded " + wantShape + " (batchReads/singleReads/keysRead/writes), measured "
                    + shape(measured.carrier) + ". A rise in singleReads is the operator having gone "
                    + "back to asking one key at a time; a rise in batchReads with the same keysRead is "
                    + "it asking in smaller pages; a rise in writes is it doing more work per row");
        }
        if ("-".equals(want[6])) {
            return;
        }
        double allowed = Double.parseDouble(want[6]) * MARGIN;
        if (measured.ratio() > allowed) {
            complaints.add(String.format("%s/%s: the full phase costs %.1f times plain heap where %s "
                    + "was recorded, over the %.1f this allows (carrier %.1f ms, heap %.1f ms)",
                    scenario, tier, measured.ratio(), want[6], allowed, measured.carrierMs(),
                    measured.heapMs()));
        }
    }

    // ---------------------------------------------------------------- the golden

    private static String key(String scenario, String tier) {
        return scenario + "\t" + tier;
    }

    private static Map<String, String> readGolden() throws IOException {
        Map<String, String> rows = new LinkedHashMap<>();
        for (String line : Files.readAllLines(GOLDEN, StandardCharsets.UTF_8)) {
            String stripped = line.strip();
            if (stripped.isEmpty() || stripped.startsWith("#")) {
                continue;
            }
            String[] fields = stripped.split("\t");
            rows.put(key(fields[0], fields[1]), stripped);
        }
        return rows;
    }

    /**
     * Writes back every row, the ones this run did not measure included.
     *
     * <p>Recording a subset is the ordinary case - the default three, while the whole matrix takes far
     * longer than a person waits - and a write that kept only what it measured would silently drop the
     * other thirty rows. Nothing downstream would notice: the gate would simply stop holding them, and
     * report that as a pass.
     */
    private static void writeGolden(Map<String, String> rows) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("# How the join operator reaches its state, and what its full phase costs.");
        lines.add("# Regenerate with -Djoinperf.record=true; see JoinPerformanceGateTest.");
        lines.add("# ratio '-' means this row's cost is not held to a threshold, only its counts.");
        lines.add(String.join("\t", "# scenario", "tier", "batchReads", "singleReads", "keysRead",
                "writes", "ratio"));
        // Sorted rather than left in the order they were measured. Recording is usually a subset -
        // one tier, or the three the ordinary build runs - and a file ordered by whatever ran last
        // would show every later re-record as a reshuffle, which is a diff nobody can read past.
        lines.addAll(rows.values().stream().sorted().toList());
        Files.write(GOLDEN, lines, StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------- what one row measured

    private record Measured(JoinBenchRun.Result carrier, long carrierNanos, long heapNanos) {

        private double ratio() {
            return carrierNanos / (double) heapNanos;
        }

        private double carrierMs() {
            return carrierNanos / 1e6;
        }

        private double heapMs() {
            return heapNanos / 1e6;
        }

        private String row(String scenario, String tier) {
            return String.join("\t", scenario, tier, Integer.toString(carrier.batchReads()),
                    Integer.toString(carrier.singleReads()), Long.toString(carrier.keysRead()),
                    Integer.toString(carrier.writes()), String.format("%.1f", ratio()),
                    String.format("%.1f", carrierMs()), String.format("%.1f", heapMs()),
                    Long.toString(carrier.trips()), Long.toString(carrier.coldKeys()));
        }

        private String goldenRow(String scenario, String tier) {
            return String.join("\t", scenario, tier, Integer.toString(carrier.batchReads()),
                    Integer.toString(carrier.singleReads()), Long.toString(carrier.keysRead()),
                    Integer.toString(carrier.writes()),
                    TIMED.contains(scenario) ? String.format("%.1f", ratio()) : "-");
        }
    }
}
