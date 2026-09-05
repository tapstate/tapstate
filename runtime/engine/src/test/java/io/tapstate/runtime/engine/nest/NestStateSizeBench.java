package io.tapstate.runtime.engine.nest;

import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;

/**
 * Measures what one unit of nest state costs in memory. Four limits are counts - elements in a document,
 * changes held for something that has not arrived, records of deletion, entries kept in memory per
 * namespace - and every one of them is really about bytes. This is the multiplication between the two,
 * and until it exists none of the four can be set to anything but a round number.
 *
 * <p><b>The cost measured is marginal, not average.</b> A limit governs what the next unit adds, so what
 * is wanted is the difference between a state holding n and one holding 2n, divided by n. An average
 * would fold in the fixed cost of the state itself - the maps, the root row, the object headers - and
 * report it as though every element carried a share of it, which overstates a narrow element several
 * times over and understates nothing.
 *
 * <p><b>Both a measured size and a computed one are reported, because neither alone can be trusted.</b>
 * What is in memory is a live object graph, so the heap is the quantity the budget is really about - but
 * it is read by asking the runtime what it has collected, which is an approximation. Serialized length is
 * exact and reproducible to the byte, but it is not what occupies the heap. Reported together, the ratio
 * between them is the useful part: a deployment can measure the cheap exact one on its own data and
 * multiply.
 *
 * <p><b>The calibration is not decoration.</b> Every number here comes from a heap delta across a
 * garbage collection the runtime was only asked, never made, to perform - so the instrument is capable of
 * returning a plausible wrong answer with nothing to show it. {@link #theInstrumentIsCalibrated()}
 * measures allocations whose size is known by arithmetic and prints the error. Read it first: if it is
 * not within a few percent, nothing below it means anything.
 *
 * <p>This is an instrument, not a regression test. Heap numbers vary with the collector, the heap size
 * and the machine, and none of that belongs in a build gate. Its name keeps it out of the default
 * surefire selection, so it costs a build nothing and is run deliberately:
 *
 * <pre>{@code mvn -o test -pl runtime/engine -am -Dtest=NestStateSizeBench -Dsurefire.failIfNoSpecifiedTests=false \
 *   -Dsurefire.failIfNoSpecifiedTests=false -DargLine="-Xmx2g"}</pre>
 */
class NestStateSizeBench {

    /**
     * How many rows a measured row carries, and how long each value is. A row is the one thing here whose
     * size is a property of the deployment's data rather than of this design, so every per-unit number is
     * reported against all three rather than as a single figure: a narrow row and a wide one differ by
     * more than the structure around them does.
     */
    private static final List<Row> ROWS = List.of(
            new Row("narrow", 4, 12),
            new Row("typical", 12, 24),
            new Row("wide", 40, 32));

    /** The two counts a marginal measurement is taken between. */
    private static final int BASE = 2_000;

    /** How many independent copies are allocated per sample, to put the delta well above the noise. */
    private static final int COPIES = 48;

    /** How many times each sample is taken; the median is reported, so one unlucky collection cannot set it. */
    private static final int SAMPLES = 5;

    @Test
    void theInstrumentIsCalibrated() {
        System.out.println("== calibration: allocations whose size is known by arithmetic ==");
        System.out.println("subject,expected bytes/unit,measured bytes/unit,error %");
        // A byte array's retained size is its payload plus a header the JVM rounds up to 8. Anything else
        // measured here shares whatever error this shows.
        reportCalibration("byte[1024]", 1024 + 16, 4_096, i -> new byte[1024]);
        reportCalibration("byte[8192]", 8192 + 16, 512, i -> new byte[8192]);
        // A long[] of 1024 is 8 KiB of payload: a second shape, so that agreement is not an artefact of
        // measuring the same allocation twice.
        reportCalibration("long[1024]", 8192 + 16, 512, i -> new long[1024]);
    }

    @Test
    void whatOneElementOfADocumentCosts() {
        System.out.println("== one live element of a document ==");
        System.out.println("row,fields,value chars,heap bytes/element,serialized bytes/element,heap/serialized");
        for (Row row : ROWS) {
            report(row,
                    marginalHeapBytes(n -> documentHolding(n, row)),
                    marginalSerializedBytes(n -> documentHolding(n, row)));
        }
    }

    @Test
    void whatOneChangeHeldForAnAbsentParentCosts() {
        System.out.println("== one change held for a parent that has not arrived, in a document ==");
        System.out.println("row,fields,value chars,heap bytes/change,serialized bytes/change,heap/serialized");
        for (Row row : ROWS) {
            report(row,
                    marginalHeapBytes(n -> documentWaitingOn(n, row)),
                    marginalSerializedBytes(n -> documentWaitingOn(n, row)));
        }

        System.out.println("== the same change held at a resolver key instead ==");
        System.out.println("row,fields,value chars,heap bytes/change,serialized bytes/change,heap/serialized");
        for (Row row : ROWS) {
            report(row,
                    marginalHeapBytes(n -> resolverHolding(n, row)),
                    marginalSerializedBytes(n -> resolverHolding(n, row)));
        }
    }

    @Test
    void whatOneRecordOfADeletionCosts() {
        System.out.println("== one record of a deletion, kept until a replay can no longer undo it ==");
        System.out.println("row,fields,value chars,heap bytes/record,serialized bytes/record,heap/serialized");
        // A record of a deletion keeps no row, so what it costs should not follow the row width. Measured
        // against all three anyway: if it does follow, the row is being kept somewhere it should not be.
        for (Row row : ROWS) {
            report(row,
                    marginalHeapBytes(n -> documentOfDeletions(n, row)),
                    marginalSerializedBytes(n -> documentOfDeletions(n, row)));
        }
    }

    @Test
    void whatOneWholeEntryCosts() {
        System.out.println("== one whole entry, which is what a memory budget counts ==");
        System.out.println("entry,elements,row,heap bytes,serialized bytes,heap/serialized");
        // The budget counts entries and cannot see inside them, so what one costs is set by how many
        // elements it has absorbed. A namespace of roots with ten elements each and one of roots with a
        // thousand differ by two orders of magnitude at the same configured budget.
        for (int elements : List.of(0, 10, 100, 1_000)) {
            Row row = ROWS.get(1);
            reportEntry("document", elements, row,
                    heapBytesOf(COPIES, i -> documentHolding(elements, row)) / (double) COPIES,
                    serializedBytesOf(documentHolding(elements, row)));
        }
        for (int held : List.of(0, 10, 100, 1_000)) {
            Row row = ROWS.get(1);
            reportEntry("resolver key", held, row,
                    heapBytesOf(COPIES, i -> resolverHolding(held, row)) / (double) COPIES,
                    serializedBytesOf(resolverHolding(held, row)));
        }
    }

    // ---- the states being measured, built through the paths that really build them ----

    /**
     * A document holding {@code elements} live elements and nothing else. What was absorbed on the way in
     * is released first: it is a real cost but it is the one the next measurement is about, and left in
     * it would be counted twice.
     */
    private static RootAssembly documentHolding(int elements, Row row) {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row.fields(0), order(0), positions(0));
        for (int i = 0; i < elements; i++) {
            assembly.applyElement(elementAt(i), row.fields(i), order(i + 1), positions(i + 1));
        }
        assembly.documentSent();
        return assembly;
    }

    /**
     * A document holding {@code changes} changes for an ancestor that never arrives, and no live element:
     * each names a parent under an embed one level deeper than the root, so nothing can attach them.
     */
    private static RootAssembly documentWaitingOn(int changes, Row row) {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row.fields(0), order(0), positions(0));
        for (int i = 0; i < changes; i++) {
            assembly.applyElement(orphanAt(i), row.fields(i), order(i + 1), positions(i + 1));
        }
        return assembly;
    }

    /** A document holding {@code records} records of deletion and no live element. */
    private static RootAssembly documentOfDeletions(int records, Row row) {
        RootAssembly assembly = new RootAssembly();
        assembly.applyRoot(row.fields(0), order(0), positions(0));
        for (int i = 0; i < records; i++) {
            assembly.applyElement(elementAt(i), row.fields(i), order(2L * i + 1), positions(i));
            assembly.deleteElement(elementAt(i), order(2L * i + 2), positions(i));
        }
        assembly.documentSent();
        return assembly;
    }

    /** A resolver key holding {@code changes} children for a parent row that has not been declared. */
    private static ResolverState resolverHolding(int changes, Row row) {
        ResolverState state = new ResolverState();
        for (int i = 0; i < changes; i++) {
            state.resolve(new NestElement(elementAt(i), row.fields(i), order(i), positions(i)), i);
        }
        return state;
    }

    private static ElementRef elementAt(int i) {
        return new ElementRef(List.of("items"), null, List.of("k-" + i), "id-" + i);
    }

    /** Names a parent under an embed one level down, which no root row can supply, so it is never attached. */
    private static ElementRef orphanAt(int i) {
        return new ElementRef(List.of("items", "lines"), "absent-" + i, List.of("k-" + i), "id-" + i);
    }

    private static SourceOrder order(long seq) {
        return new SourceOrder(1L, seq);
    }

    private static Map<String, ChainPosition> positions(long seq) {
        return Map.of("chain-0", new ChainPosition(order(seq), "token-" + seq));
    }

    /** How wide a measured row is: how many fields it carries, and how long each value is. */
    private record Row(String name, int fields, int valueChars) {

        Map<String, Object> fields(int i) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int f = 0; f < fields; f++) {
                row.put("field_" + f, value(i, f));
            }
            return row;
        }

        private String value(int i, int f) {
            StringBuilder text = new StringBuilder(valueChars);
            text.append(i).append(':').append(f).append(':');
            while (text.length() < valueChars) {
                text.append('x');
            }
            return text.substring(0, valueChars);
        }
    }

    // ---- measurement ----

    /**
     * What one more unit adds to the heap: the difference between a state holding twice the base count
     * and one holding the base, over the base. Taken {@link #SAMPLES} times, reported as the median, so a
     * single collection that ran at the wrong moment cannot set the answer.
     */
    private static double marginalHeapBytes(IntFunction<Object> stateHolding) {
        List<Double> samples = new ArrayList<>();
        for (int s = 0; s < SAMPLES; s++) {
            long large = heapBytesOf(COPIES, i -> stateHolding.apply(2 * BASE));
            long small = heapBytesOf(COPIES, i -> stateHolding.apply(BASE));
            samples.add((large - small) / (double) (COPIES * BASE));
        }
        samples.sort(Double::compare);
        return samples.get(samples.size() / 2);
    }

    /** The same difference in serialized length, which is exact and needs no repetition. */
    private static double marginalSerializedBytes(IntFunction<Object> stateHolding) {
        long large = serializedBytesOf(stateHolding.apply(2 * BASE));
        long small = serializedBytesOf(stateHolding.apply(BASE));
        return (large - small) / (double) BASE;
    }

    /**
     * The heap taken by {@code copies} independently built instances. Independent rather than one built
     * once and referenced repeatedly: what is wanted is what holding another of these costs, and copies
     * that share their strings would report the sharing rather than the cost.
     */
    private static long heapBytesOf(int copies, IntFunction<Object> make) {
        Object[] held = new Object[copies];
        long before = settledHeap();
        for (int i = 0; i < copies; i++) {
            held[i] = make.apply(i);
        }
        long after = settledHeap();
        // Reached after the second reading, so nothing built above may be collected before it is taken.
        if (held[copies - 1] == null) {
            throw new AssertionError("built nothing");
        }
        return after - before;
    }

    /**
     * What the heap holds once the runtime has been given every chance to drop what is unreachable. The
     * collection is requested and not commanded, which is why it is requested repeatedly and why the
     * calibration above exists to say what the residual error is.
     */
    private static long settledHeap() {
        Runtime runtime = Runtime.getRuntime();
        long used = Long.MAX_VALUE;
        for (int i = 0; i < 6; i++) {
            System.gc();
            try {
                Thread.sleep(40);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            // The lowest reading across several attempts: a collection that has not finished can only
            // report more than is really held, never less.
            used = Math.min(used, runtime.totalMemory() - runtime.freeMemory());
        }
        return used;
    }

    /** How long this state is once written out, which is exact and is also what reaches the cold layer. */
    private static long serializedBytesOf(Object value) {
        CountingStream counter = new CountingStream();
        try (ObjectOutputStream out = new ObjectOutputStream(counter)) {
            out.writeObject(value);
        } catch (IOException failed) {
            throw new UncheckedIOException(failed);
        }
        return counter.written;
    }

    private static final class CountingStream extends OutputStream {

        private long written;

        @Override
        public void write(int b) {
            written++;
        }

        @Override
        public void write(byte[] bytes, int off, int len) {
            written += len;
        }
    }

    // ---- reporting ----

    private static void report(Row row, double heap, double serialized) {
        System.out.printf("%s,%d,%d,%.0f,%.0f,%.2f%n",
                row.name(), row.fields(), row.valueChars(), heap, serialized, heap / serialized);
    }

    private static void reportEntry(String entry, int held, Row row, double heap, double serialized) {
        System.out.printf("%s,%d,%s,%.0f,%.0f,%.2f%n",
                entry, held, row.name(), heap, serialized, heap / serialized);
    }

    private static void reportCalibration(String subject, long expected, int copies, IntFunction<Object> make) {
        double measured = heapBytesOf(copies, make) / (double) copies;
        System.out.printf("%s,%d,%.0f,%+.1f%%%n",
                subject, expected, measured, 100.0 * (measured - expected) / expected);
    }
}
