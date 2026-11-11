package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.noPositions;

import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.RollupField;
import io.tapstate.core.model.RollupFunction;
import io.tapstate.core.model.RollupPick;
import io.tapstate.core.model.TransformBody;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * What one rollup level costs per document as it widens: the time a send spends rendering a root whose
 * level holds N live elements, with one and with five rollup fields beside the same level rendered as a
 * plain array; the time one element change costs end to end in the assembly (apply plus the send it causes);
 * and the bytes the root's state takes, written the way the state layer writes it.
 *
 * <p>An instrument, not a gate: its name keeps it out of the default selection. Run it on the bench box,
 * pinned like the bench engine:
 *
 * <pre>{@code taskset -c 1,5 mvn -o test -pl runtime/engine -Dtest=NestRollupFanOutBench
 *     -Dsurefire.failIfNoSpecifiedTests=false -DargLine=-Xmx5g}</pre>
 *
 * The table it prints is the evidence the rollup sizing guidance is taken from; the end-to-end change
 * throughput is measured separately against a running engine.
 */
class NestRollupFanOutBench {

    private static final int[] SIZES = {10, 100, 1_000, 10_000, 100_000};
    private static final long BUDGET_NANOS = 3_000_000_000L;

    private static final Map<String, NestTable> TABLES = Map.of(
            "t", new NestTable("roots", List.of("id"), List.of(), List.of("id"), Map.of("id", ValueType.INT64)),
            "i", new NestTable("items", List.of("id"), List.of(), List.of("id", "root_id", "amount", "kind", "ts"),
                    Map.of("id", ValueType.INT64, "root_id", ValueType.INT64, "amount", ValueType.INT64,
                            "kind", ValueType.of(TapstateType.STRING), "ts", ValueType.INT64)));

    @Test
    void renderCostStateSizeAndChangeCostByElementsPerRoot() throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add(String.format("%-8s %-14s %14s %14s %14s %16s", "elements", "level", "render p50 ms",
                "render p90 ms", "change p50 ms", "state bytes"));
        for (int n : SIZES) {
            for (String shape : List.of("array-0", "rollup-1", "rollup-5")) {
                NestTopology topology = topology(shape);
                List<String> path = List.of(topology.slots().getFirst().stateField());
                RootAssembly root = new RootAssembly();
                root.applyRoot(Map.of("id", 1L), at(1));
                for (long i = 0; i < n; i++) {
                    root.applyElement(new ElementRef(path, null, List.of(i), i), item(i, i), at(2 + i), noPositions());
                }
                long stateBytes = bytes(root);
                double[] render = time(() -> root.render(topology.slots(), Map.of(), "1"));
                long[] seq = {10L * n + 10};
                long[] value = {0};
                double[] change = time(() -> {
                    long id = value[0]++ % n;
                    root.applyElement(new ElementRef(path, null, List.of(id), id), item(id, value[0]), at(seq[0]++),
                            noPositions());
                    root.render(topology.slots(), Map.of(), "1");
                });
                lines.add(String.format("%-8d %-14s %14.3f %14.3f %14.3f %16d", n, shape, render[0], render[1],
                        change[0], stateBytes));
                System.out.println(lines.getLast());
            }
        }
        System.out.println(String.join(System.lineSeparator(), lines));
    }

    private static Map<String, Object> item(long id, long amount) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("root_id", 1L);
        row.put("amount", amount);
        row.put("kind", id % 3 == 0 ? "reward" : "cash");
        row.put("ts", 1_700_000_000L + id);
        return row;
    }

    private static NestTopology topology(String shape) {
        Map<String, RollupField> fields = new LinkedHashMap<>();
        if (!shape.equals("array-0")) {
            fields.put("total", RollupField.of(RollupFunction.SUM, "$amount", null, null, 0));
        }
        if (shape.equals("rollup-5")) {
            fields.put("n", RollupField.of(RollupFunction.COUNT, true, null, null, null));
            fields.put("mean", RollupField.of(RollupFunction.AVG, "$amount", null, null, null));
            fields.put("top", RollupField.of(RollupFunction.MAX, "$amount", "=after.kind == 'reward'", null, null));
            fields.put("latest", RollupField.of(RollupFunction.LAST, new RollupPick("$amount", List.of("$ts")),
                    null, null, null));
        }
        Embed items = new Embed("i", Map.of("root_id", "id"), shape.equals("array-0") ? EmbedAs.ARRAY : EmbedAs.ROLLUP,
                shape.equals("array-0") ? "items" : null, null, List.of("id"), null, null, null,
                fields.isEmpty() ? null : fields);
        return NestTopology.compile("bench", "doc",
                new TransformBody.Nest(null, null, new NestRoot("t", List.of("id"), null, null, List.of(items))),
                TABLES::get);
    }

    /** Median and 90th percentile in milliseconds, warmed, within a fixed time budget per point. */
    private static double[] time(Runnable action) {
        for (int i = 0; i < 5; i++) {
            action.run();
        }
        List<Long> samples = new ArrayList<>();
        long deadline = System.nanoTime() + BUDGET_NANOS;
        while (samples.size() < 2000 && (samples.size() < 5 || System.nanoTime() < deadline)) {
            long start = System.nanoTime();
            action.run();
            samples.add(System.nanoTime() - start);
        }
        long[] sorted = samples.stream().mapToLong(Long::longValue).sorted().toArray();
        return new double[] {sorted[sorted.length / 2] / 1e6, sorted[(int) (sorted.length * 0.9)] / 1e6};
    }

    private static long bytes(Object state) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ObjectOutputStream stream = new ObjectOutputStream(out)) {
            stream.writeObject(state);
        }
        return out.size();
    }
}
