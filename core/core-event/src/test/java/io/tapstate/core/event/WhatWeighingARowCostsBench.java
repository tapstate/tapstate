package io.tapstate.core.event;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * What it costs to weigh a row, per row, against what the same row costs to copy.
 *
 * <p>The figure is defined rather than read off anything, so it is walked: every field's name and every
 * character of every text value, once where a source hands the row over and once where a batch of them
 * settles at a target. Those are two boundaries and two quantities — a transform between them rewrites
 * the row — so neither walk can be computed from the other, and the question is what the walk costs.
 *
 * <p>The answer is proportional to payload and is worth having in numbers rather than in adjectives: a
 * row of twenty ordinary columns is a few hundred nanoseconds, and a row carrying a megabyte of text is
 * a few hundred microseconds. Copying the same row's map, which every stage of a pipeline does, is where
 * a reader can anchor that: it is flat in the payload, because it copies references and touches none of
 * the bytes.
 *
 * <p>Not a test and not run with them — the name keeps it out of the default set, since a measurement
 * that fails a build on a busy machine teaches nobody anything. Run it by name when the walk changes:
 * {@code -Dtest=WhatWeighingARowCostsBench}.
 */
class WhatWeighingARowCostsBench {

    private static Map<String, Object> ordinaryRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 4_711L);
        row.put("customer_id", 90_210L);
        row.put("status", "SHIPPED");
        row.put("total", new BigDecimal("1234.56"));
        row.put("currency", "EUR");
        row.put("created_at", Instant.parse("2026-09-18T08:00:00Z"));
        row.put("updated_at", Instant.parse("2026-09-18T09:00:00Z"));
        row.put("note", "delivered to the front desk");
        row.put("shipped", Boolean.TRUE);
        row.put("weight_kg", 2.4d);
        for (int flag = 0; flag < 10; flag++) {
            row.put("flag_" + flag, flag % 2 == 0);
        }
        return row;
    }

    private static Map<String, Object> withText(int bytes) {
        Map<String, Object> row = ordinaryRow();
        row.put("document", "x".repeat(bytes));
        return row;
    }

    private static long weigh(Map<String, Object> row, int rounds) {
        long sink = 0L;
        for (int round = 0; round < rounds; round++) {
            sink += PayloadBytes.ofRow(row);
        }
        return sink;
    }

    private static long copy(Map<String, Object> row, int rounds) {
        long sink = 0L;
        for (int round = 0; round < rounds; round++) {
            sink += new LinkedHashMap<>(row).size();
        }
        return sink;
    }

    /** Both paths run warm before either is timed, so the first is not charged for compiling the walk. */
    private static void report(String what, Map<String, Object> row, int rounds) {
        long sink = weigh(row, rounds / 10) + copy(row, rounds / 10);
        long began = System.nanoTime();
        sink += weigh(row, rounds);
        double weighNanos = (System.nanoTime() - began) / (double) rounds;
        began = System.nanoTime();
        sink += copy(row, rounds);
        double copyNanos = (System.nanoTime() - began) / (double) rounds;
        long bytes = PayloadBytes.ofRow(row);
        System.out.printf(Locale.ROOT,
                "%-24s payload=%-9d weigh=%9.0f ns/row  copy=%7.0f ns/row  weigh/byte=%.3f ns (sink=%d)%n",
                what, bytes, weighNanos, copyNanos, weighNanos / bytes, sink);
    }

    @Test
    void whatOneRowCostsToWeigh() {
        report("20 ordinary columns", ordinaryRow(), 200_000);
        report("+ 1 KB of text", withText(1_024), 200_000);
        report("+ 64 KB of text", withText(64 * 1_024), 5_000);
        report("+ 1 MB of text", withText(1_024 * 1_024), 500);
    }
}
