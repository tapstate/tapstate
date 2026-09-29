package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Performance gates reject incomplete or substituted forks even when their numbers look faster. */
class HistoryQueryBenchmarkComparisonTest {
    private static final String SHA = "a".repeat(64);
    private static final Instant ANCHOR = Instant.parse("2026-09-20T10:03:17Z");

    @Test
    void forkPercentilesAreReducedBeforeComparingAndEveryForkRemainsInTheReport() {
        var pairs = pairs(new long[] {1_000, 1_000, 10_000, 1_000, 1_000},
                new long[] {700, 700, 700, 700, 700}, 1_100);
        var result = HistoryQueryBenchmarkComparison.evaluate(SHA, ANCHOR, pairs);
        assertThat(result.passed()).isTrue();
        var daily = result.windows().get("1d");
        assertThat(daily.raw().p95MedianNanos()).isEqualTo(1_000);
        assertThat(daily.raw().forkP95Nanos()).containsExactly(1_000L, 1_000L, 10_000L, 1_000L, 1_000L);
        assertThat(daily.improvement()).isEqualTo(0.30);
        Object parsed = JsonReader.parse(JsonWriter.write(HistoryQueryBenchmarkComparison.evidence(result)));
        assertThat(((Map<?, ?>) parsed).containsKey("windows")).isTrue();
        assertThat(HistoryQueryBenchmarkComparison.evidence(result).get("comparisonScope"))
                .isEqualTo("ORIGINAL_REQUESTED_WINDOWS");
    }

    @Test
    void twentyPercentMeetsTheBenefitGateButMustExceedTwiceBothArmsNoise() {
        var quiet = pairs(new long[] {950, 980, 1_000, 1_020, 1_050},
                new long[] {760, 784, 800, 816, 840}, 1_000);
        assertThat(HistoryQueryBenchmarkComparison.evaluate(SHA, ANCHOR, quiet).passed()).isTrue();
        var noisy = pairs(new long[] {800, 900, 1_000, 1_100, 1_200},
                new long[] {640, 720, 800, 880, 960}, 1_000);
        var result = HistoryQueryBenchmarkComparison.evaluate(SHA, ANCHOR, noisy);
        assertThat(result.passed()).isFalse();
        assertThat(result.failures()).containsExactly(
                "1d requested p95 improvement did not exceed twice measurement noise",
                "15d requested p95 improvement did not exceed twice measurement noise");
    }

    @Test
    void nearestRankP95DoesNotBecomeTheMaximumOfFortyReadings() {
        var pairs = new ArrayList<>(pairs(fill(1_000), fill(700), 1_000));
        var old = pairs.getFirst();
        var daily = new ArrayList<>(old.raw().get("1d"));
        daily.set(0, 100_000L);
        daily.set(5, 100_000L);
        var raw = new LinkedHashMap<>(old.raw());
        raw.put("1d", daily);
        pairs.set(0, new HistoryQueryBenchmarkComparison.Pair(old.fork(), SHA, ANCHOR, 5, 40,
                old.cachedFirst(), raw, old.cached()));
        var result = HistoryQueryBenchmarkComparison.evaluate(SHA, ANCHOR, pairs);
        assertThat(result.windows().get("1d").raw().forkP95Nanos()).containsOnly(1_000L);
        assertThat(pairs.getFirst().raw().get("1d")).containsExactlyElementsOf(daily);
    }

    @Test
    void originalOneHourRegressionAndBothLargeRequestedWindowBenefitsAreIndependentGates() {
        var pairs = new ArrayList<>(pairs(fill(1_000), fill(801), 1_101));
        var result = HistoryQueryBenchmarkComparison.evaluate(SHA, ANCHOR, pairs);
        assertThat(result.passed()).isFalse();
        assertThat(result.failures()).containsExactly(
                "1h requested raw path regressed by more than ten percent",
                "1d requested p95 improvement is below twenty percent",
                "15d requested p95 improvement is below twenty percent");
    }

    @ParameterizedTest
    @ValueSource(strings = {"sha", "anchor", "schedule", "duplicate", "warmup", "smoke",
            "supplemental", "zero", "negative", "missing"})
    void invalidEvidenceCannotBecomeSuccessfulAcceptance(String mutation) {
        var pairs = new ArrayList<>(pairs(fill(1_000), fill(700), 1_000));
        var old = pairs.get(0);
        Map<String, List<Long>> cached = new LinkedHashMap<>(old.cached());
        if (mutation.equals("supplemental")) {
            cached.put("15d-tier-aligned-14d18h", cached.remove("15d"));
        } else if (mutation.equals("missing")) {
            cached.remove("15d");
        } else if (mutation.equals("zero") || mutation.equals("negative")) {
            var readings = new ArrayList<>(cached.get("15d"));
            readings.set(0, mutation.equals("zero") ? 0L : -1L);
            cached.put("15d", readings);
        } else if (mutation.equals("smoke")) {
            cached.put("15d", cached.get("15d").subList(0, 5));
        }
        pairs.set(0, new HistoryQueryBenchmarkComparison.Pair(
                mutation.equals("duplicate") ? 2 : old.fork(),
                mutation.equals("sha") ? "b".repeat(64) : old.artifactSha256(),
                mutation.equals("anchor") ? ANCHOR.plusSeconds(60) : ANCHOR,
                mutation.equals("warmup") ? 0 : old.warmupReads(), old.hotReads(),
                mutation.equals("schedule") || old.cachedFirst(), old.raw(), cached));
        assertThatThrownBy(() -> HistoryQueryBenchmarkComparison.evaluate(SHA, ANCHOR, pairs))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void fewerThanFivePairsCannotBeEvaluatedAsComplete() {
        var incomplete = pairs(fill(1_000), fill(700), 1_000).subList(0, 4);
        assertThatThrownBy(() -> HistoryQueryBenchmarkComparison.evaluate(SHA, ANCHOR, incomplete))
                .isInstanceOf(AssertionError.class).hasMessageContaining("five complete pairs");
    }

    @Test
    void fixtureExpiryIsRejectedAtTheExactCacheAndRetentionBoundaries() {
        Instant computedAt = ANCHOR.plusSeconds(180);
        var fixture = new HistoryQueryBenchmarkIT.Fixture(ANCHOR,
                ANCHOR.minusSeconds(15 * 86_400).plusSeconds(600), ANCHOR,
                ANCHOR.minusSeconds(1_800), 21_561, computedAt.plusSeconds(300));
        HistoryQueryBenchmarkIT.assertFresh(fixture, computedAt.plusSeconds(299), true);
        assertThatThrownBy(() -> HistoryQueryBenchmarkIT.assertFresh(fixture, computedAt.plusSeconds(300), true))
                .isInstanceOf(AssertionError.class).hasMessageContaining("validity deadline");
        HistoryQueryBenchmarkIT.assertFresh(fixture, ANCHOR.plusSeconds(599), false);
        assertThatThrownBy(() -> HistoryQueryBenchmarkIT.assertFresh(fixture, ANCHOR.plusSeconds(600), false))
                .isInstanceOf(AssertionError.class).hasMessageContaining("moving retention");
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "malformed", "reversed", "unexpected-clipping"})
    void invalidEffectiveBoundsCannotMasqueradeAsTheFrozenWindow(String mutation) {
        Instant from = ANCHOR.minusSeconds(86_400);
        Map<String, Object> bounds = new LinkedHashMap<>(Map.of(
                "effectiveFrom", from.toString(), "effectiveTo", ANCHOR.toString(),
                "retentionCutoff", ANCHOR.minusSeconds(15 * 86_400).toString()));
        switch (mutation) {
            case "missing" -> bounds.remove("effectiveFrom");
            case "malformed" -> bounds.put("effectiveFrom", "not-an-instant");
            case "reversed" -> bounds.put("effectiveFrom", ANCHOR.plusSeconds(1).toString());
            default -> bounds.put("effectiveFrom", from.plusSeconds(1).toString());
        }
        assertThatThrownBy(() -> HistoryQueryBenchmarkIT.assertBounds(bounds, from, ANCHOR, false))
                .isInstanceOf(AssertionError.class);
    }

    private static List<HistoryQueryBenchmarkComparison.Pair> pairs(long[] raw, long[] cached, long hourly) {
        List<HistoryQueryBenchmarkComparison.Pair> result = new ArrayList<>();
        for (int fork = 1; fork <= 5; fork++) {
            result.add(new HistoryQueryBenchmarkComparison.Pair(fork, SHA, ANCHOR, 5, 40,
                    HistoryQueryBenchmarkComparison.cachedFirst(fork),
                    Map.of("1h", readings(1_000), "1d", readings(raw[fork - 1]), "15d", readings(raw[fork - 1])),
                    Map.of("1h", readings(hourly), "1d", readings(cached[fork - 1]), "15d", readings(cached[fork - 1]))));
        }
        return result;
    }

    private static List<Long> readings(long value) {
        return Collections.nCopies(40, value);
    }

    private static long[] fill(long value) {
        return new long[] {value, value, value, value, value};
    }
}
