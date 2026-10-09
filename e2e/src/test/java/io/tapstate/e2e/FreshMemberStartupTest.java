package io.tapstate.e2e;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FreshMemberStartupTest {
    private static final FreshMemberStartup.Freshness EMPTY = new FreshMemberStartup.Freshness(true, true, true, true);
    private static String refused(int port) {
        return "APPLICATION FAILED TO START\nDescription:\nWeb server failed to start. Port " + port + " was already in use.\n";
    }

    @Test void onlyTheExactEndedHttpBindRefusalCanSelectAgain() {
        assertThat(new FreshMemberStartup.Failure(60436, true, 1, refused(60436)).httpBindInUse()).isTrue();
        assertThat(new FreshMemberStartup.Failure(60436, true, 1, refused(60435)).httpBindInUse()).isFalse();
        assertThat(new FreshMemberStartup.Failure(60436, false, 1, refused(60436)).httpBindInUse()).isFalse();
        assertThat(new FreshMemberStartup.Failure(60436, true, 0, refused(60436)).httpBindInUse()).isFalse();
        assertThat(new FreshMemberStartup.Failure(60436, true, 1, "boot.node-id-in-use").httpBindInUse()).isFalse();
    }

    @Test void twoAttemptsShareOneBudgetAndKeepBothReceipts() {
        AtomicLong clock = new AtomicLong();
        AtomicInteger launches = new AtomicInteger();
        List<Duration> budgets = new ArrayList<>();
        List<Integer> retained = new ArrayList<>();
        String result = FreshMemberStartup.start(() -> EMPTY, () -> attempt(
                launches.incrementAndGet() == 1, clock, budgets, retained), clock::get);
        assertThat(result).isEqualTo("started");
        assertThat(launches).hasValue(2);
        assertThat(budgets).containsExactly(Duration.ofSeconds(120), Duration.ofSeconds(90));
        assertThat(retained).containsExactly(1, 2);
    }

    @Test void anExpiredBudgetCannotBeResetForAnotherAttempt() {
        AtomicLong clock = new AtomicLong(); AtomicInteger launches = new AtomicInteger();
        assertThatThrownBy(() -> FreshMemberStartup.start(() -> EMPTY, () -> {
            launches.incrementAndGet();
            return attempt(true, clock, new ArrayList<>(), new ArrayList<>(), 121);
        }, clock::get)).isInstanceOf(AssertionError.class).hasMessage("controlled HTTP bind refusal");
        assertThat(launches).hasValue(1);
    }

    @Test void aWorkloadAppearingAfterTheFirstRefusalPreventsAnotherLaunch() {
        AtomicInteger reads = new AtomicInteger(), launches = new AtomicInteger(); AtomicLong clock = new AtomicLong();
        assertThatThrownBy(() -> FreshMemberStartup.start(() -> reads.incrementAndGet() == 1 ? EMPTY
                : new FreshMemberStartup.Freshness(true, true, true, false), () -> {
            launches.incrementAndGet(); return attempt(true, clock, new ArrayList<>(), new ArrayList<>());
        }, clock::get)).isInstanceOf(AssertionError.class).hasMessageContaining("empty pipeline workload")
                .hasRootCauseMessage("controlled HTTP bind refusal");
        assertThat(launches).hasValue(1);
    }

    @Test void aSecondBindRefusalNeverSelectsAThirdPort() {
        AtomicInteger launches = new AtomicInteger(); AtomicLong clock = new AtomicLong();
        assertThatThrownBy(() -> FreshMemberStartup.start(() -> EMPTY, () -> {
            launches.incrementAndGet(); return attempt(true, clock, new ArrayList<>(), new ArrayList<>());
        }, clock::get)).isInstanceOf(AssertionError.class).hasMessage("controlled HTTP bind refusal");
        assertThat(launches).hasValue(2);
    }

    @Test void incorrectEvidencePreservesTheOriginalFailureWithoutAnotherLaunch() {
        for (var evidence : List.of(
                new FreshMemberStartup.Failure(60436, true, 1, refused(60435)),
                new FreshMemberStartup.Failure(60436, false, 1, refused(60436)),
                new FreshMemberStartup.Failure(60436, true, 0, refused(60436)),
                new FreshMemberStartup.Failure(60436, true, -1, refused(60436)),
                new FreshMemberStartup.Failure(60436, true, 1, "boot.node-id-in-use"))) {
            AtomicInteger launches = new AtomicInteger();
            AssertionError original = new AssertionError("controlled non-reselectable refusal");
            assertThatThrownBy(() -> FreshMemberStartup.start(() -> EMPTY, () -> {
                launches.incrementAndGet();
                return new FreshMemberStartup.Attempt<String>() {
                    @Override public String await(Duration remaining) { throw original; }
                    @Override public FreshMemberStartup.Failure failure() { return evidence; }
                    @Override public void retain(int ordinal, FreshMemberStartup.Freshness fresh, Throwable cause) { }
                    @Override public void close() { }
                };
            }, () -> 0L)).isSameAs(original);
            assertThat(launches).hasValue(1);
        }
    }

    @Test void everyNonfreshKindPreventsEvenTheFirstLaunch() {
        for (var state : List.of(
                new FreshMemberStartup.Freshness(false, true, true, true),
                new FreshMemberStartup.Freshness(true, false, true, true),
                new FreshMemberStartup.Freshness(true, true, false, true),
                new FreshMemberStartup.Freshness(true, true, true, false))) {
            AtomicInteger launches = new AtomicInteger();
            assertThatThrownBy(() -> FreshMemberStartup.start(() -> state, () -> {
                launches.incrementAndGet();
                return attempt(false, new AtomicLong(), new ArrayList<>(), new ArrayList<>());
            }, () -> 0L)).isInstanceOf(AssertionError.class).hasMessageContaining("empty pipeline workload");
            assertThat(launches).hasValue(0);
        }
    }

    @Test void timeSpentLaunchingIsTakenFromTheSameBudget() {
        AtomicLong clock = new AtomicLong(); List<Duration> budgets = new ArrayList<>();
        String result = FreshMemberStartup.start(() -> EMPTY, () -> {
            clock.addAndGet(Duration.ofSeconds(5).toNanos());
            return attempt(false, clock, budgets, new ArrayList<>());
        }, clock::get);
        assertThat(result).isEqualTo("started");
        assertThat(budgets).containsExactly(Duration.ofSeconds(115));
    }

    @Test void aSecondFreshnessProbeFailureKeepsTheFirstBindFailure() {
        AtomicInteger reads = new AtomicInteger();
        AtomicLong clock = new AtomicLong();
        IllegalStateException unavailable = new IllegalStateException("controlled freshness probe failure");
        assertThatThrownBy(() -> FreshMemberStartup.start(() -> {
            if (reads.incrementAndGet() == 2) { throw unavailable; }
            return EMPTY;
        }, () -> attempt(true, clock, new ArrayList<>(), new ArrayList<>()), clock::get))
                .isSameAs(unavailable);
        assertThat(unavailable.getSuppressed()).anyMatch(failure ->
                "controlled HTTP bind refusal".equals(failure.getMessage()));
    }

    @Test void aSecondLaunchFailureKeepsTheFirstBindFailure() {
        AtomicInteger launches = new AtomicInteger();
        AtomicLong clock = new AtomicLong();
        IllegalStateException unavailable = new IllegalStateException("controlled launch failure");
        assertThatThrownBy(() -> FreshMemberStartup.start(() -> EMPTY, () -> {
            if (launches.incrementAndGet() == 2) { throw unavailable; }
            return attempt(true, clock, new ArrayList<>(), new ArrayList<>());
        }, clock::get)).isSameAs(unavailable);
        assertThat(unavailable.getSuppressed()).anyMatch(failure ->
                "controlled HTTP bind refusal".equals(failure.getMessage()));
    }

    @Test void aSecondLaunchExhaustingTheBudgetKeepsTheFirstBindFailure() {
        AtomicInteger launches = new AtomicInteger();
        AtomicLong clock = new AtomicLong();
        assertThatThrownBy(() -> FreshMemberStartup.start(() -> EMPTY, () -> {
            if (launches.incrementAndGet() == 2) { clock.addAndGet(Duration.ofSeconds(91).toNanos()); }
            return attempt(launches.get() == 1, clock, new ArrayList<>(), new ArrayList<>());
        }, clock::get)).isInstanceOf(AssertionError.class).hasMessageContaining("shared budget")
                .hasRootCauseMessage("controlled HTTP bind refusal");
        assertThat(launches).hasValue(2);
    }

    @Test void unconfirmedCleanupPreventsASecondLaunch() {
        AtomicInteger launches = new AtomicInteger();
        AssertionError original = new AssertionError("controlled HTTP bind refusal");
        assertThatThrownBy(() -> FreshMemberStartup.start(() -> EMPTY, () -> {
            launches.incrementAndGet();
            return new FreshMemberStartup.Attempt<String>() {
                @Override public String await(Duration remaining) { throw original; }
                @Override public FreshMemberStartup.Failure failure() {
                    return new FreshMemberStartup.Failure(60436, true, 1, refused(60436));
                }
                @Override public void retain(int ordinal, FreshMemberStartup.Freshness state, Throwable cause) { }
                @Override public void close() { throw new AssertionError("controlled unconfirmed termination"); }
            };
        }, () -> 0L)).isSameAs(original);
        assertThat(launches).hasValue(1);
        assertThat(original.getSuppressed()).anyMatch(failure ->
                "controlled unconfirmed termination".equals(failure.getMessage()));
    }

    private static FreshMemberStartup.Attempt<String> attempt(boolean failed, AtomicLong clock,
            List<Duration> budgets, List<Integer> retained) {
        return attempt(failed, clock, budgets, retained, 30);
    }
    private static FreshMemberStartup.Attempt<String> attempt(boolean failed, AtomicLong clock,
            List<Duration> budgets, List<Integer> retained, long seconds) {
        return new FreshMemberStartup.Attempt<>() {
            @Override public String await(Duration remaining) {
                budgets.add(remaining); clock.addAndGet(Duration.ofSeconds(seconds).toNanos());
                if (failed) { throw new AssertionError("controlled HTTP bind refusal"); }
                return "started";
            }
            @Override public FreshMemberStartup.Failure failure() {
                return new FreshMemberStartup.Failure(60436, true, 1, refused(60436));
            }
            @Override public void retain(int ordinal, FreshMemberStartup.Freshness fresh, Throwable cause) { retained.add(ordinal); }
            @Override public void close() { }
        };
    }
}
