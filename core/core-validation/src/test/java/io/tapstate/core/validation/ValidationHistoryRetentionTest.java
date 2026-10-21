package io.tapstate.core.validation;

import io.tapstate.core.common.TapstateException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.tapstate.core.validation.ValidationHistoryRetention.*;
import static org.assertj.core.api.Assertions.*;

class ValidationHistoryRetentionTest {
    private static final Scope SCOPE = new Scope("cluster", "pipeline", "task");
    private static final Limits LIMITS = new Limits(1000, 1024 * 1024);

    @Test void pendingMirrorsAndReportsShareOneSlotWhileFullAndIncrementalRemainSeparate() {
        var facts = List.of(fact("full-old", ValidationMode.FULL, 1, Copy.CONFIG_REPORT),
                fact("full-new", ValidationMode.FULL, 2, Copy.CACHE_PENDING), fact("full-new", ValidationMode.FULL, 2, Copy.CONFIG_REPORT),
                fact("inc-old", ValidationMode.INCREMENTAL, 1, Copy.CONFIG_REPORT), fact("inc-new", ValidationMode.INCREMENTAL, 3, Copy.CACHE_PENDING));
        var selected = select(SCOPE, 7, new Policy(1, 2), facts, Set.of("active-full", "terminal-unknown"), LIMITS);
        assertThat(selected.retained()).extracting(entry -> entry.run().runId()).containsExactly("full-new", "inc-new", "inc-old");
        assertThat(selected.retained().getFirst().copies()).containsExactlyInAnyOrder(Copy.CACHE_PENDING, Copy.CONFIG_REPORT);
        assertThat(selected.expirationCandidates()).extracting(entry -> entry.run().runId()).containsExactly("full-old");
        assertThat(selected.protectedRunIds()).containsExactlyInAnyOrder("active-full", "terminal-unknown");
        assertThat(selected.controlRevision()).isEqualTo(7);
    }

    @Test void shrinkingRetentionAlsoSelectsUnarchivedRunsAndUsesOriginalEndTime() {
        var facts = new ArrayList<Fact>();
        for (int i = 1; i <= 12; i++) facts.add(fact("run-" + i, ValidationMode.FULL, i, i % 2 == 0 ? Copy.CACHE_PENDING : Copy.CONFIG_REPORT));
        // An old report arriving last cannot displace a newer pending mirror.
        facts.add(fact("run-1", ValidationMode.FULL, 1, Copy.CONFIG_REPORT));
        var defaults = select(SCOPE, 1, Policy.DEFAULT, facts, Set.of(), LIMITS);
        assertThat(defaults.retained()).hasSize(10);
        assertThat(defaults.expirationCandidates()).extracting(entry -> entry.run().runId()).containsExactly("run-2", "run-1");
        var reduced = select(SCOPE, 2, new Policy(1, 1), facts, Set.of(), LIMITS);
        assertThat(reduced.retained()).singleElement().satisfies(entry -> assertThat(entry.run().runId()).isEqualTo("run-12"));
        assertThat(reduced.expirationCandidates()).hasSize(11).anyMatch(entry -> entry.copies().contains(Copy.CACHE_PENDING));
    }

    @Test void failedAndStoppedRunsCountAndEqualTimesUseStableIdentityRegardlessOfArrivalOrder() {
        var complete = fact("b", ValidationMode.FULL, 10, Copy.CONFIG_REPORT);
        var stopped = new Fact(run("a", ValidationMode.FULL), ValidationRunState.STOPPED, Instant.ofEpochSecond(10), Copy.CACHE_PENDING);
        var failed = new Fact(run("c", ValidationMode.FULL), ValidationRunState.FAILED, Instant.ofEpochSecond(10), Copy.CACHE_PENDING);
        var input = new ArrayList<>(List.of(complete, stopped, failed));
        var first = select(SCOPE, 1, new Policy(2, 1), input, Set.of(), LIMITS);
        Collections.reverse(input);
        var reversed = select(SCOPE, 1, new Policy(2, 1), input, Set.of(), LIMITS);
        assertThat(first).isEqualTo(reversed);
        assertThat(first.retained()).extracting(entry -> entry.run().runId()).containsExactly("a", "b");
        assertThat(first.expirationCandidates()).extracting(entry -> entry.run().runId()).containsExactly("c");
    }

    @Test void conflictingCopiesCannotChooseAWinnerOrProduceExpirationCandidates() {
        var original = fact("run", ValidationMode.FULL, 1, Copy.CACHE_PENDING);
        for (var conflicting : List.of(fact("run", ValidationMode.FULL, 2, Copy.CONFIG_REPORT),
                new Fact(original.run(), ValidationRunState.FAILED, original.endedAt(), Copy.CONFIG_REPORT),
                fact("run", ValidationMode.INCREMENTAL, 1, Copy.CONFIG_REPORT))) {
            assertThatThrownBy(() -> select(SCOPE, 1, Policy.DEFAULT, List.of(original, conflicting), Set.of(), LIMITS))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.args()).containsEntry("field", "history.copyConflict"));
        }
    }

    @Test void unknownAndActiveRecordsCannotBeInventedAsEndedHistory() {
        for (var state : List.of(ValidationRunState.STARTING, ValidationRunState.RUNNING, ValidationRunState.STOPPING))
            assertThatThrownBy(() -> new Fact(run("active", ValidationMode.FULL), state, Instant.EPOCH, Copy.CACHE_PENDING))
                    .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Fact(run("incremental", ValidationMode.INCREMENTAL), ValidationRunState.COMPLETED,
                Instant.EPOCH, Copy.CONFIG_REPORT)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> select(SCOPE, 1, Policy.DEFAULT, List.of(fact("unknown", ValidationMode.FULL, 1, Copy.CONFIG_REPORT)),
                Set.of("unknown"), LIMITS)).isInstanceOf(TapstateException.class);
    }

    @Test void scopeAndHardMetadataLimitsCannotBeBypassedByDuplicateCopies() {
        var fact = fact("run", ValidationMode.FULL, 1, Copy.CONFIG_REPORT);
        assertThatThrownBy(() -> select(new Scope("cluster", "other", "task"), 1, Policy.DEFAULT, List.of(fact), Set.of(), LIMITS))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> select(SCOPE, 0, Policy.DEFAULT, List.of(), Set.of(), LIMITS)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> select(SCOPE, 1, Policy.DEFAULT, Collections.nCopies(3, fact), Set.of(), new Limits(2, 65536)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> select(SCOPE, 1, Policy.DEFAULT, List.of(fact), Set.of(), new Limits(2, 1024)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> select(SCOPE, 1, Policy.DEFAULT, List.of(fact), Set.of("active", "unknown"), new Limits(2, 65536)))
                .isInstanceOf(TapstateException.class);
    }

    @Test void configuredRetentionHasInclusiveOneToOneHundredBounds() {
        assertThat(new Policy(1, 100)).isNotNull(); assertThat(Policy.DEFAULT).isEqualTo(new Policy(10, 10));
        for (int invalid : List.of(0, 101, -1)) {
            assertThatThrownBy(() -> new Policy(invalid, 10)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> new Policy(10, invalid)).isInstanceOf(TapstateException.class);
        }
    }

    private static Fact fact(String id, ValidationMode mode, long end, Copy copy) {
        return new Fact(run(id, mode), mode == ValidationMode.FULL ? ValidationRunState.COMPLETED : ValidationRunState.STOPPED,
                Instant.ofEpochSecond(end), copy);
    }
    private static ValidationRunIdentity run(String id, ValidationMode mode) {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", id, mode, "spec", 1, "rules");
    }
}
