package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.StageQueueReading;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StageQueueAccountsTest {
    @Test
    void aNewScopeWithAnEarlierValidClockStartsItsOwnPeak() {
        Engine.StageQueueAccounts accounts = new Engine.StageQueueAccounts(2);
        accounts.accept("flow", 1, sample("run-a", 100, 10));
        var replacement = accounts.accept("flow", 2, sample("run-b", 10, 2));
        assertThat(replacement).isPresent();
        assertThat(high(replacement.orElseThrow())).isEqualTo(2);
        assertThat(high(accounts.accept("flow", 2, sample("run-c", 5, 1)).orElseThrow())).isEqualTo(1);
    }

    @Test
    void aJobNullReadCannotRemoveTheFenceAgainstAnAlreadyValidatedCancelledFrame() {
        Engine.StageQueueAccounts accounts = new Engine.StageQueueAccounts(2);
        accounts.accept("flow", 1, sample("run-a", 10, 10));
        accounts.cancel("flow", 1L);
        accounts.forget("flow");
        assertThat(accounts.accept("flow", 1, sample("run-a", 11, 2))).isEmpty();
    }

    @Test
    void quietScopesKeepNoPointsAndSampledPeaksResetOnExecutionOrJobChange() {
        Engine.StageQueueAccounts accounts = new Engine.StageQueueAccounts(2);
        assertThat(accounts.accept("flow", 1, sample("run-a", 1, 0)).orElseThrow()).isEqualTo(StageQueueReading.NONE);
        assertThat(high(accounts.accept("flow", 1, sample("run-a", 2, 10)).orElseThrow())).isEqualTo(10);
        assertThat(high(accounts.accept("flow", 1, sample("run-a", 3, 2)).orElseThrow())).isEqualTo(10);
        assertThat(accounts.accept("flow", 1, sample("run-a", 2, 16))).isEmpty();
        assertThat(high(accounts.accept("flow", 1, sample("run-a", 4, 1)).orElseThrow())).isEqualTo(10);
        assertThat(high(accounts.accept("flow", 1, sample("run-b", 5, 3)).orElseThrow())).isEqualTo(3);
        assertThat(high(accounts.accept("flow", 2, sample("run-c", 6, 1)).orElseThrow())).isEqualTo(1);
        accounts.cancel("flow", 2L);
        assertThat(accounts.accept("flow", 2, sample("run-c", 7, 16))).isEmpty();
        assertThat(accounts.accept("flow", 3, sample("run-d", 7, 0)).orElseThrow()).isEqualTo(StageQueueReading.NONE);
        accounts.cancel("flow", null);
        assertThat(accounts.accept("flow", 3, sample("run-d", 8, 16))).isEmpty();
        accounts.accept("flow", 4, sample("run-e", 9, 1));
        accounts.forget("flow");
        assertThat(accounts.size()).isZero();
    }

    @Test
    void aPreCancelReadTicketCannotReplaceANewerJobOrReenterAfterEviction() {
        Engine.StageQueueAccounts accounts = new Engine.StageQueueAccounts(2);
        accounts.accept("flow", 1, sample("old", 100, 10));
        var old = accounts.ticket("flow", 1);
        accounts.cancel("flow", 1L);
        accounts.forget("flow");
        accounts.accept("flow", 2, sample("new", 10, 3));
        assertThat(accounts.accept("flow", 1, sample("old", 101, 16), old)).isEmpty();
        assertThat(high(accounts.accept("flow", 2, sample("new", 11, 1)).orElseThrow())).isEqualTo(3);
        var evicted = accounts.ticket("flow", 2);
        accounts.accept("a", 3, sample("a", 12, 1));
        accounts.accept("b", 4, sample("b", 13, 1));
        assertThat(accounts.accept("flow", 2, sample("new", 14, 16), evicted)).isEmpty();
        assertThat(accounts.size()).isEqualTo(2);
    }

    @Test
    void stageSampleTimeCannotRegressEvenWhenTheOverallFrameAdvances() {
        Engine.StageQueueAccounts accounts = new Engine.StageQueueAccounts(2);
        accounts.accept("flow", 1, new Engine.StageQueuesSample("run", 1,
                Map.of("transform", new Engine.QueueSample("run", 10, 16, 5))));
        assertThat(accounts.accept("flow", 1, new Engine.StageQueuesSample("run", 2,
                Map.of("transform", new Engine.QueueSample("run", 16, 16, 4))))).isEmpty();
        assertThat(high(accounts.accept("flow", 1, sample("run", 6, 1)).orElseThrow())).isEqualTo(10);
    }

    @Test
    void accountCapacityIsBoundedAndEvictedPipelinesDoNotCarryTheirOldPeaks() {
        Engine.StageQueueAccounts accounts = new Engine.StageQueueAccounts(2);
        accounts.accept("a", 1, sample("run", 1, 10));
        accounts.accept("b", 2, sample("run", 2, 4));
        accounts.accept("c", 3, sample("run", 3, 2));
        assertThat(accounts.size()).isEqualTo(2);
        assertThat(high(accounts.accept("a", 1, sample("run", 4, 1)).orElseThrow())).isEqualTo(1);
        assertThat(accounts.size()).isEqualTo(2);
    }

    private static Engine.StageQueuesSample sample(String execution, long at, long depth) {
        return new Engine.StageQueuesSample(execution, at,
                Map.of("transform", new Engine.QueueSample(execution, depth, 16, at)));
    }

    private static long high(StageQueueReading reading) {
        return reading.byStage().get("transform").queue().highWater();
    }
}
