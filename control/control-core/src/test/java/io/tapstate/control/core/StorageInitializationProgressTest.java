package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.StorageSetupScope;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class StorageInitializationProgressTest {
    private static final StorageSetupScope SCOPE = new StorageSetupScope("cluster", "installation", "setup", 1);
    private final AtomicLong nanos = new AtomicLong();
    private final StorageInitializationProgress progress = new StorageInitializationProgress(
            Clock.fixed(Instant.parse("2026-10-27T00:00:00Z"), ZoneOffset.UTC), nanos::get);

    @Test void snapshotsRetainTheSameOperationAndRealTimingsAcrossReaders() {
        progress.begin(SCOPE, 0, false);
        advance(1200);
        progress.checked(SCOPE, StorageSetupService.Layer.CACHE);
        progress.completed(SCOPE, StorageInitializationProgress.Stage.SAVE_SETTINGS);
        progress.started(SCOPE, StorageInitializationProgress.Stage.PREPARE_STORAGE);
        advance(900);
        var first = view();
        assertThat(first.milestones()).extracting(StorageInitializationProgress.Milestone::state)
                .containsExactly(StorageInitializationProgress.State.COMPLETE, StorageInitializationProgress.State.RUNNING,
                        StorageInitializationProgress.State.PENDING, StorageInitializationProgress.State.PENDING);
        assertThat(first.milestones().get(0).elapsedMillis()).isEqualTo(1200);
        assertThat(first.milestones().get(1).elapsedMillis()).isEqualTo(900);
        assertThat(first.elapsedMillis()).isEqualTo(2100);
        advance(500);
        assertThat(view().attemptId()).isEqualTo(first.attemptId());
        assertThat(view().elapsedMillis()).isEqualTo(2600);
        assertThat(first.milestones().get(1).elapsedMillis()).isEqualTo(900);
        assertThat(view().connectionChecks()).containsOnlyKeys(StorageSetupService.Layer.CACHE);
    }

    @Test void completedFactsFromAnEarlierProcessHaveUnknownDurations() {
        progress.begin(SCOPE, 2, true);
        advance(3200);
        var snapshot = view();
        assertThat(snapshot.restored()).isTrue();
        assertThat(snapshot.milestones().get(0).state()).isEqualTo(StorageInitializationProgress.State.COMPLETE);
        assertThat(snapshot.milestones().get(0).elapsedMillis()).isNull();
        assertThat(snapshot.milestones().get(1).elapsedMillis()).isNull();
        assertThat(snapshot.milestones().get(2).elapsedMillis()).isEqualTo(3200);
        assertThat(snapshot.connectionChecks()).isEmpty();
    }

    @Test void aFailedStepStopsItsTimerAndPublishesOnlyTheCodedFailure() {
        progress.begin(SCOPE, 2, true);
        advance(4500);
        progress.failed(SCOPE, new TapstateException(ControlError.STORAGE_UNAVAILABLE,
                Map.of("secret", "mongodb://user:private@host"), new IllegalStateException("private cause")));
        advance(6000);
        assertThat(view().state()).isEqualTo(StorageInitializationProgress.State.FAILED);
        assertThat(view().elapsedMillis()).isEqualTo(4500);
        assertThat(view().milestones().get(2).errorCode()).isEqualTo("control.storage-unavailable");
        assertThat(view().toString()).doesNotContain("mongodb://", "private cause");
    }

    @Test void anotherOperationCannotReadOrCompleteTheCurrentAttempt() {
        progress.begin(SCOPE, 0, false);
        var other = new StorageSetupScope("cluster", "installation", "other", 1);
        assertThat(progress.snapshot(other)).isEmpty();
        progress.completed(other, StorageInitializationProgress.Stage.SAVE_SETTINGS);
        progress.failed(other, new IllegalStateException());
        assertThat(view().state()).isEqualTo(StorageInitializationProgress.State.RUNNING);
        assertThat(view().milestones().get(0).state()).isEqualTo(StorageInitializationProgress.State.RUNNING);
        progress.begin(other, 0, false);
        progress.checked(SCOPE, StorageSetupService.Layer.MASTER);
        assertThat(progress.snapshot(other).orElseThrow().connectionChecks()).isEmpty();
    }

    @Test void retryPreservesProvenCompletedWorkButDoesNotRestartOnAReadOrDuplicateBegin() {
        progress.begin(SCOPE, 0, false);
        advance(1000);
        progress.completed(SCOPE, StorageInitializationProgress.Stage.SAVE_SETTINGS);
        progress.started(SCOPE, StorageInitializationProgress.Stage.PREPARE_STORAGE);
        advance(2000);
        progress.failed(SCOPE, new IllegalStateException());
        var failed = view();
        advance(3000);
        progress.begin(SCOPE, 1, false);
        var resumed = view();
        assertThat(resumed.attemptId()).isNotEqualTo(failed.attemptId());
        assertThat(resumed.elapsedMillis()).isEqualTo(6000);
        assertThat(resumed.milestones().get(0).elapsedMillis()).isEqualTo(1000);
        progress.begin(SCOPE, 1, false);
        assertThat(view().attemptId()).isEqualTo(resumed.attemptId());
    }

    @Test void aPreBindingRetryDoesNotPublishChecksFromThePreviousCandidate() {
        progress.begin(SCOPE, 0, false);
        progress.checked(SCOPE, StorageSetupService.Layer.CACHE);
        progress.checked(SCOPE, StorageSetupService.Layer.MASTER);
        progress.failed(SCOPE, new IllegalStateException("binding publication failed"));
        assertThat(view().connectionChecks()).hasSize(2);
        String firstAttempt = view().attemptId();
        progress.begin(SCOPE, 0, false);
        assertThat(view().attemptId()).isNotEqualTo(firstAttempt);
        assertThat(view().connectionChecks()).isEmpty();
    }

    @Test void readinessIsPublishedOnlyAfterAllActualStagesFinish() {
        progress.begin(SCOPE, 0, false);
        assertThatThrownBy(() -> progress.started(SCOPE, StorageInitializationProgress.Stage.PUBLISH_READY))
                .isInstanceOf(IllegalStateException.class);
        for (var stage : StorageInitializationProgress.Stage.values()) {
            progress.started(SCOPE, stage);
            advance(500);
            progress.completed(SCOPE, stage);
        }
        String attempt = view().attemptId();
        advance(5000);
        progress.begin(SCOPE, 0, false);
        assertThat(view().attemptId()).isEqualTo(attempt);
        assertThat(view().state()).isEqualTo(StorageInitializationProgress.State.COMPLETE);
        assertThat(view().elapsedMillis()).isEqualTo(2000);
        assertThat(view().milestones()).allMatch(step -> step.state() == StorageInitializationProgress.State.COMPLETE);
    }

    private void advance(long millis) { nanos.addAndGet(millis * 1_000_000L); }
    private StorageInitializationProgress.View view() { return progress.snapshot(SCOPE).orElseThrow(); }
}
