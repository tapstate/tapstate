package io.tapstate.spi.capture;

import io.tapstate.core.validation.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.validation.ValidationTiming.*;

/** Connects optional provider ports to core evidence gates; not a production CDC or writer test. */
class ValidationPortsFixtureTest {
    private static final Instant TIME = Instant.parse("2026-01-01T00:00:00Z");

    @Test void aProviderWindowCanFeedFreshSnapshotIngressWithoutTreatingSubmissionAsAck() {
        var scope = scope(); var timing = new TimingFixture();
        assertThat(timing.capabilities(scope.execution()).getFirst().supportsStrictWindow()).isTrue();
        Window window;
        try (var watch = timing.observe(scope).toCompletableFuture().join()) {
            var l0 = watch.sample(Phase.L0, "l0").toCompletableFuture().join();
            var l0Coverage = watch.coverage(l0).toCompletableFuture().join();
            var l1 = watch.sample(Phase.L1, "l1").toCompletableFuture().join();
            window = new Window(l0Coverage, watch.coverage(l1).toCompletableFuture().join(),
                    read("s1", 1), read("target", 2), read("s2", 3), true, true);
        }
        var calls = new AtomicInteger();
        ValidationRepairPort.SnapshotReader reader = (attempt, guard) -> {
            guard.run(); calls.incrementAndGet();
            return new ValidationRepair.Snapshot(attempt, attempt.source(),
                    ValidationValues.ownRow(Map.of("source_id", 7L), new ValidationValues.Limits(4096, 16, 4)), read("snapshot", 5));
        };
        var ingress = new IngressFixture(); var first = attempt("first");
        var submission = ingress.submit(first, window, reader).toCompletableFuture().join();
        assertThat(submission.actualWrite().toCompletableFuture()).isNotDone();
        assertThat(ingress.submit(first, window, reader).toCompletableFuture().join()).isSameAs(submission);
        assertThat(calls).hasValue(1);
        // A distinct user request may proceed while the old write result is unknown.
        var second = attempt("second"); var newer = ingress.submit(second, window, reader).toCompletableFuture().join();
        assertThat(calls).hasValue(2);
        var oldAck = ack(first); ingress.writes.get(first.repairId()).complete(oldAck);
        assertThatThrownBy(() -> ValidationRepair.afterRecheck(second, newer.submissionId(), oldAck, null))
                .isInstanceOf(io.tapstate.core.common.TapstateException.class);
        ingress.writes.get(second.repairId()).complete(ack(second));
        assertThat(ValidationRepair.afterRecheck(second, newer.submissionId(), newer.actualWrite().toCompletableFuture().join(), agreement(second.current())))
                .isEqualTo(ValidationRepair.Outcome.REPAIRED);
    }

    @Test void unsettledEvidenceStopsBeforeTheSnapshotReaderAndNoDefaultCapabilityIsAssumed() {
        var scope = scope(); var domain = scope.domains().getFirst();
        var l0 = new Batch(scope, "l0", Phase.L0, TIME, Map.of(domain, "opaque"));
        var l1 = new Batch(scope, "l1", Phase.L1, TIME.plusSeconds(4), Map.of(domain, "opaque"));
        var partial = new Progress(Set.of(Milestone.CAPTURED), false);
        var window = new Window(new Coverage(l0, TIME, Map.of(domain, partial)), new Coverage(l1, TIME.plusSeconds(4), Map.of(domain, partial)),
                read("s1", 1), read("target", 2), read("s2", 3), true, true);
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> new IngressFixture().submit(attempt("blocked"), window, (attempt, guard) -> {
            calls.incrementAndGet(); throw new AssertionError("unsettled evidence reached source reader");
        })).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        assertThat(calls).hasValue(0);
    }

    private static final class TimingFixture implements ValidationTimingPort {
        @Override public List<Capabilities> capabilities(Execution execution) {
            return List.of(new Capabilities(scope().domains().getFirst(), Set.of(Capability.values())));
        }
        @Override public CompletionStage<Watch> observe(Scope scope) {
            return CompletableFuture.completedFuture(new Watch() {
                @Override public Scope scope() { return scope; }
                @Override public CompletionStage<Batch> sample(Phase phase, String id) {
                    return CompletableFuture.completedFuture(new Batch(scope, id, phase, TIME.plusSeconds(phase == Phase.L0 ? 0 : 4),
                            Map.of(scope.domains().getFirst(), "native-token")));
                }
                @Override public CompletionStage<Coverage> coverage(Batch batch) {
                    return CompletableFuture.completedFuture(new Coverage(batch, batch.sampledAt(),
                            Map.of(scope.domains().getFirst(), new Progress(Set.of(Milestone.values()), false))));
                }
                @Override public void close() {}
            });
        }
    }
    private static final class IngressFixture implements ValidationRepairPort {
        private final Map<String, Submission> requests = new java.util.HashMap<>();
        private final Map<String, CompletableFuture<ValidationRepair.Ack>> writes = new java.util.HashMap<>();
        @Override public CompletionStage<Submission> submit(ValidationRepair.Attempt attempt, Window window, SnapshotReader reader) {
            ValidationRepair.requireWindow(attempt, window);
            if (requests.containsKey(attempt.requestId())) return CompletableFuture.completedFuture(requests.get(attempt.requestId()));
            // This fixture supplies the admitted handoff; production providers must prove native ordering.
            var snapshot = reader.read(attempt, () -> {});
            assertThat(snapshot.attempt()).isEqualTo(attempt);
            var ack = new CompletableFuture<ValidationRepair.Ack>(); writes.put(attempt.repairId(), ack);
            var submission = new Submission() {
                @Override public ValidationRepair.Attempt attempt() { return attempt; }
                @Override public String submissionId() { return "submit-" + attempt.repairId(); }
                @Override public CompletionStage<ValidationRepair.Ack> actualWrite() { return ack; }
                @Override public void cancel() {}
                @Override public CompletionStage<Void> workersExited() { return ack.thenApply(ignored -> null); }
            };
            requests.put(attempt.requestId(), submission);
            return CompletableFuture.completedFuture(submission);
        }
    }
    private static Scope scope() {
        var run = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "boot", 1, 1, 0),
                "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
        return new Scope(new ValidationObservation(run, new ValidationRoot("mapping", "output", key(), List.of()), 2),
                new Execution("cluster", "pipeline", "definition", "production", 1), "timing", "dependency-scope",
                List.of(new Domain("source", "capture", "member", "boot", "epoch", "v1")), TIME, 10_000);
    }
    private static ValidationKey key() { return new ValidationKey(List.of(ValidationKey.Part.number(7))); }
    private static ValidationRepair.Attempt attempt(String id) {
        var scope = scope(); var current = scope.observation();
        return new ValidationRepair.Attempt("request-" + id, id, new ValidationObservation(current.run(), current.root(), 1), current,
                scope.execution(), new ValidationPhysicalIdentity("source", "db", null, "input", List.of(List.of("source_id")), key()),
                new ValidationPhysicalIdentity("target", "db", null, "output", List.of(List.of("renamed_id")), key()));
    }
    private static ValidationCheckEvent.ReadBoundary read(String id, int seconds) {
        return new ValidationCheckEvent.ReadBoundary(id, "fresh-v1", TIME.plusSeconds(seconds), TIME.plusSeconds(seconds));
    }
    private static ValidationRepair.Ack ack(ValidationRepair.Attempt attempt) {
        return new ValidationRepair.Ack(attempt, "submit-" + attempt.repairId(), "output", attempt.target(),
                ValidationRepair.WriteStatus.COMMITTED, TIME.plusSeconds(6));
    }
    private static ValidationRootCheck agreement(ValidationObservation observation) {
        try (var builder = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN,
                ValidationCheckEvent.Participation.SHOULD_EXIST, ValidationRootCheck.TargetPresence.PRESENT,
                new ValidationRootCheck.Limits(32768, 10, 10, 10, 0))) {
            builder.read(ValidationRootCheck.Side.SOURCE, read("recheck-source", 7));
            builder.read(ValidationRootCheck.Side.TARGET, read("recheck-target", 8));
            for (var check : ValidationRootCheck.Check.values()) builder.checked(check);
            return builder.finish();
        }
    }
}
