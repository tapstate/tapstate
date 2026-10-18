package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.FirstPipelineCreation;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineCachePreparations;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelinePreparationReceipt;
import io.tapstate.spi.store.PipelineStatePreparations;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** App orchestration tests; concrete one-time capabilities and CACHE transactions have adapter ITs. */
class PipelineFirstStatePreparerTest {
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster", 1, "cache-a");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache-a", "flow", 1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimFence NODE = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"), OWNER, 1, 0, 1);
    private static final WorkloadClaimFence PIPELINE = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "flow"), OWNER, 1, 0, 1);
    private static final String ID = UUID.randomUUID().toString();
    private static final Instant AT = Instant.parse("2026-10-18T08:00:00Z");
    private final List<String> calls = new ArrayList<>();
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final Configuration configuration = new Configuration();
    private final Cache cache = new Cache();
    private Optional<WorkloadClaimFence> node = Optional.of(NODE);
    private Clock clock = Clock.fixed(AT, ZoneOffset.UTC);

    private PipelineFirstStatePreparer preparer() {
        return new PipelineFirstStatePreparer(BUSINESS, configuration, cache, () -> { calls.add("node"); return node; },
                () -> { if (!active.get()) throw fail(IoError.WORKLOAD_CLAIM_FENCED); }, clock, Duration.ofSeconds(30));
    }
    private static PipelinePreparation issued() { return new PipelinePreparation("flow", 1, PipelinePreparation.Phase.ISSUED, SCOPE, ID, NODE); }
    private static PipelinePreparation complete() { return new PipelinePreparation("flow", 1, PipelinePreparation.Phase.COMPLETE, SCOPE, ID, NODE); }

    @Test void firstPreparationUsesOneNodeAndOneGrantAndCompletesOnlyAfterItsCacheReceipt() {
        assertThat(preparer().prepare(SCOPE)).isEqualTo(complete());
        assertThat(calls).containsExactly("read", "node", "issue", "reserve", "consume", "seed", "require-issued", "complete");
        assertThat(cache.seedTime).isEqualTo(AT);
        assertThat(configuration.node).isEqualTo(NODE);
        assertThat(cache.original).isEqualTo(new PipelineCachePreparations.Reservation(SCOPE, ID, PipelineCachePreparations.Phase.RESERVED, PIPELINE));
    }

    @Test void theFirstGrantWinnerAlsoQueriesItsOriginalResultAfterAnotherAttemptCompletes() {
        cache.beforeSeed = () -> configuration.facts = complete();
        assertThat(preparer().prepare(SCOPE)).isEqualTo(complete());
        assertThat(calls).containsExactly("read", "node", "issue", "reserve", "consume", "seed", "require-issued", "read");
    }

    @Test void anExistingCompleteStatusDoesNotIssueOrTouchCacheOrNodeAgain() {
        configuration.facts = complete();
        assertThat(preparer().prepare(SCOPE)).isEqualTo(complete());
        assertThat(calls).containsExactly("read");
    }

    @Test void retainedIssuedFactsResumeWithoutReissuingAnInsertCapability() {
        configuration.facts = issued();
        assertThat(preparer().prepare(SCOPE)).isEqualTo(complete());
        assertThat(calls).containsExactly("read", "node", "resume", "seed", "require-issued", "complete");
    }

    @Test void losingTheUnassignedCasUsesOnlyTheOtherWinnersRetainedFacts() {
        configuration.otherWon = true;
        assertThat(preparer().prepare(SCOPE)).isEqualTo(complete());
        assertThat(calls).containsExactly("read", "node", "issue", "resume", "seed", "require-issued", "complete");
    }

    @Test void aConcurrentCompleteResultDoesNotTryToAcquireOrSeedAgain() {
        configuration.otherWon = true; configuration.afterIssue = () -> configuration.facts = complete();
        assertThat(preparer().prepare(SCOPE)).isEqualTo(complete());
        assertThat(calls).containsExactly("read", "node", "issue");
    }

    @Test void missingIssuedHistoryNeverFallsBackToFirstCreateOrCompletesWithoutAReceipt() {
        configuration.facts = issued(); cache.resumeFailure = fail(IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        assertCode(() -> preparer().prepare(SCOPE), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        assertThat(calls).containsExactly("read", "node", "resume", "read");
        assertThat(configuration.facts).isEqualTo(issued());
    }

    @Test void concurrentOriginalCompletionIsResolvedOnlyAsStatusAndDoesNotRetryCache() {
        configuration.facts = issued();
        cache.afterResume = () -> configuration.facts = complete();
        cache.resumeFailure = fail(IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        assertThat(preparer().prepare(SCOPE)).isEqualTo(complete());
        assertThat(calls).containsExactly("read", "node", "resume", "read");
    }

    @Test void aLaterGenerationCannotReplaceTheOriginalDuringUnknownResultResolution() {
        configuration.facts = issued();
        cache.afterResume = () -> configuration.facts = new PipelinePreparation("flow", 2, PipelinePreparation.Phase.COMPLETE,
                new PipelineIntentScope("cluster", 1, "cache-a", "flow", 2), ID, NODE);
        cache.resumeFailure = fail(IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        assertCode(() -> preparer().prepare(SCOPE), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        assertThat(calls).containsExactly("read", "node", "resume", "read");
    }

    @Test void aDifferentNonceCannotResolveTheOriginalOperation() {
        configuration.facts = issued();
        cache.afterResume = () -> configuration.facts = new PipelinePreparation("flow", 1, PipelinePreparation.Phase.COMPLETE, SCOPE, UUID.randomUUID().toString(), NODE);
        cache.resumeFailure = fail(IoError.WORKLOAD_CLAIM_FENCED);
        assertCode(() -> preparer().prepare(SCOPE), IoError.WORKLOAD_CLAIM_FENCED);
    }

    @Test void storeUnavailabilityIsNotConvertedIntoCompletionOrANewCreation() {
        configuration.facts = issued(); cache.resumeFailure = fail(IoError.STORE_UNAVAILABLE);
        assertCode(() -> preparer().prepare(SCOPE), IoError.STORE_UNAVAILABLE);
        assertThat(calls).containsExactly("read", "node", "resume");
    }

    @Test void noActiveNodeCannotSpendConfigurationCreationAuthority() {
        node = Optional.empty();
        assertCode(() -> preparer().prepare(SCOPE), IoError.WORKLOAD_CLAIM_FENCED);
        assertThat(calls).containsExactly("read", "node");
        assertThat(configuration.facts.phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
    }

    @Test void aForeignNodeOrWrongRoleCannotIssueTheGrant() {
        for (var key : List.of(new WorkloadClaimKey("other", WorkloadClaimType.NODE_SESSION, "node"),
                new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "source"))) {
            calls.clear(); node = Optional.of(new WorkloadClaimFence(key, OWNER, 1, 0, 1));
            assertCode(() -> preparer().prepare(SCOPE), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(calls).containsExactly("read", "node");
        }
    }

    @Test void anOldPhysicalScopeNeverAdoptsALaterGenerationOrAnotherCacheBinding() {
        configuration.facts = new PipelinePreparation("flow", 2, PipelinePreparation.Phase.UNASSIGNED, null, null, null);
        assertCode(() -> preparer().prepare(SCOPE), IoError.PIPELINE_INTENT_FENCED);
        assertThat(calls).containsExactly("read");
        calls.clear();
        assertCode(() -> preparer().prepare(new PipelineIntentScope("cluster", 2, "cache-b", "flow", 1)), IoError.PIPELINE_INTENT_FENCED);
        assertThat(calls).isEmpty();
    }

    @Test void lossAfterIssueLeavesStrictIssuedFactsAndCannotEnterFirstCacheCreation() {
        configuration.afterIssue = () -> active.set(false);
        assertCode(() -> preparer().prepare(SCOPE), IoError.WORKLOAD_CLAIM_FENCED);
        assertThat(configuration.facts).isEqualTo(issued());
        assertThat(calls).containsExactly("read", "node", "issue");
        active.set(true); calls.clear(); cache.resumeFailure = fail(IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        assertCode(() -> preparer().prepare(SCOPE), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
        assertThat(calls).containsExactly("read", "node", "resume", "read");
    }

    @Test void expirationDuringTimestampCaptureCannotSeedOrComplete() {
        clock = new Clock() {
            @Override public ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { active.set(false); return AT; }
        };
        assertCode(() -> preparer().prepare(SCOPE), IoError.WORKLOAD_CLAIM_FENCED);
        assertThat(calls).containsExactly("read", "node", "issue", "reserve", "consume");
        assertThat(configuration.facts).isEqualTo(issued());
    }

    @Test void aReceiptForAnotherOperationCannotCompleteTheOriginal() {
        cache.receiptId = UUID.randomUUID().toString();
        assertCode(() -> preparer().prepare(SCOPE), IoError.PIPELINE_INTENT_FENCED);
        assertThat(calls).doesNotContain("complete");
        assertThat(configuration.facts).isEqualTo(issued());
    }

    @Test void originalConfigRevocationIsRecheckedBeforeTheCacheSeed() {
        cache.beforeSeed = () -> configuration.revoked = true;
        assertCode(() -> preparer().prepare(SCOPE), IoError.PIPELINE_INTENT_FENCED);
        assertThat(calls).containsExactly("read", "node", "issue", "reserve", "consume", "seed", "require-issued", "read");
        assertThat(configuration.facts).isEqualTo(issued());
    }

    private final class Configuration implements PipelineStatePreparations {
        private PipelinePreparation facts = new PipelinePreparation("flow", 1, PipelinePreparation.Phase.UNASSIGNED, null, null, null);
        private boolean otherWon; private boolean revoked;
        private WorkloadClaimFence node; private Runnable guard = () -> {}; private Runnable afterIssue = () -> {};
        @Override public PipelineStatePreparations guardedBy(Runnable active) { guard = active; return this; }
        @Override public Optional<PipelinePreparation> read(String pipelineId) { guard.run(); calls.add("read"); return Optional.of(facts); }
        @Override public FirstPipelineCreationResult issueFirstCreate(PipelineIntentScope scope, WorkloadClaimFence originalNode) {
            guard.run(); calls.add("issue"); node = originalNode; facts = issued(); afterIssue.run();
            return otherWon ? new FirstPipelineCreationResult.AlreadyIssued(facts) : new FirstPipelineCreationResult.Issued(new FirstPipelineCreation() {
                private boolean consumed;
                @Override public PipelinePreparation preparation() { return issued(); }
                @Override public void consume() { assertThat(consumed).isFalse(); consumed = true; calls.add("consume"); guard.run(); }
                @Override public void requireActive() { guard.run(); }
            });
        }
        @Override public void requireIssued(PipelineIntentScope scope, String id) {
            guard.run(); calls.add("require-issued");
            if (revoked || !facts.scope().equals(scope) || !facts.preparationId().equals(id) || facts.phase() != PipelinePreparation.Phase.ISSUED) throw fail(IoError.PIPELINE_INTENT_FENCED);
        }
        @Override public PipelinePreparation complete(PipelinePreparationReceipt receipt) { guard.run(); calls.add("complete"); facts = PipelineFirstStatePreparerTest.complete(); return facts; }
    }
    private final class Cache implements PipelineCachePreparations {
        private RuntimeException resumeFailure; private Runnable afterResume = () -> {}; private Runnable beforeSeed = () -> {};
        private Reservation original; private Instant seedTime; private String receiptId = ID;
        @Override public Reservation reserveFirst(FirstPipelineCreation grant, Duration ttl) { calls.add("reserve"); grant.consume(); return new Reservation(SCOPE, ID, Phase.RESERVED, PIPELINE); }
        @Override public Reservation resume(PipelinePreparation facts, WorkloadClaimFence node, Duration ttl) {
            calls.add("resume"); assertThat(facts).isEqualTo(issued()); assertThat(node).isEqualTo(NODE); afterResume.run();
            if (resumeFailure != null) throw resumeFailure;
            return new Reservation(SCOPE, ID, Phase.RESERVED, PIPELINE);
        }
        @Override public PipelinePreparationReceipt seed(Reservation original, PipelineStatePreparations configuration, Instant time) {
            calls.add("seed"); this.original = original; this.seedTime = time; beforeSeed.run(); configuration.requireIssued(original.scope(), original.preparationId());
            return new Receipt(SCOPE, receiptId);
        }
    }
    private record Receipt(PipelineIntentScope scope, String preparationId) implements PipelinePreparationReceipt {}
    private static TapstateException fail(IoError code) { return new TapstateException(code, code == IoError.PIPELINE_STATE_REBUILD_REQUIRED ? Map.of("pipeline", "flow")
            : code == IoError.STORE_UNAVAILABLE ? Map.of("detail", "unavailable") : Map.of(), null); }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(code));
    }
}
