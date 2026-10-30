package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class AlertEvaluationHostTest {
    static final Instant T = Instant.parse("2026-10-28T00:00:00Z");
    static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("installation", "cluster");
    static final AlarmKey.Resource RESOURCE = new AlarmKey.Resource("pipeline", "orders", "1");
    final AtomicReference<Instant> now = new AtomicReference<>(T);
    final Clock clock = new Clock() {
        public ZoneId getZone() { return ZoneOffset.UTC; }
        public Clock withZone(ZoneId zone) { return this; }
        public Instant instant() { return now.get(); }
    };
    final AlertStore store = mock(AlertStore.class);
    final Map<AlarmKey, AlertStore.Stored<AlertStateRecord.Head>> rows = new HashMap<>();
    final List<AlertStore.Commit> commits = new ArrayList<>();
    final AlertEvaluationHost host = new AlertEvaluationHost(store, BuiltinAlarmTypes.catalog(),
            AlertChannelPolicy.Deployment.ON_PREM, clock, "owner-a", () -> { });
    AlertEvaluationHostTest() {
        when(store.state(any(), any())).thenAnswer(c -> Optional.ofNullable(rows.get(c.getArgument(0))));
        when(store.commit(any())).thenAnswer(c -> {
            AlertStore.Commit command = c.getArgument(0); var change = command.state();
            var existing = rows.get(change.after().snapshot().rule().key());
            if (existing == null ? change.revision() != 0 : existing.revision() != change.revision()
                    || !existing.value().equals(change.before())) return AlertStore.Outcome.CONFLICT;
            rows.put(change.after().snapshot().rule().key(), new AlertStore.Stored<>(change.revision() + 1, change.after()));
            commits.add(command); return AlertStore.Outcome.STAGED;
        });
    }
    static AlertConfiguration.Rule definition(String type) {
        var header = AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE, SCOPE, "rule"),
                AlertChannelPolicy.Deployment.ON_PREM, T, "admin");
        return new AlertConfiguration.Rule(header, "Rule", type, 1,
                new AlertConfiguration.Resources(Set.of("pipeline"), true, Set.of()), Map.of(), null, true, null)
                .validated(BuiltinAlarmTypes.catalog());
    }
    static AlertEvaluationHost.Binding binding(AlertConfiguration.Rule rule, String producer) {
        return new AlertEvaluationHost.Binding(rule, RESOURCE, producer, 0, () -> { });
    }
    void condition(AlertEvaluationHost.Binding binding, long sequence, long second, AlertSignal.Condition condition) {
        now.set(T.plusSeconds(second));
        assertThat(host.accept(binding, sequence, new AlertInput.Conditions(now.get(), condition, null)))
                .isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
    }
    @Test void operationalFailuresRequireEachOriginalCapabilityEvenAfterHostRestart() {
        var header=definition("pipeline.failed").header();
        var rule=new AlertConfiguration.Rule(header,"Source failure","source.connection-failed",1,
                new AlertConfiguration.Resources(Set.of("source"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
        var binding=new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("source","erp","source-uuid"),"source-binding",1,()->{});
        String digest="a".repeat(64);
        assertThat(host.accept(binding,1,new AlertInput.Operational(T,digest,OperationalEvidence.Capability.STREAM_READ,false))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
        now.set(T.plusSeconds(1));
        host.accept(binding,2,new AlertInput.Operational(now.get(),digest,OperationalEvidence.Capability.SNAPSHOT_READ,false));
        var restarted=new AlertEvaluationHost(store,BuiltinAlarmTypes.catalog(),AlertChannelPolicy.Deployment.ON_PREM,clock,"owner-a",()->{});
        now.set(T.plusSeconds(2));restarted.accept(binding,3,new AlertInput.Operational(now.get(),digest,OperationalEvidence.Capability.INITIALIZE,true));
        assertThat(rows.get(binding.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        now.set(T.plusSeconds(3));restarted.accept(binding,4,new AlertInput.Operational(now.get(),digest,OperationalEvidence.Capability.STREAM_READ,true));
        assertThat(rows.get(binding.key()).value().operationalEvidence().failedCapabilities()).isEqualTo(1<<OperationalEvidence.Capability.SNAPSHOT_READ.ordinal());
        assertThat(rows.get(binding.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        now.set(T.plusSeconds(4));
        assertThat(restarted.accept(binding,5,new AlertInput.Operational(now.get(),"b".repeat(64),OperationalEvidence.Capability.SNAPSHOT_READ,true))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
        restarted.accept(binding,5,new AlertInput.Operational(now.get(),digest,OperationalEvidence.Capability.SNAPSHOT_READ,true));
        assertThat(rows.get(binding.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
        assertThat(rows.get(binding.key()).value().operationalEvidence().failedCapabilities()).isZero();
        assertThat(restarted.accept(binding,6,new AlertInput.Operational(T,digest,OperationalEvidence.Capability.STREAM_READ,false))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
    }
    @Test void successesForOtherCapabilitiesCannotKeepAFailedOperationFresh() {
        var header=definition("pipeline.failed").header();
        var rule=new AlertConfiguration.Rule(header,"Source failure","source.connection-failed",1,
                new AlertConfiguration.Resources(Set.of("source"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
        var binding=new AlertEvaluationHost.Binding(rule,new AlarmKey.Resource("source","erp","source-uuid"),"source-binding",1,()->{});
        String digest="a".repeat(64);
        host.accept(binding,1,new AlertInput.Operational(T,digest,OperationalEvidence.Capability.STREAM_READ,false));
        now.set(T.plusSeconds(6));
        assertThat(host.accept(binding,2,new AlertInput.Operational(now.get(),digest,OperationalEvidence.Capability.INITIALIZE,true))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
        now.set(T.plusSeconds(12));
        assertThat(host.accept(binding,3,new AlertInput.Operational(now.get(),digest,OperationalEvidence.Capability.METADATA_READ,true))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
        now.set(T.plusSeconds(16));host.tick(binding);
        var head=rows.get(binding.key()).value();
        assertThat(head.snapshot().lastObservedAt()).isEqualTo(T);
        assertThat(head.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        assertThat(head.snapshot().quality()).isEqualTo(AlertSnapshot.Quality.STALE);
    }

    @Test void conditionAndIndependentTickPersistOriginalStateWithoutDispatch() {
        var b = binding(definition("pipeline.failed"), "producer-a");
        condition(b, 1, 0, AlertSignal.Condition.BAD);
        var head = rows.get(b.key()).value();
        assertThat(head.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        assertThat(head.binding().ruleIdentity()).isEqualTo(b.definition().header().identity());
        now.set(T.plusSeconds(16)); host.tick(b);
        head = rows.get(b.key()).value();
        assertThat(head.snapshot().quality()).isEqualTo(AlertSnapshot.Quality.STALE);
        assertThat(head.snapshot().episode().endedAt()).isNull();
        condition(b, 2, 17, AlertSignal.Condition.GOOD);
        assertThat(rows.get(b.key()).value().episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.RECOVERED);
        assertThat(commits).allSatisfy(c -> { assertThat(c.intents()).isEmpty(); assertThat(c.schedules()).isEmpty(); });
    }
    @Test void producerReplacementEndsOriginalEpisodeBeforeANewOne() {
        var r = definition("pipeline.failed"); var first = binding(r, "producer-a");
        condition(first, 1, 0, AlertSignal.Condition.BAD);
        var next = binding(r, "producer-b");
        condition(next, 1, 1, AlertSignal.Condition.BAD);
        assertThat(commits).hasSize(3);
        assertThat(commits.get(1).state().after().episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.SUPERSEDED);
        assertThat(rows.get(next.key()).value().snapshot().episodeSequence()).isEqualTo(2);
        assertThat(rows.get(next.key()).value().snapshot().rule().generation()).isEqualTo(2);
    }
    @Test void freshScopeProofIsRequiredBeforeAnyOldInputCanRetargetState() {
        var r = definition("pipeline.failed"); var first = binding(r, "producer-a");
        condition(first, 1, 0, AlertSignal.Condition.BAD);
        var denied = new AlertEvaluationHost.Binding(r, RESOURCE, "old-producer", 0, () -> { throw new IllegalStateException("old proof"); });
        assertThatThrownBy(() -> host.accept(denied, 2, new AlertInput.Conditions(T, AlertSignal.Condition.GOOD, null)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(commits).hasSize(1);
    }
    @Test void anotherActiveEvaluationOwnerCannotBeTakenOver() {
        var b = binding(definition("pipeline.failed"), "producer-a");
        condition(b, 1, 0, AlertSignal.Condition.BAD);
        var other = new AlertEvaluationHost(store, BuiltinAlarmTypes.catalog(), AlertChannelPolicy.Deployment.ON_PREM,
                clock, "owner-b", () -> { });
        assertThat(other.accept(b, 2, new AlertInput.Conditions(T.plusSeconds(1), AlertSignal.Condition.GOOD, null)))
                .isEqualTo(AlertEvaluationHost.Result.BUSY);
        assertThat(commits).hasSize(1);
    }
    @Test void stoppedExpectationCannotRecoverAStaleObservationAlarm() {
        var b = binding(definition("pipeline.observation-stale"), "freshness-a");
        now.set(T.plusSeconds(121));
        host.accept(b, 1, new AlertInput.Freshness(now.get(), true, T, null, Duration.ofSeconds(120), Duration.ofSeconds(120)));
        now.set(T.plusSeconds(122));
        host.accept(b, 2, new AlertInput.Freshness(now.get(), false, T, null, Duration.ofSeconds(120), Duration.ofSeconds(120)));
        assertThat(rows.get(b.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        assertThat(rows.get(b.key()).value().snapshot().quality()).isEqualTo(AlertSnapshot.Quality.UNKNOWN);
    }
    @Test void administrativeEndHasItsOwnReasonAndNeverARecoveryTimestamp() {
        var b = binding(definition("pipeline.failed"), "producer-a");
        condition(b, 1, 0, AlertSignal.Condition.BAD);
        host.end(b.key(), AlertSnapshot.EndReason.DISABLED, () -> { });
        var ended = rows.get(b.key()).value();
        assertThat(ended.episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.DISABLED);
        assertThat(ended.episode().value().recoveredAt()).isNull();
        assertThat(commits.getLast().state().transitions().getFirst().endReason()).isEqualTo(AlertSnapshot.EndReason.DISABLED);
    }
    @Test void anEnabledSelectorEditEndsTheOldExcludedResource() {
        var original = definition("pipeline.failed"); var b = binding(original, "producer-a");
        condition(b, 1, 0, AlertSignal.Condition.BAD);
        var changed = new AlertConfiguration.Rule(original.header().next(T.plusSeconds(1), "admin"), original.name(),
                original.typeId(), 1, new AlertConfiguration.Resources(Set.of("pipeline"), false,
                        Set.of(new AlarmKey.Resource("pipeline", "other", "1"))), original.parameters(), null, true, null);
        now.set(T.plusSeconds(1)); host.reconcileRules(List.of(changed), List.copyOf(rows.values()), () -> { });
        assertThat(rows.get(b.key()).value().episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.DISABLED);
    }
    @Test void restartedHostReconcilesRetainedDisabledStatesWithoutAnyNewPublication() {
        var original = definition("pipeline.failed"); var b = binding(original, "producer-a");
        condition(b, 1, 0, AlertSignal.Condition.BAD);
        now.set(T.plusSeconds(31));
        var restarted = new AlertEvaluationHost(store, BuiltinAlarmTypes.catalog(), AlertChannelPolicy.Deployment.ON_PREM,
                clock, "owner-b", () -> { });
        restarted.reconcileRules(List.of(), List.copyOf(rows.values()), () -> { });
        assertThat(rows.get(b.key()).value().episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.DISABLED);
    }
    @Test void aRefusedCounterPublicationCannotProvideAPredecessorForTheNextRate() {
        var descriptor = new AlarmTypeCatalog.Type("test.rate", 1, AlarmTypeCatalog.Kind.STATEFUL, Set.of("pipeline"),
                "test.provider", "threshold", List.of(), AlarmSeverity.MAJOR, true, "test.condition", "test.recovery", "test.guidance");
        var catalog = new AlarmTypeCatalog(List.of(descriptor));
        var metric = new MetricAlertEvaluator.Rule("test.quantity", io.tapstate.core.lifecycle.MetricType.COUNTER, "1", Map.of(),
                MetricAlertEvaluator.Direction.ABOVE, java.math.BigDecimal.TEN, java.math.BigDecimal.ONE);
        var original = definition("pipeline.failed");
        var rule = new AlertConfiguration.Rule(original.header(), "Rate", "test.rate", 1, original.resources(), Map.of(), null, true, metric);
        var evaluator = new AlertEvaluationHost(store, catalog, AlertChannelPolicy.Deployment.ON_PREM, clock, "owner-a", () -> { });
        var b = binding(rule, "counter-a");
        evaluator.accept(b, 1, counter(T, 100));
        now.set(T.plusSeconds(1)); evaluator.accept(b, 2, new AlertInput.Discontinuity(now.get()));
        now.set(T.plusSeconds(2)); evaluator.accept(b, 3, counter(now.get(), 300));
        assertThat(rows.get(b.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.CLEAR);
        assertThat(rows.get(b.key()).value().snapshot().quality()).isEqualTo(AlertSnapshot.Quality.UNKNOWN);
    }
    private static AlertInput.Metric counter(Instant at, long value) {
        var point = io.tapstate.core.lifecycle.MetricPoint.accumulated(Map.of(), T.minusSeconds(1), at, value);
        var fact = io.tapstate.core.lifecycle.MetricFact.single("test.quantity", io.tapstate.core.lifecycle.MetricType.COUNTER, "1", point);
        return new AlertInput.Metric(new io.tapstate.core.lifecycle.Observation("orders", io.tapstate.core.lifecycle.PipelineState.RUNNING,
                Map.of(), Map.of(), Map.of(), null, at, List.of(fact)));
    }
    @Test void captureOnFirstFiringDoesNotAttachTheNewEpisodeToTheEarlierQualityTransition() {
        var b=binding(definition("pipeline.failed"),"producer-a");
        assertThatCode(()->host.accept(b,1,new AlertInput.Conditions(T,AlertSignal.Condition.BAD,
                new AlarmDiagnostics.SafeTrace("safe trace",1,true,false)))).doesNotThrowAnyException();
        var changes=commits.getLast().state().transitions();
        assertThat(changes.getFirst().value().episode()).isZero();assertThat(changes.getFirst().diagnosticReference()).isNull();
        assertThat(changes.getLast().diagnosticReference().episode()).isOne();
    }
    @Test void metadataCannotBlockClockInvalidTickOrBackdateItsSeverityChange(){
        var original=definition("pipeline.failed");var initial=binding(original,"producer-a");condition(initial,1,0,AlertSignal.Condition.BAD);
        var changed=new AlertConfiguration.Rule(original.header().next(T.plusSeconds(1),"admin"),original.name(),original.typeId(),1,original.resources(),original.parameters(),AlarmSeverity.CRITICAL,true,null).enrolled(original);
        now.set(T.minusSeconds(1));host.tick(binding(changed,"producer-a"));var invalid=rows.get(initial.key()).value().snapshot();
        assertThat(invalid.quality()).isEqualTo(AlertSnapshot.Quality.CLOCK_INVALID);assertThat(invalid.episode().severity()).isEqualTo(original.effectiveSeverity(BuiltinAlarmTypes.catalog()));
        assertThat(invalid.lastSequence()).isOne();assertThat(invalid.lastObservedAt()).isEqualTo(T);
        now.set(T.plusSeconds(2));host.tick(binding(changed,"producer-a"));var later=rows.get(initial.key()).value().snapshot();
        assertThat(later.quality()).isEqualTo(AlertSnapshot.Quality.CLOCK_INVALID);assertThat(later.episode().severity()).isEqualTo(AlarmSeverity.CRITICAL);
        assertThat(later.lastObservedAt()).isEqualTo(T);assertThat(later.episode().number()).isOne();
    }
    @Test void repeatedSeverityEditsCannotSuppressIndependentFreshnessOrRenewTheAcceptedFrontier(){
        var original=definition("pipeline.failed");var initial=binding(original,"producer-a");condition(initial,1,0,AlertSignal.Condition.BAD);
        var current=original;
        for(int second:new int[]{20,21,22}){
            now.set(T.plusSeconds(second));var severity=second%2==0?AlarmSeverity.CRITICAL:AlarmSeverity.MINOR;
            var changed=new AlertConfiguration.Rule(current.header().next(now.get(),"admin"),current.name(),current.typeId(),1,current.resources(),current.parameters(),severity,true,null).enrolled(current);
            host.tick(binding(changed,"producer-a"));var head=rows.get(initial.key()).value();
            assertThat(head.snapshot().quality()).isEqualTo(AlertSnapshot.Quality.STALE);
            assertThat(head.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);assertThat(head.snapshot().episode().number()).isOne();
            assertThat(head.snapshot().lastSequence()).isOne();assertThat(head.snapshot().lastObservedAt()).isEqualTo(T);
            assertThat(head.snapshot().episode().severity()).isEqualTo(severity);current=changed;
        }
    }
    @Test void severityMetadataChangesAreAppliedWithoutAnotherPublicationOrNewEpisode() {
        var original=definition("pipeline.failed");var b=binding(original,"producer-a");
        condition(b,1,0,AlertSignal.Condition.BAD);now.set(T.plusSeconds(1));
        var changed=new AlertConfiguration.Rule(original.header().next(now.get(),"admin"),original.name(),original.typeId(),1,
                original.resources(),original.parameters(),AlarmSeverity.CRITICAL,true,null).enrolled(original);
        var before=rows.get(b.key()).value();host.tick(binding(changed,"producer-a"));
        var head=rows.get(b.key()).value();assertThat(head.snapshot().rule()).isEqualTo(before.snapshot().rule());
        assertThat(head.snapshot().quality()).isEqualTo(before.snapshot().quality());
        assertThat(head.episode().value().endReason()).isNull();assertThat(head.episode().value().severity()).isEqualTo(AlarmSeverity.CRITICAL);
        assertThat(head.snapshot().lastSequence()).isEqualTo(before.snapshot().lastSequence());
        assertThat(head.snapshot().lastObservedAt()).isEqualTo(before.snapshot().lastObservedAt());
        assertThat(commits.getLast().state().expectedRuleRevision()).isEqualTo(changed.header().revision());
        assertThat(commits.getLast().state().transitions()).extracting(v->v.value().kind()).containsExactly(AlertUpdate.Kind.SEVERITY_CHANGED);
    }
    @Test void anAlreadyDisabledHeadCanStillAcquireItsFinalResourceRetirementTime() {
        var b=binding(definition("pipeline.failed"),"producer-a");condition(b,1,0,AlertSignal.Condition.BAD);
        host.end(b.key(),AlertSnapshot.EndReason.DISABLED,()->{});now.set(T.plusSeconds(1));
        host.end(b.key(),AlertSnapshot.EndReason.RESOURCE_REMOVED,()->{});
        var head=rows.get(b.key()).value();assertThat(head.retiredAt()).isEqualTo(now.get());assertThat(head.cleanupAt()).isNotNull();
        assertThat(head.episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.DISABLED);
    }
    @Test void ignoredCounterReplayDoesNotEraseTheLastAcceptedPredecessor() {
        var descriptor=new AlarmTypeCatalog.Type("test.rate",1,AlarmTypeCatalog.Kind.STATEFUL,Set.of("pipeline"),"test.provider",
                "threshold",List.of(),AlarmSeverity.MAJOR,true,"test.condition","test.recovery","test.guidance");
        var catalog=new AlarmTypeCatalog(List.of(descriptor));var original=definition("pipeline.failed");
        var metric=new MetricAlertEvaluator.Rule("test.quantity",io.tapstate.core.lifecycle.MetricType.COUNTER,"1",Map.of(),
                MetricAlertEvaluator.Direction.ABOVE,java.math.BigDecimal.TEN,java.math.BigDecimal.ONE);
        var rule=new AlertConfiguration.Rule(original.header(),"Rate","test.rate",1,original.resources(),Map.of(),null,true,metric);
        var evaluator=new AlertEvaluationHost(store,catalog,AlertChannelPolicy.Deployment.ON_PREM,clock,"owner-a",()->{});
        var b=binding(rule,"counter-a");evaluator.accept(b,1,counter(T,100));
        assertThat(evaluator.accept(b,0,counter(T,100))).isEqualTo(AlertEvaluationHost.Result.IGNORED);
        now.set(T.plusSeconds(1));evaluator.accept(b,2,counter(now.get(),200));
        assertThat(rows.get(b.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
    }
    @Test void aDisabledHeadIsRetiredWhenItsOriginalRuleIsLaterDeleted() {
        var b=binding(definition("pipeline.failed"),"producer-a");condition(b,1,0,AlertSignal.Condition.BAD);
        host.end(b.key(),AlertSnapshot.EndReason.DISABLED,()->{});var episode=rows.get(b.key()).value().episode().value();
        now.set(T.plusSeconds(1));host.reconcileRules(List.of(),List.copyOf(rows.values()),()->{});
        var after=rows.get(b.key()).value();assertThat(after.retiredAt()).isEqualTo(now.get());
        assertThat(after.episode().value()).isEqualTo(episode);assertThat(after.cleanupAt()).isNotNull();
    }
    @Test void sourceBindingChangesReplaceEvaluationWithoutRetargetingTheOldEpisode() {
        var original=definition("pipeline.failed");var resource=new AlarmKey.Resource("source","orders","original-source");
        var rule=new AlertConfiguration.Rule(original.header(),"Source","source.connection-failed",1,
                new AlertConfiguration.Resources(Set.of("source"),true,Set.of()),Map.of(),null,true,null).validated(BuiltinAlarmTypes.catalog());
        var first=new AlertEvaluationHost.Binding(rule,resource,"source-producer",1,()->{});
        condition(first,1,0,AlertSignal.Condition.BAD);
        var changed=new AlertEvaluationHost.Binding(rule,resource,"source-producer",2,()->{});
        condition(changed,2,1,AlertSignal.Condition.BAD);
        var head=rows.get(first.key()).value();assertThat(head.snapshot().rule().generation()).isEqualTo(2);
        assertThat(commits.get(1).state().after().episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.SUPERSEDED);
        assertThat(head.binding().sourceBindingRevision()).isEqualTo(2);
    }
}
