package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.spi.store.AlertStore;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** Driver-free evaluation commands over the original CONFIG store. External IO precedes these commands. */
final class AlertEvaluationHost {
    enum Result { ACCEPTED, IGNORED, BUSY, CONFLICT }
    record Binding(AlertConfiguration.Rule definition, AlarmKey.Resource resource, String producer,
            long sourceBindingRevision, Runnable requireCurrent, java.util.function.Supplier<AlertStore.EvaluationProof> proof) {
        Binding(AlertConfiguration.Rule definition, AlarmKey.Resource resource, String producer,long revision,Runnable current){this(definition,resource,producer,revision,current,()->null);}
        Binding { Objects.requireNonNull(definition); Objects.requireNonNull(resource); Objects.requireNonNull(producer); Objects.requireNonNull(requireCurrent);Objects.requireNonNull(proof); }
        AlarmKey key() { return new AlarmKey(definition.header().identity().scope(), resource,
                definition.typeId(), definition.header().identity().incarnation()); }
    }
    private record Previous(AlertRule rule, MetricPoint point) { }
    private final AlertStore store;
    private AlertNotificationPlanner planner;
    AlertEvaluationHost withPlanner(AlertNotificationPlanner planner){this.planner=Objects.requireNonNull(planner);return this;}
    private AlertStore.Outcome persist(AlertStore.Commit commit){return planner==null?store.commit(commit):planner.accept(commit);}
    private final AlarmTypeCatalog catalog;
    private final AlertChannelPolicy.Deployment deployment;
    private final Clock clock;
    private final String owner;
    private final Map<AlarmKey, Previous> points = new ConcurrentHashMap<>();
    private final AlertIntent.Policy policy;
    private final AlertStore.ReadAccess read;

    AlertEvaluationHost(AlertStore store, AlarmTypeCatalog catalog, AlertChannelPolicy.Deployment deployment,
            Clock clock, String owner, Runnable active) {
        this.store = Objects.requireNonNull(store); this.catalog = Objects.requireNonNull(catalog);
        this.deployment = Objects.requireNonNull(deployment); this.clock = Objects.requireNonNull(clock);
        this.owner = Objects.requireNonNull(owner);
        this.policy = AlertIntent.Policy.capture(AlertRetentionPolicy.defaults(deployment));
        this.read = new AlertStore.ReadAccess(true, Set.of(), active);
    }

    Result accept(Binding binding, long sequence, AlertInput input) {
        binding.requireCurrent().run();
        var definition = binding.definition().validated(catalog);
        if (!(input instanceof AlertInput.Discontinuity)
                && ((definition.metric()!=null)!=(input instanceof AlertInput.Metric)
                || definition.typeId().equals("pipeline.observation-stale")!=(input instanceof AlertInput.Freshness))) {
            throw new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field","alertInput","reason","provider-type-mismatch"),null);
        }
        if (!definition.header().active() || !definition.enabled()) {
            return end(binding.key(), AlertSnapshot.EndReason.DISABLED, binding.requireCurrent());
        }
        if (!definition.matches(binding.key()) || input.observedAt() == null
                || input.observedAt().isBefore(definition.header().updatedAt())) return Result.IGNORED;
        for (int retry = 0; retry < 3; retry++) {
            binding.requireCurrent().run();
            Instant now = clock.instant();
            var retained = store.state(binding.key(), read);
            var before = retained.map(AlertStore.Stored::value).orElse(null);
            if (before != null && before.retiredAt() != null) return Result.IGNORED;
            AlertOwnership ownership = ownership(before, now);
            if (ownership == null) return Result.BUSY;
            AlertRule rule = rule(binding, before);
            if (before != null && !rule.equals(before.snapshot().rule())) {
                var replaced = AlertLifecycle.replace(before.snapshot(), rule, now);
                var after = head(binding, before, replaced, ownership, null, null);
                var outcome = commit(retained, before, after, replaced, null, binding);
                if (outcome == AlertStore.Outcome.CONFLICT) continue;
                points.remove(binding.key());
                continue;
            }
            AlertSnapshot original = before == null ? AlertSnapshot.initial(rule) : before.snapshot();
            AlertUpdate update;
            AlertSignal signal = null;
            MetricPoint point = null;
            AlarmDiagnostics.SafeTrace trace = null;
            if (input instanceof AlertInput.Discontinuity) {
                update = AlertLifecycle.discontinuity(original, now);
            } else {
                AlertSignal.Condition condition;
                Instant observed = input.observedAt();
                if (input instanceof AlertInput.Conditions c) {
                    condition = AlertConditionGroup.evaluate(c.operator(), c.values()); trace = c.trace();
                } else if (input instanceof AlertInput.Freshness f) {
                    condition = ObservationFreshness.evaluate(f.expected(), f.expectedSince(), f.lastPublication(),
                            f.observedAt(), f.startupGrace(), f.maxAge());
                } else {
                    var metric = (AlertInput.Metric) input;
                    if (definition.metric() == null) throw new IllegalArgumentException("metric input requires a metric definition");
                    Previous previous = original.lastSequence() == sequence - 1 ? points.get(binding.key()) : null;
                    var reading = ObservationMetricInput.evaluate(rule, definition.metric(),
                            new ObservationMetricInput.Publication(rule, sequence, metric.observation()),
                            new ObservationMetricInput.Budget(4096, 65536),
                            previous != null && previous.rule().equals(rule) ? previous.point() : null,
                            original.status() == AlertSnapshot.Status.FIRING, definition.effectiveSeverity(catalog), now);
                    if (reading.status() != ObservationMetricInput.Status.READY) {
                        if(reading.status()==ObservationMetricInput.Status.BINDING_MISMATCH)return Result.IGNORED;
                        update = AlertLifecycle.discontinuity(original, now);
                        var outcome = commit(retained, before, head(binding, before, update, ownership, null, null),
                                update, null, binding);
                        points.remove(binding.key());
                        if (outcome == AlertStore.Outcome.CONFLICT) continue;
                        throw new TapstateException(AlertError.INVALID_CONFIGURATION,
                                Map.of("field", "alertInput", "reason", reading.status().name()), null);
                    }
                    condition = reading.signal().condition(); observed = reading.signal().observedAt(); point = reading.point();
                }
                signal = new AlertSignal(rule.key(), rule.revision(), rule.generation(), rule.producerIncarnation(),
                        sequence, observed, condition, definition.effectiveSeverity(catalog));
                update = AlertLifecycle.evaluate(original, signal, now);
            }
            if (update.ignored()) return Result.IGNORED;
            var after = head(binding, before, update, ownership, signal, trace);
            var outcome = commit(retained, before, after, update, signal, binding);
            if (outcome == AlertStore.Outcome.CONFLICT) continue;
            if (input instanceof AlertInput.Discontinuity) points.remove(binding.key());
            if (point != null && point.observedAt().equals(after.snapshot().lastObservedAt())) {
                synchronized(points){
                    if (!points.containsKey(binding.key()) && points.size() >= 8192) throw new TapstateException(AlertError.INVALID_CONFIGURATION,
                            Map.of("field","metricPredecessors","reason","capacity"),null);
                    points.put(binding.key(), new Previous(rule, point));
                }
            }
            return Result.ACCEPTED;
        }
        return Result.CONFLICT;
    }

    /** Only a complete, validated CONFIG snapshot may terminate definitions no longer enabled. */
    void reconcileRules(List<AlertConfiguration.Rule> definitions, List<AlertStore.Stored<AlertStateRecord.Head>> retained, Runnable current) {
        var byIdentity = new HashMap<AlertConfiguration.Identity, AlertConfiguration.Rule>();
        definitions.forEach(r -> byIdentity.put(r.header().identity(), r));
        for (var row : retained) {
            var head = row.value();
            if(head.binding()==null)throw new TapstateException(io.tapstate.spi.store.IoError.DOCUMENT_UNREADABLE,
                    Map.of("id","alert-state","field","missing-original-binding"),null);
            if(head.retiredAt()!=null)continue;
            var rule = byIdentity.get(head.binding().ruleIdentity());
            if(!head.snapshot().enabled()&&rule!=null&&rule.header().active())continue;
            if (rule == null || !rule.matches(head.snapshot().rule().key())) {
                requireResult(end(head.snapshot().rule().key(), AlertSnapshot.EndReason.DISABLED, current,rule==null||!rule.header().active()));
            }
        }
    }

    Result tick(Binding binding) {
        for (int retry = 0; retry < 3; retry++) {
            binding.requireCurrent().run();
            var retained = store.state(binding.key(), read);
            if (retained.isEmpty()) return Result.IGNORED;
            var before = retained.orElseThrow().value();
            if (before.retiredAt() != null) return Result.IGNORED;
            if(!binding.definition().header().active()||!binding.definition().enabled())return end(binding.key(),AlertSnapshot.EndReason.DISABLED,binding.requireCurrent());
            Instant now = clock.instant();
            var ownership = ownership(before, now);
            if (ownership == null) return Result.BUSY;
            var currentRule=rule(binding,before);
            if(!currentRule.equals(before.snapshot().rule())){
                var replacement=AlertLifecycle.replace(before.snapshot(),currentRule,now);
                var outcome=commit(retained,before,head(binding,before,replacement,ownership,null,null),replacement,null,binding);
                if(outcome==AlertStore.Outcome.CONFLICT)continue;
                points.remove(binding.key());return Result.ACCEPTED;
            }
            var update = AlertLifecycle.tick(before.snapshot(), now);
            if (update.snapshot().equals(before.snapshot())) return Result.IGNORED;
            var outcome = commit(retained, before, head(binding, before, update, ownership, null, null),
                    update, null, binding);
            if (outcome != AlertStore.Outcome.CONFLICT) return Result.ACCEPTED;
        }
        return Result.CONFLICT;
    }

    Result end(AlarmKey key, AlertSnapshot.EndReason reason, Runnable requireCurrent) {
        return end(key,reason,requireCurrent,reason==AlertSnapshot.EndReason.RESOURCE_REMOVED);
    }
    private Result end(AlarmKey key, AlertSnapshot.EndReason reason, Runnable requireCurrent,boolean retire) {
        for (int retry = 0; retry < 3; retry++) {
            requireCurrent.run();
            var retained = store.state(key, read);
            if (retained.isEmpty()) return Result.IGNORED;
            var before = retained.orElseThrow().value();
            if (before.retiredAt()!=null||!before.snapshot().enabled()&&!retire) return Result.IGNORED;
            var ownership = ownership(before, clock.instant());
            if (ownership == null) return Result.BUSY;
            var update = AlertLifecycle.terminate(before.snapshot(), reason, clock.instant());
            var episode = before.episode() == null ? null : new AlertStateRecord.Episode(before.episode().originalBinding(),
                    update.snapshot().episode(), before.episode().policy(), before.episode().diagnostics(), before.episode().summariesHandedOff());
            var after = new AlertStateRecord.Head(update.snapshot(), deployment, ownership,
                    retire ? clock.instant() : before.retiredAt(),
                    before.summariesHandedOff(), episode, before.binding());
            var changes = transitions(update, episode, policy);
            requireCurrent.run();
            var outcome = persist(new AlertStore.Commit(AlertStore.CommitKind.END,
                    new AlertStore.StateChange(retained.orElseThrow().revision(), before, after, changes, null), List.of(), List.of()));
            requireCurrent.run();
            if (outcome != AlertStore.Outcome.CONFLICT) { points.remove(key); return Result.ACCEPTED; }
        }
        return Result.CONFLICT;
    }
    static void requireResult(Result outcome){
        if(outcome==Result.CONFLICT||outcome==Result.BUSY)throw new TapstateException(AlertError.COMMAND_CONFLICT,Map.of(),null);
    }

    private AlertRule rule(Binding b, AlertStateRecord.Head before) {
        var r = b.definition();
        long generation = before == null ? 1 : before.snapshot().rule().generation();
        if (before != null && (!before.snapshot().rule().producerIncarnation().equals(b.producer())
                || before.snapshot().rule().revision() != r.header().revision() || !before.snapshot().enabled()
                || !before.binding().ruleIdentity().equals(r.header().identity())
                || before.binding().sourceBindingRevision()!=b.sourceBindingRevision())) generation = Math.incrementExact(generation);
        return new AlertRule(b.key(), r.header().revision(), generation, b.producer(),
                seconds(r, "trigger-for", 0), seconds(r, "resolve-for", 0), Duration.ofSeconds(15), Duration.ofSeconds(5));
    }
    static Duration seconds(AlertConfiguration.Rule r, String name, int fallback) {
        return Duration.ofSeconds(r.parameters().getOrDefault(name, java.math.BigDecimal.valueOf(fallback)).longValueExact());
    }
    private AlertOwnership ownership(AlertStateRecord.Head before, Instant now) {
        var previous = before == null ? AlertOwnership.unclaimed() : before.owner();
        if (previous.activeAt(now) && !owner.equals(previous.ownerId())) return null;
        long generation = previous.activeAt(now) ? previous.generation() : Math.incrementExact(previous.generation());
        Instant until = now.plusSeconds(30);
        if (previous.activeAt(now) && previous.leaseUntil().isAfter(until)) until = previous.leaseUntil();
        return new AlertOwnership(generation, owner, until);
    }
    private AlertStateRecord.Head head(Binding b, AlertStateRecord.Head before, AlertUpdate update,
            AlertOwnership ownership, AlertSignal signal, AlarmDiagnostics.SafeTrace trace) {
        var value = update.snapshot().episode();
        AlertStateRecord.Episode episode = null;
        if (value != null) {
            boolean same = before != null && before.episode() != null && before.episode().value().number() == value.number();
            var original = same ? before.episode().originalBinding() : update.snapshot().rule();
            var diagnostics = same ? before.episode().diagnostics() : AlertStateRecord.Diagnostics.empty(b.definition().typeId().equals("pipeline.failed"));
            if (trace != null && signal != null) {
                var captured = AlarmDiagnostics.attach(update.snapshot(), signal, trace, diagnostics.history());
                if (captured.isPresent()) diagnostics = AlertStateRecord.Diagnostics.captured(captured.orElseThrow());
            }
            episode = new AlertStateRecord.Episode(original, value, same ? before.episode().policy() : policy,
                    diagnostics, same && before.episode().summariesHandedOff());
        }
        var member = new AlertIntent.Member(update.snapshot().rule(), update.snapshot().episodeSequence(),
                b.definition().header().identity(), b.sourceBindingRevision());
        return new AlertStateRecord.Head(update.snapshot(), deployment, ownership, before == null ? null : before.retiredAt(),
                before != null && before.summariesHandedOff(), episode, member);
    }
    private AlertStore.Outcome commit(Optional<AlertStore.Stored<AlertStateRecord.Head>> retained,
            AlertStateRecord.Head before, AlertStateRecord.Head after, AlertUpdate update, AlertSignal signal, Binding binding) {
        binding.requireCurrent().run();
        var proof=binding.proof().get();if(proof!=null)proof.requireCurrent().run();
        var result = persist(new AlertStore.Commit(AlertStore.CommitKind.STATE_AND_PLAN,
                new AlertStore.StateChange(retained.map(AlertStore.Stored::revision).orElse(0L), before, after,
                        transitions(update, after.episode(), policy), signal,proof), List.of(), List.of()));
        if(proof!=null)proof.requireCurrent().run();binding.requireCurrent().run(); return result;
    }
    private static List<AlertStateRecord.Transition> transitions(AlertUpdate update,
            AlertStateRecord.Episode episode, AlertIntent.Policy fallback) {
        return update.transitions().stream().map(t -> {
            boolean same=episode!=null&&t.episode()==episode.value().number();
            return new AlertStateRecord.Transition(t,
                t.kind() == AlertUpdate.Kind.ADMINISTRATIVELY_ENDED || t.kind() == AlertUpdate.Kind.RESOLVED
                        ? Objects.requireNonNull(episode).value().endReason() : null,
                same ? episode.diagnostics().status() : AlertStateRecord.DiagnosticStatus.NOT_APPLICABLE,
                same ? episode.diagnostics().latest() : null, same ? episode.policy() : fallback);
        }).toList();
    }
}
