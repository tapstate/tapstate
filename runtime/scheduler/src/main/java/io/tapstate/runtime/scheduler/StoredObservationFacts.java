package io.tapstate.runtime.scheduler;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Keeps the stored facts within their existing cardinality budgets without changing the other fields. */
final class StoredObservationFacts {
    private record Budget(PipelineIntentScope scope, CardinalityBudget.Folder folder) {}

    private final Map<String, Budget> budgets = new HashMap<>();

    Observation project(Observation original, PipelineIntentScope scope) {
        String id = original.pipelineId();
        var budget = budget(id, scope);
        synchronized (budget) {
            var facts = original.facts().stream().map(budget.folder()::fold).toList();
            synchronized (this) {
                if (budgets.get(id) != budget) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
            }
            return facts.equals(original.facts()) ? original : new Observation(id, original.state(), original.metrics(),
                    original.snapshot(), original.positions(), original.failure(), original.observedAt(), facts,
                    original.initialLoad());
        }
    }

    private synchronized Budget budget(String id, PipelineIntentScope scope) {
        Budget budget = budgets.get(id);
        if (budget != null && !Objects.equals(budget.scope(), scope)) {
            if (budget.scope() != null && scope != null
                    && budget.scope().clusterId().equals(scope.clusterId())
                    && budget.scope().bindingVersion() == scope.bindingVersion()
                    && budget.scope().cacheIncarnation().equals(scope.cacheIncarnation())
                    && budget.scope().stateGeneration() > scope.stateGeneration()) {
                throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
            }
            budget = null;
        }
        if (budget == null) {
            budget = new Budget(scope, CardinalityBudget.folder());
            budgets.put(id, budget);
        }
        return budget;
    }

    synchronized void retain(Collection<String> live) {
        budgets.keySet().retainAll(Set.copyOf(live));
    }
}
