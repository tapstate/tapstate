package io.tapstate.e2e;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies and persists complete logical admission boundaries without inferring physical database commands. */
final class ExecutionAdmissionStages {
    private final BenchmarkLiveReport report;
    private final boolean enabled;
    private ExecutionAdmissionJdiSession observer;
    private ExecutionAdmissionJdiSession.Boundary previous;

    ExecutionAdmissionStages(BenchmarkLiveReport report, boolean enabled) {
        this.report = report;
        this.enabled = enabled;
    }

    void bind(ExecutionAdmissionJdiSession current) {
        observer = current;
        previous = null;
    }

    void stage(String action, long advances, long submits) throws Exception {
        if (enabled) { recordCounts(action, observer.boundary(), advances, submits); }
    }

    void shutdown(String action) throws Exception {
        if (enabled) {
            var finalCounts = observer.shutdownAndFinish();
            assertThat(finalCounts.fullyDrained()).as("%s actual VM death and disconnect", action).isTrue();
            recordCounts(action, finalCounts, 0, 0);
        }
    }

    private void recordCounts(String action, ExecutionAdmissionJdiSession.Boundary current,
            long advances, long submits) {
        assertThat(current.drained()).as("%s drained exact method boundaries", action).isTrue();
        assertThat(current.leaseObservationEnabled()).isTrue();
        assertThat(current.leasesDrained()).isTrue();
        assertThat(current.bindings()).containsOnlyKeys(ExecutionAdmissionJdiSession.Target.values());
        assertThat(current.leaseBindings()).containsOnlyKeys(ExecutionAdmissionJdiSession.LeaseTarget.values());
        assertThat(current.leaseCounts()).containsOnlyKeys(ExecutionAdmissionJdiSession.LeaseTarget.values());
        Map<String, Object> deltas = new LinkedHashMap<>();
        for (var target : ExecutionAdmissionJdiSession.Target.values()) {
            var after = current.counts().get(target);
            var before = previous == null ? new ExecutionAdmissionJdiSession.Counts(0, 0, 0, 0)
                    : previous.counts().get(target);
            long expected = target == ExecutionAdmissionJdiSession.Target.ADVANCE_STANDALONE ? advances : submits;
            assertThat(after.entries() - before.entries()).as("%s %s entries", action, target).isEqualTo(expected);
            assertThat(after.normalReturns() - before.normalReturns()).as("%s %s normal returns", action, target)
                    .isEqualTo(expected);
            assertThat(after.exceptionalExits()).isZero();
            assertThat(after.inFlight()).isZero();
            deltas.put(target.name(), Map.of("entries", after.entries() - before.entries(),
                    "normalReturns", after.normalReturns() - before.normalReturns(),
                    "exceptionalExits", after.exceptionalExits(), "inFlight", after.inFlight()));
        }
        for (var target : ExecutionAdmissionJdiSession.LeaseTarget.values()) {
            var count = current.leaseCounts().get(target);
            assertThat(count.entries()).as("%s VM-wide %s entries", action, target).isZero();
            assertThat(count.normalReturns()).isZero();
            assertThat(count.exceptionalExits()).isZero();
            assertThat(count.inFlight()).isZero();
        }
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("action", action);
        evidence.put("logicalMethodDeltas", deltas);
        evidence.put("logicalMethodTotals", countValues(current.counts()));
        evidence.put("methodBindings", bindingValues(current.bindings()));
        evidence.put("vmWideLeaseCounts", countValues(current.leaseCounts()));
        evidence.put("leaseBindings", bindingValues(current.leaseBindings()));
        evidence.put("atNanos", current.atNanos());
        evidence.put("applicationSha256", current.artifactSha256());
        evidence.put("pipelineId", current.pipelineId());
        evidence.put("events", current.events());
        evidence.put("openObservedCalls", current.openObservedCalls());
        evidence.put("eventQueueDrained", current.eventQueueDrained());
        evidence.put("drained", current.drained());
        evidence.put("leaseObservationEnabled", current.leaseObservationEnabled());
        evidence.put("leasesDrained", current.leasesDrained());
        evidence.put("ownedVmDeath", current.ownedVmDeath());
        evidence.put("ownedVmDisconnected", current.ownedVmDisconnected());
        evidence.put("physicalMongoCommandsInferred", false);
        report.addFork(evidence);
        previous = current;
    }

    private static <E extends Enum<E>> Map<String, Object> countValues(
            Map<E, ExecutionAdmissionJdiSession.Counts> counts) {
        Map<String, Object> result = new LinkedHashMap<>();
        counts.forEach((target, count) -> result.put(target.name(), Map.of(
                "entries", count.entries(), "normalReturns", count.normalReturns(),
                "exceptionalExits", count.exceptionalExits(), "inFlight", count.inFlight())));
        return result;
    }

    private static <E extends Enum<E>> Map<String, Object> bindingValues(
            Map<E, ExecutionAdmissionJdiSession.Binding> bindings) {
        Map<String, Object> result = new LinkedHashMap<>();
        bindings.forEach((target, binding) -> result.put(target.name(), Map.of(
                "type", binding.type(), "method", binding.method(), "signature", binding.signature(),
                "artifactOrigin", binding.artifactOrigin(), "methodSha256", binding.methodSha256(),
                "loaderIdentity", binding.loaderIdentity(), "loaderType", binding.loaderType(),
                "entryOffset", binding.entryOffset(), "normalReturnOffsets", binding.normalReturnOffsets())));
        return result;
    }
}
