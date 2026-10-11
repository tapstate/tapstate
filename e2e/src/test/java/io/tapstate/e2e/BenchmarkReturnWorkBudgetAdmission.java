package io.tapstate.e2e;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

/** Recomputes root work bounds from a separately registered definition, never from receipt upper fields. */
final class BenchmarkReturnWorkBudgetAdmission {
    private static final Set<String> UNITS = Set.of("calls", "requests", "records", "frames", "bytes", "passes", "elements", "updates");

    private BenchmarkReturnWorkBudgetAdmission() { }

    record Runtime(String runId, long pid, long jvmStartTimeMillis) {
        Runtime {
            require(runId != null && !runId.isBlank() && runId.length() <= 128 && pid > 0 && jvmStartTimeMillis > 0,
                    "registered runtime is missing");
        }
    }

    record Recomputed(Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> actual,
                      Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> upper,
                      BenchmarkReturnWorkBudget.Work totalActual, BenchmarkReturnWorkBudget.Work totalUpper) {
        Recomputed { actual = Map.copyOf(actual); upper = Map.copyOf(upper); }
    }

    /** Counter closure is one proof component; source coverage, producer sealing and output binding are separate. */
    static Recomputed verifyCounters(BenchmarkReturnWorkBudget.Definition registered, Runtime runtime,
                                     Map<String, Object> receipt) {
        require(registered != null && runtime != null && receipt != null, "registered definition or actual receipt is missing");
        require(number(receipt, "schemaVersion") == 1, "counter schema differs");
        require(registered.definitionId().equals(receipt.get("definitionId"))
                        && registered.sourceFormulaSha256().equals(receipt.get("sourceFormulaSha256")),
                "source formula registration differs");
        require(runtime.runId().equals(receipt.get("runId")), "run identity differs");
        var owner = object(receipt.get("rootRuntime"), "root runtime");
        require(owner.keySet().equals(Set.of("pid", "jvmStartTimeMillis"))
                        && number(owner, "pid") == runtime.pid()
                        && number(owner, "jvmStartTimeMillis") == runtime.jvmStartTimeMillis(),
                "root runtime identity differs");
        require("CLOSED".equals(receipt.get("state")) && !receipt.containsKey("reason") && !receipt.containsKey("failureType"),
                "root collection is open or refused");
        require(number(receipt, "pendingOperations") == 0 && number(receipt, "omittedOperations") == 0
                        && receipt.get("pending") instanceof java.util.List<?> pending && pending.isEmpty(),
                "root operations are pending or omitted");
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) {
            require(Boolean.FALSE.equals(receipt.get(flag)), "counter receipt claimed a qualification");
        }
        for (String flag : Set.of("structuralWorkBudgetQualified", "wholeMethodCostQualified", "causalOverheadQualified")) {
            require(Boolean.FALSE.equals(receipt.get(flag)), "counter receipt claimed a qualification");
        }
        var output = object(receipt.get("output"), "actual output outcome");
        require("WRITE_AND_CLOSE_COMPLETED".equals(output.get("state")), "output has not completed");
        long outputBytes = number(output, "actualBytes"), outputUpper = number(output, "independentUpperBytes");
        require(outputUpper > 0 && outputBytes <= outputUpper && output.get("sha256") instanceof String digest
                        && digest.matches("[a-f0-9]{64}"), "output material is outside its declared bound");

        var rows = object(receipt.get("activities"), "activity roster");
        var expectedNames = java.util.Arrays.stream(BenchmarkReturnWorkBudget.Activity.values())
                .map(Enum::name).collect(java.util.stream.Collectors.toUnmodifiableSet());
        require(rows.keySet().equals(expectedNames), "activity roster is missing, repeated or extended");
        var actual = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        BenchmarkReturnWorkBudget.Work totalActual = BenchmarkReturnWorkBudget.Work.zero();
        BenchmarkReturnWorkBudget.Work totalUpper = BenchmarkReturnWorkBudget.Work.zero();
        try {
            for (var activity : BenchmarkReturnWorkBudget.Activity.values()) {
                var row = object(rows.get(activity.name()), "activity row");
                long attempts = number(row, "attempts"), completed = number(row, "completedAttempts");
                require(attempts == completed && number(row, "failedAttempts") == 0 && !row.containsKey("gap")
                                && !row.containsKey("upperState"), "activity attempts are incomplete");
                var measured = work(row.get("actual"));
                BenchmarkReturnWorkBudget.Work limit;
                if (registered.priorInputUpper().containsKey(activity)) {
                    require("PRIOR_INPUT_CAP".equals(row.get("basis")), "prior input basis differs");
                    limit = registered.priorInputUpper().get(activity);
                    require(work(row.get("upper")).equals(limit), "receipt substituted a prior upper bound");
                } else if (registered.perOperationUpper().containsKey(activity)) {
                    require("STATIC_PER_OPERATION_BOUND_TIMES_COMPLETE_ACTUAL_COUNT".equals(row.get("basis"))
                                    && Boolean.TRUE.equals(row.get("actualCountComplete")), "operation count basis is incomplete");
                    var perOperation = registered.perOperationUpper().get(activity);
                    require(work(row.get("perOperationUpper")).equals(perOperation), "receipt substituted a source formula");
                    limit = perOperation.times(completed);
                    require(work(row.get("upper")).equals(limit), "receipt upper does not reproduce the source formula");
                } else {
                    require(registered.explicitExcludedActivities().contains(activity)
                                    && "EXPLICIT_EXCLUDED_BRANCH".equals(row.get("basis")) && attempts == 0
                                    && measured.equals(BenchmarkReturnWorkBudget.Work.zero()), "excluded activity was used");
                    limit = BenchmarkReturnWorkBudget.Work.zero();
                }
                require(measured.within(limit), "actual work exceeds its independently recomputed upper");
                actual.put(activity, measured); upper.put(activity, limit);
                totalActual = totalActual.plus(measured); totalUpper = totalUpper.plus(limit);
            }
        } catch (ArithmeticException overflow) {
            throw new AssertionError("return work admission checked arithmetic overflowed", overflow);
        }
        return new Recomputed(actual, upper, totalActual, totalUpper);
    }

    private static BenchmarkReturnWorkBudget.Work work(Object value) {
        var data = object(value, "work vector");
        require(data.keySet().equals(UNITS), "work vector has missing or unknown units");
        return new BenchmarkReturnWorkBudget.Work(number(data, "calls"), number(data, "requests"), number(data, "records"),
                number(data, "frames"), number(data, "bytes"), number(data, "passes"), number(data, "elements"), number(data, "updates"));
    }

    private static Map<?, ?> object(Object value, String label) {
        require(value instanceof Map<?, ?>, label + " is missing");
        return (Map<?, ?>) value;
    }

    private static long number(Map<?, ?> data, String key) {
        Object value = data.get(key);
        require(value instanceof Long || value instanceof Integer, "counter is not an exact integer");
        long result = ((Number) value).longValue();
        require(result >= 0, "counter is negative");
        return result;
    }

    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return work admission " + reason); }
    }
}
