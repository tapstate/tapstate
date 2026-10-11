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

    /** Bind the cached write outcome to a prior registered tree plan and the actual local output bytes. */
    static void verifyOutputFile(java.nio.file.Path path, BenchmarkReturnWorkBudget.PreparedOutput prepared,
                                 BenchmarkReturnWorkBudgetDefinitions.OutputPlan prior,
                                 Map<String, Object> receipt) throws java.io.IOException {
        require(path != null && path.isAbsolute() && path.equals(path.normalize()) && prepared != null && prior != null,
                "registered output binding is missing");
        require(prior.encodedUpperBytes() == prepared.independentUpperBytes(), "output substituted its registered prior plan");
        require(prior.sha256().equals(prepared.priorTreePlanSha256()), "output substituted its registered material plan");
        var output = object(receipt.get("output"), "output outcome");
        require("WRITE_AND_CLOSE_COMPLETED".equals(output.get("state"))
                        && number(output, "actualBytes") == prepared.actualBytes()
                        && number(output, "independentUpperBytes") == prior.encodedUpperBytes()
                        && prior.sha256().equals(output.get("priorTreePlanSha256"))
                        && prepared.sha256().equals(output.get("sha256")), "cached write outcome does not bind the prepared bytes");
        var scope = BenchmarkReturnWorkBudget.current();
        var ticket = BenchmarkReturnWorkBudget.begin(scope, BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ,
                BenchmarkReturnWorkBudget.Work.call());
        try {
            BenchmarkReturnWorkBudget.issued(ticket);
            require(!java.nio.file.Files.isSymbolicLink(path)
                            && java.nio.file.Files.isRegularFile(path, java.nio.file.LinkOption.NOFOLLOW_LINKS),
                    "actual output is missing or linked");
            require(prepared.actualBytes() <= Integer.MAX_VALUE - 1, "actual output exceeds the bounded local reader");
            byte[] actual;
            BenchmarkReturnWorkBudget.issued(ticket);
            var input = java.nio.file.Files.newInputStream(path);
            Throwable primary = null;
            try {
                BenchmarkReturnWorkBudget.issued(ticket);
                actual = input.readNBytes(Math.toIntExact(prepared.actualBytes()) + 1);
            } catch (java.io.IOException | RuntimeException | Error failure) {
                primary = failure; throw failure;
            } finally {
                BenchmarkReturnWorkBudget.issued(ticket);
                try { input.close(); }
                catch (java.io.IOException | RuntimeException | Error closeFailure) {
                    if (primary == null) { throw closeFailure; }
                    primary.addSuppressed(closeFailure);
                }
            }
            require(actual.length == prepared.actualBytes(), "actual output length differs");
            String digest;
            try {
                BenchmarkReturnWorkBudget.issued(ticket);
                digest = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(actual));
            }
            catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
            require(prepared.sha256().equals(digest), "actual output digest differs");
            BenchmarkReturnWorkBudget.done(ticket, BenchmarkReturnWorkBudget.Work.bytes(actual.length));
        } catch (java.io.IOException | RuntimeException | Error failure) {
            BenchmarkReturnWorkBudget.failed(ticket, failure); throw failure;
        }
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
        verifySelf(receipt);
        var output = object(receipt.get("output"), "actual output outcome");
        require("WRITE_AND_CLOSE_COMPLETED".equals(output.get("state")), "output has not completed");
        long outputBytes = number(output, "actualBytes"), outputUpper = number(output, "independentUpperBytes");
        require(outputUpper > 0 && outputBytes <= outputUpper && output.get("sha256") instanceof String digest
                        && digest.matches("[a-f0-9]{64}") && output.get("priorTreePlanSha256") instanceof String planDigest
                        && planDigest.matches("[a-f0-9]{64}"), "output material is outside its declared bound");

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
                long issued = number(row, "issuedRequests");
                require(issued == number(row, "completedRequests") && number(row, "failedRequests") == 0
                                && measured.requests() == issued, "issued requests are pending, failed or inconsistent");
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

    private static void verifySelf(Map<String, Object> receipt) {
        require(Boolean.FALSE.equals(receipt.get("selfOverflow")) && !receipt.containsKey("selfOverflowField")
                        && !receipt.containsKey("selfSourceUpperState")
                        && "SOURCE_ENTRY_VISITS_VECTOR_PASSES_FIXED_RECEIPT_COPY_AND_PRIVATE_COUNTER_TAIL_V1"
                        .equals(receipt.get("selfAccountingRule")), "self accounting is missing or overflowed");
        var counters = object(receipt.get("selfCounters"), "self counters");
        require(counters.keySet().equals(Set.of("beginRequests", "completeRequests", "issuedEvents", "gapRequests",
                "snapshotRequests", "snapshotRowVisits", "materialNodeVisits", "outputTreeRequests", "outputTreeNodeVisits",
                "outputCloneRequests", "outputCloneBytes", "outputHashRequests", "outputHashBytes", "serializerRequests",
                "plannedEncodingUpperBytes", "bindingRequests", "failureRequests", "verificationRequests", "finishRequests", "closeRequests")),
                "self counter roster differs");
        long inventory = BenchmarkReturnWorkBudget.Activity.values().length;
        try {
            long snapshots = number(counters, "snapshotRequests"), material = number(counters, "materialNodeVisits");
            long tree = number(counters, "outputTreeNodeVisits"), clones = number(counters, "outputCloneRequests");
            long hashes = number(counters, "outputHashRequests"), serializers = number(counters, "serializerRequests");
            long nodes = add(1024, mul(128, inventory), mul(8, 128));
            long characters = add(4096, mul(1024, inventory), mul(128, 128));
            long entries = add(number(counters, "beginRequests"), number(counters, "completeRequests"),
                    number(counters, "issuedEvents"), number(counters, "gapRequests"), material, tree, snapshots,
                    clones, hashes, number(counters, "outputTreeRequests"), serializers, 2,
                    number(counters, "bindingRequests"), number(counters, "failureRequests"),
                    number(counters, "verificationRequests"), number(counters, "finishRequests"), number(counters, "closeRequests"));
            long copies = mul(snapshots, add(mul(5, inventory), 128, 12));
            long vectors = add(number(counters, "beginRequests"), number(counters, "completeRequests"),
                    number(counters, "issuedEvents"), mul(2, number(counters, "verificationRequests")));
            long requests = add(copies, clones, mul(3, hashes), mul(2, serializers), mul(64, vectors), mul(32, entries),
                    mul(16, add(material, tree)), mul(16, mul(snapshots, nodes)), 128, mul(80, inventory));
            long bytes = add(mul(snapshots, add(mul(32, nodes), mul(6, characters))), number(counters, "outputCloneBytes"),
                    number(counters, "outputHashBytes"), mul(2, number(counters, "plannedEncodingUpperBytes")));
            long elements = add(mul(16, vectors), material, tree, mul(snapshots, nodes));
            long passes = add(mul(2, vectors), copies, material, tree, clones, hashes);
            long updates = add(mul(32, entries), mul(snapshots, nodes));
            var expected = new BenchmarkReturnWorkBudget.Work(entries, requests, 0, 0, bytes, passes, elements, updates);
            require(work(receipt.get("selfSourceUpper")).equals(expected), "self upper does not reproduce its source rule");
            require(number(counters, "snapshotRowVisits") == mul(snapshots, inventory), "snapshot activity visits are incomplete");
            var release = object(receipt.get("privateReleaseTail"), "release tail");
            require("ONE_IDEMPOTENT_RELEASE_CALL_INCLUDING_ATOMIC_SCOPE_CLEAR".equals(release.get("scope"))
                            && work(release.get("upper")).equals(new BenchmarkReturnWorkBudget.Work(1, 1, 0, 0, 0, 1, 4, 4)),
                    "release source tail differs");
        } catch (ArithmeticException overflow) { throw new AssertionError("return work admission self arithmetic overflowed", overflow); }
    }

    private static long add(long... values) { long total = 0; for (long value : values) { total = Math.addExact(total, value); } return total; }
    private static long mul(long left, long right) { return Math.multiplyExact(left, right); }

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
