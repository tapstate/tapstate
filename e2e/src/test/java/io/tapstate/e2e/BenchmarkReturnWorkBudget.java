package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Source-level work facts; an independent verifier decides eligibility from source and fresh receipts. */
final class BenchmarkReturnWorkBudget {
    static final int MAX_EVIDENCE_BYTES = 128 * 1024;
    private static final int MAX_PENDING = 128;
    private static final AtomicReference<Scope> ACTIVE = new AtomicReference<>();

    enum Activity {
        INSTALL_BINDINGS, INPUT_FINGERPRINT, RUNTIME_READ, REGISTRATION_READ, NATIVE_METADATA,
        CLOCK_REQUEST, CONTROL_RPC, SUMMARY_RPC, PAGE_RPC, RESOURCE_OPEN, RESOURCE_READ,
        RESOURCE_CHECKPOINT, RESOURCE_FINAL, PROCESS_INFO, CPU_FALLBACK, HEAP_READ, GC_ROSTER,
        GC_READ, RSS_PROCESS, COMMAND_CLIENT, COMMAND_READ, HELLO_CLIENT, HELLO_READ, PING_REQUEST,
        TARGET_STREAM, TABLE_STREAM, STREAM_READ, STREAM_ACCEPT, STREAM_BARRIER, ACK_POLL,
        NATIVE_STATUS_POLL, DOCUMENT_READ, SHARED_SUPPORT, DECODE, FRAME_VISIT, ROW_VISIT,
        RAW_RECHECK, COPY, BASE64, ASSOCIATION, RETURN_MAP, SOURCE_MAP, CALL_POINT_CHECK,
        SORT, SPAN, COMMON_WINDOW, RESOURCE_BOUNDS, ROOT_CPU, CLEANUP, FAILURE_RETENTION,
        BOOKKEEPING, REPORT_SERIALIZE, REPORT_WRITE, REPORT_CLOSE, PRODUCER_TRANSPORT, PRODUCER_FINAL_SEAL
    }

    record Work(long calls, long requests, long records, long frames, long bytes, long passes, long elements, long updates) {
        Work {
            for (long value : new long[]{calls, requests, records, frames, bytes, passes, elements, updates}) {
                if (value < 0) { throw new AssertionError("work quantities must be nonnegative"); }
            }
        }
        static Work zero() { return new Work(0, 0, 0, 0, 0, 0, 0, 0); }
        static Work call() { return new Work(1, 0, 0, 0, 0, 0, 0, 0); }
        static Work request() { return new Work(1, 1, 0, 0, 0, 0, 0, 0); }
        static Work records(long count) { return new Work(1, 0, count, 0, 0, 1, count, 0); }
        static Work bytes(long count) { return new Work(0, 0, 0, 0, count, 0, 0, 0); }
        static Work pass(long elements) { return new Work(1, 0, 0, 0, 0, 1, elements, 0); }
        Work plus(Work other) {
            return new Work(Math.addExact(calls, other.calls), Math.addExact(requests, other.requests),
                    Math.addExact(records, other.records), Math.addExact(frames, other.frames), Math.addExact(bytes, other.bytes),
                    Math.addExact(passes, other.passes), Math.addExact(elements, other.elements), Math.addExact(updates, other.updates));
        }
        Work times(long count) {
            return new Work(Math.multiplyExact(calls, count), Math.multiplyExact(requests, count),
                    Math.multiplyExact(records, count), Math.multiplyExact(frames, count), Math.multiplyExact(bytes, count),
                    Math.multiplyExact(passes, count), Math.multiplyExact(elements, count), Math.multiplyExact(updates, count));
        }
        boolean within(Work upper) {
            return calls <= upper.calls && requests <= upper.requests && records <= upper.records && frames <= upper.frames
                    && bytes <= upper.bytes && passes <= upper.passes && elements <= upper.elements && updates <= upper.updates;
        }
        Map<String, Object> evidence() {
            return Map.of("calls", calls, "requests", requests, "records", records, "frames", frames,
                    "bytes", bytes, "passes", passes, "elements", elements, "updates", updates);
        }
    }

    record Definition(String definitionId, String sourceFormulaSha256, Map<Activity, Work> priorInputUpper,
                      Map<Activity, Work> perOperationUpper, Set<Activity> explicitExcludedActivities) {
        Definition {
            text(definitionId, 128); digest(sourceFormulaSha256);
            priorInputUpper = Map.copyOf(priorInputUpper); perOperationUpper = Map.copyOf(perOperationUpper);
            explicitExcludedActivities = Set.copyOf(explicitExcludedActivities);
            for (Activity activity : Activity.values()) {
                int alternatives = (priorInputUpper.containsKey(activity) ? 1 : 0)
                        + (perOperationUpper.containsKey(activity) ? 1 : 0) + (explicitExcludedActivities.contains(activity) ? 1 : 0);
                if (alternatives != 1) { throw new AssertionError("each work activity needs exactly one source-bound basis"); }
            }
        }
        Definition(String id, String hash, Map<Activity, Work> prior, Set<Activity> excluded) {
            this(id, hash, prior, Map.of(), excluded);
        }
    }

    static Scope current() { return ACTIVE.get(); }
    static Scope activate(Definition definition, String runId) {
        Scope scope = new Scope(definition, runId);
        if (!ACTIVE.compareAndSet(null, scope)) { throw new AssertionError("one root work scope is already active"); }
        return scope;
    }
    static Ticket begin(Scope scope, Activity activity, Work attempted) { return scope == null ? null : scope.begin(activity, attempted); }
    static void done(Ticket ticket) { if (ticket != null) { ticket.scope.completeIfPending(ticket, Work.zero(), null); } }
    static void done(Ticket ticket, Work returned) { if (ticket != null) { ticket.scope.completeIfPending(ticket, returned, null); } }
    static void failed(Ticket ticket, Throwable failure) { if (ticket != null) { ticket.scope.completeIfPending(ticket, Work.zero(), failure); } }
    static void add(Scope scope, Activity activity, Work work) { if (scope != null) { scope.add(activity, work); } }
    static void add(Activity activity, Work work) { add(current(), activity, work); }
    static void gap(Scope scope, Activity activity, String reason) { if (scope != null) { scope.gap(activity, reason); } }
    static void returned(Ticket ticket, Object material) {
        if (ticket != null) { ticket.scope.complete(ticket, material(ticket.scope, ticket.activity, material), null); }
    }
    static Work material(Scope scope, Activity activity, Object value) {
        if (scope == null) { return Work.zero(); }
        try {
            long[] totals = new long[2]; material(value, 0, totals);
            return new Work(0, 0, 0, 0, totals[0], 0, totals[1], 0);
        } catch (RuntimeException | Error unavailable) {
            scope.gap(activity, "RETURNED_MATERIAL_BOUND_UNAVAILABLE"); return Work.zero();
        }
    }
    private static void material(Object value, int depth, long[] totals) {
        if (depth > 16 || totals[1] >= 4096) { throw new AssertionError("returned material shape exceeds the finite observer bound"); }
        totals[1] = Math.incrementExact(totals[1]);
        if (value == null) { return; }
        if (value instanceof byte[] bytes) { totals[0] = Math.addExact(totals[0], bytes.length); return; }
        if (value instanceof String text) { totals[0] = Math.addExact(totals[0], Math.multiplyExact(4L, text.length())); return; }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double || value instanceof Boolean || value instanceof java.time.Instant
                || value instanceof org.bson.types.ObjectId || value instanceof org.bson.BsonTimestamp
                || value instanceof org.bson.BsonObjectId || value instanceof org.bson.BsonInt32
                || value instanceof org.bson.BsonInt64 || value instanceof org.bson.BsonBoolean) {
            totals[0] = Math.addExact(totals[0], 128); return;
        }
        if (value instanceof javax.management.Attribute attribute) {
            material(attribute.getName(), depth + 1, totals); material(attribute.getValue(), depth + 1, totals); return;
        }
        if (value instanceof Map<?, ?> map) {
            if (map.size() > 4096 - totals[1]) { throw new AssertionError("returned map exceeds its observer shape bound"); }
            for (var entry : map.entrySet()) { material(entry.getKey(), depth + 1, totals); material(entry.getValue(), depth + 1, totals); }
            return;
        }
        if (value instanceof java.util.Collection<?> values) {
            if (values.size() > 4096 - totals[1]) { throw new AssertionError("returned collection exceeds its observer shape bound"); }
            for (Object item : values) { material(item, depth + 1, totals); } return;
        }
        if (value instanceof Object[] values) {
            if (values.length > 4096 - totals[1]) { throw new AssertionError("returned array exceeds its observer shape bound"); }
            for (Object item : values) { material(item, depth + 1, totals); } return;
        }
        throw new AssertionError("returned material has no source-level byte formula");
    }

    @FunctionalInterface interface Action<T> { T run(); }
    static <T> T request(Scope scope, Activity activity, Action<T> action) {
        if (scope == null) { return action.run(); }
        Ticket ticket = scope.begin(activity, Work.request());
        try { T result = action.run(); scope.complete(ticket, material(scope, activity, result), null); return result; }
        catch (RuntimeException | Error failure) { scope.complete(ticket, Work.zero(), failure); throw failure; }
    }
    static <T> T request(Activity activity, Action<T> action) { return request(current(), activity, action); }
    static <T> T opaqueRequest(Scope scope, Activity activity, Action<T> action) {
        if (scope == null) { return action.run(); }
        Ticket ticket = scope.begin(activity, Work.request());
        try { T value = action.run(); scope.complete(ticket, Work.zero(), null); return value; }
        catch (RuntimeException | Error failure) { scope.complete(ticket, Work.zero(), failure); throw failure; }
    }

    static final class Ticket implements AutoCloseable {
        private final Scope scope;
        private final Activity activity;
        private final long ordinal;
        private boolean completed;
        private Ticket(Scope scope, Activity activity, long ordinal) { this.scope = scope; this.activity = activity; this.ordinal = ordinal; }
        @Override public void close() { scope.completeIfPending(this, Work.zero(), null); }
        boolean completed() { return completed; }
    }

    static final class PreparedOutput {
        private final Scope scope;
        private final String kind;
        private final byte[] bytes;
        private final long independentUpperBytes;
        private final String sha;
        private boolean attempted;
        private PreparedOutput(Scope scope, String kind, byte[] bytes, long upper) {
            this.scope = scope; this.kind = kind; this.bytes = bytes; independentUpperBytes = upper;
            synchronized (scope) { scope.outputHashBytes = Math.addExact(scope.outputHashBytes, bytes.length); }
            scope.add(Activity.INPUT_FINGERPRINT, new Work(1, 1, 0, 0, bytes.length, 1, bytes.length, 0));
            sha = hash(bytes);
        }
        byte[] bytes() {
            synchronized (scope) { scope.outputCloneBytes = Math.addExact(scope.outputCloneBytes, bytes.length); }
            scope.add(Activity.COPY, Work.bytes(bytes.length)); return bytes.clone();
        }
        long actualBytes() { return bytes.length; }
        long independentUpperBytes() { return independentUpperBytes; }
        String sha256() { return sha; }
    }
    @FunctionalInterface interface OutputWriter { void writeAndClose(byte[] bytes) throws Exception; }
    record TreePlan(long nodes, long stringCharacters, long encodedUpperBytes, int depth) {
        TreePlan {
            if (nodes <= 0 || stringCharacters < 0 || encodedUpperBytes <= 0 || depth < 1 || depth > 32) {
                throw new AssertionError("an independent bounded output tree plan is required");
            }
        }
    }

    static final class Scope implements AutoCloseable {
        private static final class Row {
            Work actual = Work.zero(); long attempts, completed, failures;
        }
        private final Definition definition;
        private final String runId;
        private final EnumMap<Activity, Row> rows = new EnumMap<>(Activity.class);
        private final EnumMap<Activity, String> gaps = new EnumMap<>(Activity.class);
        private final Map<Long, Ticket> pending = new LinkedHashMap<>();
        private long ordinal, pendingCount, omitted;
        private Long pid, start;
        private String unknown;
        private Throwable primary;
        private PreparedOutput pendingOutput;
        private Map<String, Object> output = Map.of("state", "NOT_PREPARED");
        private boolean sealed, released;
        private long bookkeepingUpdates;
        private long snapshotRequests, snapshotRowVisits, outputTreeNodeVisits, outputCloneBytes, outputHashBytes;
        private Scope(Definition definition, String runId) {
            this.definition = Objects.requireNonNull(definition); text(runId, 128); this.runId = runId;
            for (Activity activity : Activity.values()) { rows.put(activity, new Row()); }
        }
        synchronized void bindRoot(long pid, long start) {
            if (pid <= 0 || start <= 0 || this.pid != null && (this.pid != pid || this.start != start)) { refuse("RUNTIME_BINDING", null); return; }
            this.pid = pid; this.start = start;
        }
        synchronized Ticket begin(Activity activity, Work work) {
            Objects.requireNonNull(activity); Objects.requireNonNull(work);
            if (sealed || released) { refuse("WORK_AFTER_SEAL", null); }
            Row row = rows.get(activity);
            try { bookkeepingUpdates = Math.incrementExact(bookkeepingUpdates); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
            try { row.attempts = Math.incrementExact(row.attempts); ordinal = Math.incrementExact(ordinal); pendingCount = Math.incrementExact(pendingCount); }
            catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
            Ticket ticket = new Ticket(this, activity, ordinal);
            if (pending.size() < MAX_PENDING) { pending.put(ordinal, ticket); }
            else { try { omitted = Math.incrementExact(omitted); } catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); } refuse("OMITTED_OPERATION", null); }
            accumulate(activity, work); return ticket;
        }
        synchronized void complete(Ticket ticket, Work work, Throwable failure) {
            if (ticket == null || ticket.scope != this || ticket.completed) { refuse("OPERATION_IDENTITY", failure); return; }
            ticket.completed = true; pending.remove(ticket.ordinal);
            try { bookkeepingUpdates = Math.incrementExact(bookkeepingUpdates); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
            Row row = rows.get(ticket.activity);
            try { pendingCount = Math.decrementExact(pendingCount); row.completed = Math.incrementExact(row.completed); }
            catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
            accumulate(ticket.activity, work);
            if (failure != null) { try { row.failures = Math.incrementExact(row.failures); } catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); } refuse("FAILED_OPERATION", failure); }
            verify(ticket.activity);
        }
        private synchronized void completeIfPending(Ticket ticket, Work work, Throwable failure) {
            if (!ticket.completed) { complete(ticket, work, failure); }
        }
        synchronized void add(Activity activity, Work work) { Ticket ticket = begin(activity, work); complete(ticket, Work.zero(), null); }
        synchronized void gap(Activity activity, String reason) { text(reason, 128); gaps.putIfAbsent(activity, reason); refuse("UNACCOUNTED_ACTIVITY", null); }
        private void accumulate(Activity activity, Work work) {
            try { rows.get(activity).actual = rows.get(activity).actual.plus(work); }
            catch (ArithmeticException overflow) { refuse("WORK_OVERFLOW", overflow); }
            if (definition.explicitExcludedActivities().contains(activity)) { refuse("EXCLUDED_ACTIVITY_USED", null); }
            Work upper = definition.priorInputUpper().get(activity);
            if (upper != null && !rows.get(activity).actual.within(upper)) { refuse("PRIOR_INPUT_BUDGET_EXCEEDED", null); }
        }
        private void verify(Activity activity) {
            Work perOperation = definition.perOperationUpper().get(activity); Row row = rows.get(activity);
            if (perOperation != null && row.attempts == row.completed) {
                try { if (!row.actual.within(perOperation.times(row.completed))) { refuse("SOURCE_OPERATION_BOUND_EXCEEDED", null); } }
                catch (ArithmeticException overflow) { refuse("UPPER_OVERFLOW", overflow); }
            }
        }
        private void refuse(String reason, Throwable failure) { if (unknown == null) { unknown = reason; } if (primary == null && failure != null) { primary = failure; } }
        Throwable primaryCause() { synchronized (this) { return primary; } }
        synchronized void failed(Throwable failure) { refuse("RUN_FAILED", Objects.requireNonNull(failure)); }

        PreparedOutput prepareOutput(String kind, long independentUpperBytes, Supplier<String> serializer) {
            gap(Activity.REPORT_SERIALIZE, "PRIOR_SERIALIZER_TREE_PROOF_REQUIRED");
            return prepareOutputUnchecked(kind, independentUpperBytes, serializer);
        }
        PreparedOutput prepareOutput(String kind, TreePlan plan, Object boundedTree) {
            Objects.requireNonNull(plan);
            long[] observed = new long[3];
            try { checkTree(boundedTree, 0, plan, observed); }
            catch (RuntimeException | Error failure) { failed(failure); throw failure; }
            synchronized (this) { outputTreeNodeVisits = Math.addExact(outputTreeNodeVisits, observed[0]); }
            add(Activity.BOOKKEEPING, new Work(1, 0, 0, 0, 0, 1, observed[0], observed[0]));
            return prepareOutputUnchecked(kind, plan.encodedUpperBytes(), () -> JsonWriter.write(boundedTree));
        }
        private void checkTree(Object value, int depth, TreePlan plan, long[] observed) {
            if (depth > plan.depth() || (observed[0] = Math.incrementExact(observed[0])) > plan.nodes()) {
                throw new AssertionError("report tree exceeds its independent node or depth plan");
            }
            long local = 0;
            if (value == null || value instanceof Boolean) { local = 5; }
            else if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                    || value instanceof Float || value instanceof Double) {
                if (value instanceof Float single && !Float.isFinite(single)
                        || value instanceof Double decimal && !Double.isFinite(decimal)) { throw new AssertionError("report number is nonfinite"); }
                local = 32;
            }
            else if (value instanceof String text) {
                observed[1] = Math.addExact(observed[1], text.length());
                if (observed[1] > plan.stringCharacters()) { throw new AssertionError("report strings exceed their independent plan"); }
                local = Math.addExact(2, Math.multiplyExact(6L, text.length()));
            } else if (value instanceof Map<?, ?> map) {
                if (map.size() > plan.nodes() - observed[0]) { throw new AssertionError("report map exceeds its prior tree plan"); }
                local = Math.addExact(2, Math.multiplyExact(2L, map.size()));
                for (var entry : map.entrySet()) {
                    if (!(entry.getKey() instanceof String)) { throw new AssertionError("report map key is not bounded text"); }
                    checkTree(entry.getKey(), depth + 1, plan, observed); checkTree(entry.getValue(), depth + 1, plan, observed);
                }
            } else if (value instanceof java.util.Collection<?> list) {
                if (list.size() > plan.nodes() - observed[0]) { throw new AssertionError("report list exceeds its prior tree plan"); }
                local = Math.addExact(2, list.size());
                for (Object item : list) { checkTree(item, depth + 1, plan, observed); }
            } else { throw new AssertionError("report value has no bounded serialization formula"); }
            observed[2] = Math.addExact(observed[2], local);
            if (observed[2] > plan.encodedUpperBytes()) { throw new AssertionError("report syntax exceeds its independent encoded plan"); }
        }
        private PreparedOutput prepareOutputUnchecked(String kind, long independentUpperBytes, Supplier<String> serializer) {
            text(kind, 64);
            synchronized (this) {
                if (pendingOutput != null || sealed || independentUpperBytes <= 0) { refuse("OUTPUT_PLAN", null); throw new AssertionError("one positive independent output plan is required"); }
            }
            Ticket ticket = begin(Activity.REPORT_SERIALIZE, Work.call());
            try {
                String text = serializer.get(); byte[] encoded = text.getBytes(StandardCharsets.UTF_8);
                complete(ticket, Work.bytes(encoded.length), null);
                PreparedOutput prepared = new PreparedOutput(this, kind, encoded, independentUpperBytes);
                synchronized (this) {
                    if (encoded.length > independentUpperBytes) { refuse("OUTPUT_INPUT_BOUND_EXCEEDED", null); }
                    pendingOutput = prepared; output = Map.of("state", "OUTPUT_PENDING", "kind", kind,
                            "independentUpperBytes", independentUpperBytes, "actualBytes", encoded.length, "sha256", prepared.sha256());
                }
                return prepared;
            } catch (RuntimeException | Error failure) { if (!ticket.completed) { complete(ticket, Work.zero(), failure); } throw failure; }
        }
        void writeOutput(PreparedOutput prepared, OutputWriter writer) throws Exception {
            synchronized (this) {
                if (prepared == null || prepared.scope != this || prepared != pendingOutput || prepared.attempted) {
                    refuse("OUTPUT_IDENTITY", null); throw new AssertionError("actual output has another or repeated plan");
                }
                prepared.attempted = true;
            }
            Ticket write = begin(Activity.REPORT_WRITE, Work.request());
            try {
                writer.writeAndClose(prepared.bytes());
                complete(write, Work.bytes(prepared.bytes.length), null);
                add(Activity.REPORT_CLOSE, Work.call());
                synchronized (this) {
                    pendingOutput = null; output = Map.of("state", "WRITE_AND_CLOSE_COMPLETED", "kind", prepared.kind,
                            "independentUpperBytes", prepared.independentUpperBytes, "actualBytes", prepared.bytes.length, "sha256", prepared.sha256());
                }
            } catch (Exception | Error failure) {
                completeIfPending(write, Work.zero(), failure); gap(Activity.REPORT_CLOSE, "FAILED_OUTPUT_CLOSE_NOT_OBSERVED"); throw failure;
            }
        }
        synchronized Map<String, Object> finish() {
            if (pid == null) { refuse("RUNTIME_UNBOUND", null); }
            if (pendingCount != 0 || pendingOutput != null) { refuse("PENDING_OPERATION_OR_OUTPUT", null); }
            if (!"WRITE_AND_CLOSE_COMPLETED".equals(output.get("state"))) { refuse("OUTPUT_NOT_COMPLETED", null); }
            for (Activity activity : Activity.values()) { verify(activity); }
            sealed = true; return snapshot();
        }
        synchronized Map<String, Object> snapshot() {
            try { snapshotRequests = Math.incrementExact(snapshotRequests); snapshotRowVisits = Math.addExact(snapshotRowVisits, rows.size()); }
            catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
            var out = new LinkedHashMap<String, Object>(); out.put("schemaVersion", 1); out.put("definitionId", definition.definitionId());
            out.put("sourceFormulaSha256", definition.sourceFormulaSha256()); out.put("runId", runId);
            out.put("state", unknown != null ? "UNKNOWN" : sealed ? "CLOSED" : pendingOutput != null ? "OUTPUT_PENDING" : "OPEN");
            out.put("rootRuntime", pid == null ? Map.of("state", "UNBOUND") : Map.of("pid", pid, "jvmStartTimeMillis", start));
            var activities = new LinkedHashMap<String, Object>();
            for (var entry : rows.entrySet()) {
                Activity activity = entry.getKey(); Row row = entry.getValue(); var facts = new LinkedHashMap<String, Object>();
                facts.put("attempts", row.attempts); facts.put("completedAttempts", row.completed); facts.put("failedAttempts", row.failures);
                facts.put("actual", row.actual.evidence());
                if (definition.priorInputUpper().containsKey(activity)) {
                    facts.put("basis", "PRIOR_INPUT_CAP"); facts.put("upper", definition.priorInputUpper().get(activity).evidence());
                } else if (definition.perOperationUpper().containsKey(activity)) {
                    facts.put("basis", "STATIC_PER_OPERATION_BOUND_TIMES_COMPLETE_ACTUAL_COUNT");
                    facts.put("perOperationUpper", definition.perOperationUpper().get(activity).evidence());
                    facts.put("actualCountComplete", row.attempts == row.completed && pendingCount == 0 && omitted == 0);
                    try { facts.put("upper", definition.perOperationUpper().get(activity).times(row.completed).evidence()); }
                    catch (ArithmeticException overflow) { facts.put("upperState", "UNKNOWN"); }
                } else { facts.put("basis", "EXPLICIT_EXCLUDED_BRANCH"); }
                if (gaps.containsKey(activity)) { facts.put("gap", gaps.get(activity)); }
                activities.put(activity.name(), Map.copyOf(facts));
            }
            out.put("activities", Map.copyOf(activities)); out.put("pendingOperations", pendingCount); out.put("omittedOperations", omitted);
            out.put("bookkeepingUpdates", bookkeepingUpdates);
            out.put("selfCounters", Map.of("snapshotRequests", snapshotRequests, "snapshotRowVisits", snapshotRowVisits,
                    "outputTreeNodeVisits", outputTreeNodeVisits, "outputCloneBytes", outputCloneBytes, "outputHashBytes", outputHashBytes));
            out.put("materialUnitScope", "CHECKED_SOURCE_MATERIAL_UPPER_BYTES_NOT_WIRE_CPU_OR_ELAPSED");
            out.put("pending", pending.values().stream().map(ticket -> Map.of("ordinal", ticket.ordinal, "activity", ticket.activity.name())).toList());
            out.put("output", output); if (unknown != null) { out.put("reason", unknown); }
            if (primary != null) { String type = primary.getClass().getName(); out.put("failureType", type.length() <= 128 ? type : "TYPE_NAME_EXCEEDS_BOUND"); }
            BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> out.put(flag, false));
            out.put("structuralWorkBudgetQualified", false); out.put("wholeMethodCostQualified", false); out.put("causalOverheadQualified", false);
            Map<String, Object> receipt = Collections.unmodifiableMap(out);
            if (JsonWriter.write(receipt).getBytes(StandardCharsets.UTF_8).length > MAX_EVIDENCE_BYTES) { throw new AssertionError("work receipt exceeds its fixed structural budget"); }
            return receipt;
        }
        @Override public synchronized void close() {
            if (released) { return; } released = true;
            if (!sealed) { refuse("SCOPE_RELEASED_BEFORE_FINALIZATION", null); }
            ACTIVE.compareAndSet(this, null);
        }
    }
    private static void text(String value, int max) {
        if (value == null || value.isBlank() || value.length() > max || !value.chars().allMatch(c -> c >= 32 && c <= 126)) { throw new AssertionError("work identity text exceeds its bound"); }
    }
    private static void digest(String value) { if (value == null || !value.matches("[a-f0-9]{64}")) { throw new AssertionError("work source formula fingerprint is invalid"); } }
    private static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
