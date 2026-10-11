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
    static final int MAX_MATERIAL_NODES = 4096;
    static final int MAX_MATERIAL_DEPTH = 16;
    static final int MAX_MATERIAL_LEAF_CHARACTERS = 65_536;
    static final long MAX_MATERIAL_BYTES = 2L * 1024 * 1024;
    static final long MAX_CONNECTOR_INPUT_BYTES = 128L * 1024 * 1024;
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
    static long rootNanoTime() {
        long value;
        try { value = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime(); }
        catch (RuntimeException | Error failure) { recordRootClockFailure(current(), failure); throw failure; }
        add(current(), Activity.CLOCK_REQUEST, Work.request());
        return value;
    }
    static java.util.function.LongSupplier rootClock() {
        Scope scope = current();
        return scope == null ? io.tapstate.adapters.pdk.PdkBenchmarkClock::nanoTime
                : () -> rootNanoTime(scope, io.tapstate.adapters.pdk.PdkBenchmarkClock::nanoTime);
    }
    /** The original raw read remains first; accounting never requests another clock point. */
    static long rootNanoTime(Scope scope, java.util.function.LongSupplier original) {
        long value;
        try { value = original.getAsLong(); }
        catch (RuntimeException | Error failure) {
            recordRootClockFailure(scope, failure);
            throw failure;
        }
        add(scope, Activity.CLOCK_REQUEST, Work.request());
        return value;
    }
    private static void recordRootClockFailure(Scope scope, Throwable failure) {
        if (scope != null) {
            Ticket ticket = scope.begin(Activity.CLOCK_REQUEST, Work.request());
            scope.complete(ticket, Work.zero(), failure);
        }
    }
    static Scope activate(Definition definition, String runId) {
        Scope scope = new Scope(definition, runId);
        if (!ACTIVE.compareAndSet(null, scope)) { throw new AssertionError("one root work scope is already active"); }
        return scope;
    }
    static Ticket begin(Scope scope, Activity activity, Work attempted) { return scope == null ? null : scope.begin(activity, attempted); }
    static void done(Ticket ticket) { if (ticket != null) { ticket.scope.completeIfPending(ticket, Work.zero(), null); } }
    static void done(Ticket ticket, Work returned) { if (ticket != null) { ticket.scope.completeIfPending(ticket, returned, null); } }
    static void failed(Ticket ticket, Throwable failure) { if (ticket != null) { ticket.scope.completeIfPending(ticket, Work.zero(), failure); } }
    /** Records issuance at the actual call site, after any local guard or setup. */
    static void issued(Ticket ticket) { if (ticket != null) { ticket.scope.issued(ticket); } }
    static void add(Scope scope, Activity activity, Work work) { if (scope != null) { scope.add(activity, work); } }
    static void add(Activity activity, Work work) { add(current(), activity, work); }
    static void gap(Scope scope, Activity activity, String reason) { if (scope != null) { scope.gap(activity, reason); } }
    static void failure(Scope scope, Throwable failure) { if (scope != null && failure != null) { scope.failed(failure); } }
    static void requireLocalMaterial(Scope scope, Activity activity, Object value) {
        if (scope == null) { return; }
        long[] totals = new long[2];
        try {
            material(scope, value, 0, totals);
            scope.add(activity, new Work(0, 0, 0, 0, totals[0], 1, totals[1], 0));
        } catch (RuntimeException | Error invalid) {
            scope.gap(activity, "LOCAL_CONSUMPTION_BOUND_UNAVAILABLE"); scope.failed(invalid); throw invalid;
        }
    }
    static void connectorBytes(Scope scope, Activity activity, byte[] value) {
        if (scope == null) { return; }
        if (value == null || value.length > MAX_CONNECTOR_INPUT_BYTES) {
            scope.gap(activity, "CONNECTOR_INPUT_PRIOR_BOUND_EXCEEDED"); throw new AssertionError("connector input exceeds its prior local bound");
        }
        scope.add(activity, new Work(0, 0, 0, 0, value.length, 1, value.length, 0));
    }
    static void returned(Ticket ticket, Object material) {
        if (ticket != null) { ticket.scope.complete(ticket, material(ticket.scope, ticket.activity, material), null); }
    }
    static Work material(Scope scope, Activity activity, Object value) {
        if (scope == null) { return Work.zero(); }
        try {
            long[] totals = new long[2]; material(scope, value, 0, totals);
            return new Work(0, 0, 0, 0, totals[0], 0, totals[1], 0);
        } catch (RuntimeException | Error unavailable) {
            scope.gap(activity, "RETURNED_MATERIAL_BOUND_UNAVAILABLE"); return Work.zero();
        }
    }
    private static void material(Scope scope, Object value, int depth, long[] totals) {
        if (depth > MAX_MATERIAL_DEPTH || totals[1] >= MAX_MATERIAL_NODES) { throw new AssertionError("returned material shape exceeds the finite observer bound"); }
        totals[1] = Math.incrementExact(totals[1]);
        scope.materialNode();
        if (value == null) { return; }
        if (value instanceof byte[] bytes) {
            if (bytes.length > MAX_MATERIAL_BYTES) { throw new AssertionError("returned byte array exceeds its local consumption bound"); }
            materialBytes(totals, bytes.length); return;
        }
        if (value instanceof String text) {
            if (text.length() > MAX_MATERIAL_LEAF_CHARACTERS) { throw new AssertionError("returned text exceeds its local consumption bound"); }
            materialBytes(totals, Math.multiplyExact(4L, text.length())); return;
        }
        if (value instanceof org.bson.BsonString text) {
            material(scope, text.getValue(), depth + 1, totals); return;
        }
        if (value instanceof org.bson.BsonBinary binary) {
            material(scope, binary.getData(), depth + 1, totals); return;
        }
        if (value instanceof org.bson.BsonArray array) {
            material(scope, array.getValues(), depth + 1, totals); return;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double || value instanceof Boolean || value instanceof java.time.Instant
                || value instanceof org.bson.types.ObjectId || value instanceof org.bson.BsonTimestamp
                || value instanceof org.bson.BsonObjectId || value instanceof org.bson.BsonInt32
                || value instanceof org.bson.BsonInt64 || value instanceof org.bson.BsonBoolean) {
            materialBytes(totals, 128); return;
        }
        if (value instanceof org.bson.BsonNull || value instanceof org.bson.BsonDouble
                || value instanceof org.bson.BsonDateTime || value instanceof org.bson.BsonDecimal128
                || value instanceof org.bson.BsonMinKey || value instanceof org.bson.BsonMaxKey) {
            materialBytes(totals, 128); return;
        }
        if (value instanceof javax.management.Attribute attribute) {
            material(scope, attribute.getName(), depth + 1, totals); material(scope, attribute.getValue(), depth + 1, totals); return;
        }
        if (value instanceof Map<?, ?> map) {
            if (map.size() > 4096 - totals[1]) { throw new AssertionError("returned map exceeds its observer shape bound"); }
            for (var entry : map.entrySet()) { material(scope, entry.getKey(), depth + 1, totals); material(scope, entry.getValue(), depth + 1, totals); }
            return;
        }
        if (value instanceof java.util.Collection<?> values) {
            if (values.size() > 4096 - totals[1]) { throw new AssertionError("returned collection exceeds its observer shape bound"); }
            for (Object item : values) { material(scope, item, depth + 1, totals); } return;
        }
        if (value instanceof Object[] values) {
            if (values.length > 4096 - totals[1]) { throw new AssertionError("returned array exceeds its observer shape bound"); }
            for (Object item : values) { material(scope, item, depth + 1, totals); } return;
        }
        throw new AssertionError("returned material has no source-level byte formula");
    }
    private static void materialBytes(long[] totals, long bytes) {
        totals[0] = Math.addExact(totals[0], bytes);
        if (totals[0] > MAX_MATERIAL_BYTES) { throw new AssertionError("returned material exceeds its local consumption byte bound"); }
    }

    @FunctionalInterface interface CheckedAction<T, E extends Exception> { T run() throws E; }
    /** Counts one opaque service request, retaining its actual completion or exact primary failure. */
    static <T, E extends Exception> T external(Scope scope, Activity activity, CheckedAction<T, E> action) throws E {
        if (scope == null) { return action.run(); }
        Ticket ticket = scope.begin(activity, Work.call()); issued(ticket);
        try { T value = action.run(); done(ticket); return value; }
        catch (RuntimeException | Error failure) { failed(ticket, failure); throw failure; }
        catch (Exception failure) {
            failed(ticket, failure);
            @SuppressWarnings("unchecked") E original = (E) failure;
            throw original;
        }
    }

    @FunctionalInterface interface Action<T> { T run(); }
    static <T> T operation(Scope scope, Activity activity, Action<T> action) {
        if (scope == null) { return action.run(); }
        Ticket ticket = scope.begin(activity, Work.call());
        try { T value = action.run(); done(ticket); return value; }
        catch (RuntimeException | Error failure) { failed(ticket, failure); throw failure; }
    }
    static <T> T operation(Activity activity, Action<T> action) { return operation(current(), activity, action); }
    static <T> T request(Scope scope, Activity activity, Action<T> action) {
        if (scope == null) { return action.run(); }
        Ticket ticket = scope.begin(activity, Work.call()); issued(ticket);
        try { T result = action.run(); scope.complete(ticket, material(scope, activity, result), null); return result; }
        catch (RuntimeException | Error failure) { scope.complete(ticket, Work.zero(), failure); throw failure; }
    }
    static <T> T request(Activity activity, Action<T> action) { return request(current(), activity, action); }
    static <T> T opaqueRequest(Scope scope, Activity activity, Action<T> action) {
        if (scope == null) { return action.run(); }
        Ticket ticket = scope.begin(activity, Work.call()); issued(ticket);
        try { T value = action.run(); scope.complete(ticket, Work.zero(), null); return value; }
        catch (RuntimeException | Error failure) { scope.complete(ticket, Work.zero(), failure); throw failure; }
    }

    static final class Ticket implements AutoCloseable {
        private final Scope scope;
        private final Activity activity;
        private final long ordinal;
        private boolean completed;
        private boolean failed;
        private long issuedRequests;
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
        private final String priorTreePlanSha256;
        private boolean attempted;
        private PreparedOutput(Scope scope, String kind, byte[] bytes, long upper, String planSha256) {
            this.scope = scope; this.kind = kind; this.bytes = bytes; independentUpperBytes = upper;
            priorTreePlanSha256 = planSha256;
            scope.outputHash(bytes.length);
            scope.add(Activity.INPUT_FINGERPRINT, new Work(1, 0, 0, 0, bytes.length, 1, bytes.length, 0));
            sha = hash(scope, bytes);
        }
        byte[] bytes() {
            scope.outputClone(bytes.length);
            scope.add(Activity.COPY, Work.bytes(bytes.length)); return bytes.clone();
        }
        long actualBytes() { return bytes.length; }
        long independentUpperBytes() { return independentUpperBytes; }
        String sha256() { return sha; }
        String priorTreePlanSha256() { return priorTreePlanSha256; }
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
            Work actual = Work.zero(); long attempts, completed, failures, issuedRequests, completedRequests, failedRequests;
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
        private long materialNodeVisits, outputCloneRequests, outputHashRequests, outputTreeRequests;
        private long beginRequests, completeRequests, issuedEvents, gapRequests;
        private long bindingRequests, failureRequests, verificationRequests, finishRequests, closeRequests;
        private long plannedEncodingUpperBytes, serializerRequests;
        private boolean selfOverflow;
        private String selfOverflowField;
        private Scope(Definition definition, String runId) {
            this.definition = Objects.requireNonNull(definition); text(runId, 128); this.runId = runId;
            for (Activity activity : Activity.values()) { rows.put(activity, new Row()); }
        }
        synchronized void bindRoot(long pid, long start) {
            try { bindingRequests = Math.incrementExact(bindingRequests); } catch (ArithmeticException overflow) { selfOverflow("bindingRequests", overflow); }
            if (pid <= 0 || start <= 0 || this.pid != null && (this.pid != pid || this.start != start)) { refuse("RUNTIME_BINDING", null); return; }
            this.pid = pid; this.start = start;
        }
        synchronized void requirePriorBytes(Activity activity, long bytes) {
            Work upper = definition.priorInputUpper().get(activity);
            if (upper == null) { upper = definition.perOperationUpper().get(activity); }
            if (bytes < 0 || upper == null || bytes > upper.bytes()) {
                gap(activity, "PRIOR_SOURCE_BYTE_LIMIT_MISSING_OR_EXCEEDED");
                throw new AssertionError("source material lacks its declared prior byte limit");
            }
        }
        synchronized Ticket begin(Activity activity, Work work) {
            Objects.requireNonNull(activity); Objects.requireNonNull(work);
            if (sealed || released) { refuse("WORK_AFTER_SEAL", null); }
            Row row = rows.get(activity);
            try { beginRequests = Math.incrementExact(beginRequests); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
            try { bookkeepingUpdates = Math.incrementExact(bookkeepingUpdates); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
            try { row.attempts = Math.incrementExact(row.attempts); ordinal = Math.incrementExact(ordinal); pendingCount = Math.incrementExact(pendingCount); }
            catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
            Ticket ticket = new Ticket(this, activity, ordinal);
            ticket.issuedRequests = work.requests();
            try { row.issuedRequests = Math.addExact(row.issuedRequests, work.requests()); } catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
            if (pending.size() < MAX_PENDING) { pending.put(ordinal, ticket); }
            else { try { omitted = Math.incrementExact(omitted); } catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); } refuse("OMITTED_OPERATION", null); }
            accumulate(activity, work); return ticket;
        }
        synchronized void complete(Ticket ticket, Work work, Throwable failure) {
            if (ticket == null || ticket.scope != this || ticket.completed) { refuse("OPERATION_IDENTITY", failure); return; }
            ticket.completed = true; pending.remove(ticket.ordinal);
            try { completeRequests = Math.incrementExact(completeRequests); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
            try { bookkeepingUpdates = Math.incrementExact(bookkeepingUpdates); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
            Row row = rows.get(ticket.activity);
            try { pendingCount = Math.decrementExact(pendingCount); row.completed = Math.incrementExact(row.completed); }
            catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
            accumulate(ticket.activity, work);
            try { row.completedRequests = Math.addExact(row.completedRequests, ticket.issuedRequests); }
            catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
            if (failure != null) {
                ticket.failed = true;
                try { row.failures = Math.incrementExact(row.failures); row.failedRequests = Math.addExact(row.failedRequests, ticket.issuedRequests); }
                catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
                refuse("FAILED_OPERATION", failure);
            }
            verify(ticket.activity);
        }
        private synchronized void completeIfPending(Ticket ticket, Work work, Throwable failure) {
            if (!ticket.completed) { complete(ticket, work, failure); }
            else if (failure != null && !ticket.failed) {
                ticket.failed = true; Row row = rows.get(ticket.activity);
                try { row.failures = Math.incrementExact(row.failures); row.failedRequests = Math.addExact(row.failedRequests, ticket.issuedRequests); }
                catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
                refuse("FAILED_OPERATION", failure);
            }
        }
        synchronized void add(Activity activity, Work work) { Ticket ticket = begin(activity, work); complete(ticket, Work.zero(), null); }
        synchronized void issued(Ticket ticket) {
            if (ticket == null || ticket.scope != this || ticket.completed) { refuse("OPERATION_IDENTITY", null); return; }
            try {
                issuedEvents = Math.incrementExact(issuedEvents); ticket.issuedRequests = Math.incrementExact(ticket.issuedRequests);
                rows.get(ticket.activity).issuedRequests = Math.incrementExact(rows.get(ticket.activity).issuedRequests);
            } catch (ArithmeticException overflow) { refuse("COUNT_OVERFLOW", overflow); }
            accumulate(ticket.activity, new Work(0, 1, 0, 0, 0, 0, 0, 0));
        }
        synchronized void gap(Activity activity, String reason) {
            text(reason, 128); gaps.putIfAbsent(activity, reason); refuse("UNACCOUNTED_ACTIVITY", null);
            try { gapRequests = Math.incrementExact(gapRequests); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
        }
        private synchronized void materialNode() {
            try { materialNodeVisits = Math.incrementExact(materialNodeVisits); } catch (ArithmeticException overflow) { refuse("BOOKKEEPING_OVERFLOW", overflow); }
        }
        private synchronized void outputClone(long bytes) {
            try { outputCloneRequests = Math.incrementExact(outputCloneRequests); outputCloneBytes = Math.addExact(outputCloneBytes, bytes); }
            catch (ArithmeticException overflow) { selfOverflow("outputCloneBytes", overflow); }
        }
        private synchronized void outputHash(long bytes) {
            try { outputHashRequests = Math.incrementExact(outputHashRequests); outputHashBytes = Math.addExact(outputHashBytes, bytes); }
            catch (ArithmeticException overflow) { selfOverflow("outputHashBytes", overflow); }
        }
        private void selfOverflow(String field, ArithmeticException failure) {
            selfOverflow = true; if (selfOverflowField == null) { selfOverflowField = field; }
            refuse("BOOKKEEPING_OVERFLOW", failure);
        }
        private synchronized void outputNode() {
            try { outputTreeNodeVisits = Math.incrementExact(outputTreeNodeVisits); }
            catch (ArithmeticException overflow) { selfOverflow("outputTreeNodeVisits", overflow); }
        }
        private void accumulate(Activity activity, Work work) {
            try { rows.get(activity).actual = rows.get(activity).actual.plus(work); }
            catch (ArithmeticException overflow) { refuse("WORK_OVERFLOW", overflow); }
            if (definition.explicitExcludedActivities().contains(activity)) { refuse("EXCLUDED_ACTIVITY_USED", null); }
            Work upper = definition.priorInputUpper().get(activity);
            if (upper != null && !rows.get(activity).actual.within(upper)) { refuse("PRIOR_INPUT_BUDGET_EXCEEDED", null); }
        }
        private void verify(Activity activity) {
            try { verificationRequests = Math.incrementExact(verificationRequests); } catch (ArithmeticException overflow) { selfOverflow("verificationRequests", overflow); }
            Work perOperation = definition.perOperationUpper().get(activity); Row row = rows.get(activity);
            if (perOperation != null && row.attempts == row.completed) {
                try { if (!row.actual.within(perOperation.times(row.completed))) { refuse("SOURCE_OPERATION_BOUND_EXCEEDED", null); } }
                catch (ArithmeticException overflow) { refuse("UPPER_OVERFLOW", overflow); }
            }
        }
        private void refuse(String reason, Throwable failure) { if (unknown == null) { unknown = reason; } if (primary == null && failure != null) { primary = failure; } }
        Throwable primaryCause() { synchronized (this) { return primary; } }
        synchronized void failed(Throwable failure) {
            try { failureRequests = Math.incrementExact(failureRequests); } catch (ArithmeticException overflow) { selfOverflow("failureRequests", overflow); }
            refuse("RUN_FAILED", Objects.requireNonNull(failure));
        }

        PreparedOutput prepareOutput(String kind, long independentUpperBytes, Supplier<String> serializer) {
            gap(Activity.REPORT_SERIALIZE, "PRIOR_SERIALIZER_TREE_PROOF_REQUIRED");
            return prepareOutputUnchecked(kind, independentUpperBytes, serializer, "UNAVAILABLE");
        }
        PreparedOutput prepareOutput(String kind, TreePlan plan, Object boundedTree) {
            Objects.requireNonNull(plan);
            byte[] tuple = java.nio.ByteBuffer.allocate(32).putLong(plan.nodes()).putLong(plan.stringCharacters())
                    .putLong(plan.encodedUpperBytes()).putLong(plan.depth()).array();
            outputHash(tuple.length);
            add(Activity.INPUT_FINGERPRINT, Work.bytes(tuple.length));
            return prepareOutput(kind, plan, boundedTree, hash(this, tuple));
        }
        PreparedOutput prepareOutput(String kind, TreePlan plan, Object boundedTree, String independentlyRegisteredPlanSha256) {
            Objects.requireNonNull(plan);
            digest(independentlyRegisteredPlanSha256);
            synchronized (this) {
                try { outputTreeRequests = Math.incrementExact(outputTreeRequests); }
                catch (ArithmeticException overflow) { selfOverflow("outputTreeRequests", overflow); }
            }
            long[] observed = new long[3];
            try { checkTree(boundedTree, 0, plan, observed); }
            catch (RuntimeException | Error failure) { failed(failure); throw failure; }
            add(Activity.BOOKKEEPING, new Work(1, 0, 0, 0, 0, 1, observed[0], observed[0]));
            return prepareOutputUnchecked(kind, plan.encodedUpperBytes(), () -> JsonWriter.write(boundedTree), independentlyRegisteredPlanSha256);
        }
        private void checkTree(Object value, int depth, TreePlan plan, long[] observed) {
            outputNode();
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
        private PreparedOutput prepareOutputUnchecked(String kind, long independentUpperBytes, Supplier<String> serializer, String planSha256) {
            text(kind, 64);
            synchronized (this) {
                if (pendingOutput != null || sealed || independentUpperBytes <= 0) { refuse("OUTPUT_PLAN", null); throw new AssertionError("one positive independent output plan is required"); }
            }
            Ticket ticket = begin(Activity.REPORT_SERIALIZE, Work.call());
            try {
                synchronized (this) {
                    try { serializerRequests = Math.incrementExact(serializerRequests); plannedEncodingUpperBytes = Math.addExact(plannedEncodingUpperBytes, independentUpperBytes); }
                    catch (ArithmeticException overflow) { selfOverflow("plannedEncodingUpperBytes", overflow); }
                }
                String text = serializer.get(); byte[] encoded = text.getBytes(StandardCharsets.UTF_8);
                complete(ticket, Work.bytes(encoded.length), null);
                PreparedOutput prepared = new PreparedOutput(this, kind, encoded, independentUpperBytes, planSha256);
                synchronized (this) {
                    if (encoded.length > independentUpperBytes) { refuse("OUTPUT_INPUT_BOUND_EXCEEDED", null); }
                    pendingOutput = prepared; output = Map.of("state", "OUTPUT_PENDING", "kind", kind,
                            "independentUpperBytes", independentUpperBytes, "actualBytes", encoded.length, "sha256", prepared.sha256(),
                            "priorTreePlanSha256", prepared.priorTreePlanSha256());
                }
                return prepared;
            } catch (RuntimeException | Error failure) { BenchmarkReturnWorkBudget.failed(ticket, failure); throw failure; }
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
                            "independentUpperBytes", prepared.independentUpperBytes, "actualBytes", prepared.bytes.length, "sha256", prepared.sha256(),
                            "priorTreePlanSha256", prepared.priorTreePlanSha256());
                }
            } catch (Exception | Error failure) {
                completeIfPending(write, Work.zero(), failure); gap(Activity.REPORT_CLOSE, "FAILED_OUTPUT_CLOSE_NOT_OBSERVED"); throw failure;
            }
        }
        synchronized Map<String, Object> finish() {
            try { finishRequests = Math.incrementExact(finishRequests); } catch (ArithmeticException overflow) { selfOverflow("finishRequests", overflow); }
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
                facts.put("issuedRequests", row.issuedRequests); facts.put("completedRequests", row.completedRequests); facts.put("failedRequests", row.failedRequests);
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
            var self = new LinkedHashMap<String, Object>();
            self.put("beginRequests", beginRequests); self.put("completeRequests", completeRequests); self.put("issuedEvents", issuedEvents); self.put("gapRequests", gapRequests);
            self.put("snapshotRequests", snapshotRequests); self.put("snapshotRowVisits", snapshotRowVisits); self.put("materialNodeVisits", materialNodeVisits);
            self.put("outputTreeRequests", outputTreeRequests); self.put("outputTreeNodeVisits", outputTreeNodeVisits);
            self.put("outputCloneRequests", outputCloneRequests); self.put("outputCloneBytes", outputCloneBytes);
            self.put("outputHashRequests", outputHashRequests); self.put("outputHashBytes", outputHashBytes);
            self.put("serializerRequests", serializerRequests); self.put("plannedEncodingUpperBytes", plannedEncodingUpperBytes);
            self.put("bindingRequests", bindingRequests); self.put("failureRequests", failureRequests); self.put("verificationRequests", verificationRequests);
            self.put("finishRequests", finishRequests); self.put("closeRequests", closeRequests);
            out.put("selfCounters", Map.copyOf(self));
            out.put("selfAccountingRule", "SOURCE_ENTRY_VISITS_VECTOR_PASSES_FIXED_RECEIPT_COPY_AND_PRIVATE_COUNTER_TAIL_V1");
            out.put("scopeReleased", released);
            out.put("privateReleaseTail", Map.of("state", released ? "ACTUAL_RELEASE_RECORDED" : "SOURCE_UPPER_ONLY",
                    "scope", "ONE_IDEMPOTENT_RELEASE_CALL_INCLUDING_ATOMIC_SCOPE_CLEAR", "upper", new Work(1, 1, 0, 0, 0, 1, 4, 4).evidence()));
            out.put("selfOverflow", selfOverflow);
            if (selfOverflowField != null) { out.put("selfOverflowField", selfOverflowField); }
            try { out.put("selfSourceUpper", selfSourceUpper().evidence()); }
            catch (ArithmeticException overflow) {
                selfOverflow("selfSourceUpper", overflow); out.put("selfSourceUpperState", "UNKNOWN");
                out.put("selfOverflow", true); out.put("selfOverflowField", selfOverflowField); out.put("state", "UNKNOWN");
            }
            out.put("localMaterialBounds", Map.of("nodes", MAX_MATERIAL_NODES, "depth", MAX_MATERIAL_DEPTH,
                    "leafCharacters", MAX_MATERIAL_LEAF_CHARACTERS, "sourceBytes", MAX_MATERIAL_BYTES,
                    "connectorInputBytes", MAX_CONNECTOR_INPUT_BYTES,
                    "scope", "LOCAL_OBSERVER_CONSUMPTION_EXCLUDES_OPAQUE_RPC_ALLOCATION"));
            out.put("materialUnitScope", "CHECKED_SOURCE_MATERIAL_UPPER_BYTES_NOT_WIRE_CPU_OR_ELAPSED");
            out.put("pending", pending.values().stream().map(ticket -> Map.of("ordinal", ticket.ordinal, "activity", ticket.activity.name())).toList());
            out.put("output", output); if (unknown != null) { out.put("reason", unknown); }
            if (primary != null) { String type = primary.getClass().getName(); out.put("failureType", type.length() <= 128 ? type : "TYPE_NAME_EXCEEDS_BOUND"); }
            BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> out.put(flag, false));
            out.put("structuralWorkBudgetQualified", false); out.put("wholeMethodCostQualified", false); out.put("causalOverheadQualified", false);
            Map<String, Object> receipt = Collections.unmodifiableMap(out);
            // The map has a fixed inventory and bounded texts/pending roster. Encoding belongs to the
            // independently bounded report tree; constructing a second JSON here would recurse into reporting.
            return receipt;
        }
        /** Fixed source bounds include this calculation and its own finite counter-update tail. */
        private Work selfSourceUpper() {
            long inventory = Activity.values().length;
            long snapshotNodes = Math.addExact(1024, Math.addExact(Math.multiplyExact(128, inventory), Math.multiplyExact(8, MAX_PENDING)));
            long snapshotCharacters = Math.addExact(4096, Math.addExact(Math.multiplyExact(1024, inventory), Math.multiplyExact(128, MAX_PENDING)));
            long entries = Math.addExact(Math.addExact(beginRequests, completeRequests),
                    Math.addExact(Math.addExact(issuedEvents, gapRequests), Math.addExact(materialNodeVisits, outputTreeNodeVisits)));
            entries = Math.addExact(entries, Math.addExact(snapshotRequests, Math.addExact(outputCloneRequests,
                    Math.addExact(outputHashRequests, Math.addExact(outputTreeRequests, serializerRequests)))));
            entries = Math.addExact(entries, Math.addExact(2, Math.addExact(bindingRequests, Math.addExact(failureRequests,
                    Math.addExact(verificationRequests, Math.addExact(finishRequests, closeRequests))))));
            long snapshotCopies = Math.multiplyExact(snapshotRequests, Math.addExact(Math.multiplyExact(5, inventory), Math.addExact(MAX_PENDING, 12)));
            long requests = Math.addExact(snapshotCopies, Math.addExact(outputCloneRequests,
                    Math.addExact(Math.multiplyExact(3, outputHashRequests), Math.multiplyExact(2, serializerRequests))));
            long snapshotBytes = Math.multiplyExact(snapshotRequests,
                    Math.addExact(Math.multiplyExact(32, snapshotNodes), Math.multiplyExact(6, snapshotCharacters)));
            long bytes = Math.addExact(snapshotBytes, Math.addExact(Math.addExact(outputCloneBytes, outputHashBytes), Math.multiplyExact(2, plannedEncodingUpperBytes)));
            // Each vector pass touches eight units; both arithmetic and validation pass over them.
            long vectors = Math.addExact(Math.addExact(beginRequests, completeRequests), Math.addExact(issuedEvents, Math.multiplyExact(2, verificationRequests)));
            // Two eight-unit vector passes, with four source/JDK calls per unit, cover arithmetic,
            // validation, boxing and comparison. The fixed entry tail includes map/pending/scalar work.
            requests = Math.addExact(requests, Math.addExact(Math.multiplyExact(64, vectors), Math.multiplyExact(32, entries)));
            requests = Math.addExact(requests, Math.addExact(Math.multiplyExact(16, Math.addExact(materialNodeVisits, outputTreeNodeVisits)),
                    Math.multiplyExact(16, Math.multiplyExact(snapshotRequests, snapshotNodes))));
            requests = Math.addExact(requests, Math.addExact(128, Math.multiplyExact(80, inventory)));
            long elements = Math.addExact(Math.multiplyExact(16, vectors), Math.addExact(materialNodeVisits,
                    Math.addExact(outputTreeNodeVisits, Math.multiplyExact(snapshotRequests, snapshotNodes))));
            long passes = Math.addExact(Math.multiplyExact(2, vectors), Math.addExact(snapshotCopies,
                    Math.addExact(materialNodeVisits, Math.addExact(outputTreeNodeVisits, Math.addExact(outputCloneRequests, outputHashRequests)))));
            // The nonrecursive tail covers fixed scalar/map field updates in the event recorder itself.
            long updates = Math.addExact(Math.multiplyExact(32, entries), Math.multiplyExact(snapshotRequests, snapshotNodes));
            return new Work(entries, requests, 0, 0, bytes, passes, elements, updates);
        }
        @Override public synchronized void close() {
            try { closeRequests = Math.incrementExact(closeRequests); } catch (ArithmeticException overflow) { selfOverflow("closeRequests", overflow); }
            if (released) { return; } released = true;
            if (!sealed) { refuse("SCOPE_RELEASED_BEFORE_FINALIZATION", null); }
            ACTIVE.compareAndSet(this, null);
        }
    }
    private static void text(String value, int max) {
        if (value == null || value.isBlank() || value.length() > max || !value.chars().allMatch(c -> c >= 32 && c <= 126)) { throw new AssertionError("work identity text exceeds its bound"); }
    }
    private static void digest(String value) { if (value == null || !value.matches("[a-f0-9]{64}")) { throw new AssertionError("work source formula fingerprint is invalid"); } }
    private static String hash(Scope scope, byte[] bytes) {
        try {
            MessageDigest provider = external(scope, Activity.INPUT_FINGERPRINT, () -> MessageDigest.getInstance("SHA-256"));
            byte[] digest = opaqueRequest(scope, Activity.INPUT_FINGERPRINT, () -> provider.digest(bytes));
            return opaqueRequest(scope, Activity.INPUT_FINGERPRINT, () -> HexFormat.of().formatHex(digest));
        }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
