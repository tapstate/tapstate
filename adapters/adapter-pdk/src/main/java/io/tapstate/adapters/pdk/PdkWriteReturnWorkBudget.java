package io.tapstate.adapters.pdk;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Source-level work observations for the optional return collector. Opaque provider interiors are
 * requests, never CPU instructions or elapsed cost. Caller limits are inputs, not a certification.
 * The shutdown body describes a prefix plus a fixed seal-tail upper bound. Its shared companion is
 * checked only after the exact owned process exits; neither file claims crash durability.
 */
final class PdkWriteReturnWorkBudget {
    static final String PROPERTY = "tapstate.benchmark.write-return-work-budget";
    static final String INPUT_PROPERTY = "tapstate.benchmark.write-return-work-budget-input";
    static final String DEFINITION_ID = "WRITE_RETURN_PRODUCER_WORK_V1";
    static final int MAX_INPUT_BYTES = 16384;
    static final int MAX_RECEIPT_BYTES = 65536;
    static final int MAX_EPOCHS = 16;
    static final int SEAL_BYTES = 256;
    static final int MAX_ARGUMENTS = 128;
    static final long MAX_ARGUMENT_BYTES = 2093056;
    static final long MAX_CONNECTOR_BYTES = 134217728;
    static final long CLOSE_WAIT_NANOS = TimeUnit.SECONDS.toNanos(5);
    static final long CLOCK_HASH_MATERIAL_UPPER = Math.addExact(16L * 1024 * 1024,
            Math.addExact(4L * 256 * 1024, 5L * 8192));
    static final long CLOCK_SNAPSHOT_BYTES_UPPER = Math.addExact(CLOCK_HASH_MATERIAL_UPPER, 16593);
    static final long CLOCK_SNAPSHOT_REQUESTS_UPPER = Math.addExact(2053 + 1024,
            Math.multiplyExact(2, Math.addExact(CLOCK_HASH_MATERIAL_UPPER, 5)));
    static final long CLOCK_TRANSFER_UPPER = Math.addExact(Math.multiplyExact(4, CLOCK_SNAPSHOT_BYTES_UPPER), 65536);
    static final long CLOCK_ELEMENT_UPPER = Math.addExact(Math.multiplyExact(8, CLOCK_SNAPSHOT_BYTES_UPPER), 65536);
    static final List<String> UNITS = List.of("calls", "requests", "records", "frames", "bytes", "passes", "elements", "updates");
    static final int MAX_GAPS = 32;
    static final int MAX_REASON_CHARS = 128;
    static final int MAX_WINDOW_BYTES = 512;
    static final int WINDOW_BASE64_CHARS = 4 * ((MAX_WINDOW_BYTES + 2) / 3);
    // These are source rosters, not new admission limits. Tests compare the actual tree and enum.
    static final List<String> ROOT_FIELDS = List.of("schemaVersion", "enabled", "definitionId", "sourceFormulaSha256",
            "inputSha256", "runId", "pid", "jvmStartTimeMillis", "stage", "lifecycle", "generation", "nonceSha256",
            "state", "limits", "materialLimits", "actualMaterial", "sourceUpper", "inputRowsAttempted",
            "sourceUpperIsActualWork", "limitsIndependentlyVerified", "activities", "bookkeepingInvocations",
            "epochs", "pending", "openWriters", "omitted", "overflow", "overBudget", "gaps", "lateMutations",
            "sourceSealTailUpper", "sealTailIncludedInSourceUpper", "actualSealTailInCompanion", "processExitObserved",
            "crashDurabilityQualified", "opaqueScope", "excluded", "costUnit", "structuralBudgetQualified",
            "accountingErrorBoundQualified", "wholeMethodCostQualified", "collectionCostUpperBoundQualified",
            "samplingCostQualified", "causalOverheadQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance");
    static final List<String> ACTIVITY_NAMES = List.of("CONFIG", "INSTALL", "WRITER_INSTALL", "WRITER_ROSTER",
            "WRITER_CLOSE_ATTEMPT", "WRITER_CLOSE", "WRITE_WRAPPER", "BEGIN", "ACTIVE_BEGIN", "INACTIVE_BEGIN",
            "REJECTION", "IDENTITY_ROW", "IDENTITY_PIECE", "KEY_SHAPE", "CALLBACK_WRAPPER", "CALLBACK",
            "CALLBACK_ERROR", "CALLBACK_EXIT", "RETURN_OBSERVATION", "COMPLETE", "RECEIPT_PIECE", "PUBLICATION",
            "COST_AGGREGATION", "CLOCK_READ", "CLOCK_INITIALIZATION", "CLOCK_METADATA", "SCOPE_INSTALL",
            "SCOPE_CACHE_ENTRY", "SCOPE_BEFORE", "SCOPE_AFTER", "TRANSACTION_WRAPPER", "HASH_READ", "HASH_MATERIAL",
            "ARGUMENT", "ARGUMENT_MATERIAL", "MBEAN_SCALAR", "MBEAN_START", "MBEAN_STOP", "MBEAN_PAGE",
            "PAGE_FRAME_VISIT", "MBEAN_COST", "MBEAN_BUDGET", "ARM", "SHUTDOWN", "FINAL_OWNER", "FINAL_WAIT_ITERATION", "BOOKKEEPING");
    static final List<String> ACTIVITY_FIELDS = List.of("invocations", "completed", "failed", "pending");
    static final List<String> MATERIAL_FIELDS = List.of("connectorHashBytes", "runtimeArguments", "runtimeArgumentBytes", "primaryKeys");
    static final List<String> EPOCH_FIELDS = List.of("epoch", "windowBase64");
    static final int QUALIFICATION_FIELDS = 9;
    static final long REPORT_MAPS = add(1, 4, 1, 1, ACTIVITY_NAMES.size(), MAX_EPOCHS);
    static final long REPORT_LISTS = 2;
    static final long REPORT_ENTRIES = add(ROOT_FIELDS.size(), mul(4, UNITS.size()), MATERIAL_FIELDS.size(),
            ACTIVITY_NAMES.size(), mul(ACTIVITY_NAMES.size(), ACTIVITY_FIELDS.size()), mul(MAX_EPOCHS, EPOCH_FIELDS.size()));
    static final long REPORT_LIST_ELEMENTS = add(MAX_EPOCHS, MAX_GAPS);
    static final long REPORT_NODES = add(1, REPORT_ENTRIES, REPORT_LIST_ELEMENTS);
    static final long REPORT_KEY_CHARS = add(chars(ROOT_FIELDS), mul(4, chars(UNITS)), chars(MATERIAL_FIELDS),
            chars(ACTIVITY_NAMES), mul(ACTIVITY_NAMES.size(), chars(ACTIVITY_FIELDS)), mul(MAX_EPOCHS, chars(EPOCH_FIELDS)));
    // Every root scalar is conservatively allowed 128 ASCII characters; epochs and reasons are
    // independently bounded before material copying. Numeric rendering has at most 20 characters.
    static final long REPORT_STRING_CHARS = add(REPORT_KEY_CHARS, mul(ROOT_FIELDS.size(), 128),
            mul(MAX_EPOCHS, WINDOW_BASE64_CHARS), mul(MAX_GAPS, MAX_REASON_CHARS));
    static final long REPORT_JSON_CHARS = add(mul(2, REPORT_STRING_CHARS), mul(20, REPORT_NODES), mul(8, REPORT_NODES));
    // Construction: map factories/puts, vector lookups, enum name/ordinal, list copies, flag iterator,
    // boxing of every value node, clean(), and the five nonce UTF-8/digest/hex calls.
    static final long REPORT_BUILD_REQUESTS = add(6, ACTIVITY_NAMES.size(), 1, REPORT_ENTRIES, REPORT_NODES,
            2, mul(2, ACTIVITY_NAMES.size()), 1, add(mul(2, QUALIFICATION_FIELDS), 2), 5, mul(4, UNITS.size()), 1);
    // JsonWriter: builder+toString; each map's delimiters/entrySet/iterator/final hasNext; seven
    // entry calls; four list calls plus three per element; two quotes+terminal length per string;
    // length/charAt/append per character; and two calls per scalar. All strings are printable ASCII.
    static final long REPORT_JSON_REQUESTS = add(2, mul(5, REPORT_MAPS), mul(7, REPORT_ENTRIES),
            mul(4, REPORT_LISTS), mul(3, REPORT_LIST_ELEMENTS), mul(3, add(REPORT_ENTRIES, REPORT_NODES)),
            mul(3, REPORT_STRING_CHARS), mul(2, REPORT_NODES));
    static final long REPORT_REQUESTS = add(REPORT_BUILD_REQUESTS, REPORT_JSON_REQUESTS, 2, mul(2, REPORT_JSON_CHARS));
    static final long REPORT_PASSES = add(4, 1, 1, REPORT_MAPS, REPORT_LISTS, REPORT_ENTRIES, REPORT_NODES, 1);
    static final long REPORT_ELEMENTS_UPDATES = mul(8, add(REPORT_NODES, REPORT_ENTRIES, REPORT_STRING_CHARS, REPORT_JSON_CHARS));
    // Text transfer covers two-byte builder characters, ASCII rendering, and the nonce's maximum
    // UTF-8 input plus digest/hex output. Provider allocation/instruction interiors stay opaque.
    static final long REPORT_BYTES = add(mul(2, REPORT_STRING_CHARS), REPORT_JSON_CHARS, 64, 32, 64);
    static final long LEDGER_MUTATION_REQUESTS = add(3, UNITS.size(), 3);
    static final long LEDGER_BASIS_REQUESTS = add(2, 2, mul(2, UNITS.size()));
    static final long LEDGER_PAIR_REQUESTS = add(2, 6, 2);
    static final long LEDGER_METADATA_REQUESTS = add(mul(2, MAX_REASON_CHARS), 8, 8);
    static final long BOOKKEEPING_REQUESTS = add(LEDGER_MUTATION_REQUESTS, LEDGER_BASIS_REQUESTS,
            LEDGER_PAIR_REQUESTS, LEDGER_METADATA_REQUESTS);
    static final long BOOKKEEPING_BYTES = add(8, mul(2, 4), mul(4, MAX_WINDOW_BYTES), MAX_WINDOW_BYTES, WINDOW_BASE64_CHARS);
    static final long BOOKKEEPING_ELEMENTS_UPDATES = add(mul(8, UNITS.size()), mul(2, MAX_REASON_CHARS),
            MAX_WINDOW_BYTES, WINDOW_BASE64_CHARS, 16);
    // Raw prechecks use at most six byte/string passes. JsonReader's peek/length/charAt/append and
    // number/literal paths get twelve calls per input character; JsonWriter's control escape path
    // gets five. The preparse roster has at most 64 tokens/nodes. Startup additionally renders the
    // fixed formula rosters and eight coefficients per activity before digesting the ASCII formula.
    static final long CONFIG_BOOTSTRAP_REQUESTS = add(mul(8, REPORT_ENTRIES), mul(4, REPORT_KEY_CHARS),
            mul(4, mul(ACTIVITY_NAMES.size(), UNITS.size())), 8);
    static final long CONFIG_REQUESTS = add(mul(6, MAX_INPUT_BYTES), mul(12, MAX_INPUT_BYTES),
            mul(5, MAX_INPUT_BYTES), mul(64, 32), CONFIG_BOOTSTRAP_REQUESTS);

    /* Each invocation allocation is independent of retained rows or successful admission. Variable
       material is accounted before traversal/copy, with separate actual material observations.
       The fixed allocations bound this collector's source operations; opaque callees stay requests. */
    enum Activity {
        CONFIG(1, CONFIG_REQUESTS, 0, 0, MAX_INPUT_BYTES * 8L, 16, MAX_INPUT_BYTES * 16L, MAX_INPUT_BYTES * 16L),
        INSTALL(1, 24, 0, 0, 2048, 4, 256, 64),
        WRITER_INSTALL(1, 16, 0, 0, 2048, 4, 512, 64),
        WRITER_ROSTER(1, 4, 0, 0, 0, 1, 16, 16),
        WRITER_CLOSE_ATTEMPT(1, 0, 0, 0, 0, 1, 16, 16),
        WRITER_CLOSE(1, 0, 0, 0, 0, 1, 16, 16),
        WRITE_WRAPPER(1, 0, 0, 0, 0, 1, 32, 32),
        BEGIN(1, 12, 0, 0, 4096, 4, 512, 128),
        ACTIVE_BEGIN(1, 0, 0, 0, 0, 1, 16, 16),
        INACTIVE_BEGIN(1, 0, 0, 0, 0, 1, 16, 16),
        REJECTION(1, 0, 0, 0, 0, 1, 16, 16),
        IDENTITY_ROW(0, 128, 1, 0, 512, 4, 512, 128),
        IDENTITY_PIECE(0, 16, 0, 1, 65536L * 8, 8, 65536L * 8, 64),
        KEY_SHAPE(0, 32, 0, 0, 1024, 4, 512, 16),
        CALLBACK_WRAPPER(1, 0, 0, 0, 0, 1, 32, 16),
        CALLBACK(1, 16, 0, 0, 0, 2, 64, 64),
        CALLBACK_ERROR(0, 64, 0, 0, 4096, 8, 4096, 64),
        CALLBACK_EXIT(1, 4, 0, 0, 0, 1, 32, 32),
        RETURN_OBSERVATION(1, 4, 0, 0, 0, 1, 32, 16),
        COMPLETE(1, 16, 0, 0, 4096, 4, 512, 128),
        RECEIPT_PIECE(0, 32, 0, 1, 65536L * 8, 8, 65536L * 8, 128),
        PUBLICATION(0, 4, 0, 1, 0, 1, 16, 16),
        COST_AGGREGATION(1, 8, 0, 0, 0, 4, 64, 64),
        CLOCK_READ(1, 2, 0, 0, 0, 1, 16, 16),
        // Hash streams may return one byte per read. The independent material limit, rather than
        // an assumed full buffer, bounds both read and digest-update requests, including EOF.
        CLOCK_INITIALIZATION(1, CLOCK_SNAPSHOT_REQUESTS_UPPER, 0, 0, CLOCK_TRANSFER_UPPER, 64, CLOCK_ELEMENT_UPPER, CLOCK_ELEMENT_UPPER),
        CLOCK_METADATA(1, CLOCK_SNAPSHOT_REQUESTS_UPPER, 0, 0, CLOCK_TRANSFER_UPPER, 64, CLOCK_ELEMENT_UPPER, CLOCK_ELEMENT_UPPER),
        SCOPE_INSTALL(1, 256, 0, 0, 32768, 8, 4096, 256),
        SCOPE_CACHE_ENTRY(0, 4, 0, 0, 0, 1, 4, 4),
        SCOPE_BEFORE(1, 40, 0, 0, 32768, 4, 32768, 64),
        SCOPE_AFTER(1, 48, 0, 0, 32768, 6, 32768, 96),
        TRANSACTION_WRAPPER(1, 4, 0, 0, 0, 1, 16, 16),
        HASH_READ(0, 3, 0, 0, 0, 3, 0, 4),
        HASH_MATERIAL(0, 0, 0, 0, 4, 4, 4, 0),
        ARGUMENT(0, 16, 0, 0, 0, 4, 32, 16),
        ARGUMENT_MATERIAL(0, 0, 0, 0, 4, 4, 4, 0),
        MBEAN_SCALAR(1, 4, 0, 0, 64, 1, 8, 4),
        MBEAN_START(1, 8, 0, 0, 8192, 4, 4096, 128),
        MBEAN_STOP(1, 0, 0, 0, 0, 1, 16, 16),
        MBEAN_PAGE(1, 32, 0, 0, 65536L * 8, 8, 65536L * 8, 128),
        PAGE_FRAME_VISIT(0, 4, 0, 1, 0, 1, 8, 4),
        MBEAN_COST(1, 32, 0, 0, 8192L * 4, 6, 8192L * 4, 128),
        MBEAN_BUDGET(1, REPORT_REQUESTS, 0, 0, REPORT_BYTES, REPORT_PASSES, REPORT_ELEMENTS_UPDATES, REPORT_ELEMENTS_UPDATES),
        ARM(1, 64, 0, 0, 16384, 8, 16384, 128),
        SHUTDOWN(1, 16, 0, 0, 4096, 4, 512, 64),
        FINAL_OWNER(1, 32, 0, 0, 4096, 4, 512, 64),
        FINAL_WAIT_ITERATION(1, 4, 0, 0, 0, 2, 32, 16),
        BOOKKEEPING(1, BOOKKEEPING_REQUESTS, 0, 0, BOOKKEEPING_BYTES, 6, BOOKKEEPING_ELEMENTS_UPDATES, BOOKKEEPING_ELEMENTS_UPDATES);

        final long[] upper;
        Activity(long... upper) { this.upper = upper; }
    }

    /* The serialization/write/close/digest/companion tail is not an invented actual observation.
       It has one body open/write/force/close, two mapping forces and bounded stores. No write retry. */
    // Tail outside report rendering: eight checked vector additions; four body channel calls;
    // ByteBuffer.wrap; three body digest/hex calls (two are used); ten normal companion stores,
    // two forces, two clean() calls, and the countDown/owner-return calls. Refusal stores and
    // interrupt handling get their own finite branch reserve; gap bookkeeping is counted separately.
    static final long SEAL_NON_REPORT_REQUESTS = add(UNITS.size(), 4, 1, 3, 10, 2, 2, 2,
            add(2, 2, 1, 1, 1, 1));
    static final long[] SEAL_TAIL_UPPER = { 1, add(REPORT_REQUESTS, SEAL_NON_REPORT_REQUESTS), 0, 0,
            add(REPORT_BYTES, mul(3, MAX_RECEIPT_BYTES), mul(8, SEAL_BYTES)),
            add(REPORT_PASSES, 4), add(REPORT_ELEMENTS_UPDATES, mul(8, SEAL_BYTES), MAX_RECEIPT_BYTES),
            add(REPORT_ELEMENTS_UPDATES, mul(8, UNITS.size()), mul(8, SEAL_BYTES)) };
    static final String FORMULA = formula();
    static final String SOURCE_FORMULA_SHA256 = sha(FORMULA.getBytes(StandardCharsets.US_ASCII));
    private static PdkWriteReturnWorkBudget installed;

    private final long pid;
    private final long startMillis;
    private final Config config;
    private final long[] attempts = new long[Activity.values().length];
    private final long[] completed = new long[attempts.length];
    private final long[] failed = new long[attempts.length];
    private final long[] pendingByActivity = new long[attempts.length];
    private final long[] upper = new long[8];
    private final long[] material = new long[8];
    private final List<Map<String, Object>> epochs = new ArrayList<>();
    private final Set<String> gaps = new java.util.LinkedHashSet<>();
    private long pending;
    private long openWriters;
    private long generation;
    private long bookkeeping;
    private long omitted;
    private long lateMutations;
    private long inputRowsAttempted;
    private boolean overflow;
    private boolean overBudget;
    private boolean shutdownEntered;
    private boolean bodySnapshot;
    private boolean armAttempted;
    private String lifecycle = "LIVE";
    private MappedByteBuffer seal;
    private Path bodyPath;
    private String nonce;
    private Runnable unregister;

    private PdkWriteReturnWorkBudget(Config config, long pid, long startMillis) {
        this.config = config; this.pid = pid; this.startMillis = startMillis;
        if (config.failure != null) gap(config.failure);
        observe(Activity.CONFIG);
    }

    static PdkWriteReturnWorkBudget enabled() {
        if (!"true".equals(System.getProperty(PROPERTY))) return null;
        synchronized (PdkWriteReturnWorkBudget.class) {
            if (installed == null) {
                installed = new PdkWriteReturnWorkBudget(Config.parse(System.getProperty(INPUT_PROPERTY)),
                        ProcessHandle.current().pid(), ManagementFactory.getRuntimeMXBean().getStartTime());
            }
            return installed;
        }
    }

    static PdkWriteReturnWorkBudget testing(String encoded, long pid, long startMillis) {
        return new PdkWriteReturnWorkBudget(Config.parse(encoded), pid, startMillis);
    }

    synchronized Actor writer() {
        observe(Activity.WRITER_INSTALL);
        try { openWriters = Math.incrementExact(openWriters); }
        catch (ArithmeticException failure) { overflow = true; }
        return new Actor(this);
    }

    static final class Actor {
        final PdkWriteReturnWorkBudget budget;
        private boolean closed;
        private Actor(PdkWriteReturnWorkBudget budget) { this.budget = budget; }
        Span write() { return budget.enter(Activity.WRITE_WRAPPER); }
        void closeAttempt() { budget.observe(Activity.WRITER_CLOSE_ATTEMPT); }
        Span callback() {
            synchronized (budget) { if (closed) budget.gap("CALLBACK_AFTER_WRITER_CLOSE"); }
            return budget.enter(Activity.CALLBACK_WRAPPER);
        }
        void closed(boolean success) {
            synchronized (budget) {
                budget.observe(Activity.WRITER_CLOSE);
                if (closed) { budget.gap("DUPLICATE_WRITER_CLOSE"); return; }
                closed = true;
                if (success) budget.openWriters--;
                else budget.gap("WRITER_CLOSE_FAILED");
                budget.notifyAll();
            }
        }
    }

    Span enter(Activity activity) {
        synchronized (this) {
            observe(activity);
            try {
                pending = Math.incrementExact(pending);
                pendingByActivity[activity.ordinal()] = Math.incrementExact(pendingByActivity[activity.ordinal()]);
                if (completed[activity.ordinal()] == 0) overflow = true;
                else completed[activity.ordinal()]--;
            } catch (ArithmeticException failure) { overflow = true; }
        }
        return new Span(this, activity);
    }

    static final class Span implements AutoCloseable {
        private final PdkWriteReturnWorkBudget owner;
        private final Activity activity;
        private boolean ended;
        private boolean success;
        private Span(PdkWriteReturnWorkBudget owner, Activity activity) { this.owner = owner; this.activity = activity; }
        void success() { success = true; }
        @Override public void close() {
            synchronized (owner) {
                if (ended) { owner.gap("DUPLICATE_OPERATION_COMPLETION"); return; }
                ended = true;
                owner.mutation();
                try {
                    int i = activity.ordinal();
                    if (success) owner.completed[i] = Math.incrementExact(owner.completed[i]);
                    else { owner.failed[i] = Math.incrementExact(owner.failed[i]); owner.gap("OPERATION_FAILED"); }
                    if (owner.pending == 0 || owner.pendingByActivity[i] == 0) owner.gap("PENDING_ROSTER_MISMATCH");
                    else { owner.pending--; owner.pendingByActivity[i]--; }
                } catch (ArithmeticException failure) { owner.overflow = true; }
                owner.notifyAll();
            }
        }
    }

    synchronized void observe(Activity activity) { observe(activity, 1); }
    synchronized void observedOutcome(Activity activity, boolean success) {
        observe(activity);
        if (!success) {
            mutation();
            try {
                if (completed[activity.ordinal()] == 0) overflow = true;
                else completed[activity.ordinal()]--;
                failed[activity.ordinal()] = Math.incrementExact(failed[activity.ordinal()]);
            }
            catch (ArithmeticException failure) { overflow = true; }
            gap("OPERATION_FAILED");
        }
    }
    synchronized void observe(Activity activity, long count) {
        mutation();
        if (count < 0) { gap("INVALID_BASIS"); return; }
        try {
            attempts[activity.ordinal()] = Math.addExact(attempts[activity.ordinal()], count);
            completed[activity.ordinal()] = Math.addExact(completed[activity.ordinal()], count);
            for (int i = 0; i < 8; i++) upper[i] = Math.addExact(upper[i], Math.multiplyExact(activity.upper[i], count));
            checkLimits();
        } catch (ArithmeticException failure) { overflow = true; }
    }

    synchronized void material(int unit, long count) {
        mutation();
        if (unit < 0 || unit >= 8 || count < 0) { gap("INVALID_MATERIAL"); return; }
        try { material[unit] = Math.addExact(material[unit], count); }
        catch (ArithmeticException failure) { overflow = true; }
    }

    synchronized void inputRows(long count) {
        mutation();
        if (count < 0) { gap("INVALID_INPUT_ROW_COUNT"); return; }
        try { inputRowsAttempted = Math.addExact(inputRowsAttempted, count); }
        catch (ArithmeticException failure) { overflow = true; }
    }

    synchronized void gap(String reason) {
        mutation();
        if (!ascii(reason, MAX_REASON_CHARS)) {
            reason = "GAP_REASON_UNAVAILABLE";
            try { omitted = Math.incrementExact(omitted); } catch (ArithmeticException failure) { overflow = true; }
        }
        if (gaps.size() < MAX_GAPS) gaps.add(reason);
        else { try { omitted = Math.incrementExact(omitted); } catch (ArithmeticException failure) { overflow = true; } }
    }

    synchronized void epoch(long epoch, String window) {
        mutation();
        if (epochs.size() == MAX_EPOCHS) { gap("EPOCH_ROSTER_OVERFLOW"); return; }
        if (window == null || window.length() > MAX_WINDOW_BYTES) { gap("EPOCH_WINDOW_BOUND"); return; }
        byte[] encoded = window.getBytes(StandardCharsets.UTF_8);
        if (encoded.length > MAX_WINDOW_BYTES) { gap("EPOCH_WINDOW_BOUND"); return; }
        epochs.add(Map.of("epoch", epoch, "windowBase64", Base64.getEncoder().encodeToString(encoded)));
    }

    private void mutation() {
        try {
            generation = Math.incrementExact(generation);
            bookkeeping = Math.incrementExact(bookkeeping);
            for (int i = 0; i < 8; i++) upper[i] = Math.addExact(upper[i], Activity.BOOKKEEPING.upper[i]);
            if (bodySnapshot) {
                lateMutations = Math.incrementExact(lateMutations);
                if (seal != null) { seal.putLong(40, lateMutations); seal.putInt(12, 2); }
            }
            checkLimits();
        } catch (RuntimeException failure) {
            overflow = true;
            // A broken diagnostic mapping must not replace the business callback's result.
            if (seal != null) { try { seal.putInt(12, 2); } catch (RuntimeException unavailable) { omitted = Long.MAX_VALUE; } }
        }
    }

    private void checkLimits() {
        for (int i = 0; i < 8; i++) if (upper[i] > config.limits[i]) overBudget = true;
    }

    synchronized long connectorHashLimit() { return config.connectorBytes; }
    synchronized int argumentLimit() { return config.argumentCount; }
    synchronized long argumentBytesLimit() { return config.argumentBytes; }

    String snapshot() {
        synchronized (this) {
            observe(Activity.MBEAN_BUDGET);
            // This fixed synchronous serialization has no external read or wait.
            return json(evidence("LIVE_SNAPSHOT", false));
        }
    }

    private Map<String, Object> evidence(String stage, boolean finalBody) {
        var result = new LinkedHashMap<String, Object>();
        result.put("schemaVersion", 1); result.put("enabled", true);
        result.put("definitionId", DEFINITION_ID); result.put("sourceFormulaSha256", SOURCE_FORMULA_SHA256);
        result.put("inputSha256", config.inputSha); result.put("runId", config.runId);
        result.put("pid", pid); result.put("jvmStartTimeMillis", startMillis);
        result.put("stage", stage); result.put("lifecycle", lifecycle); result.put("generation", generation);
        result.put("nonceSha256", nonce == null ? "UNAVAILABLE" : sha(nonce.getBytes(StandardCharsets.US_ASCII)));
        result.put("state", clean() ? "RECORDED_STRUCTURAL_DIAGNOSTIC" : "UNKNOWN");
        result.put("limits", vector(config.limits)); result.put("materialLimits", config.materialLimits());
        result.put("actualMaterial", vector(material)); result.put("sourceUpper", vector(upper));
        result.put("inputRowsAttempted", inputRowsAttempted);
        result.put("sourceUpperIsActualWork", false); result.put("limitsIndependentlyVerified", false);
        var activities = new LinkedHashMap<String, Object>();
        for (Activity activity : Activity.values()) {
            int i = activity.ordinal();
            activities.put(activity.name(), Map.of("invocations", attempts[i], "completed", completed[i],
                    "failed", failed[i], "pending", pendingByActivity[i]));
        }
        result.put("activities", activities); result.put("bookkeepingInvocations", bookkeeping);
        result.put("epochs", List.copyOf(epochs)); result.put("pending", pending); result.put("openWriters", openWriters);
        result.put("omitted", omitted); result.put("overflow", overflow); result.put("overBudget", overBudget);
        result.put("gaps", List.copyOf(gaps)); result.put("lateMutations", lateMutations);
        result.put("sourceSealTailUpper", vector(SEAL_TAIL_UPPER));
        result.put("sealTailIncludedInSourceUpper", finalBody);
        result.put("actualSealTailInCompanion", finalBody);
        result.put("processExitObserved", false); result.put("crashDurabilityQualified", false);
        result.put("opaqueScope", "REQUESTS_AND_BOUNDED_MATERIAL_ONLY");
        result.put("excluded", "OPAQUE_PROVIDER_INSTRUCTIONS_CPU_ELAPSED_AND_VM_EXIT_INTERNALS");
        result.put("costUnit", "STRUCTURAL_WORK_VECTOR_NOT_CPU_OR_ELAPSED");
        for (String flag : List.of("structuralBudgetQualified", "accountingErrorBoundQualified", "wholeMethodCostQualified",
                "collectionCostUpperBoundQualified", "samplingCostQualified", "causalOverheadQualified",
                "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) result.put(flag, false);
        return result;
    }

    private boolean clean() { return config.failure == null && gaps.isEmpty() && !overflow && !overBudget && omitted == 0 && pending == 0; }

    boolean arm(String absolutePath, String requestedNonce, Runnable unregisterAction, boolean hook) {
        Span operation = enter(Activity.ARM);
        synchronized (this) {
            if (armAttempted) { gap("FINAL_ARM_REPEATED"); operation.close(); return false; }
            armAttempted = true;
        }
        try {
            if (!ascii(absolutePath, 4096) || !ascii(requestedNonce, 64) || requestedNonce.isEmpty()) {
                gap("FINAL_PATH_OR_NONCE_UNSUPPORTED"); return false;
            }
            Path path = Path.of(absolutePath);
            if (!path.isAbsolute() || !path.equals(path.normalize()) || path.getParent() == null
                    || !Files.isDirectory(path.getParent()) || !path.getParent().equals(path.getParent().toRealPath()) || Files.exists(path)) {
                gap("FINAL_PATH_UNAVAILABLE"); return false;
            }
            MappedByteBuffer mapped;
            try (FileChannel channel = FileChannel.open(Path.of(absolutePath + ".seal"),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                channel.position(SEAL_BYTES - 1);
                if (channel.write(ByteBuffer.wrap(new byte[1])) != 1) throw new IOException("incomplete companion allocation");
                mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, SEAL_BYTES);
            }
            mapped.order(ByteOrder.BIG_ENDIAN);
            mapped.put(0, "WRBSEAL1".getBytes(StandardCharsets.US_ASCII));
            mapped.putInt(8, 1); mapped.putLong(16, pid); mapped.putLong(24, startMillis);
            mapped.put(48, digest(requestedNonce.getBytes(StandardCharsets.US_ASCII)));
            mapped.put(112, HexFormat.of().parseHex(SOURCE_FORMULA_SHA256));
            mapped.put(144, HexFormat.of().parseHex(config.inputSha)); mapped.putLong(208, 1);
            synchronized (this) {
                seal = mapped; bodyPath = path; nonce = requestedNonce;
                unregister = unregisterAction;
            }
            if (hook) Runtime.getRuntime().addShutdownHook(new Thread(() -> finish(CLOSE_WAIT_NANOS), "write-return-budget-seal"));
            operation.success();
            return true;
        } catch (IOException | RuntimeException failure) {
            gap("FINAL_ARM_UNAVAILABLE"); return false;
        } finally { operation.close(); }
    }

    /** One bounded owner wait. External final IO runs on one daemon, without a bookkeeping lock. */
    void finish(long waitNanos) {
        if (waitNanos < 1 || waitNanos > CLOSE_WAIT_NANOS) { gap("FINAL_WAIT_UNSUPPORTED"); return; }
        synchronized (this) {
            if (shutdownEntered || seal == null) { gap("FINAL_NOT_ARMED_OR_REPEATED"); return; }
            shutdownEntered = true; lifecycle = "SHUTDOWN_HOOK"; seal.putLong(216, 1);
            observe(Activity.SHUTDOWN);
            observe(Activity.FINAL_OWNER);
        }
        CountDownLatch done = new CountDownLatch(1);
        long deadline = System.nanoTime() + waitNanos;
        Thread worker = new Thread(() -> {
            try { finalizeBody(deadline); }
            finally { done.countDown(); }
        }, "write-return-budget-body");
        worker.setDaemon(true);
        try { worker.start(); }
        catch (RuntimeException unavailable) { gap("FINALIZATION_LAUNCH_FAILED"); refuseSeal(); return; }
        try {
            if (!done.await(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
                gap("FINALIZATION_PENDING"); refuseSeal();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); gap("FINALIZATION_INTERRUPTED"); refuseSeal();
        }
    }

    private void finalizeBody(long deadline) {
        byte[] body;
        long atBody;
        boolean forcingMapping = false;
        try {
            Runnable action;
            synchronized (this) { action = unregister; }
            if (action != null) action.run();
            synchronized (this) {
                while (pending != 0 || openWriters != 0) {
                    // Deadline/wait invocations use the actual iteration count, including spurious wakes.
                    observe(Activity.FINAL_WAIT_ITERATION);
                    long left = deadline - System.nanoTime();
                    if (left <= 0) { gap("FINAL_QUIESCENCE_UNPROVEN"); break; }
                    TimeUnit.NANOSECONDS.timedWait(this, left);
                }
                if (pending != 0 || openWriters != 0) gap("FINAL_QUIESCENCE_UNPROVEN");
                for (int i = 0; i < 8; i++) upper[i] = Math.addExact(upper[i], SEAL_TAIL_UPPER[i]);
                checkLimits();
                bodySnapshot = true; atBody = generation;
                body = json(evidence("SHUTDOWN_HOOK", true)).getBytes(StandardCharsets.US_ASCII);
            }
            // One bounded write attempt. A short write is retained as refusal, never retried.
            try (FileChannel channel = FileChannel.open(bodyPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                if (channel.write(ByteBuffer.wrap(body)) != body.length) throw new IOException("incomplete final body write");
                channel.force(true);
            }
            synchronized (this) {
                seal.putLong(200, 1); seal.putLong(32, body.length); seal.put(80, digest(body));
                seal.putLong(176, atBody); seal.putLong(184, 1);
            }
            forcingMapping = true;
            seal.force();
            forcingMapping = false;
            synchronized (this) {
                seal.putLong(184, 2);
                seal.putInt(12, clean() && lateMutations == 0 && pending == 0 && openWriters == 0 ? 1 : 2);
            }
            forcingMapping = true;
            seal.force();
            forcingMapping = false;
            synchronized (this) {
                if (!clean() || lateMutations != 0 || pending != 0 || openWriters != 0) seal.putInt(12, 2);
            }
        } catch (IOException | RuntimeException | InterruptedException failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            synchronized (this) {
                gap("FINAL_BODY_OR_FORCE_FAILED");
                if (seal != null) { if (forcingMapping) seal.putLong(192, 1); seal.putInt(12, 2); }
            }
        }
    }

    private synchronized void refuseSeal() { if (seal != null) seal.putInt(12, 2); }

    static String disabled() {
        return "{\"schemaVersion\":1,\"enabled\":false,\"state\":\"DISABLED\",\"structuralBudgetQualified\":false,\"costAcceptanceEligible\":false,\"performanceAcceptanceEligible\":false,\"formalPerformance\":false}";
    }

    private static String formula() {
        var result = new StringBuilder("WRITE_RETURN_PRODUCER_WORK_V1;units=calls,requests,records,frames,bytes,passes,elements,updates;opaque=requests;actual=basis;upper=checked-allocation;native-copy-requests=2053;native-copy-bytes=16593;hash-material=16MiB+4*256KiB+5*8192;hash-requests=2*(hash-material+5)+2053+1024;clock-transfer=4*(hash-material+16593)+65536;clock-elements-updates=8*(hash-material+16593)+65536;key-shape=2;frame=65536;call-rows=1024;error-entries=512;epochs=16;gaps=32;reason=128;config-depth=4;config-tokens=64;config-string=1024;config-number=20;seal=256;request-unit=explicit-JDK-provider-and-boxing-invocations;json-map=5+7*entries;json-list=4+3*elements;json-string=3+3*chars;json-scalar=2;ascii=2+2*chars;");
        result.append("rootFields=").append(String.join(",", ROOT_FIELDS)).append(';');
        result.append("activityNames=").append(String.join(",", ACTIVITY_NAMES)).append(';');
        result.append("report=").append(REPORT_MAPS).append(',').append(REPORT_LISTS).append(',')
                .append(REPORT_ENTRIES).append(',').append(REPORT_NODES).append(',').append(REPORT_STRING_CHARS)
                .append(',').append(REPORT_JSON_CHARS).append(',').append(REPORT_BUILD_REQUESTS).append(',')
                .append(REPORT_JSON_REQUESTS).append(',').append(REPORT_REQUESTS).append(';');
        result.append("seal-tail="); for (long n : SEAL_TAIL_UPPER) result.append(n).append(','); result.append(';');
        for (Activity a : Activity.values()) { result.append(a.name()).append('='); for (long n : a.upper) result.append(n).append(','); result.append(';'); }
        return result.toString();
    }

    private static long add(long... values) {
        long total = 0; for (long value : values) total = Math.addExact(total, value); return total;
    }
    private static long mul(long left, long right) { return Math.multiplyExact(left, right); }
    private static long chars(List<String> values) {
        long total = 0; for (String value : values) total = Math.addExact(total, value.length()); return total;
    }

    private static Map<String, Object> vector(long[] value) {
        var result = new LinkedHashMap<String, Object>();
        for (int i = 0; i < 8; i++) result.put(UNITS.get(i), value[i]);
        return result;
    }

    private static String json(Map<String, Object> evidence) {
        String value = JsonWriter.write(evidence);
        if (!ascii(value, MAX_RECEIPT_BYTES)) throw new AssertionError("structural receipt exceeds its bounded transport");
        return value;
    }

    private static boolean ascii(String value, int max) {
        if (value == null || value.length() > max) return false;
        for (int i = 0; i < value.length(); i++) if (value.charAt(i) < 32 || value.charAt(i) > 126) return false;
        return true;
    }

    private static byte[] digest(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static String sha(byte[] bytes) { return HexFormat.of().formatHex(digest(bytes)); }

    // Aggregate limits are independently registered admission ceilings, not future lifetime bounds.
    // Complete actual invocation basis times the source matrix bounds this run independently; neither
    // caller ceilings nor the observed prefix certify cost or replace the frozen performance gates.
    private record Config(String inputSha, String runId, long[] limits, long connectorBytes,
                          int argumentCount, long argumentBytes, String failure) {
        Map<String, Object> materialLimits() {
            return Map.of("connectorHashBytes", connectorBytes, "runtimeArguments", argumentCount,
                    "runtimeArgumentBytes", argumentBytes, "primaryKeys", 2);
        }
        static Config parse(String encoded) {
            try {
                if (encoded == null || encoded.length() > 4 * ((MAX_INPUT_BYTES + 2) / 3)) throw new IllegalArgumentException();
                byte[] decoded = Base64.getDecoder().decode(encoded);
                if (decoded.length > MAX_INPUT_BYTES || !Base64.getEncoder().encodeToString(decoded).equals(encoded)) throw new IllegalArgumentException();
                String raw = new String(decoded, StandardCharsets.UTF_8);
                if (!ascii(raw, MAX_INPUT_BYTES) || !java.util.Arrays.equals(decoded, raw.getBytes(StandardCharsets.UTF_8))) throw new IllegalArgumentException();
                lexicalBounds(raw);
                Map<?, ?> root = object(JsonReader.parse(raw), Set.of("schemaVersion", "definitionId", "sourceFormulaSha256", "inputSha256", "runId", "limits", "materialLimits"));
                // Round-trip equality also refuses duplicate keys, noncanonical numbers and escapes.
                if (!JsonWriter.write(root).equals(raw) || number(root.get("schemaVersion")) != 1
                        || !DEFINITION_ID.equals(root.get("definitionId")) || !SOURCE_FORMULA_SHA256.equals(root.get("sourceFormulaSha256"))) throw new IllegalArgumentException();
                String input = hex(root.get("inputSha256"));
                if (!(root.get("runId") instanceof String run) || !ascii(run, 128) || run.isEmpty()) throw new IllegalArgumentException();
                Map<?, ?> limits = object(root.get("limits"), Set.copyOf(UNITS));
                long[] values = new long[8]; for (int i = 0; i < 8; i++) values[i] = number(limits.get(UNITS.get(i)));
                Map<?, ?> materials = object(root.get("materialLimits"), Set.of("connectorHashBytes", "runtimeArguments", "runtimeArgumentBytes", "primaryKeys"));
                long connector = number(materials.get("connectorHashBytes")), arguments = number(materials.get("runtimeArguments"));
                long argumentBytes = number(materials.get("runtimeArgumentBytes"));
                if (connector < 1 || connector > MAX_CONNECTOR_BYTES || arguments > MAX_ARGUMENTS
                        || argumentBytes > MAX_ARGUMENT_BYTES || number(materials.get("primaryKeys")) != 2) throw new IllegalArgumentException();
                return new Config(input, run, values, connector, Math.toIntExact(arguments), argumentBytes, null);
            } catch (RuntimeException refused) {
                return new Config("0".repeat(64), "UNAVAILABLE", new long[8], 0, 0, 0, "INPUT_BINDING_UNAVAILABLE");
            }
        }
        private static Map<?, ?> object(Object value, Set<String> keys) {
            if (!(value instanceof Map<?, ?> map) || !map.keySet().equals(keys)) throw new IllegalArgumentException();
            return map;
        }
        private static void lexicalBounds(String raw) {
            int depth = 0, tokens = 0, stringLength = 0, numberLength = 0;
            boolean quoted = false, escaped = false, number = false;
            for (int i = 0; i < raw.length(); i++) {
                char c = raw.charAt(i);
                if (quoted) {
                    if (++stringLength > 1024) throw new IllegalArgumentException();
                    if (escaped) escaped = false;
                    else if (c == '\\') escaped = true;
                    else if (c == '"') quoted = false;
                } else if (c >= '0' && c <= '9' || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') {
                    if (!number) { if (++tokens > 64) throw new IllegalArgumentException(); numberLength = 0; }
                    number = true;
                    if (++numberLength > 20) throw new IllegalArgumentException();
                } else if (c == '"') {
                    number = false;
                    quoted = true; stringLength = 0;
                    if (++tokens > 64) throw new IllegalArgumentException();
                } else if (c == '{' || c == '[') {
                    number = false;
                    if (++depth > 4 || ++tokens > 64) throw new IllegalArgumentException();
                } else if (c == '}' || c == ']') {
                    number = false;
                    if (--depth < 0) throw new IllegalArgumentException();
                } else { number = false; if (c == 't' || c == 'f' || c == 'n') if (++tokens > 64) throw new IllegalArgumentException(); }
            }
            if (quoted || depth != 0) throw new IllegalArgumentException();
        }
        private static long number(Object value) {
            if (!(value instanceof Long n) || n < 0) throw new IllegalArgumentException(); return n;
        }
        private static String hex(Object value) {
            if (!(value instanceof String s) || s.length() != 64) throw new IllegalArgumentException();
            for (int i = 0; i < s.length(); i++) if (!(s.charAt(i) >= '0' && s.charAt(i) <= '9' || s.charAt(i) >= 'a' && s.charAt(i) <= 'f')) throw new IllegalArgumentException();
            return s;
        }
    }
}
