package io.tapstate.e2e;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Static material/source bounds for entered return components, independent of measured sizes. */
final class BenchmarkReturnComponentSourceWork {
    static final int CLASSES = 512, CALLS = BenchmarkReturnLocalSourceWork.MAX_STORED_CALLS, SAMPLES = BenchmarkCausalClock.MAX_SAMPLES, PAGES = 512, ORIGINS = 1024, FILES = 10;
    static final int BATCHES = 960, HELLO = 6000, PHASES = 6, ROWS = 262144;
    static final int NATIVE_CHARS = 8192, RAW_PAGE = 65536, RAW_TOTAL = 2097152;
    static final int META_NODES = 4096, COLD_NODES = 32768;
    static final long META_STRINGS = 65536, COLD_STRINGS = 4L * 2080768 + 4L * 8192;
    static final int UNITS = 8, FACTORY_FIELDS = 48;
    static final int ACK_FLAGS = 18, ACK_READING_FIELDS = 9, ACK_COMPARISON_FIELDS = 9;
    static final int ACK_PAIR_FIELDS = 6 + ACK_FLAGS, ACK_SERIES_FIELDS = 11 + ACK_FLAGS;
    static final int ACK_COVERAGE_FIELDS = ACK_SERIES_FIELDS + 2, ACK_MAPPING_FIELDS = 15 + ACK_FLAGS;
    // Nineteen checked expression groups below use 176 helper/Math/header/constructor slots.
    // Two eight-coordinate guards and Work.plus's method/eight checked adds/constructor add
    // sixteen and ten slots. This allowance is repeated conservatively for every table kind.
    static final int ACK_FORMULA_SLOTS = 176 + 2 * UNITS + 10;
    // Three bounded tree formulas fit 96 checked/accessor/helper slots. Three component-wise
    // maxima use eight Math.max calls and sixteen accessors each; merge/copy/boxing uses 24 slots.
    static final int FORMULA_SLOTS = 96 + ACK_FORMULA_SLOTS, MAXIMUM_SLOTS = 3 * 24, TABLE_ENTRY_SLOTS = 24;
    enum Kind {
        STRUCTURAL_RECIPE, PREFLIGHT, PHASE_FACTS, VERIFY_PHASE, METHOD_EVIDENCE,
        INSPECTION_COPY, DELIVERIES_COPY, TREE_FREEZE, REFUSAL,
        METRIC_PHASE, METRIC_POOL, METRIC_RESULT, METRIC_BRIDGE, COMPARISON, REAL_STATISTICS, FLOAT_STATISTICS,
        AUTHORITY_CHECK, AUTHORITY_EVIDENCE, COMPARISON_INPUT, COMPONENT_FACTORY,
        RUN_TRUSTED_ACQUIRE, RUN_TRUSTED_TOKEN, RUN_ROOT_BEFORE, RUN_ROOT_BASIS, RUN_ROOT_WRAPPER, RUN_ROOT_COMPARE
    }
    record Limits(int sourceRoles, BenchmarkReturnWorkBudgetDefinitions.OutputPlan report) {
        Limits {
            require(sourceRoles == BenchmarkReturnWorkBudgetBindings.SourceRole.values().length
                    && sourceRoles <= CLASSES, "source role roster differs");
            require(report != null, "component source inputs are missing");
        }
    }
    record PreparedBounds(Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> additions,
                          Map<String, Object> sourceEvidence) {
        PreparedBounds { additions = Map.copyOf(additions); sourceEvidence = Map.copyOf(sourceEvidence); }
    }
    private BenchmarkReturnComponentSourceWork() { }

    static BenchmarkReturnWorkBudget.Activity actor(Kind kind) {
        return switch (kind) {
            case STRUCTURAL_RECIPE, PREFLIGHT, METHOD_EVIDENCE, INSPECTION_COPY, AUTHORITY_CHECK, AUTHORITY_EVIDENCE, COMPONENT_FACTORY,
                    RUN_TRUSTED_ACQUIRE, RUN_TRUSTED_TOKEN, RUN_ROOT_BEFORE, RUN_ROOT_BASIS, RUN_ROOT_WRAPPER, RUN_ROOT_COMPARE -> BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS;
            case PHASE_FACTS, VERIFY_PHASE, DELIVERIES_COPY, METRIC_PHASE, METRIC_POOL, METRIC_RESULT, METRIC_BRIDGE -> BenchmarkReturnWorkBudget.Activity.ASSOCIATION;
            case TREE_FREEZE -> BenchmarkReturnWorkBudget.Activity.COPY;
            case REFUSAL -> BenchmarkReturnWorkBudget.Activity.FAILURE_RETENTION;
            case COMPARISON, REAL_STATISTICS, FLOAT_STATISTICS, COMPARISON_INPUT -> BenchmarkReturnWorkBudget.Activity.COMMON_WINDOW;
        };
    }

    static Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> additions(Limits limits) {
        return prepare(limits).additions();
    }

    static PreparedBounds prepare(Limits limits) {
        return entered(Kind.COMPONENT_FACTORY, () -> prepareBounded(limits));
    }

    private static PreparedBounds prepareBounded(Limits limits) {
        require(limits != null, "component source inputs are missing");
        var out = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (Kind kind : Kind.values()) out.merge(actor(kind), upper(kind, limits), BenchmarkReturnComponentSourceWork::maximum);
        // Existing p99/COPY entries can consume the whole pooled roster rather than one phase.
        // Do not create new SORT entries or invent a workload/batch multiplier for this material cap.
        out.merge(BenchmarkReturnWorkBudget.Activity.SORT, pooledSortUpper(), BenchmarkReturnComponentSourceWork::maximum);
        out.merge(BenchmarkReturnWorkBudget.Activity.COPY, pooledCopyUpper(), BenchmarkReturnComponentSourceWork::maximum);
        BenchmarkReturnCaptureMaterialBounds.upper().forEach((activity, bound) ->
                out.merge(activity, bound, BenchmarkReturnComponentSourceWork::maximum));
        BenchmarkReturnBridgeSourceWork.additions().forEach((activity, bound) ->
                out.merge(activity, bound, BenchmarkReturnComponentSourceWork::maximum));
        out.merge(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                BenchmarkReturnWorkBudgetRun.nativeClassPacketAfterExitUpper(), BenchmarkReturnComponentSourceWork::maximum);
        var vectors = new LinkedHashMap<String, Object>();
        for (var actor : BenchmarkReturnWorkBudget.Activity.values()) {
            var value = out.get(actor);
            if (value != null) vectors.put(actor.name(), value.evidence());
        }
        var evidence = new LinkedHashMap<String, Object>();
        evidence.put("state", "STATIC_SOURCE_BOUNDS_REQUIRING_COMPLETE_ACTUAL_ENTRIES");
        evidence.put("basis", "PER_ENTERED_BODY_THIS_RUN_ONLY");
        evidence.put("sourceRoleCount", limits.sourceRoles()); evidence.put("kindCount", Kind.values().length);
        evidence.put("perOperationUpper", Map.copyOf(vectors));
        evidence.put("factoryUpperPerEntry", factoryUpper(1).evidence());
        evidence.put("pooledChildSourceUpper", Map.of("SORT", pooledSortUpper().evidence(), "COPY", pooledCopyUpper().evidence()));
        evidence.put("pooledMaterialRows", ROWS);
        evidence.put("pooledMaterialBasis", "DECLARED_POOLED_INPUT_CAP_NOT_SINGLE_PHASE_MAX_OR_FUTURE_CALL_COUNT");
        evidence.put("materialLimits", Map.of("calls", CALLS, "samples", SAMPLES, "pages", PAGES,
                "rows", ROWS, "sourceBatches", BATCHES, "serialHello", HELLO, "phases", PHASES));
        evidence.put("priorReportPlan", Map.of("nodeUpper", limits.report().nodeUpper(), "depthUpper", limits.report().depthUpper(),
                "leafUtf8Upper", limits.report().leafUtf8Upper(), "stringUtf8Upper", limits.report().stringUtf8Upper(),
                "keyUtf8Upper", limits.report().keyUtf8Upper(), "numberTextUpper", limits.report().numberTextUpper()));
        evidence.put("opaqueExcluded", "PROVIDER_INSTRUCTIONS_CPU_ELAPSED_AND_VM_ALLOCATION_INTERIORS");
        evidence.put("actualUpperUsedAsPrior", false);
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> evidence.put(flag, false));
        return new PreparedBounds(out, evidence);
    }

    static BenchmarkReturnWorkBudget.Work upper(Kind kind, Limits limits) {
        long roles = limits.sourceRoles();
        return switch (kind) {
            // requireDefinition() hashes the fixed definition/input/row/seed scalars, optional
            // bounded writer tuple and enum-ordered eight-long rows. It constructs no workload.
            // The fixed 65536 text envelope covers those source fields; append/accessor/Map/enum
            // and the one UTF-8/hash/hex request pipeline fit 256+64 requests per activity row.
            case STRUCTURAL_RECIPE -> row(256L + 64L * BenchmarkReturnWorkBudget.Activity.values().length,
                    1, 0, 4L * 65536, 4, 65536, 128);
            // One bounded native JSON parse, two source scans, four full class scans and fixed binding comparisons.
            case PREFLIGHT -> row(add(512, mul(32, roles), mul(64, CLASSES), mul(16, ORIGINS), mul(32, FILES),
                    jsonRequests(NATIVE_CHARS)), roles + CLASSES, 0,
                    add(mul(8, NATIVE_CHARS), mul(16, roles + CLASSES + ORIGINS + FILES)), 12,
                    add(NATIVE_CHARS, roles, mul(4, CLASSES), ORIGINS, FILES), roles + CLASSES + ORIGINS);
            case PHASE_FACTS -> row(add(128, mul(24, CALLS), mul(16, SAMPLES), mul(8, PAGES), mul(16, BATCHES), mul(12, HELLO)),
                    ROWS, CALLS, add(mul(16, BATCHES + HELLO + CALLS + SAMPLES), mul(4, RAW_TOTAL)),
                    8, ROWS + BATCHES + HELLO + CALLS + SAMPLES + PAGES, BATCHES + HELLO + CALLS + SAMPLES);
            // Comparator key extraction can run twice per comparison; C^2 is a source bound, not an observed total.
            // Existing raw decode/association/map/p99/common helpers keep their independently counted child entries.
            // Six phase projections, two target accessors/maps per phase and four bounded chain
            // descriptions add fixed source calls. SQL scalar traversal is the separately entered shared guard.
            case VERIFY_PHASE -> row(add(1024, mul(8, mul(CALLS, CALLS)), mul(64, CALLS), mul(32, SAMPLES),
                    mul(32, BATCHES), mul(128, HELLO), mul(4, jsonRequests(NATIVE_CHARS)), mul(64, 6 * 2 + 4)),
                    ROWS, CALLS, add(mul(32, CALLS + BATCHES + HELLO), mul(32, ROWS), mul(4, 4 * 6L * 512)), 16,
                    add(mul(CALLS, CALLS), mul(4, SAMPLES), mul(4, BATCHES), mul(4, HELLO), ROWS, 6 * 2 + 4 * 6),
                    add(CALLS, BATCHES, HELLO, ROWS, 6 * 2 + 4)).plus(ackCalibrationUpper());
            case METHOD_EVIDENCE -> row(256, 0, 0, 8192, 4, 128, 64);
            case INSPECTION_COPY -> row(64, 0, 0, 512, 2, 16, 16);
            case DELIVERIES_COPY -> row(add(32, mul(8, ROWS)), ROWS, 0, mul(16, ROWS), 2, ROWS, ROWS);
            case TREE_FREEZE -> maximum(treeUpper(META_NODES, META_STRINGS),
                    maximum(treeUpper(COLD_NODES, COLD_STRINGS), treeUpper(limits.report().nodeUpper(), limits.report().stringUtf8Upper())));
            case REFUSAL -> row(128, 0, 0, 8192, 2, 64, 32);
            case METRIC_PHASE -> row(add(64, mul(8, ROWS)), ROWS, 0, mul(16, ROWS), 2, ROWS, ROWS);
            // Six phase references, four checked accumulators, one pooled reference copy and the fixed decimal rate pipeline.
            case METRIC_POOL -> row(add(256, mul(32, PHASES), mul(8, ROWS)), ROWS, 0,
                    add(mul(16, ROWS), 8192), 4, ROWS + 8L * PHASES, ROWS + 4L * PHASES);
            case METRIC_RESULT -> row(64, 0, 0, 4096, 2, PHASES, PHASES);
            // Four owned certificate scalar lookups and one full immutable latency projection.
            // The steady gate and Phase copy retain their independent child entries.
            case METRIC_BRIDGE -> row(add(128, mul(8, ROWS)), ROWS, 0, mul(16, ROWS), 2, ROWS, ROWS);
            // At most four five-point summaries per workload: initial A/B plus legacy A/B.
            case COMPARISON -> row(add(512, mul(3, 10 * 32L), mul(12, 64)), 30, 0, 32768, 16, 256, 128);
            case REAL_STATISTICS -> row(512, 5, 0, 16384, 12, 128, 64);
            case FLOAT_STATISTICS -> row(768, 5, 0, 32768, 16, 192, 96);
            // Six invocation entries, sixteen route fields, four classes and bounded printable scalars.
            case AUTHORITY_CHECK -> row(add(128, mul(8, 16), mul(8, 4), mul(4, 6), mul(2, 512 + 36 + 20)),
                    0, 0, 4L * (512 + 36 + 20), 4, 16 + 4 + 6 + 512 + 36 + 20, 32);
            case AUTHORITY_EVIDENCE -> row(128, 0, 0, 8192, 4, 64, 48);
            case COMPARISON_INPUT -> row(128, 0, 0, 16384, 2, 64, 32);
            case COMPONENT_FACTORY -> maximum(factoryUpper(1).plus(bridgeProjectionUpper()), BenchmarkReturnWorkBudgetReportPlan.returnCatalogueUpper(1));
            // Exact entered Run body: expected ROOT map <=1024, observed ROOT <=512, runtime
            // tuple (128/4096/128 text), writer equality and original one cold ASCII reply <=8192.
            // The real native metadata and structural recipe children keep separate actual entries.
            case RUN_TRUSTED_ACQUIRE -> row(add(512, mul(16, ORIGINS), mul(32, CLASSES),
                    mul(8, NATIVE_CHARS), mul(8, 128 + 4096 + 128)), ORIGINS + CLASSES, 0,
                    add(mul(32, ORIGINS + CLASSES), mul(4, 128 + 4096 + 128), mul(2, NATIVE_CHARS)),
                    4, ORIGINS + CLASSES + NATIVE_CHARS + 128 + 4096 + 128, ORIGINS + 16);
            case RUN_TRUSTED_TOKEN -> row(64, 0, 0, 512, 1, 16, 2);
            case RUN_ROOT_BEFORE -> row(add(64, mul(4, 128)), 0, 0, 512, 2, 144, 4);
            case RUN_ROOT_BASIS -> row(add(64, mul(4, 128)), 0, 0, 512, 1, 144, 2);
            // The shallow wrapper visits at most128 keys before its copy, without walking
            // nested values. Each bounded key retains references to already acquired payloads.
            case RUN_ROOT_WRAPPER -> row(add(128, mul(16, 128)), 0, 0,
                    add(mul(64, 128), mul(4, mul(128, 128))), 3, 3L * 128, 2L * 128);
            // Two compact source-native packets: lexical guard, owned decoder and canonical
            // writer; fixed sixteen-field/four-class snapshot equality follows both parses.
            case RUN_ROOT_COMPARE -> row(add(256, mul(2, jsonRequests(NATIVE_CHARS)), mul(8, 16), mul(8, 4)),
                    0, 0, add(mul(16, NATIVE_CHARS), mul(64, 512)), 8,
                    add(mul(4, NATIVE_CHARS), mul(4, 512), 16, 4), 4L * 512 + 32);
        };
    }

    /** Additional source allowance for each actually entered ACK helper or phase body. */
    static BenchmarkReturnWorkBudget.Work ackCalibrationUpper() {
        // All four helpers enter VERIFY_PHASE. The coverage wrapper enters the series child
        // separately. Summing all four local bodies for each entry is permitted overlap, not
        // a limit on future invocations. HELLO/SAMPLES are checked before owned roster copies.
        // Reading guards: one helper call, eight identity accessor/length/isEmpty APIs and
        // three bracket accessors. Pair adds two guarded readings, six equality APIs and
        // six serial-order accessors. String/List/provider interiors remain opaque requests.
        long readingGuard = 1 + 8 + 3;
        long pairGuard = add(1, mul(2, readingGuard), 6, 6);
        // Numeric comparison: one helper, twenty-two field accessors, five subtractExact
        // plus one ceilDiv, nine put calls, seven boxing APIs, constructor/copy/get/unbox.
        // The legacy numeric predicate is reproduced without message formatting or encoding.
        long comparison = 1 + 22 + 6 + 9 + 7 + 4;
        long carrier = 1 + 1 + 6; // Reading.evidence, Map.of and six numeric boxing APIs
        long pairRequests = add(16, pairGuard, headerRequests(ACK_PAIR_FIELDS), mul(2, carrier), comparison);
        // Per series row: three list APIs, raw carrier + mutable/final map + two puts/add,
        // pair guard, gap/max arithmetic, two comparisons and two get/equals status checks.
        long rowRequests = add(3, carrier, 1, 2, 1, 1, pairGuard, 2, mul(2, comparison), 4);
        long rosterRow = add(1, readingGuard, 1); // indexed get, guard, owned add
        long seriesRequests = add(16, 8, 6, headerRequests(ACK_SERIES_FIELDS), mul(HELLO, add(rowRequests, rosterRow)));
        // Coverage's series child has its own actual entry. Its local work is one additional
        // bounded roster copy, four endpoint accessors and eight state/copy/boxing APIs.
        long coverageRequests = add(16, 8, 4, 8, mul(HELLO, rosterRow));
        // Each mapping sample uses two iterator APIs, its owned accessor, seven enclosure
        // constructor/helper/checked APIs, three width APIs, max and four bracket APIs.
        // The fixed integer mapper evidence has at most 10 + 12 + 2 put/copy source fields.
        long bindingFields = 10 + 12 + 2;
        long mappingRequests = add(16, 2, 4, 6, 2, headerRequests(ACK_MAPPING_FIELDS),
                headerRequests(bindingFields), mul(SAMPLES, 2 + 1 + 7 + 3 + 1 + 4));
        long requests = add(pairRequests, seriesRequests, coverageRequests, mappingRequests);

        // Map entries carry two eight-byte scalar/reference slots. The series retains an
        // original nine-field carrier, two eleven-field copies, and two comparisons with
        // mutable/final nine-field maps. Boxing carries eight-byte scalar material; this
        // inventory is not a claim about VM object size, exclusive allocation or CPU cost.
        long diagnosticBytes = add(mul(16, mul(2, ACK_COMPARISON_FIELDS)), 8L * 7);
        long seriesRowBytes = add(mul(16, ACK_READING_FIELDS + 2L * (ACK_READING_FIELDS + 2)),
                8L * 6, mul(2, diagnosticBytes), 32);
        long pairBytes = add(mul(48, ACK_PAIR_FIELDS), mul(2, 16L * ACK_READING_FIELDS + 8L * 6), diagnosticBytes, 16);
        long seriesBytes = add(mul(HELLO, seriesRowBytes), mul(48, ACK_SERIES_FIELDS));
        long coverageBytes = add(mul(16, HELLO), mul(48, ACK_COVERAGE_FIELDS));
        long mappingBytes = add(mul(32, SAMPLES), mul(48, ACK_MAPPING_FIELDS + bindingFields));
        long fields = add(mul(2, ACK_PAIR_FIELDS + ACK_SERIES_FIELDS + ACK_COVERAGE_FIELDS + ACK_MAPPING_FIELDS + bindingFields),
                mul(2, ACK_READING_FIELDS), mul(2, ACK_COMPARISON_FIELDS),
                mul(HELLO, ACK_READING_FIELDS + 2L * (ACK_READING_FIELDS + 2) + 4L * ACK_COMPARISON_FIELDS));
        long references = mul(2, add(SAMPLES, HELLO, HELLO));
        // At most eight owned primitive argument/local/update slots surround a source API.
        // Variable Map.of operands and copied entries are added explicitly as two field slots,
        // so JDK requests are never replaced by elements or updates. There are no new frames,
        // JSON operations, clock observations, native provider requests or IO in these bodies.
        long cells = add(mul(8, requests), mul(2, fields), references);
        return new BenchmarkReturnWorkBudget.Work(4, requests, add(SAMPLES, 2, mul(2, HELLO)), 0,
                add(pairBytes, seriesBytes, coverageBytes, mappingBytes), 3 + 7 + 3 + 4, cells, cells);
    }

    private static long headerRequests(long fields) {
        // Construction/copy/forEach plus put, possible boxing and the owned flag callback.
        return add(3, mul(3, fields));
    }

    static BenchmarkReturnWorkBudget.Work pooledSortUpper() {
        // Two explicit stream/map/sorted/skip/find/orElse pipelines. Four per-element slots
        // per pipeline cover the owned lambda, interval accessor, Long boxing and carrier.
        // The sorting provider's internal comparisons/CPU remain outside this source inventory.
        return new BenchmarkReturnWorkBudget.Work(add(4, mul(2, ROWS)), add(32, mul(8, ROWS)),
                ROWS, 0, mul(32, ROWS), 2, mul(2, ROWS), mul(2, ROWS));
    }

    static BenchmarkReturnWorkBudget.Work pooledCopyUpper() {
        // One List.copyOf or stream/map/toList projection over the declared full pooled material.
        // Complete actual COPY entries multiply this bound; provider implementation is opaque.
        return new BenchmarkReturnWorkBudget.Work(add(8, ROWS), add(64, mul(4, ROWS)), ROWS, 0,
                mul(16, ROWS), 2, mul(2, ROWS), ROWS);
    }

    /** Actual table entries multiply this source bound; the count is not a future invocation cap. */
    static BenchmarkReturnWorkBudget.Work factoryUpper(long entries) {
        require(entries >= 0, "factory entry count is negative");
        long kinds = Kind.values().length, actors = BenchmarkReturnWorkBudget.Activity.values().length;
        long fields = add(FACTORY_FIELDS, mul(UNITS, actors));
        // The 24 per-field slots include accessors, boxing, Map entries/put/copy and immutable
        // carriers. Ninety-six scalar operands plus eight temporary eight-long vectors cover
        // formula varargs, Work validation/material and maxima. No JSON encoder is invoked here.
        // Ninety-six additional slots cover the two independent pooled row formulas and merges:
        // each has at most ten arithmetic/helper slots, 24 maximum APIs and one map merge.
        long requests = add(64, 96, 256, 64, mul(kinds, FORMULA_SLOTS + MAXIMUM_SLOTS + TABLE_ENTRY_SLOTS), mul(24, fields));
        long scalarSlots = add(mul(kinds, FORMULA_SLOTS + 8L * UNITS), mul(2, actors), kinds, 32, mul(12, fields));
        return new BenchmarkReturnWorkBudget.Work(1, requests, kinds, 0, mul(8, scalarSlots),
                4, add(mul(2, scalarSlots), fields), add(scalarSlots, fields)).times(entries);
    }

    static BenchmarkReturnWorkBudget.Work bridgeProjectionUpper() {
        // The child table supplies five actor maxima. Parent performs one forEach, five merge
        // calls and five eight-vector maxima;24 APIs/vector plus16 parent APIs per row.
        return new BenchmarkReturnWorkBudget.Work(1, 32 + 5L * (16 + 24), 5, 0, 5L * 16 * 8, 2, 5L * 32, 5L * 24);
    }

    /** The reused private profile token adds one bounded canonical recipe/cache construction. */
    static BenchmarkReturnWorkBudget.Work trustedCacheUpper() {
        long actors = BenchmarkReturnWorkBudget.Activity.values().length;
        // Fixed header, the optional producer-only writer tuple and one enum-ordered eight-long
        // row each. Numeric text has at most 20 characters and names/paths retain their source caps.
        long characters = add(128, 64, 40, 4L * 4096, 2L * 512, 2L * 256, 4L * 64, 4L * 20,
                mul(actors, add(128, mul(UNITS, 21), 16)));
        require(characters <= 65536, "cached recipe exceeds its fixed source text envelope");
        return new BenchmarkReturnWorkBudget.Work(1, add(256, mul(64, actors), mul(3, characters)),
                0, 0, add(mul(8, characters), mul(16, mul(actors, UNITS))), 4,
                add(characters, mul(16, mul(actors, UNITS))), add(characters, mul(8, actors)));
    }

    /** Fixed owned singleton/List/Map/Set initializer; it performs no clock, file or runtime read. */
    static BenchmarkReturnWorkBudget.Work authorityInitializationUpper() {
        long literals = 16L + 6 + 4 + 5;
        return new BenchmarkReturnWorkBudget.Work(1, add(32, mul(8, literals)), 0, 0,
                mul(4, mul(512, literals)), 2, literals, literals);
    }

    /** Per-node maximum: four node/plan/checked operations, at most 25 scalar APIs or 32 map/list edge APIs. */
    static BenchmarkReturnWorkBudget.Work treeUpper(long nodes, long strings) {
        require(nodes > 0 && strings > 0, "tree source inputs are missing");
        return row(add(32, mul(64, nodes)), nodes, 0, add(mul(16, nodes), mul(4, strings)),
                4, mul(4, nodes), mul(2, nodes));
    }

    static <T, E extends Exception> T entered(Kind kind, BenchmarkReturnWorkBudget.CheckedAction<T, E> body) throws E {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return body.run();
        var ticket = BenchmarkReturnWorkBudget.begin(scope, actor(kind), BenchmarkReturnWorkBudget.Work.call());
        try { T result = body.run(); BenchmarkReturnWorkBudget.done(ticket); return result; }
        catch (RuntimeException | Error primary) { BenchmarkReturnWorkBudget.failed(ticket, primary); throw primary; }
        catch (Exception primary) {
            BenchmarkReturnWorkBudget.failed(ticket, primary);
            @SuppressWarnings("unchecked") E exact = (E) primary; throw exact;
        }
    }

    static void semanticBody(BenchmarkReturnMethodAdmission.IndependentSemanticReview review) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope != null && review != BenchmarkNativeSourceAuthority.instance()) BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS,
                "INDEPENDENT_SEMANTIC_AUTHORITY_OWNED_BODY_NOT_ALLOCATED");
    }

    static void guardPreflight(BenchmarkReturnMethodAdmission.Preflight input) {
        if (BenchmarkReturnWorkBudget.current() == null || input == null || input.helper() == null || input.authenticatedBytes() == null) return;
        var helper = input.helper(); var bytes = input.authenticatedBytes();
        require(helper.sources().size() <= BenchmarkReturnWorkBudgetBindings.SourceRole.values().length
                && helper.classes().size() <= CLASSES && helper.origins().size() <= ORIGINS,
                "registered preflight roster exceeds prior cap");
        require(bytes.sources().size() <= BenchmarkReturnWorkBudgetBindings.SourceRole.values().length
                && bytes.classes().size() <= CLASSES && bytes.origins().size() <= ORIGINS && bytes.files().size() <= FILES,
                "authenticated preflight roster exceeds prior cap");
        text(input.actualRootMetadata(), NATIVE_CHARS);
    }

    static BenchmarkReturnWorkBudget.Ticket begin(Kind kind) {
        return BenchmarkReturnWorkBudget.begin(BenchmarkReturnWorkBudget.current(), actor(kind), BenchmarkReturnWorkBudget.Work.call());
    }

    static void guardWorkload(BenchmarkWorkloadDefinitions.Workload workload) {
        if (BenchmarkReturnWorkBudget.current() == null || workload == null) return;
        // Exact phase/SQL equality is checked by the active Run against its cached canonical input.
        // This independently bounded material walk covers all three unchanged source geometries.
        BenchmarkSharedSupportSourceWork.originalWorkload(workload);
    }

    static void guardCapture(BenchmarkWriteReturnCapture.Result capture) {
        if (BenchmarkReturnWorkBudget.current() == null || capture == null) return;
        BenchmarkReturnCaptureMaterialBounds.capture(capture.calls(), capture.samples(), capture.summary(),
                capture.pagesBase64(), capture.costStages());
        require(capture.calls().size() <= CALLS && capture.samples().size() <= SAMPLES && capture.pagesBase64().size() <= PAGES,
                "capture projection exceeds prior roster");
        long rows = 0, encoded = 0;
        for (var call : capture.calls()) {
            rows = Math.addExact(rows, call.rows().size());
            require(rows <= ROWS && call.keyFields().size() <= 2 && call.errorDetails().size() <= call.rows().size(),
                    "call material exceeds prior capture shape");
            text(call.scope(), 512); text(call.writerIdentity(), 512); text(call.stream(), 512); text(call.target(), 512);
            for (String key : call.keyFields()) text(key, 128);
        }
        for (String page : capture.pagesBase64()) {
            text(page, 4 * ((RAW_PAGE + 2) / 3)); encoded = Math.addExact(encoded, page.length());
            require(encoded <= 4L * (RAW_TOTAL + 2L * PAGES) / 3, "encoded page aggregate exceeds prior capture material");
        }
    }

    static void guardPhaseMaterial(List<BenchmarkForkEnvironment.BatchResult> sourceBatches,
                                   List<BenchmarkTargetClock.Reading> serialHello) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        require(sourceBatches != null && sourceBatches.size() <= BATCHES && serialHello != null && serialHello.size() <= HELLO,
                "phase source or hello roster exceeds prior material cap");
        // The size guard precedes either traversal or List.copyOf. Obtained immutable String
        // scalars are checked before validation/evidence creation; no UTF-8 copy is made here.
        for (var reading : serialHello) {
            require(reading != null, "serial hello reading is missing");
            text(reading.primary(), 65536); text(reading.processId(), 24);
        }
    }

    static void guardNativeJson(String raw) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        // Compact source metadata contains two fixed snapshots; bound lexical depth/nodes
        // before JsonReader recurses or JsonWriter allocates a canonical representation.
        BenchmarkReturnWorkBudgetDefinitions.requireJsonShape(raw, NATIVE_CHARS, 16, 512, NATIVE_CHARS);
    }

    static void guardColdWrapper(Map<String, Object> value) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        require(value != null && value.size() <= 128, "cold wrapper exceeds its prior field cap");
        for (String key : value.keySet()) text(key, 128);
    }

    private static void text(String value, int limit) {
        require(value != null && value.length() <= limit, "component scalar exceeds prior material cap");
    }
    private static long jsonRequests(long chars) { return add(mul(64, chars), mul(64, 512), 128); }
    private static BenchmarkReturnWorkBudget.Work row(long requests, long records, long frames, long bytes, long passes, long elements, long updates) {
        return new BenchmarkReturnWorkBudget.Work(1, add(requests, 16), records, frames, bytes, passes, elements, updates);
    }
    private static BenchmarkReturnWorkBudget.Work maximum(BenchmarkReturnWorkBudget.Work a, BenchmarkReturnWorkBudget.Work b) {
        return BenchmarkSharedSupportSourceWork.maximum(a, b);
    }
    private static long add(long... values) { long result = 0; for (long value : values) result = Math.addExact(result, value); return result; }
    private static long mul(long a, long b) { return Math.multiplyExact(a, b); }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError("return component work " + message); }
}
