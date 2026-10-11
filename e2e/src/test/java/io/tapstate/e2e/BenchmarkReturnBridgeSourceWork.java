package io.tapstate.e2e;

import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;

/** Source envelopes for typed fork entries; outer orchestration has its own independent meter. */
final class BenchmarkReturnBridgeSourceWork {
    static final int PHASES = 6, CHAINS = 4, FORKS = 30, VERIFY_FORKS = 10;
    static final int COVERAGE = 288000 + 2 + 1 + CHAINS;
    static final int CONFLICT_SLOTS = 16, EVIDENCE_FIELDS = 16 + 18;
    static final int TOKEN = 262144, JSON_BYTES = 65536, JSON_NODES = 4096;
    static final int LOCAL_BYTES = 2097152, LOCAL_DEPTH = 16, WRITERS = 64, WRITER_CHARS = 256;
    // Each of two target terms is pipeline128 + slash + table128 + equals + SHA64.
    static final int CHECKSUM = 2 * (128 + 1 + 128 + 1 + 64) + 1;
    enum Kind { SELECT, OPEN, BIND, PHASE, FINISH, CARRIER, COMPARISON, EVIDENCE,
        COVERAGE_RECIPE, ACK_VERIFY, TABLE_CONFIRMATION, TABLE_COVER, FENCE_COPY, JSON_PARSE, PROTOCOL_MODE, DRIVER_PHASE, IT_REPORT, LIVE_REPORT }
    private BenchmarkReturnBridgeSourceWork() { }

    static BenchmarkReturnWorkBudget.Activity actor(Kind kind) {
        return switch (kind) {
            case SELECT, OPEN, BIND, CARRIER, EVIDENCE, COVERAGE_RECIPE, PROTOCOL_MODE, IT_REPORT, LIVE_REPORT -> BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS;
            case PHASE, FINISH, ACK_VERIFY, TABLE_CONFIRMATION, TABLE_COVER, DRIVER_PHASE -> BenchmarkReturnWorkBudget.Activity.ASSOCIATION;
            case COMPARISON -> BenchmarkReturnWorkBudget.Activity.COMMON_WINDOW;
            case FENCE_COPY -> BenchmarkReturnWorkBudget.Activity.COPY;
            case JSON_PARSE -> BenchmarkReturnWorkBudget.Activity.DECODE;
        };
    }
    static Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> additions() {
        return BenchmarkReturnWorkBudget.operation(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, () -> {
            var rows = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
            for (Kind kind : Kind.values()) rows.merge(actor(kind), upper(kind), BenchmarkSharedSupportSourceWork::maximum);
            rows.merge(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, factoryUpper(1), BenchmarkSharedSupportSourceWork::maximum);
            return Map.copyOf(rows);
        });
    }
    static BenchmarkReturnWorkBudget.Work upper(Kind kind) {
        long roles = BenchmarkReturnWorkBudgetBindings.SourceRole.values().length;
        long logical = mul(VERIFY_FORKS, COVERAGE);
        long writerKeys = mul(2L * WRITERS, 4L * WRITER_CHARS + 4L * ((4L * WRITER_CHARS + 2) / 3));
        return switch (kind) {
            case SELECT -> work(1, 32, 0, 0, 256, 1, 32, 2);
            // The frozen body currently has15 conflicts;16 is a declared source envelope, not an observed count.
            case OPEN -> work(1, add(256, mul(16, CONFLICT_SLOTS), mul(32, mul(2, roles))), 2, 0,
                    add(4096, mul(32, roles)), 4, add(64, mul(2, roles)), add(32, roles));
            // Run canonical equality consumes the guarded original581 setup +six*1920 statements.
            case BIND -> work(1, add(128, mul(8, 6 * 1920L + 581), mul(16, PHASES)), PHASES, 0,
                    mul(16, 6 * 1920L + 581), 3, 6 * 1920L + 581 + PHASES, 32);
            case PHASE -> work(1, 128, 0, 0, 1024, 2, 32, 8);
            // Metric/oracle children have entered rows; this parent only owns phase/access/publication work.
            case FINISH -> work(1, add(256, mul(32, PHASES)), PHASES, 0, 8192, 3, mul(32, PHASES), 64);
            case CARRIER -> work(1, add(64, mul(16, 128)), 0, 0, 8192, 2, 128, 128);
            case COMPARISON -> work(1, 128, 0, 0, 4096, 2, 32, 16);
            // Exactly16 named fields +18 flags, three-field rates, three two-field integer intervals.
            case EVIDENCE -> work(1, add(128, mul(8, EVIDENCE_FIELDS + 9L), 64), 0, 0, 8192, 3, 96, 64);
            // At most six phases, two terminal targets and four chains; no workload factory or SQL generation.
            case COVERAGE_RECIPE -> work(1, add(128, mul(64, PHASES + 2L * CHAINS + 2)), 3, 0,
                    8192, 4, 128, 64);
            // At most10 retained forks.32 APIs/entry cover guard, access/boxing, owned anyMatch,
            // maps/sets/duplicate checks and returned terminal maps. Input key text is borrowed, not reencoded.
            case ACK_VERIFY -> work(1, add(256, mul(32, logical), mul(128, VERIFY_FORKS * (long) CHAINS)), logical, 0,
                    add(mul(32, logical), mul(4L * VERIFY_FORKS * CHAINS, TOKEN)), 6,
                    add(mul(8, logical), mul(64, VERIFY_FORKS * (long) CHAINS)), add(mul(4, logical), 256));
            // requireJsonShape owns<=20 local/JDK calls per character, including surrogate/whitespace
            // and checked-byte work. Document.parse is one separately issued opaque child request.
            case TABLE_CONFIRMATION -> work(1, add(128, mul(20, JSON_BYTES)), 1, 0,
                    add(mul(8, JSON_BYTES), mul(4, TOKEN)), 3, add(mul(8, JSON_BYTES), 128), mul(6, JSON_BYTES));
            // Two guarded tree walks, two currentRun/writer-list/progress passes, explicit stream
            // lambdas, SourceOrder/WriterProgress APIs, Base64 writer keys and fence equality.
            //64 API slots/node +12/UTF8 character cover the owned material guard; provider interiors are excluded.
            case TABLE_COVER -> work(1, add(256, mul(128, JSON_NODES), mul(24, LOCAL_BYTES), mul(128, WRITERS)), WRITERS, 0,
                    add(mul(32, JSON_NODES), mul(8, LOCAL_BYTES), writerKeys), 8,
                    add(mul(16, JSON_NODES), mul(8, LOCAL_BYTES), mul(16, WRITERS)),
                    add(mul(8, JSON_NODES), mul(6, LOCAL_BYTES), mul(16, WRITERS)));
            // A guarded fence tree precedes BSON formatting. Its provider encoding has a prior
            // source upper72*nodes+6*text, even if an oversized result is rejected before parse.
            case FENCE_COPY -> work(1, add(128, mul(64, JSON_NODES), mul(12, LOCAL_BYTES)), 0, 0,
                    add(mul(72, JSON_NODES), mul(10, LOCAL_BYTES)), 4,
                    add(mul(8, JSON_NODES), mul(4, LOCAL_BYTES)), add(mul(4, JSON_NODES), mul(3, LOCAL_BYTES)));
            // Guard scanning belongs to this actual entry too; conservative overlap with parents is allowed.
            case JSON_PARSE -> work(1, add(128, mul(20, JSON_BYTES)), 0, 0, mul(16, JSON_BYTES), 3,
                    add(mul(8, JSON_BYTES), mul(4, JSON_NODES)), add(mul(6, JSON_BYTES), JSON_NODES));
            // Selected-property prelude owns fixed equality/accesses; SELECT is a separate child.
            case PROTOCOL_MODE -> work(1, 64, 0, 0, 1024, 2, 32, 16);
            // Original serial hello<=6000 is guarded before addAll/stream projection. Nine fixed
            // Reading fields use accessor/boxing/Map requests; method/result/resource children stay entered.
            case DRIVER_PHASE -> work(1, add(256, mul(64, BenchmarkTargetClock.MAX_INTERIOR_SAMPLES), 16L * 128),
                    BenchmarkTargetClock.MAX_INTERIOR_SAMPLES, 0,
                    add(mul(4L * (65536 + 24 + 7L * 32), BenchmarkTargetClock.MAX_INTERIOR_SAMPLES), 32768), 4,
                    add(mul(32, BenchmarkTargetClock.MAX_INTERIOR_SAMPLES), 128), add(mul(16, BenchmarkTargetClock.MAX_INTERIOR_SAMPLES), 128));
            // The exact typed report branch owns twelve calls and a two-reference output wrapper.
            // ACK, live projection and Run writing are separately entered children.
            case IT_REPORT -> work(1, 128, 0, 0, 8192, 2, 32, 16);
            // Sixteen extra scalar/empty-array/branch calls and two unavailable maps. The existing
            // raw phase/coverage/confirmation projections retain their original source/report rows.
            case LIVE_REPORT -> work(1, 256, 0, 0, 16384, 3, 64, 32);
        };
    }
    static BenchmarkReturnWorkBudget.Work factoryUpper(long entries) {
        require(entries >= 0, "factory entry count is negative");
        long rows = Kind.values().length;
        // Eight-vector formulas/maxima, entry map and final immutable map; one extra factory row merge.
        return work(1, add(128, mul(192, rows)), rows, 0, mul(512, rows), 6, mul(64, rows), mul(48, rows)).times(entries);
    }
    static BenchmarkReturnWorkBudget.Work staticInitializationUpper() {
        // The existing TableAckGate canonical BSON settings initializer owns four builder APIs and one store.
        return work(1, 16, 0, 0, 512, 1, 16, 8);
    }
    static BenchmarkReturnWorkBudget.Work addedPreScopeUpper() {
        // Original two profile inspections each construct this table. Their parent construction
        // already has an entry; added row merges and this child table are source work, not extra workload factories.
        return factoryUpper(2).plus(BenchmarkReturnComponentSourceWork.bridgeProjectionUpper().times(2))
                .plus(staticInitializationUpper()).plus(upper(Kind.PROTOCOL_MODE))
                .plus(upper(Kind.SELECT)).plus(new BenchmarkReturnWorkBudget.Work(1, 4, 0, 0, 512, 1, 4, 2));
    }
    /** Static recipe only; the completed-only external controller owns its meter and projection body. */
    static BenchmarkReturnWorkBudget.Work harnessUpper() {
        long logicalVisits = mul(3 * (10 + 10L), COVERAGE);
        return work(1, add(512, FORKS, mul(64, FORKS), mul(32, logicalVisits)), logicalVisits, FORKS,
                add(mul(32, logicalVisits), 32768), 8, add(mul(8, logicalVisits), 10L * FORKS), add(mul(4, logicalVisits), 8L * FORKS));
    }
    static <T, E extends Exception> T entered(Kind kind, BenchmarkReturnWorkBudget.CheckedAction<T, E> body) throws E {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return body.run();
        var ticket = BenchmarkReturnWorkBudget.begin(scope, actor(kind), BenchmarkReturnWorkBudget.Work.call());
        try { T result = body.run(); BenchmarkReturnWorkBudget.done(ticket); return result; }
        catch (RuntimeException | Error failure) { BenchmarkReturnWorkBudget.failed(ticket, failure); throw failure; }
        catch (Exception failure) { BenchmarkReturnWorkBudget.failed(ticket, failure); @SuppressWarnings("unchecked") E exact = (E) failure; throw exact; }
    }
    static void driverMaterial(BenchmarkWorkloadDefinitions.Workload workload) {
        if (BenchmarkReturnWorkBudget.current() != null) BenchmarkSharedSupportSourceWork.originalWorkload(workload);
    }
    static void methodMap(Map<String, Object> value) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        require(value != null && value.size() <= 128, "admission wrapper exceeds its prior cap");
        for (String key : value.keySet()) text(key, 128);
    }
    static long coverageUpper(BenchmarkWorkloadDefinitions.Workload workload) {
        return entered(Kind.COVERAGE_RECIPE, () -> {
            require(workload != null && workload.rows() == 96000 && workload.seed() == 424242
                    && workload.phases().size() <= PHASES && workload.sourceChains().size() <= CHAINS,
                    "original coverage recipe differs");
            long rows = workload.sourceChains().size();
            for (var phase : workload.phases()) {
                if (phase.measured()) rows = Math.addExact(rows, phase.expectedLogicalOutputChanges());
                if (phase.stage() == BenchmarkWorkloadDefinitions.Stage.TERMINAL) {
                    require(phase.targets().size() <= 2 && phase.sql().size() <= CHAINS, "terminal material exceeds prior shape");
                    for (var target : phase.targets()) {
                        rows = Math.addExact(rows, BenchmarkTerminalTargetChanges.forTarget(workload, target).size());
                        rows = Math.addExact(rows, BenchmarkTerminalTargetChanges.optionalForTarget(workload, target).size());
                    }
                }
            }
            require(rows <= COVERAGE, "full fork coverage exceeds its original recipe"); return rows;
        });
    }
    static void requireCoverage(BenchmarkAckOracle.Fork fork, long maximum) {
        if (BenchmarkReturnWorkBudget.current() != null) require(fork != null && maximum <= COVERAGE
                && fork.logicalCoverage().size() <= maximum, "correctness exceeds the original full fork recipe");
    }
    static void oracleMaterial(List<BenchmarkAckOracle.Fork> forks, Map<String, String> frozen) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        require(forks != null && forks.size() <= VERIFY_FORKS, "oracle fork roster exceeds its prior cap");
        if (frozen != null) { require(frozen.size() <= CHAINS, "frozen terminal map exceeds its prior cap");
            for (var entry : frozen.entrySet()) { text(entry.getKey(), 128); text(entry.getValue(), 512); } }
        for (var fork : forks) oracleMaterial(fork, COVERAGE);
    }
    static void oracleMaterial(BenchmarkAckOracle.Fork fork, long maximum) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        require(fork != null && maximum <= COVERAGE && fork.chains().size() <= CHAINS
                && fork.logicalCoverage().size() <= maximum, "correctness roster exceeds original full fork coverage");
        text(fork.id(), 128); text(fork.checksum(), CHECKSUM);
        for (var item : fork.logicalCoverage().entrySet()) { text(item.getKey(), 512); require(item.getValue() != null, "coverage count is missing"); }
        for (var chain : fork.chains()) {
            require(chain != null && chain.sourceTerminals().size() == 1, "terminal roster exceeds prior shape");
            text(chain.id(), 128); var terminal = chain.sourceTerminals().getFirst(); require(terminal != null, "terminal is missing");
            text(terminal.logicalId(), 512); text(terminal.sourcePosition(), TOKEN);
            if (chain.authoritativeTargetAck() != null) text(chain.authoritativeTargetAck(), TOKEN);
            if (chain.tableConfirmation() == null) {
                String reason = chain.positionCoverage() instanceof BenchmarkConnectorPositionCoverage
                        ? "NAMED_CONNECTOR_POSITION_BODY_NOT_IN_THIS_TABLE_PROOF_RECIPE" : "UNINSPECTED_POSITION_CALLBACK_SOURCE";
                BenchmarkReturnWorkBudget.gap(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.ASSOCIATION, reason);
            }
        }
    }
    static Document parsedCursor(String raw) {
        if (BenchmarkReturnWorkBudget.current() == null) return Document.parse(raw);
        return entered(Kind.JSON_PARSE, () -> {
            BenchmarkReturnWorkBudgetDefinitions.requireJsonShape(raw, JSON_BYTES, LOCAL_DEPTH, JSON_NODES, JSON_BYTES);
            return BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.DECODE,
                    () -> Document.parse(raw));
        });
    }
    static String encodedFence(Document fence, org.bson.json.JsonWriterSettings settings) {
        return BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.COPY,
                () -> fence.toJson(settings));
    }
    static void tableMaterial(BenchmarkTableAckGate.Binding binding, BenchmarkTableTerminalObserver.Point point,
                              Document cursor, Document rawFence) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        require(binding != null && point != null && cursor != null, "table binding, cursor or marker is missing");
        for (String value : List.of(binding.physicalChain(), binding.pipeline(), binding.source(), binding.table(), binding.consumer())) text(value, 512);
        text(point.markerId(), 512); text(point.ring(), 512);
        require(binding.writers().size() <= WRITERS, "writer roster exceeds its prior cap");
        for (String writer : binding.writers()) text(writer, WRITER_CHARS);
        if (binding.writerRunId() != null) text(binding.writerRunId(), 512);
        cursorMaterial(cursor); cursorMaterial(rawFence);
        Object runValue = cursor.get("writerRun");
        if (runValue instanceof Document run) { writerPlan(run.get("expected"), binding.table());
            Object progressValue = run.get("progress");
            if (progressValue instanceof Document progress) { Object entriesValue = progress.get(binding.table());
                if (entriesValue instanceof Document entries) require(entries.size() <= WRITERS, "writer progress exceeds prior roster"); } }
        else { writerPlan(cursor.get("expectedSinkWriters"), binding.table()); }
    }
    private static void writerPlan(Object value, String table) {
        if (!(value instanceof Document plan)) return;
        Object list = plan.get(table);
        if (list instanceof List<?> writers) {
            require(writers.size() <= WRITERS, "writer plan exceeds its prior roster");
            for (Object writer : writers) { require(writer instanceof String, "writer identity is not text"); text((String) writer, WRITER_CHARS); }
        }
    }
    static void cursorMaterial(Document cursor) {
        if (BenchmarkReturnWorkBudget.current() != null && cursor != null) new CursorMaterial().value(cursor, 1);
    }
    private static final class CursorMaterial {
        private final IdentityHashMap<Object, Boolean> parents = new IdentityHashMap<>();
        private long nodes, bytes;
        private void value(Object value, int depth) {
            nodes = Math.incrementExact(nodes); require(nodes <= JSON_NODES && depth <= LOCAL_DEPTH, "cursor exceeds prior shape");
            if (value == null) return;
            if (value instanceof String text) { utf8(text); return; }
            if (value instanceof Map<?, ?> map) {
                require(map.size() <= (JSON_NODES - nodes) / 2, "cursor map exceeds remaining prior nodes"); enter(map);
                try { for (var item : map.entrySet()) { require(item.getKey() instanceof String, "cursor key is not text");
                    value(item.getKey(), depth + 1); value(item.getValue(), depth + 1); } } finally { parents.remove(map); } return;
            }
            if (value instanceof List<?> list) {
                require(list.size() <= JSON_NODES - nodes, "cursor list exceeds remaining prior nodes"); enter(list);
                try { for (Object item : list) value(item, depth + 1); } finally { parents.remove(list); } return;
            }
            require(value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                    || value instanceof Float || value instanceof Double || value instanceof Boolean || value instanceof java.util.Date
                    || value instanceof org.bson.types.ObjectId || value instanceof org.bson.BsonTimestamp
                    || value instanceof org.bson.types.Decimal128, "cursor scalar has no fixed BSON source recipe");
            bytes = Math.addExact(bytes, 128); require(bytes <= LOCAL_BYTES, "cursor exceeds prior scalar material");
        }
        private void enter(Object value) { require(parents.put(value, true) == null, "cursor contains a cycle"); }
        private void utf8(String text) {
            require(text.length() <= JSON_BYTES, "cursor text exceeds prior leaf bound");
            for (int index = 0; index < text.length(); index++) {
                char c = text.charAt(index); int size;
                if (c <= 127) size = 1; else if (c <= 2047) size = 2;
                else if (Character.isHighSurrogate(c)) { require(index + 1 < text.length() && Character.isLowSurrogate(text.charAt(++index)), "unpaired cursor surrogate"); size = 4; }
                else { require(!Character.isLowSurrogate(c), "unpaired cursor surrogate"); size = 3; }
                bytes = Math.addExact(bytes, size); require(bytes <= LOCAL_BYTES, "cursor exceeds prior UTF8 material");
            }
        }
    }
    static void text(String value, int maximum) { require(value != null && value.length() <= maximum, "scalar exceeds its prior source cap"); }
    private static BenchmarkReturnWorkBudget.Work work(long c, long q, long r, long f, long b, long p, long e, long u) { return new BenchmarkReturnWorkBudget.Work(c,q,r,f,b,p,e,u); }
    private static long add(long... values) { long out=0; for(long value:values) out=Math.addExact(out,value); return out; }
    private static long mul(long a,long b) { return Math.multiplyExact(a,b); }
    private static void require(boolean value,String message) { if(!value) throw new AssertionError("return bridge source " + message); }
}
