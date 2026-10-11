package io.tapstate.e2e;

import java.util.EnumMap;
import java.util.Map;
import java.util.List;

/** Per-entered owned helper work. Provider instruction, CPU and elapsed costs remain excluded. */
final class BenchmarkSharedSupportSourceWork {
    static final int TEXT = 65536, URI = 4096, RESOURCES = 64, SLOT_ROWS = 33;
    // Original pilot setup sizes are 193/194/581. Stateful measured batches hold two statements.
    static final int SQL_STATEMENTS = 4 + 1 + 3 * (96000 / 500);
    static final int PHASE_SQL_STATEMENTS = 2 * (96000 / 100), BATCHES = 960, BATCH_STATEMENTS = 2;
    static final int MAX_WORKLOAD_SQL = SQL_STATEMENTS + 4 + 1 + BATCHES + PHASE_SQL_STATEMENTS + 4;
    static final int PIPELINES = 2, CHAINS = 4, PHASES = 6, TARGETS = 2, NAME = 128;
    static final int MY_COMMAND_TEXT = 2 * TEXT + 128;
    private static final int ENTRY_REQUESTS = 16;
    // Each scalar visit reserves current/length/checked-bytes/add and six guard/failure slots.
    private static final int SCALAR_REQUESTS = 12;
    private static final int EDGE_REQUESTS = 4;
    enum Kind {
        FIXTURE_CREATE, FIXTURE_REGISTRATION, PG_SETTINGS, PG_SERVER, PG_ADMIN_CONNECT,
        PG_CONNECT, PG_PROVISION, TARGET_RESET, SQL_PARENT, SQL_COMMAND,
        FIXTURE_CLOSE_PARENT, FIXTURE_DATABASE_CLOSE, CLOSING,
        SLOT_SETUP, SLOT_READ, SLOT_ROW, SLOT_DROP, SLOT_POLL, SLOT_RENDER, AWAIT_ITERATION,
        MONGO_URI, MONGO_CONFIGURATION, MONGO_ARGUMENTS, DOCKER_GATE, PLAIN_PROCESS_CLOSE, STAGING_DELETE,
        BATCH_LAYOUT, BATCH_ROW,
        MY_IMAGE, MY_SETTINGS, MY_SERVER, MY_CONNECT, MY_ROOT_CONNECT, MY_PROVISION, MY_GRANT,
        MY_COMMAND, MY_STATEMENTS, MY_CLOSE, WORKLOAD_SHAPE, PHASE_SHAPE,
        TARGET_RESET_ROW, STATE_RESET, SQL_UPDATE
    }
    private BenchmarkSharedSupportSourceWork() { }

    static Map<Kind, BenchmarkReturnWorkBudget.Work> bounds() {
        var result = new EnumMap<Kind, BenchmarkReturnWorkBudget.Work>(Kind.class);
        put(result, Kind.FIXTURE_CREATE, 1, 8, 0, 0, 192, 1, 3, 3);
        // A contains/stream visit sees at most the existing 64 registered close resources.
        put(result, Kind.FIXTURE_REGISTRATION, 1, add(20, mul(3, RESOURCES)), 0, 0,
                mul(4, add(TEXT, mul(3, RESOURCES))), 3, add(TEXT, RESOURCES), RESOURCES);
        // The five setting values are guarded before this owned method stores or concatenates them.
        // server/provision/admin/SDK requests have separate entered helpers/provider tickets.
        put(result, Kind.PG_SETTINGS, 1, 32, 0, 0, mul(4, add(mul(5, TEXT), 64)), 2, 16, 16);
        put(result, Kind.PG_SERVER, 1, 24, 0, 0, 4096, 1, 18, 5);
        put(result, Kind.PG_ADMIN_CONNECT, 1, 8, 0, 0, mul(4, mul(3, TEXT)), 1, 3, 0);
        // Five Map.get, five String.valueOf, five scalar guards, concat, provider and scope/entry
        // wrappers fit 24 requests. The default route retains its original conversion interleaving.
        put(result, Kind.PG_CONNECT, 1, 24, 0, 0, mul(4, add(mul(5, TEXT), 64)), 1, 8, 2);
        put(result, Kind.PG_PROVISION, 1, 16, 0, 0, mul(8, add(TEXT, 128)), 2, 4, 2);
        // Parent: three URI guards, the fixed original workload guard (separate entry), at most
        // two target iterator visits and one optional state-database child. Per-reset providers
        // and closes retain their independent entries; this allowance may conservatively overlap.
        put(result, Kind.TARGET_RESET, 1, add(ENTRY_REQUESTS, add(32, add(mul(3, SCALAR_REQUESTS), mul(EDGE_REQUESTS, TARGETS)))),
                0, 0, mul(8, add(mul(3, URI), mul(2, NAME))), 3, 3 + TARGETS, TARGETS);
        // First guard size before traversal, then at most 581 bounded scalar visits, then the
        // original command iteration. Guard+iteration reserve 12+4+4 slots per statement.
        put(result, Kind.SQL_PARENT, 1, add(ENTRY_REQUESTS, add(32, mul(SCALAR_REQUESTS + 2L * EDGE_REQUESTS, SQL_STATEMENTS))),
                0, 0, mul(4, mul(TEXT, SQL_STATEMENTS)), 3, mul(2, SQL_STATEMENTS), SQL_STATEMENTS);
        put(result, Kind.SQL_COMMAND, 1, 8, 0, 0, mul(4, TEXT), 1, 1, 0);
        // Three bounded resource traversals; database traversal is separately entered per database.
        put(result, Kind.FIXTURE_CLOSE_PARENT, 1, add(16, mul(20, RESOURCES)), 0, 0,
                mul(8, mul(TEXT, RESOURCES)), 4, mul(4, RESOURCES), mul(3, RESOURCES));
        put(result, Kind.FIXTURE_DATABASE_CLOSE, 1, 20, 0, 0, mul(8, add(TEXT, 128)), 2, 8, 8);
        put(result, Kind.CLOSING, 1, 8, 0, 0, mul(4, TEXT), 1, 4, 3);
        put(result, Kind.SLOT_SETUP, 1, add(32, mul(8, SLOT_ROWS)), 0, 0, mul(8, add(TEXT, 256)), 2, 16, 8);
        // query.setMaxRows(33) is the predeclared prior row shape. Per-row getter/material visits are
        // separately entered; each raw provider String is guarded before local Map.entry/copy/print.
        put(result, Kind.SLOT_READ, 1, add(12, mul(12, SLOT_ROWS)), 0, 0,
                mul(8, mul(SLOT_ROWS, TEXT)), 2, mul(4, SLOT_ROWS), mul(4, SLOT_ROWS));
        put(result, Kind.SLOT_ROW, 1, 12, 1, 0, mul(8, TEXT), 1, 2, 2);
        put(result, Kind.SLOT_DROP, 1, 12, 0, 0, mul(8, add(TEXT, 128)), 2, 4, 2);
        put(result, Kind.SLOT_POLL, 1, add(24, mul(4, SLOT_ROWS)), 0, 0,
                mul(8, mul(SLOT_ROWS, TEXT)), 3, mul(4, SLOT_ROWS), mul(2, SLOT_ROWS));
        put(result, Kind.SLOT_RENDER, 1, add(24, mul(12, SLOT_ROWS)), 0, 0,
                mul(8, mul(SLOT_ROWS, TEXT)), 3, mul(6, SLOT_ROWS), mul(2, SLOT_ROWS));
        // Count entered iterations, including final success/timeout/exception. Keep every original
        // System.nanoTime/condition/sleep/reading call once and in its original order/domain.
        put(result, Kind.AWAIT_ITERATION, 1, 16, 0, 0, 4096, 1, 8, 4);
        // Original Phase.batches remains unchanged. The caller guards phase material first.
        // A layout has at most 960 views; a row view holds at most two statements, never copied SQL.
        put(result, Kind.BATCH_LAYOUT, 1, add(ENTRY_REQUESTS, add(16, mul(EDGE_REQUESTS, BATCHES))),
                0, 0, mul(16, BATCHES), 2, BATCHES, BATCHES);
        put(result, Kind.BATCH_ROW, 1, add(ENTRY_REQUESTS, 8), 0, 0, mul(32, BATCH_STATEMENTS), 1, BATCH_STATEMENTS, 1);
        put(result, Kind.MONGO_URI, 1, 32, 0, 0, 32768, 1, 16, 4);
        // Resolver/property supplier, present/empty checks, scalar guard, decimal parse/sign/equality/
        // normalization, Optional storage and fixed entry/return wrappers fit 24 local requests.
        put(result, Kind.MONGO_CONFIGURATION, 1, 24, 0, 0, mul(4, TEXT), 1, 8, 2);
        // Command-parts material uses the existing 512 argument/8192 scalar/two-MiB input envelope.
        put(result, Kind.MONGO_ARGUMENTS, 1, add(32, mul(16, 512)), 0, 0,
                8L * 2097152, 3, 512 + 3, 512 + 3);
        // DockerGate.require: factory instance/isAvailable, getenv, String.equals, decide/apply.
        put(result, Kind.DOCKER_GATE, 1, 16, 0, 0, 4096, 1, 8, 2);
        // Existing plain close invokes destroy, up to two waits/two forces, isAlive, duration/toMillis,
        // interrupt handling, and the separately entered owned staging-delete method.
        put(result, Kind.PLAIN_PROCESS_CLOSE, 1, 24, 0, 0, 4096, 1, 16, 4);
        put(result, Kind.STAGING_DELETE, 1, 8, 0, 0, 32768, 1, 4, 0);
        // MY_* fixed local bounds include the existing entered wrapper. Actual opaque provider
        // and JDBC close requests are separately issued. No provider instruction/CPU/time bound.
        put(result, Kind.MY_IMAGE, 1, add(ENTRY_REQUESTS, 4), 0, 0, 4L * 9, 1, 1, 1);
        put(result, Kind.MY_SETTINGS, 1, add(ENTRY_REQUESTS, add(24, mul(5, SCALAR_REQUESTS))),
                0, 0, mul(4, add(mul(5, TEXT), 64)), 2, 16, 16);
        put(result, Kind.MY_SERVER, 1, add(ENTRY_REQUESTS, 16), 0, 0, 4096, 1, 8, 2);
        put(result, Kind.MY_CONNECT, 1, add(ENTRY_REQUESTS, add(16, mul(5, SCALAR_REQUESTS))),
                0, 0, mul(4, add(mul(5, TEXT), 64)), 2, 8, 2);
        put(result, Kind.MY_ROOT_CONNECT, 1, add(ENTRY_REQUESTS, add(12, mul(2, SCALAR_REQUESTS))),
                0, 0, mul(4, add(mul(2, TEXT), 4)), 2, 3, 1);
        put(result, Kind.MY_PROVISION, 1, add(ENTRY_REQUESTS, add(24, mul(2, SCALAR_REQUESTS))),
                0, 0, mul(8, add(mul(2, TEXT), 128)), 2, 8, 4);
        put(result, Kind.MY_GRANT, 1, add(ENTRY_REQUESTS, add(16, SCALAR_REQUESTS)),
                0, 0, mul(8, add(TEXT, 128)), 2, 6, 3);
        put(result, Kind.MY_COMMAND, 1, add(ENTRY_REQUESTS, add(8, SCALAR_REQUESTS)),
                0, 0, mul(4, MY_COMMAND_TEXT), 1, 1, 0);
        // Root statement owner: connection/statement slots, one private fixed body invocation,
        // two null/primary guards and two bounded close children, with at most two suppressions.
        put(result, Kind.MY_STATEMENTS, 1, add(ENTRY_REQUESTS, 24), 0, 0, 512, 3, 6, 6);
        put(result, Kind.MY_CLOSE, 1, add(ENTRY_REQUESTS, 12), 0, 0, 256, 1, 2, 2);
        // Per field: scalar/edge/accessor slots. SQL leaves have their own 12+4 slots.
        // A stateful pilot is the maximal fixed projection: 3470 SQL leaves, six phases,
        // four source chains, two pipelines and twelve target records. No workload construction.
        long shapeFields = add(3, add(mul(6, CHAINS), add(PIPELINES, add(mul(12, PHASES), mul(6, PHASES * (long) TARGETS)))));
        put(result, Kind.WORKLOAD_SHAPE, 1,
                add(ENTRY_REQUESTS, add(64, add(mul(SCALAR_REQUESTS + EDGE_REQUESTS, MAX_WORKLOAD_SQL), mul(24, shapeFields)))),
                0, 0, add(mul(4, mul(TEXT, MAX_WORKLOAD_SQL)), mul(4 * NAME, shapeFields)),
                4, add(MAX_WORKLOAD_SQL, shapeFields), shapeFields);
        put(result, Kind.PHASE_SHAPE, 1,
                add(ENTRY_REQUESTS, add(32, mul(SCALAR_REQUESTS + EDGE_REQUESTS, PHASE_SQL_STATEMENTS))),
                0, 0, mul(4, mul(TEXT, PHASE_SQL_STATEMENTS)), 2, PHASE_SQL_STATEMENTS, 4);
        // Each collection reset: metadata/URI/database guards, six opaque provider calls and one
        // separately issued close. URI parsing is a request, not an internal parser cost claim.
        put(result, Kind.TARGET_RESET_ROW, 1, add(ENTRY_REQUESTS, add(32, mul(3, SCALAR_REQUESTS))),
                0, 0, mul(8, add(mul(2, URI), NAME)), 3, 8, 4);
        put(result, Kind.STATE_RESET, 1, add(ENTRY_REQUESTS, add(24, mul(2, SCALAR_REQUESTS))),
                0, 0, mul(8, add(mul(2, URI), NAME)), 3, 6, 4);
        // Boundary executeUpdate has a one-scalar prior before createStatement, one provider write,
        // exact affected==1 check and a real close, preserving the original public assertion.
        put(result, Kind.SQL_UPDATE, 1, add(ENTRY_REQUESTS, add(24, SCALAR_REQUESTS)),
                0, 0, mul(4, TEXT), 2, 4, 2);
        return Map.copyOf(result);
    }

    static BenchmarkReturnWorkBudget.Work sharedUpper() {
        var rows = bounds(); var result = new BenchmarkReturnWorkBudget.Work(1, 1, 1, 0, 2097152, 1, 4096, 0);
        for (var kind : new Kind[]{Kind.FIXTURE_CREATE, Kind.FIXTURE_REGISTRATION, Kind.PG_SETTINGS,
                Kind.PG_SERVER, Kind.PG_ADMIN_CONNECT, Kind.PG_CONNECT, Kind.PG_PROVISION,
                Kind.TARGET_RESET, Kind.SQL_PARENT, Kind.SQL_COMMAND, Kind.MONGO_URI,
                Kind.MONGO_CONFIGURATION, Kind.MONGO_ARGUMENTS, Kind.DOCKER_GATE, Kind.BATCH_LAYOUT, Kind.BATCH_ROW,
                Kind.MY_IMAGE, Kind.MY_SETTINGS, Kind.MY_SERVER, Kind.MY_CONNECT, Kind.MY_ROOT_CONNECT,
                Kind.MY_PROVISION, Kind.MY_GRANT, Kind.MY_COMMAND, Kind.MY_STATEMENTS,
                Kind.WORKLOAD_SHAPE, Kind.PHASE_SHAPE, Kind.TARGET_RESET_ROW, Kind.STATE_RESET, Kind.SQL_UPDATE}) result = maximum(result, rows.get(kind));
        return result;
    }

    static BenchmarkReturnWorkBudget.Work cleanupUpper() {
        var result = new BenchmarkReturnWorkBudget.Work(1, 1, 0, 0, 0, 0, 0, 0);
        var rows = bounds();
        for (var kind : new Kind[]{Kind.FIXTURE_CLOSE_PARENT, Kind.FIXTURE_DATABASE_CLOSE, Kind.CLOSING,
                Kind.SLOT_SETUP, Kind.SLOT_READ, Kind.SLOT_ROW, Kind.SLOT_DROP, Kind.SLOT_POLL,
                Kind.SLOT_RENDER, Kind.AWAIT_ITERATION, Kind.PLAIN_PROCESS_CLOSE, Kind.STAGING_DELETE, Kind.MY_CLOSE}) result = maximum(result, rows.get(kind));
        return result;
    }

    static <T, E extends Exception> T entered(BenchmarkReturnWorkBudget.Activity activity, Kind kind,
            BenchmarkReturnWorkBudget.CheckedAction<T, E> action) throws E {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return action.run();
        // The per-kind table is a static source upper, not actual work; only method entry and its
        // real completion/failure are recorded here. Real provider boundaries retain their own tickets.
        var ticket = BenchmarkReturnWorkBudget.begin(scope, activity, BenchmarkReturnWorkBudget.Work.call());
        try { T result = action.run(); BenchmarkReturnWorkBudget.done(ticket); return result; }
        catch (RuntimeException | Error failure) { BenchmarkReturnWorkBudget.failed(ticket, failure); throw failure; }
        catch (Exception failure) {
            BenchmarkReturnWorkBudget.failed(ticket, failure);
            @SuppressWarnings("unchecked") E original = (E) failure; throw original;
        }
    }

    static void local(Object value) {
        BenchmarkReturnWorkBudget.requireLocalMaterial(BenchmarkReturnWorkBudget.current(),
                BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, value);
    }
    static void cleanupMaterial(Object value) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope != null) {
            // Refuse source credit before the owned consumer. Continue the original cleanup even
            // when material is unavailable; diagnostic accounting cannot abandon an owned resource.
            scope.add(BenchmarkReturnWorkBudget.Activity.CLEANUP,
                    BenchmarkReturnWorkBudget.material(scope, BenchmarkReturnWorkBudget.Activity.CLEANUP, value));
        }
    }
    static void dockerGate() {
        entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, Kind.DOCKER_GATE,
                () -> { io.tapstate.testsupport.DockerGate.require(); return null; });
    }
    static void commandParts(String[] parts) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return;
        if (parts == null || parts.length > BenchmarkOwnedProcessReceipt.MAX_ARGUMENTS) {
            BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    "MONGO_COMMAND_PARTS_PRIOR_SHAPE_UNAVAILABLE");
            throw new AssertionError("Mongo command-parts source shape is unavailable");
        }
        long bytes = 0;
        for (String part : parts) {
            if (part == null || part.length() > BenchmarkOwnedProcessReceipt.MAX_STRING) {
                BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                        "MONGO_COMMAND_PARTS_PRIOR_SCALAR_UNAVAILABLE");
                throw new AssertionError("Mongo command-parts source scalar is unavailable");
            }
            bytes = Math.addExact(bytes, Math.multiplyExact(4L, part.length()));
        }
        if (bytes > BenchmarkOwnedProcessReceipt.MAX_BYTES) {
            BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    "MONGO_COMMAND_PARTS_PRIOR_BYTES_UNAVAILABLE");
            throw new AssertionError("Mongo command-parts source bytes are unavailable");
        }
    }
    static BenchmarkReturnWorkBudget.Work factoryUpper() {
        // openFromProperties and open each inspect the profile before activation. Each inspection
        // constructs shared and cleanup tables: four constructions and four maxima traversals.
        // At most 64 checked/accessor/boxing/map APIs cover each eight-unit row and its maximum.
        long rows = Kind.values().length;
        return new BenchmarkReturnWorkBudget.Work(4, add(128, mul(4 * 64L, rows)), 0, 0,
                mul(4 * 128L, rows), 4, mul(4 * 16L, rows), mul(4 * 16L, rows));
    }
    static void uri(String value) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return;
        if (value == null || value.length() > URI) {
            BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                    "SHARED_URI_PRIOR_MATERIAL_BOUND_UNAVAILABLE");
            throw new AssertionError("shared URI exceeds the prior local source bound");
        }
        local(value);
    }
    record Geometry(BenchmarkWorkloadDefinitions.Database database, int pipelines, int chains,
                    int phases, int setupStatements, int resetTargets, long fullRows, long cohortRows) { }
    static Geometry geometry(String id) {
        if (id == null || id.length() > 9) throw new AssertionError("shared workload id exceeds its fixed source shape");
        return switch (id) {
            case "copy" -> new Geometry(BenchmarkWorkloadDefinitions.Database.MYSQL, 1, 1, 4, 1 + 96000 / 500, 1, 96000, 48000);
            case "stateless" -> new Geometry(BenchmarkWorkloadDefinitions.Database.POSTGRES, 1, 1, 6, 2 + 96000 / 500, 1, 96000, 48000);
            case "stateful" -> new Geometry(BenchmarkWorkloadDefinitions.Database.MYSQL, 2, 4, 6, SQL_STATEMENTS, 2, 3L * 96000, 3L * 48000);
            default -> throw new AssertionError("shared workload is outside the fixed original recipes");
        };
    }

    static void scalar(String value, int maximum) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return;
        if (value == null || value.length() > maximum) refuse("SHARED_SCALAR_PRIOR_BOUND_UNAVAILABLE");
        // The unit is bounded owned material, with the same four-byte UTF-8 planning rule as the
        // existing material observer. This does not traverse text or assert actual encoded bytes.
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT,
                new BenchmarkReturnWorkBudget.Work(0, 0, 0, 0, mul(4, value.length()), 1, 1, 0));
    }

    static void sqlRoster(List<String> sql, int maximum) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        if (sql == null || sql.size() > maximum) refuse("SQL_HELPER_PRIOR_ROSTER_UNAVAILABLE");
        for (String command : sql) scalar(command, TEXT);
    }

    static void phaseSql(BenchmarkWorkloadDefinitions.Phase phase) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, Kind.PHASE_SHAPE, () -> {
            if (phase == null || phase.statementsPerBatch() < 1 || phase.statementsPerBatch() > BATCH_STATEMENTS
                    || phase.sql().size() % phase.statementsPerBatch() != 0
                    || phase.sql().size() / phase.statementsPerBatch() > BATCHES) {
                refuse("PHASE_BATCH_PRIOR_SHAPE_UNAVAILABLE");
            }
            sqlRoster(phase.sql(), PHASE_SQL_STATEMENTS); return null;
        });
    }

    /** Shape only. Exact SQL/source bytes still require the independently authenticated recipe. */
    static void originalWorkload(BenchmarkWorkloadDefinitions.Workload workload) {
        if (BenchmarkReturnWorkBudget.current() == null) return;
        entered(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, Kind.WORKLOAD_SHAPE, () -> {
            if (workload == null) refuse("SHARED_ORIGINAL_WORKLOAD_MISSING");
            scalar(workload.id(), 9); Geometry g = geometry(workload.id());
            if (workload.rows() != 96000 || workload.seed() != 424242 || workload.database() != g.database()
                    || workload.pipelineIds().size() != g.pipelines() || workload.sourceChains().size() != g.chains()
                    || workload.phases().size() != g.phases() || workload.setupSql().size() != g.setupStatements()) {
                refuse("SHARED_ORIGINAL_WORKLOAD_SHAPE_DIFFERS");
            }
            for (String pipeline : workload.pipelineIds()) scalar(pipeline, NAME);
            for (var chain : workload.sourceChains()) {
                scalar(chain.id(), NAME); scalar(chain.pipelineId(), NAME); scalar(chain.sourceId(), NAME);
                scalar(chain.table(), NAME); scalar(chain.terminalLogicalId(), NAME);
            }
            sqlRoster(workload.setupSql(), g.setupStatements());
            String[] ids = switch (workload.id()) {
                case "copy" -> new String[]{"snapshot", "warm-up", "cdc-update", "terminal"};
                case "stateless" -> new String[]{"snapshot", "warm-up", "cdc-settling-raised", "cdc-settling-restored", "cdc-update", "terminal"};
                case "stateful" -> new String[]{"snapshot", "warm-up", "hot-state", "cold-read", "cdc-update", "terminal"};
                default -> throw new AssertionError("shared original workload selector differs");
            };
            int[] statements = switch (workload.id()) {
                case "copy" -> new int[]{0, 1, 960, 1};
                case "stateless" -> new int[]{0, 1, 240, 240, 960, 1};
                case "stateful" -> new int[]{0, 4, 1, 960, 1920, 4};
                default -> throw new AssertionError("shared original SQL selector differs");
            };
            for (int i = 0; i < g.phases(); i++) {
                var phase = workload.phases().get(i); scalar(phase.id(), NAME);
                int perBatch = workload.id().equals("stateful") && phase.id().equals("cdc-update") ? 2 : 1;
                if (!ids[i].equals(phase.id()) || phase.sql().size() != statements[i] || phase.statementsPerBatch() != perBatch
                        || phase.targets().size() != g.resetTargets() || phase.expectedLogicalCoverage().size() > g.chains()) {
                    refuse("SHARED_ORIGINAL_PHASE_SHAPE_DIFFERS");
                }
                sqlRoster(phase.sql(), PHASE_SQL_STATEMENTS);
                for (String key : phase.expectedLogicalCoverage().keySet()) scalar(key, NAME);
                for (var target : phase.targets()) {
                    scalar(target.pipelineId(), NAME); scalar(target.table(), NAME); scalar(target.checksum(), 64);
                    if (target.location() == null || target.projection() == null || target.rows() < 0 || target.rows() > 96002) {
                        refuse("SHARED_ORIGINAL_TARGET_SHAPE_DIFFERS");
                    }
                }
            }
            return null;
        });
    }

    private static void refuse(String reason) {
        var scope = BenchmarkReturnWorkBudget.current();
        BenchmarkReturnWorkBudget.gap(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, reason);
        throw new AssertionError("shared support source shape is unavailable");
    }

    static BenchmarkReturnWorkBudget.Work maximum(BenchmarkReturnWorkBudget.Work a, BenchmarkReturnWorkBudget.Work b) {
        return new BenchmarkReturnWorkBudget.Work(Math.max(a.calls(), b.calls()), Math.max(a.requests(), b.requests()),
                Math.max(a.records(), b.records()), Math.max(a.frames(), b.frames()), Math.max(a.bytes(), b.bytes()),
                Math.max(a.passes(), b.passes()), Math.max(a.elements(), b.elements()), Math.max(a.updates(), b.updates()));
    }
    private static void put(EnumMap<Kind, BenchmarkReturnWorkBudget.Work> map, Kind key,
            long c, long q, long r, long f, long b, long p, long e, long u) {
        map.put(key, new BenchmarkReturnWorkBudget.Work(c, q, r, f, b, p, e, u));
    }
    private static long add(long a, long b) { return Math.addExact(a, b); }
    private static long mul(long a, long b) { return Math.multiplyExact(a, b); }
}
