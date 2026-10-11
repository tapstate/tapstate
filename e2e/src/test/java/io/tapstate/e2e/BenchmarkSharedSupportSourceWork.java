package io.tapstate.e2e;

import java.util.EnumMap;
import java.util.Map;

/** Per-entered owned helper work. Provider instruction, CPU and elapsed costs remain excluded. */
final class BenchmarkSharedSupportSourceWork {
    static final int TEXT = 65536, URI = 4096, RESOURCES = 64, SLOT_ROWS = 33;
    // The unchanged stateless recipe has 194 setup statements and one statement per phase batch.
    static final int SQL_STATEMENTS = 194;
    enum Kind {
        FIXTURE_CREATE, FIXTURE_REGISTRATION, PG_SETTINGS, PG_SERVER, PG_ADMIN_CONNECT,
        PG_CONNECT, PG_PROVISION, TARGET_RESET, SQL_PARENT, SQL_COMMAND,
        FIXTURE_CLOSE_PARENT, FIXTURE_DATABASE_CLOSE, CLOSING,
        SLOT_SETUP, SLOT_READ, SLOT_ROW, SLOT_DROP, SLOT_POLL, SLOT_RENDER, AWAIT_ITERATION,
        MONGO_URI, MONGO_CONFIGURATION, MONGO_ARGUMENTS, DOCKER_GATE, PLAIN_PROCESS_CLOSE, STAGING_DELETE,
        BATCH_LAYOUT, BATCH_ROW
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
        // One stateless target: URI constructor/getDatabase, client/create/database/collection/drop/
        // close, record accessors, iteration and guarded URI text. No stateful branch is admitted.
        put(result, Kind.TARGET_RESET, 1, 24, 0, 0, mul(8, add(mul(3, URI), 128)), 2, 4, 2);
        // Parent owns isEmpty/iterator/hasNext/next, fixed setup and one TWR close. Each command is
        // a separate entered child; the 194 bound is registered source input, not an observed total.
        put(result, Kind.SQL_PARENT, 1, add(12, mul(3, SQL_STATEMENTS)), 0, 0,
                mul(64, SQL_STATEMENTS), 2, SQL_STATEMENTS, SQL_STATEMENTS);
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
        put(result, Kind.BATCH_LAYOUT, 1, 16, 0, 0, 15360, 1, 960, 960);
        put(result, Kind.BATCH_ROW, 1, 8, 0, 0, 64, 1, 1, 1);
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
        return Map.copyOf(result);
    }

    static BenchmarkReturnWorkBudget.Work sharedUpper() {
        var rows = bounds(); var result = new BenchmarkReturnWorkBudget.Work(1, 1, 1, 0, 2097152, 1, 4096, 0);
        for (var kind : new Kind[]{Kind.FIXTURE_CREATE, Kind.FIXTURE_REGISTRATION, Kind.PG_SETTINGS,
                Kind.PG_SERVER, Kind.PG_ADMIN_CONNECT, Kind.PG_CONNECT, Kind.PG_PROVISION,
                Kind.TARGET_RESET, Kind.SQL_PARENT, Kind.SQL_COMMAND, Kind.MONGO_URI,
                Kind.MONGO_CONFIGURATION, Kind.MONGO_ARGUMENTS, Kind.DOCKER_GATE, Kind.BATCH_LAYOUT, Kind.BATCH_ROW}) result = maximum(result, rows.get(kind));
        return result;
    }

    static BenchmarkReturnWorkBudget.Work cleanupUpper() {
        var result = new BenchmarkReturnWorkBudget.Work(1, 1, 0, 0, 0, 0, 0, 0);
        var rows = bounds();
        for (var kind : new Kind[]{Kind.FIXTURE_CLOSE_PARENT, Kind.FIXTURE_DATABASE_CLOSE, Kind.CLOSING,
                Kind.SLOT_SETUP, Kind.SLOT_READ, Kind.SLOT_ROW, Kind.SLOT_DROP, Kind.SLOT_POLL,
                Kind.SLOT_RENDER, Kind.AWAIT_ITERATION, Kind.PLAIN_PROCESS_CLOSE, Kind.STAGING_DELETE}) result = maximum(result, rows.get(kind));
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
