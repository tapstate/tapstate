package io.tapstate.e2e;

import java.util.List;
import java.util.Map;

/** Scoped DTO material limits; successful validation is neither acquisition nor runtime proof. */
final class BenchmarkReturnCaptureMaterialBounds {
    private static final int CALLS = BenchmarkReturnLocalSourceWork.MAX_STORED_CALLS, ROWS = BenchmarkReturnLocalSourceWork.MAX_ROWS, CALL_ROWS = 1024, DETAILS = 512;
    private static final int COPY_LIST_CAP = CALLS > CALL_ROWS ? CALLS : CALL_ROWS;
    private static final int TOTAL = 2097152, PAGE = 65536, ENCODED_PAGE = 4 * ((PAGE + 2) / 3);
    private BenchmarkReturnCaptureMaterialBounds() { }

    static <T> List<T> copy(List<T> values, int cap) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return List.copyOf(values);
        return BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.COPY, () -> {
            require(cap >= 0 && cap <= COPY_LIST_CAP, "copy cap is outside the fixed material envelope");
            int count = count(values, cap); var owned = new java.util.ArrayList<T>(count);
            for (int index = 0; index < count; index++) owned.add(values.get(index));
            require(values.size() == count, "material roster changed during copy");
            return List.copyOf(owned);
        });
    }

    static Map<String, Object> stages(Map<String, Object> values) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return Map.copyOf(values);
        return BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.COPY, () -> {
            require(values != null, "stage wrapper is missing");
            int count = values.size();
            require(count >= 0 && count <= 512, "stage wrapper exceeds its shallow prior cap");
            var owned = new java.util.LinkedHashMap<String, Object>();
            var entries = values.entrySet().iterator();
            for (int index = 0; index < count; index++) {
                require(entries.hasNext(), "stage wrapper is shorter than declared"); var entry = entries.next();
                require(entry != null, "stage wrapper has a null entry");
                String key = entry.getKey(); Object value = entry.getValue();
                utf8(key, 128); require(value != null, "stage wrapper has a null value");
                require(owned.put(key, value) == null, "stage wrapper key is duplicated");
            }
            require(!entries.hasNext() && values.size() == count, "stage wrapper changed or has extra entries");
            return Map.copyOf(owned);
        });
    }

    static void callLists(List<BenchmarkWriteReturnLedger.ErrorDetail> details, List<String> fields,
                          List<BenchmarkWriteReturnLedger.Row> rows) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return;
        BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.COPY, () -> {
            size(details, DETAILS); size(fields, 2); size(rows, CALL_ROWS); return null;
        });
    }

    static void rowKeys(List<Integer> keys) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return;
        BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.COPY, () -> {
            size(keys, 2); return null;
        });
    }

    static void capture(List<BenchmarkWriteReturnAssembly.FullCall> calls, List<BenchmarkCausalClock.Sample> samples,
                        BenchmarkWriteReturnReader.Summary summary, List<String> pages, Map<String, Object> stages) {
        var scope = BenchmarkReturnWorkBudget.current();
        if (scope == null) return;
        BenchmarkReturnWorkBudget.operation(scope, BenchmarkReturnWorkBudget.Activity.ASSOCIATION, () -> {
            guard(calls, samples, summary, pages, stages); return null;
        });
    }

    private static void guard(List<BenchmarkWriteReturnAssembly.FullCall> calls, List<BenchmarkCausalClock.Sample> samples,
                              BenchmarkWriteReturnReader.Summary summary, List<String> pages, Map<String, Object> stages) {
        int callCount = count(calls, CALLS), pageCount = count(pages, 512); size(samples, 512);
        require(stages != null && stages.size() <= 512, "cost-stage wrapper exceeds its shallow prior cap");
        require(summary != null, "capture summary is missing");
        utf8(summary.window(), 512); utf8(summary.state(), 512);
        long rows = 0, material = 0;
        for (int callIndex = 0; callIndex < callCount; callIndex++) {
            var call = calls.get(callIndex);
            require(call != null, "call is null"); callListsPrior(call.errorDetails(), call.keyFields(), call.rows());
            rows = Math.addExact(rows, call.rows().size()); require(rows <= ROWS, "aggregate row roster exceeds its prior cap");
            material = materialAdd(material, utf8(call.failureType(), 256), utf8(call.scope(), 512),
                    utf8(call.writerIdentity(), 512), utf8(call.stream(), 512), utf8(call.target(), 512));
            for (String field : call.keyFields()) material = materialAdd(material, utf8(field, 128));
            for (var error : call.errorDetails()) {
                require(error != null, "error detail is null");
                material = materialAdd(material, utf8(error.failureType(), 256));
            }
            for (var row : call.rows()) {
                require(row != null, "row is null"); size(row.keys(), 2);
                for (Integer key : row.keys()) require(key != null, "row key is null");
                // One wire kind byte and up to two four-byte integer keys; no generic object encoding.
                material = materialAdd(material, 1L + 4L * row.keys().size());
            }
            require(material <= TOTAL, "call scalar/key material exceeds the original ledger byte envelope");
        }
        long encoded = 0;
        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            String page = pages.get(pageIndex);
            require(page != null && page.length() <= ENCODED_PAGE, "encoded page exceeds its prior scalar cap");
            encoded = Math.addExact(encoded, page.length());
            require(encoded <= 4L * (TOTAL + 2L * 512) / 3, "encoded page aggregate exceeds the prior ledger envelope");
        }
        // Semantic shape, row identity, failed/partial calls, raw equality and original ownership remain separate.
    }

    private static void callListsPrior(List<?> details, List<?> fields, List<?> rows) {
        size(details, DETAILS); size(fields, 2); size(rows, CALL_ROWS);
    }
    private static void size(List<?> values, int cap) { count(values, cap); }
    private static int count(List<?> values, int cap) {
        require(values != null, "material roster is missing");
        int count = values.size();
        require(count >= 0 && count <= cap, "material roster exceeds its prior cap");
        return count;
    }
    private static int utf8(String value, int maximum) {
        require(value != null && value.length() <= maximum, "scalar exceeds its prior character cap");
        int bytes = 0;
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            if (c <= 127) bytes = Math.addExact(bytes, 1);
            else if (c <= 2047) bytes = Math.addExact(bytes, 2);
            else if (Character.isHighSurrogate(c)) {
                require(index + 1 < value.length() && Character.isLowSurrogate(value.charAt(++index)), "unpaired surrogate");
                bytes = Math.addExact(bytes, 4);
            } else { require(!Character.isLowSurrogate(c), "unpaired surrogate"); bytes = Math.addExact(bytes, 3); }
            require(bytes <= maximum, "scalar exceeds its prior UTF8 byte cap");
        }
        return bytes;
    }
    private static long materialAdd(long... values) {
        long total = add(values); require(total <= TOTAL, "call scalar/key material exceeds the original ledger byte envelope"); return total;
    }
    private static long add(long... values) { long total = 0; for (long value : values) total = Math.addExact(total, value); return total; }

    static Map<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work> upper() {
        return BenchmarkReturnWorkBudget.operation(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, () -> {
        long slots = add(CALLS, 512, 512, (long) CALLS * DETAILS, 2L * CALLS, ROWS, 2L * ROWS);
        long visits = add(slots, TOTAL, 8192);
        // Per guarded visit: <=64 accessor/length/iterator/checked-arithmetic/JDK APIs and stores.
        // Fixed128 covers entry/failure setup. Extra8192 encloses summary/final-reject scalar visits; no encoded scalar arrays are allocated.
        var complete = new BenchmarkReturnWorkBudget.Work(1, add(128, Math.multiplyExact(64, visits)), ROWS, 0,
                Math.multiplyExact(16, visits), 8, Math.multiplyExact(32, visits), Math.multiplyExact(16, visits));
        long copyVisits = add(COPY_LIST_CAP, 512L * 128, 512);
        // Fixed size/get copies never invoke caller iterator/toArray; stage keys are bounded before hashing.
        // Includes up to128 UTF8 visits for each of512 shallow stage keys. Stage values are not traversed.
        var copies = new BenchmarkReturnWorkBudget.Work(1, add(128, Math.multiplyExact(64, copyVisits)), COPY_LIST_CAP, 0,
                add(256, Math.multiplyExact(32, copyVisits)), 8, Math.multiplyExact(32, copyVisits), Math.multiplyExact(16, copyVisits));
        return Map.of(BenchmarkReturnWorkBudget.Activity.ASSOCIATION, complete,
                BenchmarkReturnWorkBudget.Activity.COPY, copies,
                BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS, factoryUpper());
        });
    }
    static BenchmarkReturnWorkBudget.Work factoryUpper() {
        // At most64 fixed scalar slots:19 checked-add arguments,24 Work validation fields,
        // six map references and at most15 fixed locals/entry references. No input-sized loop.
        // <=four owned API/constructor/math calls per slot plus32 entry/map/cleanup calls.
        // <=32 allocated bytes per slot plus512 fixed object/array/map bytes; four fixed passes.
        return new BenchmarkReturnWorkBudget.Work(1, 32 + 4L * 64, 0, 0, 512 + 32L * 64, 4, 4L * 64, 4L * 64);
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError("capture material " + message); }
}
