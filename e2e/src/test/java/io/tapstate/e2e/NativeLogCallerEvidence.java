package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Complete caller rows sharing exact strings within one owned evidence phase. */
final class NativeLogCallerEvidence {
    static final String FORMAT = "PHASE_SYMBOLS_V1";
    static final long MAX_PHASE_BYTES = 2L * 1024 * 1024;
    static final long MAX_RECORD_BYTES = 65536;
    static final int MAX_RECORDS = 512, MAX_FRAMES = 512, MAX_OPEN_CALLS = 128;
    private NativeLogCallerEvidence() { }

    enum Compatibility { SHARED_ONLY, ALLOW_LEGACY_RECORD_LOCAL }

    record Table(String id, List<String> strings) {
        Table {
            require(id != null && !id.isBlank() && id.length() <= 128, "TABLE_ID");
            require(strings != null && bytes(Map.of("format", FORMAT, "id", id, "strings", strings)) <= MAX_PHASE_BYTES,
                    "TABLE_BYTE_BUDGET");
            strings = List.copyOf(strings);
            require(new LinkedHashSet<>(strings).size() == strings.size(), "DUPLICATE_SYMBOL");
        }
        Map<String, Object> evidence() { return Map.of("format", FORMAT, "id", id, "strings", strings); }
        static Table read(Object value) {
            require(value instanceof Map<?, ?>, "TABLE_REQUIRED");
            Map<?, ?> raw = (Map<?, ?>) value;
            require(raw.keySet().equals(java.util.Set.of("format", "id", "strings"))
                    && FORMAT.equals(raw.get("format")) && raw.get("id") instanceof String
                    && raw.get("strings") instanceof List<?>, "TABLE_SHAPE");
            require(bytes(raw) <= MAX_PHASE_BYTES, "TABLE_BYTE_BUDGET");
            return new Table((String) raw.get("id"), NativeLogCallerEvidence.strings(raw.get("strings")));
        }
    }

    record Snapshot(Table table, List<Map<String, Object>> records, List<Map<String, Object>> unpairedCalls,
            Map<String, Object> calibration) {
        Snapshot {
            Objects.requireNonNull(table, "table");
            require(records.size() <= MAX_RECORDS && unpairedCalls.size() <= MAX_OPEN_CALLS, "RECORD_COUNT_BUDGET");
            require(bytes(envelope(table, records, unpairedCalls, calibration)) <= MAX_PHASE_BYTES,
                    "PHASE_BYTE_BUDGET");
            records = immutableRecords(records); unpairedCalls = immutableRecords(unpairedCalls);
            calibration = immutableMap(calibration);
            for (var record : records) {
                require(!"LOG".equals(record.get("target")) || record.containsKey("callers")
                        || record.containsKey("decoderStatus"), "MISSING_LOG_CALLERS");
                verifyReferences(record, table);
            }
            for (var call : unpairedCalls) { verifyReferences(call, table); }
            verifyReferences(calibration, table);
            require(bytes(envelope(table, records, unpairedCalls, calibration)) <= MAX_PHASE_BYTES,
                    "PHASE_BYTE_BUDGET");
        }
        Map<String, Object> evidence() { return envelope(table, records, unpairedCalls, calibration); }
        long logicalBytes() { return bytes(evidence()); }
    }

    /** Planning an addition never modifies the retained table or record set before all checks succeed. */
    static final class Phase {
        private final String owner;
        private long sequence = 1;
        private final List<String> symbols = new ArrayList<>();
        private final Map<String, Integer> indexes = new LinkedHashMap<>();
        private final List<Map<String, Object>> records = new ArrayList<>();

        Phase(String owner) {
            require(owner != null && !owner.isBlank() && owner.length() <= 96 && !owner.contains("#"), "OWNER_ID");
            this.owner = owner;
        }
        private String id() { return owner + "#" + sequence; }

        Map<String, Object> retain(Map<String, Object> raw) {
            require(records.size() < MAX_RECORDS, "RECORD_COUNT_BUDGET");
            Planner plan = new Planner(id(), symbols, indexes);
            Map<String, Object> encoded = plan.record(raw);
            List<Map<String, Object>> candidate = new ArrayList<>(records); candidate.add(encoded);
            new Snapshot(plan.table(), candidate, List.of(), Map.of());
            // Commit the exact plan only after its whole logical envelope fits.
            for (String value : plan.added.keySet()) {
                indexes.put(value, symbols.size()); symbols.add(value);
            }
            records.add(encoded);
            return encoded;
        }

        Snapshot snapshot() { return snapshot(List.of(), Map.of()); }

        Snapshot snapshot(List<Map<String, Object>> open, Map<String, Object> calibration) {
            require(open.size() <= MAX_OPEN_CALLS, "OPEN_CALL_COUNT_BUDGET");
            Planner plan = new Planner(id(), symbols, indexes);
            List<Map<String, Object>> encodedOpen = new ArrayList<>();
            for (Map<String, Object> call : open) {
                if (call.get("entry") instanceof Map<?, ?> entry) {
                    Map<String, Object> copy = new LinkedHashMap<>(call);
                    copy.put("entry", plan.record(stringMap(entry)));
                    encodedOpen.add(immutableMap(copy));
                } else { encodedOpen.add(plan.record(call)); }
            }
            Map<String, Object> encodedCalibration = calibration.isEmpty() ? Map.of() : plan.record(calibration);
            return new Snapshot(plan.table(), records, encodedOpen, encodedCalibration);
        }

        void reset() {
            sequence = Math.addExact(sequence, 1);
            symbols.clear(); indexes.clear(); records.clear();
        }
    }

    private static final class Planner {
        private final String id;
        private final List<String> symbols;
        private final Map<String, Integer> indexes;
        private final Map<String, Integer> added = new LinkedHashMap<>();

        Planner(String id, List<String> symbols, Map<String, Integer> indexes) {
            this.id = id; this.symbols = symbols; this.indexes = indexes;
        }
        Table table() {
            long projected = bytes(Map.of("format", FORMAT, "id", id, "strings", symbols));
            for (String value : added.keySet()) { projected = Math.addExact(projected, bytes(value)); }
            require(projected <= MAX_PHASE_BYTES, "TABLE_BYTE_BUDGET");
            List<String> all = new ArrayList<>(symbols); all.addAll(added.keySet());
            return new Table(id, all);
        }
        int symbol(String value) {
            Integer existing = indexes.get(value);
            return existing != null ? existing : added.computeIfAbsent(value, ignored -> symbols.size() + added.size());
        }
        Map<String, Object> record(Map<String, Object> raw) {
            require(raw != null, "RECORD_REQUIRED");
            Map<String, Object> expanded = new LinkedHashMap<>(raw);
            List<Object> decoded = null;
            if (raw.containsKey("callers")) {
                decoded = decode(raw.get("callers"), table(), Compatibility.ALLOW_LEGACY_RECORD_LOCAL);
                expanded.put("callers", decoded);
            }
            long cap = "PRODUCE".equals(raw.get("target")) ? 131072 : MAX_RECORD_BYTES;
            require(bytes(expanded) <= cap, "EXPANDED_RECORD_BYTE_BUDGET");
            if (decoded != null) {
                List<String> local = strings(decoded.getFirst());
                List<?> rows = (List<?>) decoded.get(1);
                List<Object> shared = new ArrayList<>(rows.size());
                for (int at = 0; at < rows.size(); at++) {
                    if (numericColumn(at)) { shared.add(rows.get(at)); }
                    else {
                        int index = (Integer) rows.get(at);
                        shared.add(index < 0 ? -1 : symbol(local.get(index)));
                    }
                }
                expanded.put("callers", Map.of("format", FORMAT, "tableId", id, "rows", List.copyOf(shared)));
            }
            Map<String, Object> encoded = immutableMap(expanded);
            require(bytes(encoded) <= cap, "ENCODED_RECORD_BYTE_BUDGET");
            return encoded;
        }
    }

    /** Old record-local lists have their own explicit branch; a malformed reference never enters it. */
    static List<Object> decode(Object encoded, Table table, Compatibility compatibility) {
        Objects.requireNonNull(compatibility, "compatibility");
        if (encoded instanceof List<?> legacy) {
            require(compatibility == Compatibility.ALLOW_LEGACY_RECORD_LOCAL, "LEGACY_NOT_SELECTED");
            require(legacy.size() == 2, "LEGACY_SHAPE");
            require(bytes(legacy) <= MAX_RECORD_BYTES, "DECODED_CALLER_BYTE_BUDGET");
            List<String> symbols = strings(legacy.getFirst());
            return List.of(symbols, rows(legacy.get(1), symbols.size()));
        }
        require(encoded instanceof Map<?, ?>, "CALLER_REFERENCE_REQUIRED");
        Map<?, ?> reference = (Map<?, ?>) encoded;
        require(reference.keySet().equals(java.util.Set.of("format", "tableId", "rows"))
                && FORMAT.equals(reference.get("format")) && reference.get("tableId") instanceof String,
                "CALLER_REFERENCE_SHAPE");
        require(table != null && table.id().equals(reference.get("tableId")), "MISSING_OR_FOREIGN_TABLE");
        List<Object> shared = rows(reference.get("rows"), table.strings().size());
        Map<String, Integer> localIndexes = new LinkedHashMap<>();
        List<Object> localRows = new ArrayList<>(shared.size());
        for (int at = 0; at < shared.size(); at++) {
            if (numericColumn(at)) { localRows.add(shared.get(at)); }
            else {
                int index = (Integer) shared.get(at);
                localRows.add(index < 0 ? -1 : localIndexes.computeIfAbsent(table.strings().get(index),
                        ignored -> localIndexes.size()));
            }
        }
        List<Object> result = List.of(List.copyOf(localIndexes.keySet()), List.copyOf(localRows));
        require(bytes(result) <= MAX_RECORD_BYTES, "DECODED_CALLER_BYTE_BUDGET");
        return result;
    }

    private static boolean numericColumn(int offset) { return offset % 9 == 3 || offset % 9 == 4; }

    private static List<Object> rows(Object value, int symbolCount) {
        require(value instanceof List<?>, "ROWS_REQUIRED");
        List<?> input = (List<?>) value;
        require(input.size() % 9 == 0 && input.size() / 9 <= MAX_FRAMES, "FRAME_BOUND_OR_SHAPE");
        List<Object> result = new ArrayList<>(input.size());
        for (int at = 0; at < input.size(); at++) {
            Object item = input.get(at);
            require(item instanceof Integer || item instanceof Long, "NON_INTEGRAL_COLUMN");
            long number = ((Number) item).longValue();
            if (numericColumn(at)) {
                require(number >= -1, "NUMERIC_COLUMN_RANGE"); result.add(number);
            } else {
                require(number >= -1 && number < symbolCount && number <= Integer.MAX_VALUE, "SYMBOL_INDEX_RANGE");
                result.add((int) number);
            }
        }
        return List.copyOf(result);
    }

    private static List<String> strings(Object value) {
        require(value instanceof List<?>, "SYMBOLS_REQUIRED");
        List<String> result = new ArrayList<>();
        for (Object item : (List<?>) value) { require(item instanceof String, "NON_STRING_SYMBOL"); result.add((String) item); }
        require(new LinkedHashSet<>(result).size() == result.size(), "DUPLICATE_SYMBOL");
        return List.copyOf(result);
    }

    /** Identical logical accounting to the passive session, independent of serialized or compressed sizes. */
    static long bytes(Object value) { return bytes(value, 0); }
    private static long bytes(Object value, int depth) {
        require(depth <= 16, "EVIDENCE_DEPTH_BUDGET");
        if (value == null || value instanceof Number || value instanceof Boolean) { return 16; }
        if (value instanceof String text) { return 4L * text.length() + 16; }
        long result = 32;
        if (value instanceof Map<?, ?> map) {
            for (var entry : map.entrySet()) {
                result = Math.addExact(result, bytes(entry.getKey(), depth + 1));
                result = Math.addExact(result, bytes(entry.getValue(), depth + 1));
                if (result > MAX_PHASE_BYTES) { return result; }
            }
            return result;
        }
        if (value instanceof Collection<?> values) {
            for (Object item : values) {
                result = Math.addExact(result, bytes(item, depth + 1));
                if (result > MAX_PHASE_BYTES) { return result; }
            }
            return result;
        }
        throw invalid("UNMAPPED_EVIDENCE_TYPE");
    }

    private static Map<String, Object> envelope(Table table, List<Map<String, Object>> records,
            List<Map<String, Object>> open, Map<String, Object> calibration) {
        return Map.of("callerSymbols", table.evidence(), "records", records, "unpairedCalls", open,
                "logFamilyCalibration", calibration);
    }
    private static Map<String, Object> stringMap(Map<?, ?> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.forEach((key, value) -> { require(key instanceof String, "RECORD_KEY"); result.put((String) key, value); });
        return result;
    }
    private static Map<String, Object> immutableMap(Map<String, ?> source) {
        return freezeMap(source, 0);
    }
    private static Map<String, Object> freezeMap(Map<String, ?> source, int depth) {
        require(depth <= 16, "EVIDENCE_DEPTH_BUDGET");
        Map<String, Object> copy = new LinkedHashMap<>(); source.forEach((key, value) -> copy.put(key, freeze(value, depth + 1)));
        return Collections.unmodifiableMap(copy);
    }
    private static Object freeze(Object value, int depth) {
        require(depth <= 16, "EVIDENCE_DEPTH_BUDGET");
        if (value instanceof Map<?, ?> map) { return freezeMap(stringMap(map), depth); }
        if (value instanceof Collection<?> values) {
            List<Object> copy = new ArrayList<>(); for (Object item : values) { copy.add(freeze(item, depth + 1)); }
            return Collections.unmodifiableList(copy);
        }
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) { return value; }
        throw invalid("UNMAPPED_EVIDENCE_TYPE");
    }
    private static List<Map<String, Object>> immutableRecords(List<Map<String, Object>> source) {
        List<Map<String, Object>> copy = new ArrayList<>();
        for (var record : source) { copy.add(immutableMap(record)); }
        return List.copyOf(copy);
    }
    private static void verifyReferences(Map<String, Object> record, Table table) {
        if (record.containsKey("callers")) { decode(record.get("callers"), table, Compatibility.SHARED_ONLY); }
        if (record.get("entry") instanceof Map<?, ?> entry) { verifyReferences(stringMap(entry), table); }
    }
    private static void require(boolean valid, String reason) { if (!valid) { throw invalid(reason); } }
    private static AssertionError invalid(String reason) { return new AssertionError("Invalid native caller evidence: " + reason); }
}
