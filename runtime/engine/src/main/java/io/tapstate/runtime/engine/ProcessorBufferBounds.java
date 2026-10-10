package io.tapstate.runtime.engine;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Named transient record bounds supplied by the compiler that draws a processor vertex. */
public record ProcessorBufferBounds(Map<String, Long> fixedRecords,
        Map<String, Long> recordsPerInputRecord, List<String> diagnostics) {
    public ProcessorBufferBounds {
        fixedRecords = checked(fixedRecords);
        recordsPerInputRecord = checked(recordsPerInputRecord);
        diagnostics = List.copyOf(diagnostics);
    }

    /** Evaluates the descriptor against the processor's actual inbox or authored input-batch limit. */
    public long upperBound(long inputRecords) {
        if (inputRecords < 0) {
            throw new IllegalArgumentException("a record bound cannot be negative");
        }
        long bound = 0;
        for (long records : fixedRecords.values()) {
            bound = Math.addExact(bound, records);
        }
        for (long records : recordsPerInputRecord.values()) {
            bound = Math.addExact(bound, Math.multiplyExact(records, inputRecords));
        }
        return bound;
    }

    /** A port's one output and the generic adapter's position-stamped copy, both retained at backpressure. */
    public static ProcessorBufferBounds oneOutput() {
        return new ProcessorBufferBounds(Map.of("port-output", 1L, "position-stamped-output", 1L), Map.of(),
                List.of("input-row payload bytes and key-index metadata are not a total heap guarantee"));
    }

    private static Map<String, Long> checked(Map<String, Long> counts) {
        Map<String, Long> checked = new LinkedHashMap<>();
        counts.forEach((name, count) -> {
            if (name == null || name.isBlank() || count == null || count < 0) {
                throw new IllegalArgumentException("buffer descriptors require named nonnegative counts");
            }
            checked.put(name, count);
        });
        return Map.copyOf(checked);
    }
}
