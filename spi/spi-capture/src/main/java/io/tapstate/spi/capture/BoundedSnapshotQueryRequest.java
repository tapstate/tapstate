package io.tapstate.spi.capture;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable source selection for one bounded preview read. */
public record BoundedSnapshotQueryRequest(
        String sourceId,
        String connectorId,
        Map<String, Object> settings,
        TableSchema table,
        List<String> stableOrder,
        Selection selection,
        List<String> projection,
        int limit,
        long maxBytes,
        Instant deadline) {

    /** Maximum number of rows one query may return to its caller. */
    public static final int MAX_ROWS = 20_000;
    public static final long MAX_BYTES = 32L * 1024L * 1024L;

    public BoundedSnapshotQueryRequest(
            String sourceId,
            String connectorId,
            Map<String, Object> settings,
            TableSchema table,
            List<String> stableOrder,
            Selection selection,
            List<String> projection,
            int limit,
            Instant deadline) {
        this(sourceId, connectorId, settings, table, stableOrder, selection, projection,
                limit, MAX_BYTES, deadline);
    }

    public BoundedSnapshotQueryRequest {
        requireText(sourceId, "sourceId");
        requireText(connectorId, "connectorId");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(selection, "selection");
        Objects.requireNonNull(deadline, "deadline");
        if (settings == null) {
            settings = Map.of();
        } else {
            settings = Collections.unmodifiableMap(new LinkedHashMap<>(settings));
        }
        stableOrder = immutableNames(stableOrder, "stableOrder");
        projection = immutableNames(projection, "projection");
        if (limit < 1 || limit > MAX_ROWS) {
            throw new IllegalArgumentException("limit must be between 1 and " + MAX_ROWS);
        }
        if (maxBytes < 1 || maxBytes > MAX_BYTES) {
            throw new IllegalArgumentException("maxBytes must be between 1 and " + MAX_BYTES);
        }
        Set<String> fields = new LinkedHashSet<>();
        table.fields().forEach(field -> fields.add(field.name()));
        if (!fields.isEmpty() && !fields.containsAll(stableOrder)) {
            throw new IllegalArgumentException("stableOrder must name fields in the selected table");
        }
        if (!fields.isEmpty() && !fields.containsAll(projection)) {
            throw new IllegalArgumentException("projection must name fields in the selected table");
        }
        if (selection instanceof ExactTuples exact) {
            Set<String> expected = null;
            for (Map<String, Object> tuple : exact.tuples()) {
                Set<String> names = tuple.keySet();
                if (expected == null) {
                    expected = names;
                } else if (!expected.equals(names)) {
                    throw new IllegalArgumentException("exact tuples must have the same key fields");
                }
                if (!fields.isEmpty() && !fields.containsAll(names)) {
                    throw new IllegalArgumentException("exact tuple keys must name fields in the selected table");
                }
            }
        }
    }

    private static List<String> immutableNames(List<String> names, String name) {
        if (names == null || names.isEmpty()) {
            return List.of();
        }
        List<String> copy = new ArrayList<>(names.size());
        Set<String> seen = new LinkedHashSet<>();
        for (String each : names) {
            requireText(each, name + " entry");
            if (!seen.add(each)) {
                throw new IllegalArgumentException(name + " must not contain duplicate fields");
            }
            copy.add(each);
        }
        return List.copyOf(copy);
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    /** Selection without a row predicate, used for a finite anchor sample. */
    public sealed interface Selection permits AllRows, ExactTuples {
    }

    /** Read up to {@code limit} rows from the selected table. */
    public record AllRows() implements Selection {
    }

    /** Read every row matching one of these exact AND-key tuples. Tuples are issued independently. */
    public record ExactTuples(List<Map<String, Object>> tuples) implements Selection {
        public ExactTuples {
            Objects.requireNonNull(tuples, "tuples");
            List<Map<String, Object>> copy = new ArrayList<>(tuples.size());
            Set<String> expected = null;
            for (Map<String, Object> tuple : tuples) {
                Objects.requireNonNull(tuple, "tuple");
                if (tuple.isEmpty()) {
                    throw new IllegalArgumentException("exact tuple must contain at least one key field");
                }
                Map<String, Object> immutable = Collections.unmodifiableMap(new LinkedHashMap<>(tuple));
                Set<String> names = immutable.keySet();
                if (expected == null) {
                    expected = new LinkedHashSet<>(names);
                } else if (!expected.equals(names)) {
                    throw new IllegalArgumentException("exact tuples must have the same key fields");
                }
                copy.add(immutable);
            }
            tuples = List.copyOf(copy);
        }
    }
}
