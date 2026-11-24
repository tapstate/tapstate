package io.tapstate.e2e.benchmark;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/** Reads generator evidence back from MongoDB. Inspecting source data is timed separately. */
public final class SourceInspector {
    private SourceInspector() {
    }

    public record Size(long count, int min, double mean, int p95, int max) {
    }

    public record Table(long rows, long firstKey, long lastKey, int fields, long nullableValues, long nullValues,
                        Map<String, Map<String, Long>> bsonTypesByField, Size logicalBytes, Size bsonBytes,
                        Size stringLengths, long trueValues, long falseValues,
                        long matchedReferences, long missingReferences, Map<Integer, Long> arrayLengths,
                        boolean rowWidthWithinTolerance, String bsonSha256) {
    }

    static Map<String, Table> inspect(MongoDatabase source, WorkloadRecipe recipe, int batchSize) {
        Map<String, Table> reports = new LinkedHashMap<>();
        for (var input : recipe.inputs) {
            reports.put(input.name(), inspectTable(source, recipe, input, batchSize));
        }
        return Map.copyOf(reports);
    }

    private static Table inspectTable(MongoDatabase source, WorkloadRecipe recipe, WorkloadRecipe.Input input,
                                      int batchSize) {
        var reference = recipe.inputs.stream().filter(table -> table.name().equals(input.reference())).findFirst();
        long matched = 0;
        long missing = 0;
        long nullable = 0;
        long nulls = 0;
        long count = 0;
        long trueValues = 0;
        long falseValues = 0;
        var strings = new Histogram();
        var logical = new Histogram();
        var bson = new Histogram();
        Map<String, Map<String, Long>> types = new TreeMap<>();
        Map<Integer, Long> lengths = new TreeMap<>();
        var digest = sha256();
        try (var cursor = source.getCollection(input.name(), RawBsonDocument.class).find().sort(Sorts.ascending("_id"))
                .batchSize(batchSize).cursor()) {
            while (cursor.hasNext()) {
                var row = cursor.next();
                if (row.getInt64("_id").getValue() != input.firstKey() + count || row.size() != recipe.scalar.fields()) {
                    throw new IllegalStateException("Unexpected key or field count inspecting " + input.name());
                }
                for (var field : row.entrySet()) {
                    String name = field.getKey();
                    BsonValue value = field.getValue();
                    if (value.isString()) {
                        strings.add(value.asString().getValue().length());
                    } else if (value.getBsonType() == BsonType.BOOLEAN) {
                        if (value.asBoolean().getValue()) {
                            trueValues++;
                        } else {
                            falseValues++;
                        }
                    }
                    types.computeIfAbsent(name, ignored -> new TreeMap<>())
                            .merge(value.getBsonType().name(), 1L, Long::sum);
                    if (!name.equals("_id") && !name.equals(input.relationField()) && !name.equals(input.array())
                            && !input.required().containsKey(name)) {
                        nullable++;
                        if (value.isNull()) {
                            nulls++;
                        }
                    }
                }
                if (reference.isPresent()) {
                    var other = reference.get();
                    long key = row.getInt64(input.relationField()).getValue();
                    if (key >= other.firstKey() && key < other.firstKey() + other.rows()) {
                        matched++;
                    } else {
                        missing++;
                    }
                }
                if (input.array() != null) {
                    lengths.merge(row.getArray(input.array()).size(), 1L, Long::sum);
                }
                var bytes = row.getByteBuffer();
                try {
                    ByteBuffer buffer = bytes.asNIO().duplicate();
                    bson.add(buffer.remaining());
                    digest.update(buffer);
                } finally {
                    bytes.release();
                }
                logical.add(WorkloadRows.logicalBytes(row));
                count++;
            }
        }
        if (count != input.rows()) {
            throw new IllegalStateException("Unexpected source row count for " + input.name() + ": " + count);
        }
        types.replaceAll((name, observed) -> Map.copyOf(observed));
        Size logicalSize = logical.size();
        double drift = Math.abs(logicalSize.mean() - recipe.scalar.rowBytes()) / recipe.scalar.rowBytes();
        return new Table(count, input.firstKey(), input.firstKey() + count - 1, recipe.scalar.fields(), nullable,
                nulls, Map.copyOf(types), logicalSize, bson.size(), strings.size(), trueValues, falseValues,
                matched, missing, Map.copyOf(lengths),
                drift <= recipe.scalar.tolerance() / 100.0, HexFormat.of().formatHex(digest.digest()));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static final class Histogram {
        private final TreeMap<Integer, Long> frequencies = new TreeMap<>();
        private long count;
        private long total;

        void add(int size) {
            frequencies.merge(size, 1L, Long::sum);
            count++;
            total += size;
        }

        Size size() {
            if (count == 0) {
                return new Size(0, 0, 0, 0, 0);
            }
            long rank = (count * 95 + 99) / 100;
            long seen = 0;
            for (var entry : frequencies.entrySet()) {
                seen += entry.getValue();
                if (seen >= rank) {
                    return new Size(count, frequencies.firstKey(), (double) total / count, entry.getKey(),
                            frequencies.lastKey());
                }
            }
            throw new IllegalStateException("No source rows");
        }
    }
}
