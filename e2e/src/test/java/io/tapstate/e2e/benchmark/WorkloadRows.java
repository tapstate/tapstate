package io.tapstate.e2e.benchmark;

import static io.tapstate.e2e.benchmark.WorkloadRecipe.Kind.ARRAY;
import static io.tapstate.e2e.benchmark.WorkloadRecipe.Kind.INTEGER;
import static io.tapstate.e2e.benchmark.WorkloadRecipe.Kind.STRING;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SplittableRandom;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

final class WorkloadRows {
    private static final String ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final DateTimeFormatter MILLIS_UTC = new DateTimeFormatterBuilder().appendInstant(3).toFormatter();
    private final WorkloadRecipe recipe;
    private final WorkloadRecipe.Input input;
    private final WorkloadRecipe.Input reference;
    private final LinkedHashMap<String, WorkloadRecipe.Kind> fields = new LinkedHashMap<>();
    private final List<String> mutable;

    WorkloadRows(WorkloadRecipe recipe, WorkloadRecipe.Input input) {
        this.recipe = recipe;
        this.input = input;
        reference = recipe.inputs.stream().filter(table -> table.name().equals(input.reference())).findFirst().orElse(null);
        var remaining = new EnumMap<>(recipe.scalar.fieldCounts());
        add("_id", INTEGER, remaining);
        if (reference != null) {
            add(input.relationField(), INTEGER, remaining);
        }
        input.required().entrySet().stream().sorted(java.util.Map.Entry.comparingByKey())
                .filter(entry -> !fields.containsKey(entry.getKey()))
                .forEach(entry -> add(entry.getKey(), entry.getValue(), remaining));
        if (input.array() != null) {
            add(input.array(), STRING, remaining);
            fields.put(input.array(), ARRAY);
        }
        for (var kind : WorkloadRecipe.Kind.values()) {
            for (int index = 0; index < remaining.getOrDefault(kind, 0); index++) {
                String name = kind.prefix + index;
                fields.put(name, kind);
            }
        }
        if (fields.size() != recipe.scalar.fields()) {
            throw new IllegalArgumentException("Field budgets do not match profile for " + input.name());
        }
        mutable = fields.keySet().stream()
                .filter(name -> !name.equals("_id") && !name.equals(input.relationField())
                        && !input.protectedFields().contains(name))
                .toList();
    }

    private void add(String name, WorkloadRecipe.Kind kind, EnumMap<WorkloadRecipe.Kind, Integer> remaining) {
        int available = remaining.getOrDefault(kind, 0);
        if (available < 1 || fields.putIfAbsent(name, kind) != null) {
            throw new IllegalArgumentException("Cannot allocate required field " + name + " for " + input.name());
        }
        remaining.put(kind, available - 1);
    }

    BsonDocument row(long key, int stringLength) {
        var random = new SplittableRandom(recipe.identity.seed() ^ key);
        var row = new BsonDocument("_id", new BsonInt64(key));
        for (var field : fields.entrySet()) {
            String name = field.getKey();
            if (name.equals("_id")) {
                continue;
            }
            if (name.equals(input.relationField())) {
                row.put(name, new BsonInt64(referenceKey(key, random)));
            } else if (field.getValue() != ARRAY && !input.required().containsKey(name)
                    && random.nextInt(100) < recipe.scalar.nullPercent()) {
                row.put(name, BsonNull.VALUE);
            } else {
                row.put(name, value(field.getValue(), random.split(), stringLength));
            }
        }
        return row;
    }

    private long referenceKey(long key, SplittableRandom random) {
        long index = key - input.firstKey();
        if (input.nested()) {
            return reference.firstKey() + index / input.fanout() % reference.rows();
        }
        // A fixed cardinality of misses, with a uniform distribution within the matched set.
        if ((index + 1) * input.matchPercent() / 100 == index * input.matchPercent() / 100) {
            return -index - 1;
        }
        return reference.firstKey() + random.nextInt(reference.rows());
    }

    int calibrate() {
        int low = 1;
        int high = recipe.scalar.rowBytes();
        int sample = Math.min(128, input.rows());
        while (low < high) {
            int candidate = (low + high) / 2;
            long total = 0;
            for (int index = 0; index < sample; index++) {
                long key = input.firstKey() + (long) index * input.rows() / sample;
                total += logicalBytes(row(key, candidate));
            }
            if (total < (long) recipe.scalar.rowBytes() * sample) {
                low = candidate + 1;
            } else {
                high = candidate;
            }
        }
        return low;
    }

    List<BsonDocument> mutation(long key, long ordinal, int stringLength) {
        var random = new SplittableRandom(recipe.identity.seed() ^ key ^ Long.rotateLeft(ordinal + 1, 29));
        int count = Math.max(1, Math.toIntExact(((long) mutable.size() * recipe.updatePercent + 50) / 100));
        var available = new ArrayList<>(mutable);
        var selected = new ArrayList<String>(count);
        if (available.remove("value")) {
            selected.add("value");
        }
        while (selected.size() < count) {
            selected.add(available.remove(random.nextInt(available.size())));
        }
        var changes = new BsonDocument();
        for (String name : selected) {
            var kind = fields.get(name);
            BsonValue value = value(kind, random.split(), stringLength);
            BsonValue alternative = alternative(kind, value);
            var equals = new BsonDocument("$eq", new BsonArray(List.of(new BsonString("$" + name), literal(value))));
            changes.put(name, new BsonDocument("$cond", new BsonArray(List.of(equals, literal(alternative), literal(value)))));
        }
        return List.of(new BsonDocument("$set", changes));
    }

    private static BsonDocument literal(BsonValue value) {
        return new BsonDocument("$literal", value);
    }

    private BsonValue value(WorkloadRecipe.Kind kind, SplittableRandom random, int stringLength) {
        var scalar = recipe.scalar;
        return switch (kind) {
            case STRING -> {
                int minimum = Math.max(1, (stringLength * scalar.stringMinPercent() + 99) / 100);
                int maximum = Math.max(minimum, stringLength * scalar.stringMaxPercent() / 100);
                int length = minimum + (int) (random.nextDouble() * (maximum - minimum + 1));
                var result = new StringBuilder(length);
                for (int index = 0; index < length; index++) {
                    result.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
                }
                yield new BsonString(result.toString());
            }
            case INTEGER -> new BsonInt32((int) random.nextLong(scalar.minInt(), (long) scalar.maxInt() + 1));
            case DECIMAL -> new BsonDecimal128(new Decimal128(
                    BigDecimal.valueOf(random.nextLong(scalar.minCents(), scalar.maxCents() + 1), scalar.decimalScale())));
            case BOOLEAN -> BsonBoolean.valueOf(random.nextInt(100) < scalar.truePercent());
            case DATETIME -> new BsonDateTime(random.nextLong(scalar.dateStart(), scalar.dateEnd()));
            case ARRAY -> {
                var elements = new BsonArray();
                for (int index = 0; index < input.elements(); index++) {
                    elements.add(new BsonDocument(input.elementKey(), new BsonInt32(index))
                            .append(input.elementOrder(), new BsonInt32(index))
                            .append(input.elementValue(), new BsonInt32(random.nextInt(1_000_000))));
                }
                yield elements;
            }
        };
    }

    private BsonValue alternative(WorkloadRecipe.Kind kind, BsonValue value) {
        return switch (kind) {
            case STRING -> {
                String original = value.asString().getValue();
                yield new BsonString((original.charAt(0) == 'A' ? "B" : "A") + original.substring(1));
            }
            case INTEGER -> new BsonInt32(value.asInt32().getValue() == recipe.scalar.minInt()
                    ? recipe.scalar.maxInt() : recipe.scalar.minInt());
            case DECIMAL -> {
                var minimum = BigDecimal.valueOf(recipe.scalar.minCents(), recipe.scalar.decimalScale());
                var maximum = BigDecimal.valueOf(recipe.scalar.maxCents(), recipe.scalar.decimalScale());
                yield new BsonDecimal128(new Decimal128(value.asDecimal128().decimal128Value().bigDecimalValue()
                        .compareTo(minimum) == 0 ? maximum : minimum));
            }
            case BOOLEAN -> BsonBoolean.valueOf(!value.asBoolean().getValue());
            case DATETIME -> new BsonDateTime(value.asDateTime().getValue() == recipe.scalar.dateStart()
                    ? recipe.scalar.dateEnd() - 1 : recipe.scalar.dateStart());
            case ARRAY -> {
                var elements = new BsonArray(value.asArray().getValues());
                var first = elements.get(0).asDocument().clone();
                int old = first.getInt32(input.elementValue()).getValue();
                first.put(input.elementValue(), new BsonInt32((old + 1) % 1_000_000));
                elements.set(0, first);
                yield elements;
            }
        };
    }

    static int logicalBytes(BsonDocument row) {
        return logicalSize(row);
    }

    private static int logicalSize(BsonValue value) {
        return switch (value.getBsonType()) {
            case DOCUMENT -> {
                var document = value.asDocument();
                int count = 2 + Math.max(0, document.size() - 1);
                for (var field : document.entrySet()) {
                    count += quotedSize(field.getKey()) + 1 + logicalSize(field.getValue());
                }
                yield count;
            }
            case ARRAY -> {
                var array = value.asArray();
                int count = 2 + Math.max(0, array.size() - 1);
                for (var element : array) {
                    count += logicalSize(element);
                }
                yield count;
            }
            case STRING -> quotedSize(value.asString().getValue());
            case INT32 -> Integer.toString(value.asInt32().getValue()).length();
            case INT64 -> Long.toString(value.asInt64().getValue()).length();
            case DECIMAL128 -> value.asDecimal128().decimal128Value().bigDecimalValue().toPlainString().length();
            case BOOLEAN -> value.asBoolean().getValue() ? 4 : 5;
            case DATE_TIME -> quotedSize(MILLIS_UTC.format(Instant.ofEpochMilli(value.asDateTime().getValue())));
            case NULL -> 4;
            case BINARY -> 2 + Base64.getEncoder().encode(value.asBinary().getData()).length;
            default -> throw new IllegalArgumentException("Unsupported logical type " + value.getBsonType());
        };
    }

    private static int quotedSize(String value) {
        int count = 2;
        for (int index = 0; index < value.length();) {
            int cp = value.codePointAt(index);
            index += Character.charCount(cp);
            count += switch (cp) {
                case '"', '\\', '\b', '\t', '\n', '\f', '\r' -> 2;
                default -> cp < 32 ? 6 : cp < 128 ? 1 : cp < 2048 ? 2 : cp < 65536 ? 3 : 4;
            };
        }
        return count;
    }
}
