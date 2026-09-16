package io.tapstate.adapters.pdk;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.value.ByteData;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

/**
 * Projects a PDK {@code TapEvent} to and from the tapstate event envelope — the stable currency every
 * downstream transform sees. The projection rules are a long-lived contract: they are pinned by a
 * golden sample, so changing them is a reviewed change to the golden, never a silent drift.
 *
 * <p>Whether a row is a snapshot read ({@code op=r}) or a cdc insert ({@code op=i}) is not carried on
 * the event — a snapshot row and a cdc insert are the same insert-shaped {@code TapEvent} on the
 * wire; the phase is external truth, known from which function produced the batch. So decode is
 * split by phase: {@link #decodeSnapshotRow} for batch-read output, {@link #decodeChange} for the
 * change stream. A ddl event is projected, never dropped — swallowing it would silently break the
 * schema-evolution chain downstream.
 *
 * <p>Field projection: {@code src} is the event's table id (the logical stream name); {@code ts} is
 * the source reference time, falling back to the event time. Row data maps ({@code before}/
 * {@code after}) keep every field the connector reported, with each value carried into the value
 * model described below. A ddl event's {@code schema} carries the origin ddl when the connector
 * supplied one; precise per-kind ddl translation is deferred, so this stays a coarse, pass-through
 * "track" projection.
 */
public final class TapEventCodec {

    /** The {@code schema}-map key under which a ddl event's origin ddl travels. */
    private static final String DDL_ORIGIN = "origin";
    private static final String BSON_DECIMAL128 = "org.bson.types.Decimal128";
    private static final ClassValue<Decimal128Access> BSON_DECIMAL128_ACCESS = new ClassValue<>() {
        @Override
        protected Decimal128Access computeValue(Class<?> type) {
            try {
                return new Decimal128Access(type.getMethod("isFinite"), type.getMethod("bigDecimalValue"));
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("mongodb decimal128 has no exact-value accessor", e);
            }
        }
    };

    private static final String BSON_TIMESTAMP = "org.bson.BsonTimestamp";
    private static final ClassValue<Method> BSON_TIMESTAMP_TIME = new ClassValue<>() {
        @Override
        protected Method computeValue(Class<?> type) {
            try {
                return type.getMethod("getTime");
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("mongodb timestamp has no time accessor", e);
            }
        }
    };

    private record Decimal128Access(Method isFinite, Method bigDecimalValue) {
    }

    private TapEventCodec() {
    }

    /**
     * Decodes a change-stream event ({@code i}/{@code u}/{@code d}/{@code ddl}).
     *
     * @throws IllegalArgumentException if the event is not a mapped change type
     */
    public static Envelope decodeChange(
            TapEvent event, TapCodecsRegistry codecs, Map<String, String> columnTypes) {
        Objects.requireNonNull(codecs, "codecs");
        Objects.requireNonNull(columnTypes, "columnTypes");
        if (event instanceof TapInsertRecordEvent insert) {
            return Envelope.insert(ts(insert), src(insert), row(insert.getAfter(), codecs, columnTypes), null);
        }
        if (event instanceof TapUpdateRecordEvent update) {
            // One reading over both images, because they are two halves of one row. Taken per image, an
            // update whose before image the connector reported without the column that names a driver
            // type would decode its arrays as the text they travelled as on that side and as the driver's
            // own type on the other - the two halves of one change disagreeing about a value that never
            // changed, with nothing on either side able to see it.
            SchemaNames names =
                    SchemaNames.read(codecs, columnTypes, update.getBefore(), update.getAfter());
            return Envelope.update(ts(update), src(update),
                    walk(update.getBefore(), codecs, names, true),
                    walk(update.getAfter(), codecs, names, true),
                    null);
        }
        if (event instanceof TapDeleteRecordEvent delete) {
            return Envelope.delete(ts(delete), src(delete), row(delete.getBefore(), codecs, columnTypes), null);
        }
        if (event instanceof TapDDLEvent ddl) {
            return Envelope.ddl(ts(ddl), src(ddl), ddlSchema(ddl));
        }
        throw new IllegalArgumentException("unmapped change event type: " + event.getClass().getName());
    }

    /**
     * Decodes a snapshot row as {@code op=r}. Batch reads yield insert-shaped rows only.
     *
     * @throws IllegalArgumentException if the event is not insert-shaped
     */
    public static Envelope decodeSnapshotRow(
            TapEvent event, TapCodecsRegistry codecs, Map<String, String> columnTypes) {
        Objects.requireNonNull(codecs, "codecs");
        Objects.requireNonNull(columnTypes, "columnTypes");
        if (event instanceof TapInsertRecordEvent read) {
            return Envelope.read(ts(read), src(read), row(read.getAfter(), codecs, columnTypes), null);
        }
        throw new IllegalArgumentException(
                "snapshot rows are insert-shaped; got: " + event.getClass().getName());
    }

    // ---- the value model a decoded row speaks --------------------------------------------

    /**
     * One row's values carried into the tapstate value model, or {@code null} when the map is absent.
     *
     * <p>A row travels in two lanes, and which one a value takes is the connector's answer, not ours.
     * A driver type the connector registered a conversion for takes that conversion and arrives as
     * the portable value the connector chose; everything else — the ordinary Java boxes — arrives as
     * a bare value, normalized. Two compatibility corrections cover MongoDB conversions with known
     * loss: a Decimal128 narrowed to a double is replaced with the source's exact portable decimal,
     * and a timestamp whose seconds were read as milliseconds is rebuilt from seconds. Only those
     * exact results are replaced. A row is therefore mixed, which is the contract rather than a gap:
     * the frozen conversion surface deliberately registers nothing for the ordinary boxes, so putting
     * them through it would pay a wrapper for a conversion that does not exist.
     *
     * <p>On the bare lane: a driver hands over whatever box its own client uses — an int column may
     * arrive in any integral box, a real one as a float — while the type namespace a column resolves
     * into names one width per kind. Converting here is what makes the two agree: the boundary that
     * resolves a column's type is the boundary that delivers a value of that type, so nothing
     * downstream has to reconcile a column declared 64-bit with a value that is not.
     *
     * <p>Every conversion widens or re-wraps and none of them rounds, so no value changes on the way
     * in. Only a value the target actually holds is converted: a wider integer is left as it came,
     * where narrowing it would hand every reader downstream a different number and report it as a
     * success. An exact fixed-point number stays exact in every case — routing it through any binary
     * floating point type drops digits silently, which is the one loss nothing downstream could detect.
     */
    private static Map<String, Object> row(
            Map<String, Object> row, TapCodecsRegistry codecs, Map<String, String> columnTypes) {
        return walk(row, codecs, SchemaNames.read(codecs, columnTypes, row, null), true);
    }

    /**
     * The connector's own conversions applied to a row, at any depth, plus the compatibility corrections
     * described above; the widths the type namespace speaks are left exactly as the driver handed them
     * over.
     *
     * <p>This is what a read face wants. It reports what the database holds rather than what a pipeline
     * row speaks, so widening an integer there would answer a question nobody asked; but the registered
     * conversions still have to run, because without them the face is handed the driver's own objects
     * and has nothing to render but their addresses. No declared types are supplied: those name what a
     * sink would rebuild, and a read rebuilds nothing.
     */
    static Map<String, Object> connectorConverted(Map<String, Object> row, TapCodecsRegistry codecs) {
        return walk(row, codecs, SchemaNames.NONE, false);
    }

    private static Map<String, Object> walk(Map<String, Object> row, TapCodecsRegistry codecs,
            SchemaNames names, boolean toNamespaceWidths) {
        if (row == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>(row.size());
        boolean changed = false;
        for (Map.Entry<String, Object> column : row.entrySet()) {
            Object value =
                    converted(column.getValue(), codecs, names, column.getKey(), toNamespaceWidths);
            changed |= value != column.getValue();
            out.put(column.getKey(), value);
        }
        return changed ? out : row;
    }

    /**
     * One value in that model. Nested values are converted too, since a document's own fields and an
     * array's elements are as reachable from a reader as a top-level column is; a container whose
     * contents all pass through unchanged is returned as it is, so the ordinary row costs no copy.
     *
     * <p>{@code path} is how the schema names this value's place, which is the column's own name at
     * the top level and the dotted path below it — the spelling discovery itself uses for a field
     * inside a document, reported in the same field map the top-level columns come from. It is null
     * beneath an array, the one place the schema has no way to name at all. Wherever the field map
     * holds no row for the place - because an array ended the path, or because discovery never
     * described it - what the source calls the value is read off its own driver type instead, by
     * {@link SchemaNames}.
     */
    private static Object converted(Object value, TapCodecsRegistry codecs,
            SchemaNames names, String path, boolean toNamespaceWidths) {
        Object registered = registered(value, codecs, names.of(value, path));
        if (registered != null) {
            return registered;
        }
        if (toNamespaceWidths) {
            if (value instanceof ZonedDateTime zonedDateTime) {
                return Date.from(zonedDateTime.toInstant());
            }
            if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
                return ((Number) value).longValue();
            }
            if (value instanceof Float f) {
                return f.doubleValue();
            }
            if (value instanceof BigInteger big && big.bitLength() < Long.SIZE) {
                return big.longValue();
            }
        }
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> converted = new LinkedHashMap<>(map.size());
            boolean changed = false;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                // A field inside a document is named by the path that reaches it, which is what the
                // lookup is keyed by. Beneath an array the path is already gone and stays gone all the
                // way down, because nothing under an element has a place the schema could name either.
                Object element = converted(entry.getValue(), codecs, names,
                        path == null ? null : path + "." + entry.getKey(), toNamespaceWidths);
                changed |= element != entry.getValue();
                converted.put(entry.getKey(), element);
            }
            return changed ? converted : map;
        }
        if (value instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            boolean changed = false;
            for (Object element : list) {
                // An element has no place of its own to look up — the schema names the array and
                // stops — so the path ends here. Deliberately not the array's own name: that would
                // rebuild every element as whatever the array is declared to be, and succeed.
                Object next = converted(element, codecs, names, null, toNamespaceWidths);
                changed |= next != element;
                converted.add(next);
            }
            return changed ? converted : list;
        }
        return value;
    }

    /**
     * What the source's schema calls the values of one row: by the place it names, and — wherever it
     * names no place — by the driver type it named somewhere it did.
     *
     * <p>The way back is keyed on a name, so a value nothing names reaches the target as the portable
     * value it travelled as and is stored there as the wrong type. The place reading is tried first and
     * is never overruled: a column has a place in the field map and a field inside a document has one
     * too, spelled as the dotted path that reaches it, so wherever discovery described the place, the
     * schema's own word for it is the answer even where the value's own class would say something else.
     *
     * <p><b>An absent place is not the schema answering.</b> An array's elements have nowhere for it to
     * speak: they are positional and may each be a different type, so "the type of this array's
     * elements" is not something a field map can state, and discovery reports no row for one. And a
     * field map holding no row for a document's interior, or for a column, is the fields discovery
     * happened to meet in the documents it sampled rather than a census of the collection — the
     * ordinary case for a schemaless source, not a corner of it. Neither absence says anything about
     * the type, so both are read the same way: off the value's own driver type, as the name this same
     * schema gives that exact type where it does name a place holding one. Reading only one of them
     * decodes one document two ways — {@code refs[0]} arriving as the driver's own type while
     * {@code meta.ref} beside it, holding that very value, arrives as text — with the better-described
     * place getting the worse answer.
     *
     * <p>That name is still the source's own answer and not a guess about the value, which is why it is
     * preferred to the two alternatives — leaving the value as its portable value, which a target of
     * the same kind then stores as the wrong type, and lending it the declared name of the container
     * holding it, which would rebuild every element of an array as whatever the array is declared to be,
     * and every field of a document as whatever the document is, and report success.
     *
     * <p><b>A type this change spells two ways has no answer and gets none.</b> Picking either spelling
     * would rebuild those values as one of them and succeed; they stay portable instead, which is
     * the visible second-best rather than a silent wrong one. <b>Two ways is what the values this change
     * carries say, not what the whole schema says.</b> A declared name reaches a driver type only through
     * a value that has both, so a second spelling whose column is absent or null here is not seen at all
     * and the one spelling on offer is used — a change is ambiguous only where it shows the ambiguity.
     * A collection whose schema really does spell one type two ways therefore lands those values
     * portable in the documents that carry both columns and rebuilt in the ones that carry only one,
     * which is the same per-change reading the naming column itself gets and visible in the target
     * either way. The schema alone cannot do better: nothing in a field map says which declared name
     * belongs to which driver class until a value arrives holding the two together.
     *
     * <p><b>Taken off the whole row at once, never as the walk reaches each value.</b> Read as the walk
     * went, an unnamed value that happened to sit before the naming column would restore and the same
     * value after it would not, so one document would decode two ways depending only on the order a
     * connector reported its fields. It is taken off both images of a change together for the same
     * reason: two halves of one row must not disagree about a value that did not change, and a connector
     * is free to report a before image the named column is not in. Taking it on the first unnamed value
     * the walk reaches rather than up front is the same reading - the whole row either way - and leaves
     * a row whose every value the schema does name paying nothing for it, which is most rows.
     *
     * <p>Only driver types the connector registered a conversion for are read, because only those ever
     * reach a way back; it also keeps one ordinary kind spelled at two widths — a schema naming
     * {@code STRING(100)} beside {@code STRING(4)} — from being a conflict that means anything. A caller
     * that supplies no declared types at all, which is every read face, pays nothing for any of this.
     */
    private static final class SchemaNames {

        /** No schema was supplied, so nothing is named: every value travels as its portable value. */
        private static final SchemaNames NONE = new SchemaNames(Map.of(), null, null, null);

        private final Map<String, String> byPath;
        private final TapCodecsRegistry codecs;
        private final Map<String, Object> before;
        private final Map<String, Object> after;

        /**
         * What the schema calls each driver type it names a place for, taken whole off every image this
         * change carries, and taken only once a value the schema names no place for has asked — so a
         * row it names throughout, which is most rows on the hottest path this adapter has, never walks
         * itself a second time.
         */
        private Map<Class<?>, String> byType;

        private SchemaNames(Map<String, String> byPath, TapCodecsRegistry codecs,
                Map<String, Object> before, Map<String, Object> after) {
            this.byPath = byPath;
            this.codecs = codecs;
            this.before = before;
            this.after = after;
        }

        /**
         * The reading for one change, over the images it carries: both of an update, the one image every
         * other change has, and nothing at all where the caller supplied no declared types.
         */
        static SchemaNames read(TapCodecsRegistry codecs, Map<String, String> columnTypes,
                Map<String, Object> before, Map<String, Object> after) {
            return columnTypes.isEmpty() || (before == null && after == null)
                    ? NONE
                    : new SchemaNames(columnTypes, codecs, before, after);
        }

        /**
         * What this schema calls the value at {@code path}, or — where it names no place there, the
         * path having ended beneath an array or never been described — what it calls that value's own
         * driver type. Null when it names neither.
         */
        String of(Object value, String path) {
            String declared = path == null ? null : byPath.get(path);
            if (declared != null) {
                return declared;
            }
            // Only a class the connector registered a conversion for can ever be in the reading, so a
            // value of any other kind is answered without taking one. Without this the plain text and
            // numbers no schema happens to name - which is most of what an unnamed place holds - make
            // the first one the walk reaches walk the whole change a second time to be told nothing, on
            // the hottest path this adapter has.
            if (value == null || codecs == null
                    || codecs.getCustomToTapValueCodec(value.getClass()) == null) {
                return null;
            }
            if (byType == null) {
                byType = byType();
            }
            return byType.get(value.getClass());
        }

        private Map<Class<?>, String> byType() {
            Map<Class<?>, String> named = new LinkedHashMap<>();
            Set<Class<?>> spelledTwoWays = new LinkedHashSet<>();
            read(before, named, spelledTwoWays);
            read(after, named, spelledTwoWays);
            spelledTwoWays.forEach(named::remove);
            return named.isEmpty() ? Map.of() : named;
        }

        private void read(Map<String, Object> image, Map<Class<?>, String> named,
                Set<Class<?>> spelledTwoWays) {
            if (image != null) {
                image.forEach((column, value) -> read(value, column, named, spelledTwoWays));
            }
        }

        private void read(Object value, String path, Map<Class<?>, String> named,
                Set<Class<?>> spelledTwoWays) {
            if (value == null) {
                return;
            }
            // The walk's own precedence, so the two cannot disagree: a driver type the connector
            // converts is read as one even where its class happens to be a map, and only what is
            // left over is descended into.
            if (codecs.getCustomToTapValueCodec(value.getClass()) != null) {
                String declared = byPath.get(path);
                if (declared != null) {
                    // Two spellings are seen only where this change holds a value under each of them:
                    // a column the change does not carry, or carries as null, attaches its name to no
                    // class here and so cannot contradict one. A change that shows one spelling is
                    // therefore answered with it, which is the reading being taken off the change
                    // rather than off the schema - the same as everything else here.
                    String already = named.putIfAbsent(value.getClass(), declared);
                    if (already != null && !already.equals(declared)) {
                        spelledTwoWays.add(value.getClass());
                    }
                }
                return;
            }
            if (value instanceof Map<?, ?> document) {
                document.forEach((field, nested) ->
                        read(nested, path + "." + field, named, spelledTwoWays));
            }
            // An array is not descended into: its elements are the values this reading is being taken
            // for, and the schema names no place that reaches one.
        }
    }

    /**
     * The connector's own conversion of {@code value}, or null when it registered none for that type —
     * which is the ordinary case, and the signal to take the bare lane instead.
     *
     * <p>Only conversions the connector itself registered are consulted. The frozen surface also ships
     * a fallback that wraps anything unrecognized in a raw carrier; reaching for that here would put
     * every driver type nobody taught us about into a wrapper the rest of the pipeline would have to
     * unwrap for no gain, and would put the ordinary Java boxes in one too.
     *
     * <p>The driver's own object rides along with the result. It is what lets a sink of the same kind
     * put the value back the way it arrived — a key converted to text for travel is written back as a
     * key, not as text — and nothing else on this path keeps it. The conversion's own result object is
     * not what travels: the whole of its state is declared on a supertype that is not serializable, so
     * one crossing a wire arrives an empty shell, and the write side would restore nothing while every
     * case that never crosses one stayed green. A driver object that cannot cross a wire itself is not
     * carried at all — there would be nothing to restore from, and putting it in the row would take the
     * row down at the first hop instead of at the target.
     *
     * <p>The declared column type is not consulted, and is not needed: a conversion is chosen by the
     * value's own class, and every conversion a connector registers is free to be handed no declared
     * type, which is already what happens for a column the schema did not describe.
     */
    private static Object registered(Object value, TapCodecsRegistry codecs, String originType) {
        if (value == null) {
            return null;
        }
        ToTapValueCodec<?> codec = codecs.getCustomToTapValueCodec(value.getClass());
        if (codec == null) {
            return null;
        }
        TapValue<?, ?> converted = codec.toTapValue(value, null);
        if (converted == null || converted.getValue() == null) {
            return null;
        }
        // Handed on inside a carrier the rest of the tree can name, holding the portable result and the
        // name the source's own schema gave this column. The driver's object is deliberately not in
        // there: the target runs in a class loader of its own, so the object would be a type it cannot
        // read, while the name crosses both that boundary and the serializer a row meets on the way.
        //
        // One portable result is translated on the way in rather than passed along: the contract's box
        // for bytes declares no equality of its own, so a join key built from a binary column would
        // compare by identity and match nothing, silently. Its tag comes along, because a target of the
        // same kind writes it back, and the way out builds the box again.
        return new ConvertedValue(portable(value, converted.getValue()), originType);
    }

    /**
     * The portable result as a value the rest of the tree can compare. Everything the contract hands over
     * already behaves like one - text, a number, an instant - except its box for bytes, which declares
     * neither equality nor a hash and would key a join by identity, the known Decimal128 result already
     * narrowed to a double, and the known MongoDB timestamp result whose seconds were interpreted as
     * milliseconds.
     */
    private static Object portable(Object source, Object value) {
        if (BSON_DECIMAL128.equals(source.getClass().getName()) && value instanceof Double narrowed) {
            BigDecimal exact = decimal128Value(source);
            // Match the exact wrong result before correcting it, the same way the timestamp correction
            // below does: only the plain narrowing of this very value is replaced. A conversion that
            // answered some other double decided something of its own - rounding to a declared scale,
            // say - and replacing that with the full source value would overrule the connector rather
            // than repair it, silently and on every row.
            if (exact != null && exact.doubleValue() == narrowed.doubleValue()) {
                return exact;
            }
        }
        if (BSON_TIMESTAMP.equals(source.getClass().getName()) && value instanceof DateTime timestamp) {
            int seconds = bsonTimestampSeconds(source);
            // This connector currently treats the seconds half as epoch milliseconds. Match that exact
            // result before correcting it, so a connector that already returns the right instant wins.
            if (timestamp.toInstant().equals(Instant.ofEpochMilli(seconds))) {
                return new DateTime(Instant.ofEpochSecond(Integer.toUnsignedLong(seconds)));
            }
        }
        return value instanceof ByteData bytes ? new Bytes(bytes.getType(), bytes.getValue()) : value;
    }

    /** The finite decimal value, or null for a Decimal128 special value that has no BigDecimal form. */
    private static BigDecimal decimal128Value(Object value) {
        Decimal128Access access = BSON_DECIMAL128_ACCESS.get(value.getClass());
        try {
            if (!(Boolean) access.isFinite().invoke(value)) {
                return null;
            }
            try {
                return (BigDecimal) access.bigDecimalValue().invoke(value);
            } catch (InvocationTargetException e) {
                // The accessor documents ArithmeticException for Decimal128 forms BigDecimal cannot
                // represent. Non-finite forms returned above; the remaining form is negative zero.
                if (e.getCause() instanceof ArithmeticException) {
                    return null;
                }
                throw e;
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot read mongodb decimal128 exactly", e);
        }
    }

    private static int bsonTimestampSeconds(Object value) {
        try {
            return (Integer) BSON_TIMESTAMP_TIME.get(value.getClass()).invoke(value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot read mongodb timestamp time", e);
        }
    }

    /**
     * Encodes an envelope back to a PDK {@code TapEvent}; a snapshot read encodes insert-shaped. The
     * row maps are handed over as fresh mutable copies: the sink value-conversion path mutates them in
     * place, which the envelope's own unmodifiable maps would reject.
     */
    public static TapEvent encode(Envelope env, TapCodecsRegistry codecs) {
        Objects.requireNonNull(codecs, "codecs");
        return switch (env.op()) {
            case INSERT, READ -> TapInsertRecordEvent.create()
                    .table(env.src()).referenceTime(env.ts()).after(mutable(env.after(), codecs))
                    .removedFields(dropped(env));
            case UPDATE -> TapUpdateRecordEvent.create()
                    .table(env.src()).referenceTime(env.ts())
                    .before(mutable(env.before(), codecs)).after(mutable(env.after(), codecs))
                    .removedFields(dropped(env));
            case DELETE -> TapDeleteRecordEvent.create()
                    .table(env.src()).referenceTime(env.ts()).before(mutable(env.before(), codecs));
            case DDL -> encodeDdl(env);
        };
    }

    /**
     * A fresh mutable copy PDK can write through in place, or {@code null} when the map is absent, with
     * every carried value put back the way the target wants it.
     */
    private static Map<String, Object> mutable(Map<String, Object> map, TapCodecsRegistry codecs) {
        return map == null ? null
                : new LinkedHashMap<>(ConvertedValue.unwrapRow(map, carrier -> restored(carrier, codecs)));
    }

    /**
     * One carried value as the target should receive it.
     *
     * <p>The target's own way back decides, exactly as the source's way in decided what travelled. The
     * contract value the pair is keyed on is rebuilt here from what the row carries — the portable value
     * and the column's declared name — and handed to the way back the target registered for that value's
     * kind. That is where a key becomes a key again rather than the text it travelled as: a connector's
     * way back reads the declared name to decide what to rebuild.
     *
     * <p><b>Rebuilt rather than carried, because the source's own objects cannot come here.</b> Two
     * connectors are two isolated class loaders; the source's driver object and the target's conversion
     * for it are unrelated types that share a name, and handing one to the other threw a cast error that
     * took the whole run down on the first row. Nothing driver-owned crosses between them now.
     *
     * <p>A target that registered no way back for this kind of value is a target that cannot write the
     * driver's type, and it is handed the portable value — the same answer as for a target of another
     * kind. The framework's own fallback is deliberately not consulted: for these values it hands back
     * exactly the portable value anyway, and reaching for it drags in a runtime that only a loaded
     * connector has.
     */
    @SuppressWarnings("unchecked")
    private static Object restored(ConvertedValue carrier, TapCodecsRegistry codecs) {
        TapValue<?, ?> value = contractValue(carrier.value());
        if (value == null) {
            return carrier.value();
        }
        value.setOriginType(carrier.originType());
        FromTapValueCodec<TapValue<?, ?>> back =
                codecs.getCustomFromTapValueCodec((Class<TapValue<?, ?>>) value.getClass());
        if (back == null) {
            return carrier.value();
        }
        // A way back that answers nothing is handled like one that was never registered. A carrier cannot
        // hold null - the constructor refuses it - so a null here is never the column having been null; it
        // is the value being dropped, and the write would report success over a blanked column.
        Object restored = back.fromTapValue(value);
        return restored == null ? carrier.value() : restored;
    }

    /**
     * The contract value a portable value of this kind belongs in, or null where the contract names none.
     *
     * <p>The pairing is the frozen surface's own, read off the value classes it declares: each names the
     * one portable type it holds, and this is that reading in reverse. It is spelled out rather than
     * looked up because the lookup the surface offers goes through a registry whose defaults are
     * deliberately half switched off, and because a name-to-class resolution here would be reflection on
     * a path that has to hold up in a native image.
     *
     * <p>Null for anything else, which is the honest answer: a connector free to convert into any value
     * class may produce a portable type this does not name, and guessing one would hand the target a
     * value of the wrong kind rather than the one it can already write.
     */
    private static TapValue<?, ?> contractValue(Object portable) {
        return switch (portable) {
            case Bytes bytes -> new TapBinaryValue(new ByteData(bytes.tag(), bytes.value()));
            case DateTime instant -> new TapDateTimeValue(instant);
            case String text -> new TapStringValue(text);
            case Double number -> new TapNumberValue(number);
            default -> null;
        };
    }

    /**
     * The fields this row no longer has, as the connector reads them, or null where there are none.
     *
     * <p><b>Null rather than an empty list, so that a producer dropping nothing is indistinguishable from
     * one written before any producer could.</b> A connector tests this for emptiness either way, and the
     * two answers must not diverge on a path nothing exercises.
     *
     * <p>Without this the removal does not travel at all: a write into a keyed target sets the fields it is
     * given, so a field that stopped being produced stays in the target for as long as the row does, with
     * the write succeeding and the row that arrived correct.
     */
    private static List<String> dropped(Envelope env) {
        return env.removed().isEmpty() ? null : List.copyOf(env.removed());
    }

    private static TapEvent encodeDdl(Envelope env) {
        TapDDLUnknownEvent ddl = new TapDDLUnknownEvent();
        ddl.setTableId(env.src());
        ddl.setReferenceTime(env.ts());
        Object origin = env.schema() == null ? null : env.schema().get(DDL_ORIGIN);
        if (origin != null) {
            ddl.setOriginDDL(origin);
        }
        return ddl;
    }

    private static Map<String, Object> ddlSchema(TapDDLEvent ddl) {
        Object origin = ddl.getOriginDDL();
        return origin == null ? Map.of() : Map.of(DDL_ORIGIN, String.valueOf(origin));
    }

    /** The source reference time, falling back to the event time, or {@code 0} when neither is set. */
    private static long ts(TapBaseEvent event) {
        Long reference = event.getReferenceTime();
        if (reference != null) {
            return reference;
        }
        Long time = event.getTime();
        return time != null ? time : 0L;
    }

    private static String src(TapBaseEvent event) {
        return event.getTableId();
    }
}
