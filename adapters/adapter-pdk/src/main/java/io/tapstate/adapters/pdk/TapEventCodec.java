package io.tapstate.adapters.pdk;

import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
            return Envelope.update(ts(update), src(update),
                    row(update.getBefore(), codecs, columnTypes), row(update.getAfter(), codecs, columnTypes),
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
     * a bare value, normalized. A row is therefore mixed, which is the contract rather than a gap:
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
     * success. An exact fixed-point number is left alone in every case — routing it through any
     * binary floating point type drops digits silently, which is the one loss nothing downstream
     * could detect.
     */
    private static Map<String, Object> row(
            Map<String, Object> row, TapCodecsRegistry codecs, Map<String, String> columnTypes) {
        return walk(row, codecs, columnTypes, true);
    }

    /**
     * The connector's own conversions applied to a row, at any depth, and nothing else — the widths the
     * type namespace speaks are left exactly as the driver handed them over.
     *
     * <p>This is what a read face wants. It reports what the database holds rather than what a pipeline
     * row speaks, so widening an integer there would answer a question nobody asked; but the registered
     * conversions still have to run, because without them the face is handed the driver's own objects
     * and has nothing to render but their addresses. No declared types are supplied: those name what a
     * sink would rebuild, and a read rebuilds nothing.
     */
    static Map<String, Object> connectorConverted(Map<String, Object> row, TapCodecsRegistry codecs) {
        return walk(row, codecs, Map.of(), false);
    }

    private static Map<String, Object> walk(Map<String, Object> row, TapCodecsRegistry codecs,
            Map<String, String> columnTypes, boolean toNamespaceWidths) {
        if (row == null) {
            return null;
        }
        Map<String, Object> out = new LinkedHashMap<>(row.size());
        boolean changed = false;
        for (Map.Entry<String, Object> column : row.entrySet()) {
            Object value =
                    converted(column.getValue(), codecs, columnTypes, column.getKey(), toNamespaceWidths);
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
     * <p>{@code path} is how the schema names this value, which is the column's own name at the top
     * level and the dotted path below it — the spelling discovery itself uses for a field inside a
     * document, reported in the same field map the top-level columns come from. It is null where the
     * schema names nothing, and a null path looks nothing up rather than falling back to an enclosing
     * name: rebuilding a value as whatever its container is declared to be is worse than handing over
     * the portable value, because it succeeds.
     */
    private static Object converted(Object value, TapCodecsRegistry codecs,
            Map<String, String> columnTypes, String path, boolean toNamespaceWidths) {
        Object registered =
                registered(value, codecs, path == null ? null : columnTypes.get(path));
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
                // lookup is keyed by. Below a value the schema does not name, the path stays null and
                // stays null all the way down.
                Object element = converted(entry.getValue(), codecs, columnTypes,
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
                // An element has no name of its own — the schema names the array and stops — so there
                // is nothing to look up, and lending it the array's own name would rebuild it as
                // whatever the array is declared to be.
                Object next = converted(element, codecs, columnTypes, null, toNamespaceWidths);
                changed |= next != element;
                converted.add(next);
            }
            return changed ? converted : list;
        }
        return value;
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
        return new ConvertedValue(converted.getValue(), originType);
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
            case ByteData bytes -> new TapBinaryValue(bytes);
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
