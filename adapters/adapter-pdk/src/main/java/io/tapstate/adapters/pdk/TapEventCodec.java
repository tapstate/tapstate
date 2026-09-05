package io.tapstate.adapters.pdk;

import java.math.BigInteger;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.tapstate.core.event.Envelope;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
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
    public static Envelope decodeChange(TapEvent event, TapCodecsRegistry codecs) {
        Objects.requireNonNull(codecs, "codecs");
        if (event instanceof TapInsertRecordEvent insert) {
            return Envelope.insert(ts(insert), src(insert), row(insert.getAfter(), codecs), null);
        }
        if (event instanceof TapUpdateRecordEvent update) {
            return Envelope.update(ts(update), src(update),
                    row(update.getBefore(), codecs), row(update.getAfter(), codecs), null);
        }
        if (event instanceof TapDeleteRecordEvent delete) {
            return Envelope.delete(ts(delete), src(delete), row(delete.getBefore(), codecs), null);
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
    public static Envelope decodeSnapshotRow(TapEvent event, TapCodecsRegistry codecs) {
        Objects.requireNonNull(codecs, "codecs");
        if (event instanceof TapInsertRecordEvent read) {
            return Envelope.read(ts(read), src(read), row(read.getAfter(), codecs), null);
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
    @SuppressWarnings("unchecked")
    private static Map<String, Object> row(Map<String, Object> row, TapCodecsRegistry codecs) {
        return row == null ? null : (Map<String, Object>) converted(row, codecs);
    }

    /**
     * One value in that model. Nested values are converted too, since a document's own fields and an
     * array's elements are as reachable from a reader as a top-level column is; a container whose
     * contents all pass through unchanged is returned as it is, so the ordinary row costs no copy.
     */
    private static Object converted(Object value, TapCodecsRegistry codecs) {
        Object registered = registered(value, codecs);
        if (registered != null) {
            return registered;
        }
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
        if (value instanceof Map<?, ?> map) {
            Map<Object, Object> converted = new LinkedHashMap<>(map.size());
            boolean changed = false;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object element = converted(entry.getValue(), codecs);
                changed |= element != entry.getValue();
                converted.put(entry.getKey(), element);
            }
            return changed ? converted : map;
        }
        if (value instanceof List<?> list) {
            List<Object> converted = new ArrayList<>(list.size());
            boolean changed = false;
            for (Object element : list) {
                Object next = converted(element, codecs);
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
     * <p>The original value and the name of the type it came in as are recorded on the result. They
     * are what lets a sink of the same kind put the value back the way it arrived — a key converted
     * to text for travel is written back as a key, not as text — and nothing else on this path writes
     * them. The declared column type is not consulted, and is not needed: a conversion is chosen by
     * the value's own class, and every conversion a connector registers is free to be handed no
     * declared type, which is already what happens for a column the schema did not describe.
     */
    private static Object registered(Object value, TapCodecsRegistry codecs) {
        if (value == null) {
            return null;
        }
        ToTapValueCodec<?> codec = codecs.getCustomToTapValueCodec(value.getClass());
        if (codec == null) {
            return null;
        }
        TapValue<?, ?> converted = codec.toTapValue(value, null);
        if (converted == null) {
            return null;
        }
        converted.setOriginValue(value);
        converted.setOriginType(value.getClass().getSimpleName());
        return converted;
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
                    .table(env.src()).referenceTime(env.ts()).after(mutable(env.after()))
                    .removedFields(dropped(env));
            case UPDATE -> TapUpdateRecordEvent.create()
                    .table(env.src()).referenceTime(env.ts()).before(mutable(env.before())).after(mutable(env.after()))
                    .removedFields(dropped(env));
            case DELETE -> TapDeleteRecordEvent.create()
                    .table(env.src()).referenceTime(env.ts()).before(mutable(env.before()));
            case DDL -> encodeDdl(env);
        };
    }

    /** A fresh mutable copy PDK can write through in place, or {@code null} when the map is absent. */
    private static Map<String, Object> mutable(Map<String, Object> map) {
        return map == null ? null : new LinkedHashMap<>(map);
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
