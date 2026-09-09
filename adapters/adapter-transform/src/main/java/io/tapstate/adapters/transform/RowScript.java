package io.tapstate.adapters.transform;

import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

/**
 * One compiled js transform, ready to run against an event. The script declares {@code process(record,
 * ctx)} (required) and optionally {@code filter(record)}; both are parsed once member-side from the
 * serializable script text and then invoked per event. This is the full-power escape hatch — the script
 * is a black box (no validate-time type-check, unlike the CEL ports), so a syntax error surfaces here
 * at build time and an evaluation failure surfaces per event; both are user-diagnosable and the error
 * surface renders them as coded diagnostics — until then they propagate and fail the job.
 *
 * <p>Each event is handed to the script as a mutable record — {@code op} as its wire symbol, {@code ts}
 * / {@code src} as scalars, {@code before} / {@code after} / {@code schema} as objects (an absent map
 * is JS null). The maps are exposed through host-backed proxies, so a field the script leaves untouched
 * keeps its exact Java type on the way out (a BIGINT stays a long, a DOUBLE stays a double), while a
 * value the script writes becomes an ordinary JS value — an integer-valued number narrows to int / long
 * before double, which is the unavoidable ambiguity of JS having one number type. The script mutates the
 * record, returns one, or fans out through {@code ctx.emit}; the output is every emitted record in order,
 * followed by the return value when it is non-null (return null to drop). Unlike filter / map, js sees
 * every event including ddl.
 *
 * <p>A script holds a private GraalVM {@link Context}, reused across events on the single cooperative
 * thread that owns the port; it is not thread-safe and holds no cross-event state of its own (state /
 * lookup are not part of this tier).
 */
final class RowScript {

    private static final System.Logger LOGGER = System.getLogger("io.tapstate.transform.js");

    // One engine shared by every script: it holds the JS runtime warmup, while each script keeps its
    // own Context (a Context is single-threaded, an Engine is safe to share). The interpreter warning
    // is silenced because running on the Truffle interpreter is the expected mode on a stock JDK; the
    // option is best-effort so a runtime that rejects it still yields a working engine.
    private static final Engine ENGINE = buildEngine();

    private static Engine buildEngine() {
        try {
            return Engine.newBuilder("js").option("engine.WarnInterpreterOnly", "false").build();
        } catch (RuntimeException rejected) {
            return Engine.newBuilder("js").build();
        }
    }

    private final Context context;
    private final Value process;
    private final Value filter;
    // A JS truthiness coercion, so a filter verdict follows JS rules (0 / '' / null / undefined = false).
    private final Value toBool;

    RowScript(String source) {
        this.context = Context.newBuilder("js").engine(ENGINE).allowHostAccess(HostAccess.EXPLICIT).build();
        try {
            context.eval("js", source);
        } catch (PolyglotException e) {
            throw TransformErrors.scriptCompileFailed(e);
        }
        this.toBool = context.eval("js", "(x) => !!x");
        Value bindings = context.getBindings("js");
        this.process = bindings.getMember("process");
        if (process == null || !process.canExecute()) {
            throw TransformErrors.scriptNoProcess();
        }
        Value declaredFilter = bindings.getMember("filter");
        this.filter = declaredFilter != null && declaredFilter.canExecute() ? declaredFilter : null;
    }

    /** Runs the script for one event, returning the events it becomes (empty to drop). */
    List<Envelope> run(Envelope event) {
        GuestObject record = new GuestObject(recordOf(event));
        try {
            if (filter != null && !toBool.execute(filter.execute(record)).asBoolean()) {
                return List.of();
            }
            Sink sink = new Sink();
            Value returned = process.execute(record, sink);
            List<Envelope> out = new ArrayList<>(sink.emitted.size() + 1);
            // Emitted records were snapshotted at emit time (see Sink.emit), so re-emitting one mutable
            // record across a loop yields one output per state, not the final state repeated.
            for (Object emitted : sink.emitted) {
                out.add(toEnvelope(emitted, event));
            }
            if (returned != null && !returned.isNull()) {
                out.add(toEnvelope(fromGuest(returned), event));
            }
            return out;
        } catch (PolyglotException e) {
            // A guest-side failure (a thrown error, a bad property access) is a user-diagnosable
            // condition: surface it as a coded diagnostic, not a bare crash that fails the job opaquely.
            throw TransformErrors.scriptFailed(e);
        }
    }

    private static Map<String, Object> recordOf(Envelope event) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("op", event.op().symbol());
        record.put("ts", event.ts());
        record.put("src", event.src());
        // Unwrapped: a guest sees a carrier as an opaque host object, so `after._id === "64f0..."` is
        // false for every row and the script neither fails nor warns. The carrier does not survive the
        // round trip - a script rebuilds the whole record, so every value it hands back is a new one.
        record.put("before", ConvertedValue.unwrapRow(event.before()));
        record.put("after", ConvertedValue.unwrapRow(event.after()));
        record.put("schema", event.schema());
        return record;
    }

    private Envelope toEnvelope(Object converted, Envelope source) {
        if (!(converted instanceof Map<?, ?> record)) {
            throw TransformErrors.scriptOutputInvalid("output must be a record object");
        }
        Object op = record.get("op");
        Object ts = record.get("ts");
        Object src = record.get("src");
        // The removal a step before this one declared is carried through. It is the only way a target
        // hears that a field went, so rebuilding the record without it silently withdraws the statement.
        // Safe even where the script puts the field back: a removal is only ever applied to a field the
        // row does not carry, so a re-added one simply stops being a removal.
        return new Envelope(
                opOf(op, source),
                ts instanceof Number number ? number.longValue() : source.ts(),
                src != null ? String.valueOf(src) : source.src(),
                dataMap(record.get("before")),
                dataMap(record.get("after")),
                dataMap(record.get("schema"))).withRemoved(source.removed());
    }

    // The output op: the source's when the script left it alone, else the wire symbol it wrote. An op
    // the envelope cannot parse is author-controlled invalid output, coded like any other bad output
    // shape rather than the bare crash Op.fromSymbol raises for an unknown symbol.
    private static Op opOf(Object op, Envelope source) {
        if (op == null) {
            return source.op();
        }
        try {
            return Op.fromSymbol(String.valueOf(op));
        } catch (IllegalArgumentException e) {
            throw TransformErrors.scriptOutputInvalid("output op '" + op + "' is not a known change kind");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> dataMap(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw TransformErrors.scriptOutputInvalid("before / after / schema must be an object or null");
    }

    // Guest value -> Java, host-preserving. A record / array still backed by the host maps we handed in
    // is read straight from its backing, so a field the script never touched keeps its exact Java type;
    // anything the script produced is mapped by JS shape into the row's own currency (a whole number
    // becomes the 64-bit integer, anything wider a double), and a Java value that reached the guest as
    // an opaque host object returns as-is.
    private Object fromGuest(Value value) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isProxyObject()) {
            Object proxy = value.asProxyObject();
            if (proxy instanceof GuestObject object) {
                return fromBacking(object);
            }
            if (proxy instanceof GuestArray array) {
                return fromBacking(array);
            }
        }
        if (value.isHostObject()) {
            return value.asHostObject();
        }
        if (value.isBoolean()) {
            return value.asBoolean();
        }
        if (value.isString()) {
            return value.asString();
        }
        if (value.isNumber()) {
            // A whole number leaves in the row's own integer width rather than the narrowest box that
            // holds it: the script writes into the same rows every other step reads, and a column
            // whose type turned on the magnitude that happened to flow through it is not a type.
            if (value.fitsInLong()) {
                return value.asLong();
            }
            return value.asDouble();
        }
        if (value.isInstant()) {
            return value.asInstant();
        }
        if (value.hasArrayElements()) {
            int size = (int) value.getArraySize();
            List<Object> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                list.add(fromGuest(value.getArrayElement(i)));
            }
            return list;
        }
        if (value.canExecute() || value.canInstantiate()) {
            // A function reached a data slot; it has no data representation, so keep its source form.
            return value.toString();
        }
        if (value.hasMembers()) {
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();
            for (String key : value.getMemberKeys()) {
                map.put(key, fromGuest(value.getMember(key)));
            }
            return map;
        }
        return value.toString();
    }

    private Map<String, Object> fromBacking(GuestObject object) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        object.backing.forEach((key, value) -> map.put(key, fromBacking(value)));
        return map;
    }

    private List<Object> fromBacking(GuestArray array) {
        List<Object> list = new ArrayList<>(array.backing.size());
        for (Object value : array.backing) {
            list.add(fromBacking(value));
        }
        return list;
    }

    // A backing slot is one of: a nested proxy (untouched sub-tree, recurse), a guest Value the script
    // wrote (map by JS shape), or an original host scalar (return as-is, exact Java type preserved).
    private Object fromBacking(Object slot) {
        if (slot instanceof GuestObject object) {
            return fromBacking(object);
        }
        if (slot instanceof GuestArray array) {
            return fromBacking(array);
        }
        if (slot instanceof Value value) {
            return fromGuest(value);
        }
        return slot;
    }

    private static Object wrap(Object value) {
        if (value instanceof Map<?, ?> map) {
            return new GuestObject(map);
        }
        if (value instanceof List<?> list) {
            return new GuestArray(list);
        }
        return value;
    }

    /**
     * A record / object the script reads and mutates. It is backed by the host map we handed in, so an
     * untouched entry keeps its original Java value (and thus its exact type on the way out), while an
     * entry the script assigns holds the guest {@link Value} it wrote. Nested maps / lists are wrapped
     * on the way in so navigation and mutation reach the same backing.
     */
    static final class GuestObject implements ProxyObject {

        final Map<String, Object> backing = new LinkedHashMap<>();

        GuestObject(Map<?, ?> source) {
            source.forEach((key, value) -> backing.put(String.valueOf(key), wrap(value)));
        }

        @Override
        public Object getMember(String key) {
            return backing.get(key);
        }

        @Override
        public Object getMemberKeys() {
            return ProxyArray.fromArray((Object[]) backing.keySet().toArray(new String[0]));
        }

        @Override
        public boolean hasMember(String key) {
            return backing.containsKey(key);
        }

        @Override
        public void putMember(String key, Value value) {
            backing.put(key, value);
        }

        @Override
        public boolean removeMember(String key) {
            return backing.remove(key) != null;
        }
    }

    /** A list the script reads and mutates, backed like {@link GuestObject}. */
    static final class GuestArray implements ProxyArray {

        final List<Object> backing = new ArrayList<>();

        GuestArray(List<?> source) {
            for (Object value : source) {
                backing.add(wrap(value));
            }
        }

        @Override
        public Object get(long index) {
            return backing.get((int) index);
        }

        @Override
        public void set(long index, Value value) {
            int i = (int) index;
            if (i == backing.size()) {
                backing.add(value);
            } else {
                backing.set(i, value);
            }
        }

        @Override
        public long getSize() {
            return backing.size();
        }
    }

    /**
     * The {@code ctx} the script is handed: the emit / log surface of this tier (no state / lookup). An
     * emitted record is snapshotted to Java at the moment of the call, so a script that re-emits one
     * mutable record across a loop produces one output per state, not the final state repeated. Public so
     * the polyglot layer can reach its exported members across the module boundary.
     */
    public final class Sink {

        private final List<Object> emitted = new ArrayList<>();

        /** {@code ctx.emit(record)} — snapshots a record as an output event, in call order. */
        @HostAccess.Export
        public void emit(Value record) {
            if (record != null && !record.isNull()) {
                emitted.add(fromGuest(record));
            }
        }

        /** {@code ctx.log(message)} — records a line from the script for operators. */
        @HostAccess.Export
        public void log(Object message) {
            LOGGER.log(System.Logger.Level.INFO, String.valueOf(message));
        }
    }
}
