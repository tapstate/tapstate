package io.tapstate.core.event;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The standard event envelope: one change event as every transform sees it, the common currency of
 * the capture, transform and sink ports. An immutable value.
 *
 * <p>Seven slots — {@code op} (the change kind), {@code ts} (event time as epoch milliseconds),
 * {@code src} (the logical stream name the event came from), three data maps {@code before} /
 * {@code after} / {@code schema}, and {@code positions} (what this event covers, chain by chain).
 * Which data maps are present follows the op: insert and read carry {@code after}, delete carries
 * {@code before}, update carries both, ddl carries {@code schema} and neither row. An absent map is
 * {@code null}; the {@link #insert} / {@link #update} / {@link #delete} / {@link #read} / {@link #ddl}
 * factory methods encode the per-op shape by construction.
 *
 * <p>{@code positions} maps a chain's name to the spot on it this event covers, and it is a map rather
 * than a single position because an event need not come from one chain. A change read from a source
 * covers one spot on its own stream and nothing else; a document assembled out of several streams
 * covers a spot on each of them, and there is no one position it could report instead — reporting the
 * highest would claim the others, reporting the lowest would deny them. An event covering nothing has
 * an empty map, which is every event a transform builds and every synthetic one.
 *
 * <p>Each entry is a {@link ChainPosition}: the engine's own order, which is what any comparison is
 * computed on, paired with the connector's opaque token, which is what is persisted so a read can
 * resume. The two are one slot because they are only meaningful together — see {@link ChainPosition}.
 *
 * <p>{@link #position()} is the one-chain view: where this event sits on its own stream, or null when
 * it sits nowhere. It is what a source stamps and what a stateful node compares on; anything that
 * writes a durable frontier reads the whole map instead, because the chains an event covers are
 * exactly what it may let a frontier past.
 *
 * <p>The data maps are held as shallow-unmodifiable defensive copies: the map itself cannot be
 * mutated and a later mutation of the caller's map does not leak in, but nested values are shared.
 */
public record Envelope(
        Op op,
        long ts,
        String src,
        Map<String, Object> before,
        Map<String, Object> after,
        Map<String, Object> schema,
        Map<String, ChainPosition> positions) {

    public Envelope {
        Objects.requireNonNull(op, "op");
        Objects.requireNonNull(src, "src");
        before = copyOrNull(before);
        after = copyOrNull(after);
        schema = copyOrNull(schema);
        positions = positions == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(positions));
    }

    /** An envelope covering nothing — the shape every producer but a source builds. */
    public Envelope(Op op, long ts, String src,
            Map<String, Object> before, Map<String, Object> after, Map<String, Object> schema) {
        this(op, ts, src, before, after, schema, Map.of());
    }

    /** Where this event sits on its own stream, or null when it sits nowhere. */
    public ChainPosition position() {
        return positions.get(src);
    }

    /**
     * A copy sitting at {@code position} on its own stream, every other slot unchanged, and covering
     * nothing else. A null position leaves it covering nothing.
     */
    public Envelope withPosition(ChainPosition position) {
        return withPositions(position == null ? Map.of() : Map.of(src, position));
    }

    /** A copy covering {@code positions}, every other slot unchanged — how the runtime carries them on. */
    public Envelope withPositions(Map<String, ChainPosition> positions) {
        return new Envelope(op, ts, src, before, after, schema, positions);
    }

    /**
     * A copy carrying {@code srcPos} as the token of its own stream's position, keeping whatever order
     * that position already had — the two travel together and stamping one never drops the other.
     */
    public Envelope withSrcPos(String srcPos) {
        ChainPosition at = position();
        return withPosition(srcPos == null && at == null ? null : new ChainPosition(orderOrNull(at), srcPos));
    }

    /** A copy carrying {@code order} as the order of its own stream's position, keeping its token. */
    public Envelope withOrder(SourceOrder order) {
        ChainPosition at = position();
        return withPosition(order == null && at == null ? null : new ChainPosition(order, tokenOrNull(at)));
    }

    private static SourceOrder orderOrNull(ChainPosition at) {
        return at == null ? null : at.order();
    }

    private static String tokenOrNull(ChainPosition at) {
        return at == null ? null : at.token();
    }

    private static Map<String, Object> copyOrNull(Map<String, Object> map) {
        return map == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }

    /** An insert of a new row: {@code after} present, no {@code before}. */
    public static Envelope insert(long ts, String src, Map<String, Object> after, Map<String, Object> schema) {
        return new Envelope(Op.INSERT, ts, src, null, after, schema);
    }

    /** An update of an existing row: both {@code before} and {@code after} present. */
    public static Envelope update(long ts, String src, Map<String, Object> before, Map<String, Object> after, Map<String, Object> schema) {
        return new Envelope(Op.UPDATE, ts, src, before, after, schema);
    }

    /** A delete of a row: {@code before} present, no {@code after}. */
    public static Envelope delete(long ts, String src, Map<String, Object> before, Map<String, Object> schema) {
        return new Envelope(Op.DELETE, ts, src, before, null, schema);
    }

    /** A snapshot batch read of a full row: {@code after} present, no {@code before}. */
    public static Envelope read(long ts, String src, Map<String, Object> after, Map<String, Object> schema) {
        return new Envelope(Op.READ, ts, src, null, after, schema);
    }

    /** A schema change carried by {@code schema}: a non-row event, neither {@code before} nor {@code after}. */
    public static Envelope ddl(long ts, String src, Map<String, Object> schema) {
        return new Envelope(Op.DDL, ts, src, null, null, schema);
    }
}
