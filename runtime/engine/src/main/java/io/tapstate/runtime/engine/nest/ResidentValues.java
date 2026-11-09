package io.tapstate.runtime.engine.nest;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The values of one state namespace a vertex keeps in memory, bounded in bytes.
 *
 * <p>Values are held serialized, as the store holds them, so what one costs is known exactly rather than
 * estimated from an object graph: its length plus a fixed charge for the entry itself. The least recently used
 * clean value goes first once the budget is passed. What is beyond the budget is not lost; it is read back from
 * the store when it is asked for.
 *
 * <p>Staged values are the exception, and deliberately: a value staged for the next commit exists nowhere else,
 * so it stays resident, whatever the budget says, until the commit lands or is abandoned. That excess is bounded
 * by the commit group's own byte bound, not by this budget. A removal is staged the same way, as an entry with
 * no value, so a read in between answers absent instead of reading the old value back from the store.
 *
 * <p>Not thread-safe: one vertex instance owns it and uses it from its one thread.
 */
public final class ResidentValues<K> {

    /** What one entry costs beyond its value's bytes: the key, the map node and the bookkeeping around them. */
    public static final long ENTRY_OVERHEAD_BYTES = 64L;

    private static final byte[] REMOVED = new byte[0];

    private final long budgetBytes;
    /** Access-ordered, so iteration starts at the least recently used. */
    private final LinkedHashMap<K, byte[]> values = new LinkedHashMap<>(16, 0.75f, true);
    private final LinkedHashMap<K, byte[]> staged = new LinkedHashMap<>();
    private long residentBytes;

    public ResidentValues(long budgetBytes) {
        if (budgetBytes <= 0) {
            throw new IllegalArgumentException("a residency budget must be positive: " + budgetBytes);
        }
        this.budgetBytes = budgetBytes;
    }

    /** The budget this namespace holds clean values within. */
    public long budgetBytes() {
        return budgetBytes;
    }

    /**
     * What is resident for {@code key}: empty when nothing is, so the caller has to read it from the store; a
     * present empty value when the key is staged for removal or known absent; otherwise its bytes.
     */
    public Optional<Optional<byte[]>> get(K key) {
        Objects.requireNonNull(key, "key");
        byte[] pending = staged.get(key);
        if (pending != null) {
            return Optional.of(pending == REMOVED ? Optional.empty() : Optional.of(pending));
        }
        byte[] value = values.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(value == REMOVED ? Optional.empty() : Optional.of(value));
    }

    /**
     * Keeps {@code value}, as read from the store or as it has just been committed, as the resident value of
     * {@code key}; null records that the store has nothing for it. A staged value for the key wins over it.
     */
    public void putClean(K key, byte[] value) {
        Objects.requireNonNull(key, "key");
        if (staged.containsKey(key)) {
            return;
        }
        replace(key, value == null ? REMOVED : value);
        evictToBudget();
    }

    /** Stages {@code value} as what the next commit writes for {@code key}. */
    public void stage(K key, byte[] value) {
        staged.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value"));
    }

    /** Stages the removal of {@code key} for the next commit. */
    public void stageRemoval(K key) {
        staged.put(Objects.requireNonNull(key, "key"), REMOVED);
    }

    /** Whether anything is staged. */
    public boolean hasStaged() {
        return !staged.isEmpty();
    }

    /** What the next commit has to write, in staging order: a key's bytes, or empty to remove it. */
    public Map<K, Optional<byte[]>> staged() {
        Map<K, Optional<byte[]>> out = new LinkedHashMap<>();
        staged.forEach((key, value) -> out.put(key, value == REMOVED ? Optional.empty() : Optional.of(value)));
        return Collections.unmodifiableMap(out);
    }

    /** How many bytes are staged, as {@link #ENTRY_OVERHEAD_BYTES} plus value length per key. */
    public long stagedBytes() {
        long bytes = 0;
        for (byte[] value : staged.values()) {
            bytes += cost(value);
        }
        return bytes;
    }

    /** The staged values are in the store now: they become clean resident values, within the budget. */
    public void committed() {
        for (Map.Entry<K, byte[]> entry : staged.entrySet()) {
            replace(entry.getKey(), entry.getValue());
        }
        staged.clear();
        evictToBudget();
    }

    /**
     * The commit did not land: what was staged is dropped, and so is anything resident for the same keys, since
     * what the store holds for them is no longer known here.
     */
    public void abandonStaged() {
        for (K key : staged.keySet()) {
            byte[] gone = values.remove(key);
            if (gone != null) {
                residentBytes -= cost(gone);
            }
        }
        staged.clear();
    }

    /** Drops everything, staged or not: the state this was a copy of is no longer this vertex's to hold. */
    public void clear() {
        values.clear();
        staged.clear();
        residentBytes = 0;
    }

    /** The bytes held by clean resident values. */
    public long residentBytes() {
        return residentBytes;
    }

    /** How many clean values are resident. */
    public int residentCount() {
        return values.size();
    }

    private void replace(K key, byte[] value) {
        byte[] before = values.put(key, value);
        if (before != null) {
            residentBytes -= cost(before);
        }
        residentBytes += cost(value);
    }

    private void evictToBudget() {
        Iterator<Map.Entry<K, byte[]>> oldest = values.entrySet().iterator();
        while (residentBytes > budgetBytes && oldest.hasNext()) {
            residentBytes -= cost(oldest.next().getValue());
            oldest.remove();
        }
    }

    private static long cost(byte[] value) {
        return ENTRY_OVERHEAD_BYTES + value.length;
    }
}
