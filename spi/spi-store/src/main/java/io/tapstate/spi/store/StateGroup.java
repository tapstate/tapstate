package io.tapstate.spi.store;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The commit group of one vertex instance, opened by {@link GroupCommitStateStore#openGroup}.
 *
 * <p>Not thread-safe: the instance that opened it commits through it from its own thread.
 */
public interface StateGroup {

    /**
     * Writes every change in {@code changes}, keyed by the namespace as the group was opened with it, in one
     * store transaction. Either all of it is stored or none of it is, and a failure leaves the group unproved,
     * so the next commit proves again. A group with nothing in it opens no transaction at all.
     *
     * <p>A refusal because the instance's authority has passed is {@link IoError#WORKLOAD_CLAIM_FENCED}; the
     * caller fails closed and drops whatever it held for this group.
     */
    void commit(Map<String, Changes> changes);

    /**
     * What one namespace's share of a group writes: values to store, the subset of them stored with a
     * pending-release marker and its routing key, and keys to remove. A key is in at most one of
     * {@code writes} and {@code removals}.
     */
    record Changes(Map<String, byte[]> writes, Map<String, byte[]> releaseKeys, Set<String> removals) {
        public Changes {
            writes = copy(writes);
            releaseKeys = copy(releaseKeys);
            removals = Set.copyOf(new LinkedHashSet<>(Objects.requireNonNull(removals, "removals")));
            for (String key : releaseKeys.keySet()) {
                if (!writes.containsKey(key)) throw new IllegalArgumentException("a release key goes with a value written in the same group");
            }
            for (String key : removals) {
                if (writes.containsKey(key)) throw new IllegalArgumentException("a key is written or removed, not both");
            }
        }

        public boolean isEmpty() {
            return writes.isEmpty() && removals.isEmpty();
        }

        private static Map<String, byte[]> copy(Map<String, byte[]> values) {
            Map<String, byte[]> copied = new LinkedHashMap<>();
            Objects.requireNonNull(values, "values").forEach((key, value) ->
                    copied.put(Objects.requireNonNull(key, "key"), Objects.requireNonNull(value, "value").clone()));
            return java.util.Collections.unmodifiableMap(copied);
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof Changes that) || !removals.equals(that.removals)) return false;
            return same(writes, that.writes) && same(releaseKeys, that.releaseKeys);
        }

        @Override public int hashCode() { return Objects.hash(writes.keySet(), releaseKeys.keySet(), removals); }

        @Override public String toString() {
            return "Changes[writes=" + writes.size() + ", releases=" + releaseKeys.size() + ", removals=" + removals.size() + "]";
        }

        private static boolean same(Map<String, byte[]> left, Map<String, byte[]> right) {
            if (!left.keySet().equals(right.keySet())) return false;
            for (var entry : left.entrySet()) if (!Arrays.equals(entry.getValue(), right.get(entry.getKey()))) return false;
            return true;
        }
    }
}
