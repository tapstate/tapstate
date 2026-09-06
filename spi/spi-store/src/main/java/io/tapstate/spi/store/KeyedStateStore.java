package io.tapstate.spi.store;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The cold layer under a stateful operator: one opaque state document per key, within a namespace.
 *
 * <p>Distinct from {@link StateStore}, which keeps one epoch-fenced checkpoint per pipeline and is read
 * and written by the control plane. This one is on the data path — an entry per key a running operator
 * is partitioned by, written as that key is handled and read back when a key that is no longer in memory
 * is asked for again. It carries no epoch and no fencing: the caller above it is the single writer of any
 * one key, because the key is what the work was partitioned by in the first place.
 *
 * <p><b>There is deliberately no way to enumerate.</b> A store that could list a namespace would be asked
 * to on the way up from a restart, and the whole keyspace would be read to serve the first event — which
 * is the warm-up this layer exists to avoid. The absence is the contract: an implementation must not add
 * a listing method, and a caller must never need one. Dropping a namespace whole is the one bulk
 * operation, and it names the namespace rather than the keys in it, so it needs no listing either.
 *
 * <p>The value is bytes and stays bytes. What is inside is the running operator's business and changes
 * with it; an implementation that looked in would become a second place that has to agree about the
 * shape of something it does not own.
 */
public interface KeyedStateStore {

    /** The state held under {@code key} in {@code namespace}, or empty if there is none. */
    Optional<byte[]> load(String namespace, String key);

    /**
     * The states held under {@code keys} in {@code namespace}, by the key each was found under. A key with
     * nothing under it is absent from the answer rather than mapped to null, so the size of what comes back
     * is how many of them existed.
     *
     * <p><b>This is not the listing the contract above forbids.</b> The caller names every key it wants, so
     * this reads exactly what the same keys passed one at a time would have read and never touches the
     * keyspace. The distinction is between being handed keys and being asked to find them.
     *
     * <p>It exists because the per-key form is a round trip each, and that cost is invisible from above: a
     * caller holding a whole set of keys would otherwise turn one reach into as many trips as it has keys,
     * and the only trace is a number that grows with the keys asked for while the reach count stays at one.
     *
     * <p><b>How much it saves depends on how the keys reach here, and a caller in front of a partitioned
     * map should not expect much.</b> Measured on the nest operator, whose batches arrive through such a
     * map: the substrate hands a store the missing keys one partition at a time, so forty identities in
     * forty partitions arrive as forty calls of one key each and the count is unchanged. What it saves
     * there is only the keys that happen to share a partition - measured, two trips out of sixty-nine.
     * A caller that holds its keys directly saves the whole difference.
     *
     * <p>The default asks one at a time, so an implementation with no batch read of its own stays correct
     * without writing anything. One that has a batch read should override this - that is the whole point of
     * the method, and an implementation that leaves the default in place has not made the cost go away.
     */
    default Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
        Map<String, byte[]> found = new LinkedHashMap<>();
        for (String key : keys) {
            load(namespace, key).ifPresent(state -> found.put(key, state));
        }
        return found;
    }

    /**
     * Stores {@code state} under {@code key}, replacing whatever was there. It must have reached durable
     * storage by the time this returns: the caller keeps no queue and no replica of what it handed over,
     * so a write that is still in flight is a write that a crash loses with nothing reporting it.
     */
    void save(String namespace, String key, byte[] state);

    /** Removes the entry under {@code key}, if there is one. Removing what is not there is not an error. */
    void delete(String namespace, String key);

    /**
     * Removes everything in {@code namespace}. This is how a pipeline that is being taken down for good
     * lets go of its state, and why nothing needs to be able to list the keys in order to drop them.
     */
    void dropNamespace(String namespace);

    /**
     * How many entries {@code namespace} holds. The one question about a namespace as a whole that can be
     * asked, and it is not the one the absence above forbids: what comes back is a number, and nothing
     * that reads it learns a single key from it.
     *
     * <p><b>An implementation must not answer it by listing.</b> Counting through the keys would be the
     * forbidden read wearing a different name - the whole namespace off the store to produce one number -
     * and it would be reached on a cadence rather than once, which is worse than the case the rule was
     * written for. A store that cannot count without listing should say so by refusing rather than by
     * doing it.
     *
     * <p>Asked when someone is looking rather than as state is written: it is the only operation here that
     * is about a namespace rather than a key, so it is the only one whose cost does not shrink with what
     * is being handled. Callers on the event path have {@code load} and {@code save}; this is for whoever
     * is reporting.
     */
    long count(String namespace);
}
