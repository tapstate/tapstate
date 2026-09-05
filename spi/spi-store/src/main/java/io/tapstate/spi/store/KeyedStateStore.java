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
     * The states held under {@code keys} in {@code namespace}, under the names they were asked for, with
     * a key that has no state simply absent. Order is not part of the answer.
     *
     * <p><b>This is not the enumeration the class note forbids, and the difference is which side holds
     * the keys.</b> The caller arrives already knowing every key it wants — they came off a reverse index
     * or off a batch of changes — and asks for their states; it never asks what a namespace contains. A
     * listing would answer a question nobody here can pose.
     *
     * <p><b>An implementation must not answer it by scanning</b>, for the reason {@link #count} must not
     * either: a whole-collection read wearing the name of a keyed one is the same cost with none of the
     * warning, and it would be paid per batch rather than once. It must also not answer it by looping over
     * {@link #load} while <em>believing</em> it batched — the default below loops openly, so a store that
     * cannot do better inherits the honest version and a store that overrides is saying it did better.
     *
     * <p>Its reason for existing is a caller that would otherwise ask for a million keys one at a time.
     * Where the store is remote that is a million round trips: the same work as a few thousand batched
     * ones, taking three orders of magnitude longer, with nothing anywhere reporting a problem — the run
     * is merely slow, and the most natural diagnosis (the store is slow) points away from the cause.
     *
     * <p><b>How much it saves depends on how the keys reach here, and a caller in front of a
     * partitioned map should not expect much.</b> Measured on the nest operator, whose batches arrive
     * through such a map: the substrate hands a store the missing keys one partition at a time, so
     * forty identities in forty partitions arrive as forty calls of one key each and the count is
     * unchanged. What it saves there is only the keys that happen to share a partition - measured, two
     * trips out of sixty-nine. A caller that holds its keys directly saves the whole difference.
     *
     * <p>An empty {@code keys} reaches the store not at all. A caller on the event path arrives with one
     * routinely — a change touching only keys already in memory — so the round trip it would otherwise
     * cost is paid in the common case rather than the odd one.
     */
    default Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
        Map<String, byte[]> loaded = new LinkedHashMap<>();
        for (String key : keys) {
            load(namespace, key).ifPresent(state -> loaded.put(key, state));
        }
        return loaded;
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
