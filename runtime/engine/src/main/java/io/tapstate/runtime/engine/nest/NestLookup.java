package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Where the rows one level points at are kept: one entry per row, keyed by what identifies that row, and
 * holding the row's fields and nothing else. It is not a second copy of anything - it is the only copy
 * this tree keeps of that table, which is why a row referred to by ten thousand documents is stored once.
 *
 * <p>It is deliberately not a {@link NestVertex}. A vertex holds assembly - what has been gathered under
 * one key of it, and what is still waiting - and is read and written by the one processor that owns the
 * partition. This holds no assembly at all, is written by one vertex and read by another, and takes no
 * parking area because nothing here ever moves between documents.
 *
 * <p>{@code partitionKey} names the columns on the referred-to row that identify it, in the order the
 * embed's {@code on} map wrote them. The level pointing at it reads the matching columns of its own row in
 * that same order, so the two sides build the same key without either having to know the other's table.
 *
 * <p>The last three components describe the other side of the reference - the level doing the pointing -
 * and are here because that level's rows are delivered a second time, to record which of them point at
 * what. {@code referrerAlias} is the stream they arrive on, {@code referenceFields} the columns of their
 * own rows holding the reference, and {@code referrerIdentity} what identifies one of them. That second
 * delivery leaves from where the first one does and never passes through the vertex assembling documents,
 * which is what keeps the graph free of a cycle.
 */
public record NestLookup(
        List<String> pathId,
        String alias,
        String name,
        String mapName,
        List<String> partitionKey,
        String referrerAlias,
        List<String> referenceFields,
        List<String> referrerIdentity) implements Serializable {

    /**
     * How many buckets the identities pointing at one row are spread over.
     *
     * <p>Sized so a row sitting on the fanout limit still leaves an order of magnitude of headroom inside
     * any single entry, while a row pointed at three times costs three entries rather than sixty-four -
     * an empty bucket is never written, so a generous count costs nothing at the small end, and that is
     * what allows it to be generous. Reading them all back is one batch of keys, so it costs nothing at
     * the read end either.
     *
     * <p><b>A constant rather than a setting, and that is a decision rather than an omission.</b> A bucket
     * number is computed when an identity is filed and computed again when it is looked for, so changing
     * the count changes every number: what was written under the old one is not wrong, it is unreachable,
     * and nothing reports that. A knob whose every turn silently abandons the index is worse than no knob,
     * so what could have been configured is fixed here for the life of the namespace instead.
     */
    public static final int BUCKETS = 64;

    public NestLookup {
        Objects.requireNonNull(alias, "alias");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(mapName, "mapName");
        Objects.requireNonNull(referrerAlias, "referrerAlias");
        pathId = List.copyOf(pathId);
        partitionKey = List.copyOf(partitionKey);
        referenceFields = List.copyOf(referenceFields);
        referrerIdentity = List.copyOf(referrerIdentity);
        if (partitionKey.isEmpty()) {
            throw new IllegalArgumentException("lookup " + name + " has nothing to key rows by");
        }
        if (referenceFields.size() != partitionKey.size()) {
            throw new IllegalArgumentException("lookup " + name + " keys rows by " + partitionKey.size()
                    + " column(s) but reads the reference out of " + referenceFields.size());
        }
        if (referrerIdentity.isEmpty()) {
            throw new IllegalArgumentException(
                    "lookup " + name + " has nothing to identify the rows pointing at it by");
        }
    }

    /**
     * The namespace holding which identities point at each row, beside the one holding the rows
     * themselves. Suffixed off the same name for the reason parking is: the two belong to one level and
     * are taken down together, so deriving one from the other is what stops a rename leaving half of it
     * standing under a name nothing looks at again.
     */
    public String referencesMapName() {
        return mapName + ".refs";
    }

    /**
     * Which bucket the identity {@code referrer} is filed in. Taken off the text of the identity rather
     * than off its own hash: these entries outlive the process that wrote them, so the same identity has
     * to reach the same bucket on the member filing it, on the member reading it back, and after a restart
     * on a different build. A string's hash is specified to the bit and answers the same everywhere; the
     * hash of a list of whatever a source happened to put in a column is not.
     */
    public static int bucketOf(List<Object> referrer) {
        return Math.floorMod(spread(String.valueOf(referrer).hashCode()), BUCKETS);
    }

    /**
     * Stirs a hash so that every bit of it reaches the few this is taken apart by. A string's own hash
     * carries most of its information high up, and identities that are consecutive numbers - which is what
     * most tables key on - come out of it in clusters: measured over a hundred thousand of them, the
     * fullest bucket held 2.25 times its share before this and 1.06 times after. The headroom the bucket
     * count was chosen for is measured against the even share, so a hash that misses it by twice spends
     * that headroom before anything else does.
     *
     * <p>Written out rather than reached for, because it has to answer the same on every build for as long
     * as the entries live: this is arithmetic on a specified hash and stays put, where a library's
     * spreading function is free to change and would silently re-file the whole index if it did.
     */
    private static int spread(int hash) {
        int mixed = hash ^ (hash >>> 16);
        mixed *= 0x85ebca6b;
        mixed ^= mixed >>> 13;
        mixed *= 0xc2b2ae35;
        return mixed ^ (mixed >>> 16);
    }

    /**
     * The entry one bucket of the identities pointing at {@code referenced} is filed under: that row's own
     * key with the bucket number appended.
     *
     * <p>Flat, and that is load-bearing rather than tidy. A key is filed in the layer behind the map under
     * a name built by naming the kind of every value in it, and there is no kind for a key nested inside
     * another - so a key holding one is refused outright, at the moment a bucket first has to be read back
     * from the cold layer rather than when it is written. Appending keeps every value a scalar, and stays
     * injective because the number of columns identifying a row is fixed for a namespace, which makes the
     * last value the bucket in every key of it.
     */
    public static List<Object> bucketKey(List<Object> referenced, int bucket) {
        List<Object> key = new ArrayList<>(referenced.size() + 1);
        key.addAll(referenced);
        key.add(bucket);
        return Collections.unmodifiableList(key);
    }
}
