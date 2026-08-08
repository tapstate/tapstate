package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.MapConfig;
import io.tapstate.core.common.TapstateException;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The one place a nest is told what it is allowed to be: what each of its levels may hold, and what the
 * maps those levels keep their state in are configured to be.
 *
 * <p>Both belong here rather than in two places because they are two ends of one capacity decision. How
 * much a level may keep in memory and how much it may hold at all are separate numbers that only mean
 * anything against each other, and a deployment that sets them from two different places will eventually
 * set them into a combination that cannot work - with nothing able to say so, because neither place can
 * see the other's number.
 *
 * <p><b>How much a level holds is not a number anyone sets.</b> A level holds one key per row of its
 * source and a nest one document per root of its own, neither of which the tree bounds, and both are
 * absorbed by the layer behind the memory rather than refused: growing past what is held in memory is what
 * the store is there for. What a deployment sets is how much stays in memory; what is beyond it is on
 * disk, and how much of that there may be is a question for the disk.
 *
 * <p>The one exception is per document, and is here for a reason the others are not: a document is
 * rendered whole, so a document that has absorbed more than fits is not something a store behind the map
 * can help with. That one is per namespace, since one tree's documents differ in width by orders of
 * magnitude and a single number covering all of them catches nothing. A namespace nobody configured takes
 * the default.
 *
 * <p>This is carried to the member that runs each vertex, because that is where a limit is enforced while
 * here is where it is chosen. Anything added to it has to survive that trip: a knob that stayed behind
 * would leave every vertex running unconfigured while the configuration reads as set.
 */
public final class NestSettings implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * How many elements one document may hold when nothing says otherwise.
     *
     * <p>Per document rather than per namespace, and the only count that fails a run rather than being
     * absorbed by the layer behind it: a document is assembled whole, so however wide it has grown is what
     * has to be in memory at once, and no eviction reaches inside one.
     *
     * <p>Provisional. What a deployment should run with follows from measuring what one element of its
     * documents costs, and until that measurement exists this is the honest placeholder: high enough not to
     * fail work that was always going to fit, low enough to be reached before nothing is left to reach it
     * with.
     */
    public static final long DEFAULT_ELEMENT_LIMIT = 100_000L;

    /**
     * How many entries each namespace keeps in memory when nothing says otherwise.
     *
     * <p>High enough that a deployment whose state fits never evicts at all - eviction buys capacity with a
     * read of the layer behind the map, and a level that was always going to fit should not pay it - and
     * low enough that a level which does not fit is bounded by this rather than by the heap running out.
     *
     * <p>Provisional. What a deployment should run with follows from measuring what one entry of its levels
     * costs in memory, which is the multiplication this number cannot do for itself: it is a count of
     * entries, and what has to fit is bytes.
     */
    public static final long DEFAULT_ENTRIES_HELD_IN_MEMORY = 100_000L;

    private final Map<String, Long> elementLimits;
    private final long entriesHeldInMemory;

    private NestSettings(Map<String, Long> elementLimits, long entriesHeldInMemory) {
        this.elementLimits = Map.copyOf(elementLimits);
        this.entriesHeldInMemory = entriesHeldInMemory;
    }

    /** Every level on the default limit, which is what a deployment that configured nothing gets. */
    public static NestSettings defaults() {
        return new NestSettings(Map.of(), DEFAULT_ENTRIES_HELD_IN_MEMORY);
    }

    /** These settings, with each document of the nest at {@code namespace} allowed {@code limit} elements. */
    public NestSettings withElementLimit(String namespace, long limit) {
        return new NestSettings(with(elementLimits, namespace, limit, "elements"), entriesHeldInMemory);
    }

    /**
     * These settings, with each namespace keeping {@code entries} of its state in memory and the rest on
     * the layer behind it.
     *
     * <p>One number for every namespace rather than one each. What it bounds is the memory of the process
     * they all share, and a deployment that had to name each level to bound the whole would be bounding it
     * by however many it remembered to name.
     *
     * <p>Refused below the partition count, where it stops meaning what it says: the substrate spends this
     * budget per partition rather than per map, so a number smaller than the partitions has already been
     * rounded up to one entry each by the time it is enforced - measured at 82 resident against a
     * configured 10 - and a deployment reading its own configuration back would not learn that.
     */
    public NestSettings withEntriesHeldInMemory(long entries) {
        if (entries < NestMaps.SMALLEST_MEANINGFUL_MEMORY_BUDGET) {
            throw new TapstateException(NestError.MEMORY_BUDGET_BELOW_PARTITION_COUNT,
                    Map.of("entries", entries,
                            "partitions", (long) NestMaps.SMALLEST_MEANINGFUL_MEMORY_BUDGET), null);
        }
        return new NestSettings(elementLimits, entries);
    }

    /** How many entries each namespace keeps in memory before the rest is left to the layer behind it. */
    public long entriesHeldInMemory() {
        return entriesHeldInMemory;
    }

    /** How many elements one document of {@code namespace} may hold before the job is failed. */
    public long elementsAllowedIn(String namespace) {
        return elementLimits.getOrDefault(namespace, DEFAULT_ELEMENT_LIMIT);
    }

    private static Map<String, Long> with(Map<String, Long> limits, String namespace, long limit,
            String held) {
        Objects.requireNonNull(namespace, "namespace");
        if (limit <= 0) {
            throw new IllegalArgumentException(
                    "allowed " + limit + " " + held + ", it could hold nothing at all: " + namespace);
        }
        Map<String, Long> widened = new LinkedHashMap<>(limits);
        widened.put(namespace, limit);
        return widened;
    }

    /**
     * What every nest state map is configured to be, with nothing behind it: what it holds lives only as
     * long as the member does.
     */
    public MapConfig stateMaps() {
        return NestMaps.stateMaps();
    }

    /**
     * As above, reading through the member's nest state store for a key that is not in memory and writing
     * through it, and holding only {@link #entriesHeldInMemory()} of each namespace in memory at a time.
     *
     * <p>The store is named in the configuration and resolved on the member that runs the map, so the
     * member has to have been told its store - see {@code NestStateMapStoreFactory.bindTo} - before any
     * nest map on it is used.
     */
    public MapConfig backedStateMaps() {
        return NestMaps.backedStateMaps(entriesHeldInMemory);
    }

    /**
     * As above, for the one namespace {@code namespace} rather than for all of them, so that this budget
     * applies to that namespace alone. This is the form a pipeline's own budget takes: it names a
     * namespace, and namespaces are not known until there is a pipeline, which is after the member is
     * already running.
     */
    public MapConfig backedStateMaps(String namespace) {
        return NestMaps.backedStateMaps(Objects.requireNonNull(namespace, "namespace"), entriesHeldInMemory);
    }
}
