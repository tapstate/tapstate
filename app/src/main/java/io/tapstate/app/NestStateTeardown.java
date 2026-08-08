package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.nest.NestStateStats;
import io.tapstate.spi.store.KeyedStateStore;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Lets go of the state a pipeline's nests kept, in the two places it is kept: the maps holding it on the
 * member, and the store behind them holding what those maps wrote through. Both, because either alone
 * leaves the state readable - dropping only the store leaves the live map answering from memory, and
 * destroying only the map leaves the next run's first read pulling it all back off disk.
 *
 * <p>A drop is written down before it is done and forgotten only once it is finished, so that a process
 * that dies halfway through one leaves behind a note saying so rather than a pipeline half let go of. The
 * note is what makes this resumable: a stop is driven once, on the transition, and never again - so
 * without it a drop that got two namespaces into five would be the state of the world from then on, and
 * the next run would read the other three as its own.
 *
 * <p>The note carries the names rather than only the fact, because between the stop that wrote it and the
 * start that finishes it the pipeline may have been applied again. Working the names out afresh at that
 * point would name where the <em>next</em> run will write; what has to be dropped is where the last one
 * did.
 *
 * <p>For the same reason the names cannot first be worked out at the stop either: an apply lands whenever
 * the author makes one, and neither waits for nor is refused by a running pipeline, so by the time a stop
 * asks, the pipeline it asks about may already be a different one. A run therefore says where it keeps
 * state as it starts - the one moment the pipeline being asked is the pipeline the run was built from - and
 * a stop is dropped by what was said rather than by what the definition now compiles to. Taking a nest step
 * out is the case that makes the difference total rather than partial: the edited pipeline compiles to no
 * namespaces at all, so a stop that asked it would name nothing, drop nothing, and leave a whole run's
 * state behind with nothing anywhere that can name it again.
 *
 * <p>Nothing here enumerates: a namespace is dropped by naming it, which is the one bulk operation the
 * state store offers and the reason it needs no way to list what is in one. That is also why state left
 * unnamed is left for good - there is no sweep that would find it later.
 */
final class NestStateTeardown {

    /** The namespace what is known about a pipeline's teardown is kept in, named for that pipeline. */
    private static final String NAMESPACE_PREFIX = "nest.teardown.";

    /** The outstanding drop. Both entries are per pipeline, so neither needs a name of its own. */
    private static final String KEY = "namespaces";

    /** Everywhere this pipeline's runs have said they keep state, which is what a stop is dropped by. */
    private static final String KEPT_KEY = "kept";

    private static final String SEPARATOR = "\n";

    private final HazelcastInstance member;
    private final KeyedStateStore store;

    NestStateTeardown(HazelcastInstance member, KeyedStateStore store) {
        this.member = Objects.requireNonNull(member, "member");
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * Records that a run of this pipeline keeps state in {@code namespaces}, said as that run starts so
     * that the answer comes from the tree the run is built from rather than from whatever the pipeline
     * reads as by the time it is taken down.
     *
     * <p>What is recorded adds to what is already there rather than replacing it. A run keeping state
     * somewhere new does not move what an earlier run left where it left it - an embed renamed, or taken
     * out and put back, keeps its old entries under the old names - and a record holding only the most
     * recent run's names would strand precisely the entries this exists to reach. They add up until a stop
     * lets go of them, which is the point they stop being anybody's.
     *
     * <p>A run that keeps state nowhere records nothing, so a pipeline with no nest in it never grows one
     * of these and a later reader is never handed an empty record to tell apart from a full one.
     */
    void willKeepStateIn(String pipelineId, Set<String> namespaces) {
        if (namespaces.isEmpty()) {
            // Not what keeps a pipeline with no nest from getting a record - the check below does that on
            // its own. This spares every such pipeline a read of the store on every start it ever makes.
            return;
        }
        Set<String> kept = read(pipelineId, KEPT_KEY);
        if (!kept.addAll(namespaces)) {
            // Every name is already down, which is the ordinary case: a pipeline restarted unchanged says
            // what it said last time. Rewriting the same bytes would be a write per start for no difference.
            return;
        }
        write(pipelineId, KEPT_KEY, kept);
    }

    /**
     * Notes what is to be dropped for this pipeline: everywhere its runs said they keep state, together
     * with {@code namespaces} - where the pipeline as it now reads would keep it.
     *
     * <p>Both, because each covers what the other misses. The record misses state written before there was
     * a record to write it in, which is every entry a pipeline carries across an upgrade to this. Compiling
     * the pipeline misses everything an edit moved, up to and including all of it. Naming a namespace that
     * turns out to hold nothing costs a drop that finds nothing, so the union is free in the direction it
     * can be wrong.
     *
     * <p>Writing the note first is what makes the drop that follows resumable; noting nothing to drop
     * writes nothing, so a pipeline with no nest in it leaves no note for a later start to read.
     */
    void note(String pipelineId, Set<String> namespaces) {
        Set<String> dropping = read(pipelineId, KEPT_KEY);
        dropping.addAll(namespaces);
        if (dropping.isEmpty()) {
            return;
        }
        write(pipelineId, KEY, dropping);
    }

    /**
     * Carries out the drop this pipeline has a note outstanding for, and forgets the note once every
     * namespace in it is gone. Does nothing where there is no note - which is what keeps a start that
     * follows a crash rather than a stop from dropping state the run was still entitled to, and so keeps
     * the shape comparison something to compare against.
     *
     * <p>Safe to call again after it has already run, and safe to call again after it died partway: a
     * namespace already dropped drops as a no-op, and the note stays until the last one has.
     */
    void finishPending(String pipelineId) {
        Set<String> pending = noted(pipelineId);
        if (pending.isEmpty()) {
            return;
        }
        NestStateStats stats = NestStateStats.of(member);
        for (String namespace : pending) {
            member.getMap(namespace).destroy();
            store.dropNamespace(namespace);
            // What was counted about a namespace goes with the namespace. Left behind, the counts would
            // keep describing a run that is over, and a reader cannot tell a count standing still from one
            // that is merely quiet.
            stats.forget(namespace);
        }
        // Last, and only once the namespaces above are gone: what is written here outliving what it
        // describes costs a repeated drop, where what it describes outliving it is state nothing will name
        // again. This takes the record of where the runs kept state with it, which is right - the state it
        // named is gone, and a run starting after this says where it keeps state for itself.
        store.dropNamespace(namespaceOf(pipelineId));
    }

    /** The namespaces noted as outstanding for this pipeline, empty where none are. */
    private Set<String> noted(String pipelineId) {
        return read(pipelineId, KEY);
    }

    /**
     * One of this pipeline's lists of namespaces, empty where it has none. Mutable, because both callers
     * read a list in order to add to it.
     */
    private Set<String> read(String pipelineId, String key) {
        return store.load(namespaceOf(pipelineId), key)
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .filter(stored -> !stored.isEmpty())
                .map(stored -> new LinkedHashSet<>(List.of(stored.split(SEPARATOR, -1))))
                .orElseGet(LinkedHashSet::new);
    }

    /**
     * Writes one of this pipeline's lists of namespaces, a line each and sorted, so that the same set is
     * the same bytes however it was arrived at.
     */
    private void write(String pipelineId, String key, Set<String> namespaces) {
        store.save(namespaceOf(pipelineId), key,
                String.join(SEPARATOR, new TreeSet<>(namespaces)).getBytes(StandardCharsets.UTF_8));
    }

    private static String namespaceOf(String pipelineId) {
        return NAMESPACE_PREFIX + pipelineId;
    }
}
