package io.tapstate.runtime.engine.nest;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A nest store that keeps everything on the heap of the member running the vertex - a test double, and
 * only that.
 *
 * <p>It is the whole store, not a cache in front of one: nothing spills, and a restart begins with
 * nothing. A run drives no pipeline on this. Nest state is what a change let past the source's read
 * offset is being kept in, so state that dies with the process is not a smaller version of the real
 * thing, it is a way to lose those changes with nothing reporting it - which is why this lives in test
 * sources, where the compiler is what keeps it out of a running system rather than a rule someone reads.
 *
 * <p>What it is good for: driving a vertex whose subject is not durability, where a real store would add
 * a container and tell the case nothing.
 *
 * <p><b>Concurrent because it is shared, not because any case asked for it.</b> One of these stands behind
 * a namespace several vertices reach, and each vertex runs a processor per unit of parallelism on a thread
 * of its own, so these entries are written and read from several threads at once. Partitioning makes every
 * key one processor's alone, so no two threads ever touch the same entry - and that is not what an
 * unsynchronised map loses. What it loses is a write to a key nobody else was touching, because another
 * thread was growing the same table at that moment: measured with four writers on keys of their own,
 * entries written and then read back absent, up to a quarter of them in one run.
 *
 * <p><b>What that cost, and why a plain map here is worse than useless.</b> The store this stands in for
 * is a distributed map, where those writes cannot collide, so a lost one is a red that the product has no
 * way of producing. One such loss made a lookup file a row while the registration naming it was gone,
 * so the row read as one nobody points at, no word of its arrival was sent, and the document waiting for
 * it waited for ever - the job running, nothing thrown, every other document correct.
 */
public final class HeapNestStore<S> implements NestStore<S> {

    private final Map<Object, S> entries = new ConcurrentHashMap<>();

    @Override
    public S load(Object key) {
        return entries.get(key);
    }

    @Override
    public void save(Object key, S state) {
        entries.put(key, state);
    }

    @Override
    public void remove(Object key) {
        entries.remove(key);
    }

    @Override
    public long count() {
        return entries.size();
    }
}
