package io.tapstate.runtime.engine.nest;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A nest store that keeps everything on the heap of the member running the vertex.
 *
 * <p>It is the whole store, not a cache in front of one: nothing spills, and a restart begins with
 * nothing. That makes it right for a single run whose state fits in memory and for driving a vertex in a
 * test, and wrong for anything that has to outlive the process or outgrow the heap. Whoever builds the
 * vertex chooses; the vertex itself cannot tell the difference.
 */
public final class HeapNestStore<S> implements NestStore<S> {

    private final Map<Object, S> entries = new LinkedHashMap<>();

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
