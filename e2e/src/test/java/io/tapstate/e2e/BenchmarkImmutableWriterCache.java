package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/** A bounded classification cache owned by one command invocation, never by the VM or thread. */
final class BenchmarkImmutableWriterCache<T> {
    interface Access<T> {
        long identity(T writer);
        Node<T> inspect(T writer) throws Exception;
    }

    /** Inspection verifies the pinned loader, bytecodes and a final primary delegate field. */
    record Node<T>(String terminalType, T delegate, boolean immutableDelegate) { }
    private record Cached<T>(T retainedMirror, String terminalType, int depth) { }
    private static final int MAX_DEPTH = 8;
    private static final int MAX_WRITERS = 128;
    private final Map<Long, Cached<T>> writers = new HashMap<>();

    String classify(T writer, Access<T> access) throws Exception {
        var path = new ArrayList<T>();
        var visited = new HashSet<Long>();
        String terminal;
        int suffixDepth;
        while (true) {
            long id = access.identity(writer);
            if (!visited.add(id)) { throw invalid("BSON writer delegate cycle"); }
            Cached<T> cached = writers.get(id);
            if (cached != null) {
                terminal = cached.terminalType(); suffixDepth = cached.depth();
                if (path.size() + suffixDepth > MAX_DEPTH) {
                    throw invalid("BSON writer delegate depth exceeded its bound");
                }
                break;
            }
            if (path.size() >= MAX_DEPTH) { throw invalid("BSON writer delegate depth exceeded its bound"); }
            path.add(writer);
            Node<T> node = access.inspect(writer);
            if (node.delegate() == null) {
                if (node.terminalType() == null) { throw invalid("BSON writer terminal type unavailable"); }
                terminal = node.terminalType(); suffixDepth = 0; break;
            }
            if (!node.immutableDelegate() || node.terminalType() != null) {
                throw invalid("BSON writer primary delegate is not immutable");
            }
            writer = node.delegate();
        }
        if (writers.size() + path.size() > MAX_WRITERS) {
            throw invalid("command writer cache exceeded its bound");
        }
        for (int i = 0; i < path.size(); i++) {
            T mirror = path.get(i);
            // Retain each JDI mirror until the owning command returns, preserving its unique ID.
            writers.put(access.identity(mirror), new Cached<>(mirror, terminal, path.size() - i + suffixDepth));
        }
        return terminal;
    }

    private static AssertionError invalid(String reason) {
        return new AssertionError("Invalid boot telemetry capture: " + reason);
    }
}
