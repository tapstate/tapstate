package io.tapstate.control.core;

import io.tapstate.core.model.OnFullLoad;
import io.tapstate.core.model.PipelineResource;

import java.util.List;
import java.util.Objects;

/**
 * Where a pipeline writes: one entry per write element and target table, named the way that element's
 * sink binds it.
 *
 * <p>A port for the same reason {@link PipelineChains} is one. A sync element's table names come from
 * the source tables that reach it, the assemblies built along the way and the element's rename rules,
 * and the side that binds the sink is where that is worked out. A second derivation here would be the
 * first to drift, and it would drift the way that hurts most: a start check reading a table nobody
 * writes, and telling the person starting the pipeline that their target is empty.
 */
public interface PipelineWriteTargets {

    /**
     * The targets {@code pipeline} writes, in declaration order: every sync element's tables, then the
     * view's collection. Reads only; a definition whose targets cannot be named yet -- a source never
     * discovered, say -- is refused with the coded error that says why.
     */
    List<WriteTarget> of(PipelineResource pipeline);

    /**
     * One table a write element lands in.
     *
     * @param element    the sync element's id, or the view's
     * @param kind       which kind of write element it is
     * @param connection the source id of the connection the table lives on
     * @param table      the table or collection name, as the sink names it
     * @param onFullLoad what a new full load does to rows already there, the default applied
     * @param definedIn  null when the element is written in the pipeline's own definition; otherwise the
     *                   id of the shared {@code kind: serve} or {@code kind: view} definition it comes from
     */
    record WriteTarget(
            String element, Kind kind, String connection, String table, OnFullLoad onFullLoad,
            String definedIn) {

        public WriteTarget {
            Objects.requireNonNull(element, "element");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(connection, "connection");
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(onFullLoad, "onFullLoad");
        }

        /** The kinds of element that write a target. */
        public enum Kind {
            SYNC,
            VIEW
        }
    }
}
