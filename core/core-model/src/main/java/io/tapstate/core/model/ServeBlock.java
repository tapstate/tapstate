package io.tapstate.core.model;

import java.util.List;
import java.util.Objects;

/**
 * The {@code serve:} block of a pipeline (ADR-0016 §8): inline publish surface or a
 * {@code use:} reference to a {@code kind: serve} definition body. The serve block is a
 * terminal container — nothing in the grammar references it, so it stays anonymous
 * (canonical-form.md §5 coverage ruling).
 */
@Doc("The publish surface of a pipeline: either an inline serve definition or a reference to a reusable serve body.")
public sealed interface ServeBlock {

    @Doc("An inline serve definition that declares the sync, query and push surfaces directly on the pipeline.")
    record Inline(
            @Doc("Optional id for this serve block.")
            String id,
            @Doc(value = "The data source this serve block exposes.", required = true)
            FromClause from,
            @Doc("Tables continuously synchronized to the serving layer.")
            List<SyncElement> sync,
            @Doc("Read endpoints exposed for querying the served data.")
            List<QueryElement> query,
            @Doc("Push endpoints that stream changes to downstream consumers.")
            List<PushElement> push) implements ServeBlock {
        public Inline {
            Objects.requireNonNull(from, "from");
            sync = sync == null ? null : List.copyOf(sync);
            query = query == null ? null : List.copyOf(query);
            push = push == null ? null : List.copyOf(push);
        }

        /** Keeps source-level Java callers source-compatible while the YAML model gains list wiring. */
        public Inline(String id, FromRef from, List<SyncElement> sync,
                List<QueryElement> query, List<PushElement> push) {
            this(id, FromClause.list(from), sync, query, push);
        }
    }

    @Doc("A serve block that references a reusable serve definition body by name.")
    record Use(
            @Doc("Optional id for this serve block; defaults to the referenced definition name.")
            String id,
            @Doc(value = "Name of the reusable serve definition to use.", required = true)
            String use,
            @Doc(value = "The data source this serve block exposes.", required = true)
            FromClause from) implements ServeBlock {
        public Use {
            Objects.requireNonNull(use, "use");
            Objects.requireNonNull(from, "from");
            id = id == null ? use : id;
        }

        /** Keeps source-level Java callers source-compatible while the YAML model gains list wiring. */
        public Use(String id, String use, FromRef from) {
            this(id, use, FromClause.list(from));
        }
    }
}
