package io.tapstate.core.model;

import java.util.Objects;

/**
 * The {@code view:} block of a pipeline (§7): an inline private MDM sink or a
 * {@code use:} reference to a {@code kind: view} definition body. Post-normalization an
 * inline view always carries an id (auto-generated {@code view} when omitted,
 * canonical-form.md §5); a use-reference's id defaults to its target.
 */
@Doc("The view block of a pipeline: an inline private MDM sink, or a use-reference to a separately defined view body.")
public sealed interface ViewBlock {

    @Doc("An inline view that defines its private MDM sink directly inside the pipeline.")
    record Inline(
                  @Doc(value = "Unique resource id across the workspace; must not contain a dot.", required = true)
                  String id,
                  @Doc(value = "The upstream source this view consumes records from.", required = true)
                  FromRef from,
                  @Doc(value = "Field or fields that uniquely identify a record in this view.",
                       required = true)
                  String primaryKey,
                  @Doc("Storage backend used to persist this view.")
                  Storage storage,
                  @Doc(value = "How rows are written to the view — for example upsert or append.", def = "upsert")
                  WriteMode writeMode,
                  @Doc(value = "Treatment of existing view rows before a new full load; resume, recovery and CDC-only never clear rows.",
                          def = "append")
                  OnFullLoad onFullLoad,
                  @Doc("How this node runs: its target total parallelism across the cluster and its batch.")
                  ExecutionSpec execution) implements ViewBlock {
        public Inline(String id, FromRef from, String primaryKey, Storage storage, ExecutionSpec execution) {
            this(id, from, primaryKey, storage, null, null, execution);
        }

        public Inline {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(from, "from");
        }

        /** An inline view written with the default write settings: upsert, and keep rows on a full load. */
        public Inline(String id, FromRef from, String primaryKey, Storage storage) {
            this(id, from, primaryKey, storage, null, null, null);
        }

        public Inline(String id, FromRef from, String primaryKey, Storage storage,
                WriteMode writeMode, OnFullLoad onFullLoad) {
            this(id, from, primaryKey, storage, writeMode, onFullLoad, null);
        }
    }

    @Doc("A reference to a separately defined view body, reused inside this pipeline.")
    record Use(
               @Doc("Unique resource id across the workspace; must not contain a dot. Defaults to the referenced view name.")
               String id,
               @Doc(value = "Name of the externally defined view to reuse.", required = true)
               String use,
               @Doc(value = "The upstream source this view consumes records from.", required = true)
               FromRef from) implements ViewBlock {
        public Use {
            Objects.requireNonNull(use, "use");
            Objects.requireNonNull(from, "from");
            id = id == null ? use : id;
        }
    }
}
