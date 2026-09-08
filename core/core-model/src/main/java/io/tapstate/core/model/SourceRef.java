package io.tapstate.core.model;

import java.util.Objects;

/**
 * One element of a pipeline's {@code source} list: a bare source id, or an object carrying
 * this pipeline's own srs switch for that source.
 *
 * <p>The switch lives on the reference rather than on the source body so that changing a
 * source's srs configuration does not silently re-route every pipeline already reading it.
 * A pipeline records one value per source it reads, and the runtime reads only that value --
 * it never falls back to the source.
 *
 * <p>{@link Bare} means "no switch recorded on this reference yet"; the first apply
 * materializes the source's own value into a {@link Spec}. A Spec with no switch would be
 * the same state as Bare, so the object form requires one and parsing normalizes an object
 * written without it back to Bare.
 */
@Doc("A source this pipeline reads from: a bare source id, or an object carrying its srs switch.")
public sealed interface SourceRef {

    /** Id of the referenced source. */
    String id();

    static Bare bare(String id) {
        return new Bare(id);
    }

    static Spec spec(String id, Boolean srs) {
        return new Spec(id, srs);
    }

    /** Bare source id -- no srs switch recorded on this reference. */
    @YamlForm(YamlForm.Form.UNWRAP)
    record Bare(String id) implements SourceRef {
        public Bare {
            Objects.requireNonNull(id, "id");
        }
    }

    /** Object form: source id plus this pipeline's own srs switch for that source. */
    @Doc("A source reference carrying this pipeline's own srs switch for that source.")
    record Spec(
            @Doc(value = "Id of the source this pipeline reads from.", required = true)
            String id,
            @Doc(value = "Whether this pipeline reads that source through the shared replay store. "
                    + "Materialized from the source on first apply, and only follows this file after.",
                    required = true)
            Boolean srs)
            implements SourceRef {
        public Spec {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(srs, "srs");
        }
    }
}
