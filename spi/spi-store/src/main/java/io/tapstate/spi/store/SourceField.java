package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.common.NumericType;

import java.util.Objects;

/**
 * One field of a discovered stream: its name, the type its source declared it with, the tapstate type
 * that resolves to, and - where nothing resolved one - why. An immutable value.
 *
 * <p>{@code name} is always present. {@code dataType} is the type string the connector reported, kept
 * verbatim because it is what a sink needs to create the column again in a database of the same kind, and
 * null when discovery could not resolve it. {@code type} is the same column in the tapstate type
 * namespace - the form anything that reasons about the column rather than merely carrying it reads. It is
 * resolved where the connector is still open, since the database's own spelling means nothing away from
 * the connector that declared it; a field carrying no resolved type is
 * {@link TapstateType#UNKNOWN}, never null.
 *
 * <p><b>An unknown type says which unknown it is, and one that does not is refused.</b> There are
 * several ways a column arrives without a type and they want different things done about them: a
 * connector that declared nothing, a spelling the mapping has no member for, a number the connector
 * described in a way that names no width, a field read back from a record written before types were
 * kept. They are the same value and different problems - only the cause says whether the schema is what
 * needs fixing, whether a mapping is short a case, or whether nothing is wrong at all. Folded into one
 * bare UNKNOWN they all read as "the type merely failed to resolve", which is an ordinary state nobody
 * investigates. So the reason is a component rather than a convention, and a resolved type may not
 * carry one: a reason beside a known type is a caller that has confused the two.
 */
public record SourceField(String name, String dataType, TapstateType type, String unknownBecause, NumericType numericType) {

    /** Compatibility constructor for observations that do not carry numeric attributes. */
    public SourceField(String name, String dataType, TapstateType type, String unknownBecause) {
        this(name, dataType, type, unknownBecause, null);
    }

    /** What the type-less constructor means, said once so every field built that way says the same thing. */
    static final String NOTHING_ASKED = "nothing resolved a tapstate type for this field";

    public SourceField {
        Objects.requireNonNull(name, "name");
        type = type == null ? TapstateType.UNKNOWN : type;
        if (type == TapstateType.UNKNOWN && (unknownBecause == null || unknownBecause.isBlank())) {
            // Bare rather than coded: whoever builds a field is inside this product, and an unknown
            // nobody can attribute is the defect this component exists to end - reported as a coded
            // outcome it would be filed away as one more column whose type did not resolve.
            throw new IllegalArgumentException(
                    "field '" + name + "' has no resolved type and no reason; say which unknown it is");
        }
        if (type != TapstateType.UNKNOWN && unknownBecause != null) {
            throw new IllegalArgumentException(
                    "field '" + name + "' resolved to " + type + " and also carries a reason for being unknown");
        }
    }

    /** A field whose type resolved. Handing this an unknown is refused - say why, through the full form. */
    public SourceField(String name, String dataType, TapstateType type) {
        this(name, dataType, type, null);
    }

    /** A field nothing resolved a tapstate type for: the source's own spelling and no more. */
    public SourceField(String name, String dataType) {
        this(name, dataType, TapstateType.UNKNOWN, NOTHING_ASKED);
    }
}
