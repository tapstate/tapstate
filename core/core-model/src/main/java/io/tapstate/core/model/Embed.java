package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One embedded child of a nest tree. {@code from} names an alias of the step's {@code from:}
 * map; {@code on} maps child join fields to parent fields.
 */
@Doc("One child of a nest tree, joined onto its parent and either placed at a path or merged into it.")
public record Embed(
        @Doc(value = "Alias of the nest step's from map that supplies this child's rows.", required = true)
        String from,
        @Doc(value = "Maps this child's join fields to the parent fields they match.", required = true)
        Map<String, String> on,
        @Doc(value = "How matched child rows contribute under the parent: an array, an object, flat fields, or "
                + "only the rollup fields computed over them.", required = true)
        EmbedAs as,
        @Doc("Target field path for array and object embeds. Flat and rollup embeds must omit it.")
        String path,
        @Doc(value = "Fields that identify one row of this embed's stream. Absent means the stream's own "
                + "declared key is used, and a stream that declares none is refused.",
                key = "key")
        List<String> key,
        @Doc(value = "Fields that tell one element apart from the others in the same array, which may be "
                + "unique only within that array. Absent means the row identity above is used.",
                key = "arrayKey")
        List<String> arrayKey,
        @Doc(value = "When true, updates to the child rows are not propagated into the parent.",
                key = "ignoreUpdates")
        Boolean ignoreUpdates,
        @Doc(value = "When true, changes to this child's structural keys are tracked so embedded data "
                + "is moved accordingly: the key identifying an element within its array, the key "
                + "saying which parent it hangs under, and the key its own children point at. "
                + "Requires the source to supply a before image.",
                key = "trackKeyChanges")
        Boolean trackKeyChanges,
        @Doc("Further children embedded beneath this one, forming a nested tree.")
        List<Embed> embed,
        @Doc("Fields computed over this embed's live elements and written on its parent, keyed by field name. "
                + "Allowed on array and rollup embeds; a rollup embed must declare at least one.")
        Map<String, RollupField> rollup) {

    /**
     * An embed that names no row identity of its own, which is what every embed written before this field
     * existed is: the identity comes from what the stream declares. What was not written stays out of the
     * canonical form, where it would otherwise change the identity of artifacts nobody edited.
     */
    public Embed(String from, Map<String, String> on, EmbedAs as, String path, List<String> arrayKey,
            Boolean ignoreUpdates, Boolean trackKeyChanges, List<Embed> embed) {
        this(from, on, as, path, null, arrayKey, ignoreUpdates, trackKeyChanges, embed, null);
    }

    /** An embed that declares no rollup fields, which is every embed written before they existed. */
    public Embed(String from, Map<String, String> on, EmbedAs as, String path, List<String> key,
            List<String> arrayKey, Boolean ignoreUpdates, Boolean trackKeyChanges, List<Embed> embed) {
        this(from, on, as, path, key, arrayKey, ignoreUpdates, trackKeyChanges, embed, null);
    }

    public Embed {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(on, "on");
        Objects.requireNonNull(as, "as");
        if (as == EmbedAs.FLAT) {
            if (path != null) {
                throw new IllegalArgumentException("a flat embed has no target path");
            }
            if (arrayKey != null) {
                throw new IllegalArgumentException("a flat embed has no array key");
            }
        } else if (as == EmbedAs.ROLLUP) {
            if (path != null) {
                throw new IllegalArgumentException("a rollup embed renders no array and so has no target path");
            }
        } else {
            Objects.requireNonNull(path, "path");
        }
        on = Collections.unmodifiableMap(new LinkedHashMap<>(on));
        key = key == null ? null : List.copyOf(key);
        arrayKey = arrayKey == null ? null : List.copyOf(arrayKey);
        embed = embed == null ? null : List.copyOf(embed);
        rollup = rollup == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(rollup));
    }

    /**
     * The stable segment identifying this embed in the recursive tree. Flat and rollup embeds have no output
     * path, so their declared alias supplies the segment instead; aliases already identify streams inside one
     * nest step.
     */
    public String treeSegment() {
        return switch (as) {
            case FLAT -> "$flat[" + from + "]";
            case ROLLUP -> "$rollup[" + from + "]";
            default -> path;
        };
    }

    /** The rollup fields this embed declares, empty when it declares none. */
    public Map<String, RollupField> rollupFields() {
        return rollup == null ? Map.of() : rollup;
    }
}
