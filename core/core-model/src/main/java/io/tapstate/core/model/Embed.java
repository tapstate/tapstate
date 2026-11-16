package io.tapstate.core.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * One embedded child of a nest tree. {@code from} names an alias of the step's {@code from:}
 * map; {@code on} maps child join fields to parent fields.
 *
 * <p>{@code orOn} lists further ways of matching the same child, tried in order after {@code on}: the
 * child row is the one the first alternative with a live match names. It turns an embed into a
 * reference - the parent points at one shared child row - because an ordered choice between rows
 * only means something when there is at most one row to choose per alternative.
 */
@Doc("One child of a nest tree, joined onto its parent and either placed at a path or merged into it.")
public record Embed(
        @Doc(value = "Alias of the nest step's from map that supplies this child's rows.", required = true)
        String from,
        @Doc(value = "Maps this child's join fields to the parent fields they match.", required = true)
        Map<String, String> on,
        @Doc(value = "How matched child rows contribute under the parent: an array, an object, or flat fields.", required = true)
        EmbedAs as,
        @Doc("Target field path for array and object embeds. Flat embeds must omit it.")
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
        @Doc(value = "Further ways to match the child row, each mapping child fields to parent fields like on. "
                + "They are tried in order after on, and the first one that matches a live row supplies the "
                + "child. Only for an embed that points at one shared row (many to one).",
                key = "orOn")
        List<Map<String, String>> orOn) {

    /**
     * An embed that names no row identity of its own, which is what every embed written before this field
     * existed is: the identity comes from what the stream declares. What was not written stays out of the
     * canonical form, where it would otherwise change the identity of artifacts nobody edited.
     */
    public Embed(String from, Map<String, String> on, EmbedAs as, String path, List<String> arrayKey,
            Boolean ignoreUpdates, Boolean trackKeyChanges, List<Embed> embed) {
        this(from, on, as, path, null, arrayKey, ignoreUpdates, trackKeyChanges, embed, null);
    }

    /** An embed matched on {@code on} alone, which is every embed written before alternatives existed. */
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
        } else {
            Objects.requireNonNull(path, "path");
        }
        on = Collections.unmodifiableMap(new LinkedHashMap<>(on));
        key = key == null ? null : List.copyOf(key);
        arrayKey = arrayKey == null ? null : List.copyOf(arrayKey);
        embed = embed == null ? null : List.copyOf(embed);
        if (orOn != null) {
            if (orOn.isEmpty()) {
                throw new IllegalArgumentException("orOn names no alternative; omit it instead");
            }
            List<Map<String, String>> copied = new ArrayList<>(orOn.size());
            for (Map<String, String> alternative : orOn) {
                Objects.requireNonNull(alternative, "orOn alternative");
                if (alternative.isEmpty()) {
                    throw new IllegalArgumentException("an orOn alternative maps no field");
                }
                copied.add(Collections.unmodifiableMap(new LinkedHashMap<>(alternative)));
            }
            orOn = Collections.unmodifiableList(copied);
        }
    }

    /**
     * Every way this embed matches its child row, in the order they are tried: {@code on} first, then each
     * {@code orOn} alternative. One entry for an embed that declares no alternatives.
     */
    public List<Map<String, String>> alternatives() {
        if (orOn == null) {
            return List.of(on);
        }
        List<Map<String, String>> all = new ArrayList<>(1 + orOn.size());
        all.add(on);
        all.addAll(orOn);
        return Collections.unmodifiableList(all);
    }

    /** Whether this embed declares alternatives beyond {@code on}. */
    public boolean hasAlternatives() {
        return orOn != null;
    }

    /**
     * The stable segment identifying this embed in the recursive tree. A flat embed has no output path,
     * so its declared alias supplies the segment instead; aliases already identify streams inside one
     * nest step.
     */
    public String treeSegment() {
        return as == EmbedAs.FLAT ? "$flat[" + from + "]" : path;
    }
}
