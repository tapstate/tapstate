package io.tapstate.core.model;

import java.util.List;
import java.util.Objects;

/**
 * Root of a nest tree: parent stream alias, upsert key, optional append-only mode, and the
 * embedded children.
 */
@Doc("Root of a nest tree: the parent stream, its upsert key, write mode, and the embedded child streams.")
public record NestRoot(
        @Doc(value = "Alias of the parent stream that anchors this nest tree.", required = true)
        String from,
        @Doc("Upsert key fields that identify a parent document for updates.")
        List<String> key,
        @Doc("Write mode for the parent stream, such as append-only or upsert.")
        String mode,
        @Doc(value = "When true, changes to the root key are tracked so the whole document is moved "
                + "to its new identity. The root declares its own because nothing above it does. "
                + "Requires the source to supply a before image.",
                key = "trackKeyChanges")
        Boolean trackKeyChanges,
        @Doc("Child streams embedded under each parent document.")
        List<Embed> embed) {

    public NestRoot {
        Objects.requireNonNull(from, "from");
        key = key == null ? null : List.copyOf(key);
        embed = embed == null ? null : List.copyOf(embed);
    }
}
